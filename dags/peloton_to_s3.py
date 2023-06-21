from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook  import S3Hook
import pandas as pd
import numpy as np
import requests
import os
from dotenv import dotenv_values
import datetime



def peloton_to_local(**kwargs):

    email = kwargs['email']
    password = kwargs['password']
    filter_date = kwargs['filter_date']
    #authentican endpoint
    api_url = 'https://api.onepeloton.com/auth/login'

    jason = {
        'username_or_email': email,
        'password': password
    }

    sesh = requests.Session()

    #post request to authenticate to peloton
    sesh.post(url = api_url, json = jason)

    #endpoint for a given user's information, after authentication
    my_data_url = 'https://api.onepeloton.com/api/me'

    request = sesh.get(my_data_url)
    resp= request.json()

    #user's peloton user id, will be needed to pull workouts for this user
    my_pel_id = resp['id']
    #Grab user's actual name
    rider_name = resp['name']

    workout_ids_collection = []
    count = 0

    #Loop through all pages with user's workout ids, collect workout ids in a list
    while True:
        current_workout_url = f'https://api.onepeloton.com/api/user/{my_pel_id}/workouts?limit=100&page={count}'
        current_workouts_req = sesh.get(current_workout_url)
        current_workouts_resp = current_workouts_req.json()


        if current_workouts_resp['data'] == []:
            break

        else:
            
            current_start_times_seconds = [x['start_time'] for x in current_workouts_resp['data']] 
            start_times = [pd.to_datetime(x, unit= 's').date() for x in current_start_times_seconds]
            index_check = [idx for idx,x in enumerate(start_times) if x == filter_date ]
            
                        
            current_workout_ids = [x['id'] for x in current_workouts_resp['data']]

            current_workouts_ids_filtered = [current_workout_ids[i] for i in index_check]

            workout_ids_collection = workout_ids_collection + current_workouts_ids_filtered
            count += 1
    

    #return tuple of the user's name (because you can't pull it on the workout endpoint), 
    # and the list of all the user's workout ids
    
    #blank dataframe for concating all data
    collection_df = pd.DataFrame()

    #use the instructor endpoint to grab all instructor IDs and names
    inst = sesh.get('https://api.onepeloton.com/api/instructor?limit=100')
    
    #dictionary for instructor IDs and names (to convert data later)
    instructors_names = {x['id']:x['name'] for x in inst.json()['data']}
    
    #Lanebreak classes have no instructor.  When I map later, I need a "No Instructor" key for lanebreak
    instructors_names['no instructor'] = 'no instructor'
    
    instructors_names['5a19bfe66e644a2fa3e6387a91ebc5ce'] = 'Christine Dercole'
    
    try:

        for workout in workout_ids_collection:

            #endpoint for a singular workout
            workout_url = f'https://api.onepeloton.com/api/workout/{workout}'
            workout_request = sesh.get(workout_url)
            workout_json = workout_request.json()

            #Lanebreak classes have an instructor key, all other class types do not (which is funny because lanebreak is
            # one of the only class types without an instructor)
            #Therefore, if this key exists, hard code 'no instructor' into its instructor
            if 'instructor' in workout_json['ride']:
                current_dict = {
                    'workout_id': workout,
                    'date': [workout_json['start_time']],
                    'class_title': workout_json['ride']['title'],
                    'instructor': 'no instructor',
                    'class_type': 'lanebreak',
                    'length_min': [workout_json['ride']['duration']],
                    'total_output_kj': [workout_json['total_work']]   
                }  
            #else if, this instructor key doesn't exist, that means there is an instructor id, and an actual instructor          
            else:
                current_dict = {
                    'workout_id': workout,
                    'date': [workout_json['start_time']],
                    'class_title': workout_json['ride']['title'],
                    'instructor': workout_json['ride']['instructor_id'],
                    'class_type': workout_json['fitness_discipline'],
                    'length_min': [workout_json['ride']['duration']],
                    'total_output_kj': [workout_json['total_work']]   
                }

            #concat current run to the running collection
            collection_df = pd.concat([collection_df, pd.DataFrame(current_dict)]).reset_index(drop = True)
        

        collection_df['rider_name'] = rider_name
        
        #start_time above is in epoch second.  convert to date time
        collection_df['date'] = pd.to_datetime(collection_df['date'],unit='s')
        collection_df['length_min'] = collection_df['length_min']/60
        collection_df['total_output_kj'] = (collection_df['total_output_kj']/1000)
        
        #use instructor dictionary above to map instructor ID to their actual names
        collection_df['instructor'] = collection_df['instructor'].map(instructors_names)

        collection_df['length_min'] = collection_df['length_min'].astype('int')

        collection_df['class_title'] = collection_df['class_title'].str.replace("'", "")

        collection_df['instructor'] = collection_df['instructor'].fillna('no instructor')

        collection_df = collection_df.round({ 'total_output_kj': 1})
        
        
        #create output file path
        output_path = f'/home/x7824/airflow_new/data/{filter_date.year}_{filter_date.month:02d}_{filter_date.day:02d}.csv'

        #if the file does not exist, create it and write headers.  If it does exist, from Bryan run to Hillary run,
        #then just open it and append to it without header.
        if not os.path.exists(output_path):
            collection_df.to_csv(output_path, index= False)
        else:
            collection_df.to_csv(output_path, mode = 'a', header=False, index = False) 

        return output_path
    
    except KeyError as e:
        return f"No data for {filter_date.year}-{filter_date.month}-{filter_date.day}"

def upload_to_s3(filter_date: datetime, key: str, bucket_name: str) -> None:
    hook = S3Hook('peloton_s3')
    file_path = f'/home/x7824/airflow_new/data/{filter_date.year}_{filter_date.month:02d}_{filter_date.day:02d}.csv'

    if os.path.exists(file_path):
        hook.load_file(filename= file_path, key=key, bucket_name=bucket_name, replace = True)


password_config = dotenv_values("/home/x7824/.env")

bryan_pass = password_config['PELOTON_PASS_BRYAN']
hillary_pass = password_config['PELOTON_PASS_HILLARY']

filter_date = datetime.date.today()
# filter_date = datetime.date(2023,5,29)

password_dict = {

    'x78240@yahoo.com': bryan_pass,
    'hillary.scherer@gmail.com': hillary_pass
}

default_args = {
    'owner': 'bryankolano',
    'retries':2,
    'retry_delay': datetime.timedelta(seconds= 5),
    }



with DAG(
    default_args = default_args,
    dag_id = 'peloton_to_s3',
    description = 'Grab updated data from Peloton API and write locally to csv',
    schedule_interval = '50 23 * * *',
    start_date = datetime.datetime(2023,5,12),
    catchup= False
    

) as dag:

    task1 = PythonOperator(
        task_id = f'data_to_local_bryan',
        python_callable=peloton_to_local,
        op_kwargs={'email': 'x78240@yahoo.com',
                    'password': bryan_pass,
                    'filter_date': filter_date,
                    
                }
    )

    task2 = PythonOperator(
        task_id = f'data_to_local_hillary',
        python_callable=peloton_to_local,
        op_kwargs={'email': 'hillary.scherer@gmail.com',
                    'password': hillary_pass,
                    'filter_date': filter_date
                }
    )

    task3 = PythonOperator(
        task_id = 'local_to_s3_bucket',
        python_callable=upload_to_s3,
        op_kwargs= {
            'filter_date': filter_date,
            'key':f"{filter_date.year}/{filter_date.month:02d}/daily_upload{filter_date.year}_{filter_date.month:02d}_{filter_date.day:02d}.csv",
            'bucket_name': 'bk-peloton'
        }
    )

    task1 >> task2 >> task3
    