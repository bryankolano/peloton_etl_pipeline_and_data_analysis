import pandas as pd
import datetime
import numpy as np
import requests
import os
from dotenv import dotenv_values
import psycopg2


def create_sesh(email: str, password: str):

    #authentican endpoint
    api_url = 'https://api.onepeloton.com/auth/login'

    jason = {
        'username_or_email': email,
        'password': password
    }

    req = requests.Session()

    #post request to authenticate to peloton
    req.post(url = api_url, json = jason)

    #return request object
    return req

def get_user_workout_ids(sesh: requests.Session ) -> list:

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
            current_workouts_ids = [x['id'] for x in current_workouts_resp['data']]
            workout_ids_collection = workout_ids_collection + current_workouts_ids
            count += 1
    
    #return tuple of the user's name (because you can't pull it on the workout endpoint), 
    # and the list of all the user's workout ids
    return rider_name, workout_ids_collection


def get_user_workout_details(session: requests.Session, rider_name: str, workout_ids: list, return_workout_id=False) -> pd.DataFrame:
    
    #blank dataframe for concating all data
    collection_df = pd.DataFrame()

    #use the instructor endpoint to grab all instructor IDs and names
    inst = session.get('https://api.onepeloton.com/api/instructor?limit=100')
    
    #dictionary for instructor IDs and names (to convert data later)
    instructors_names = {x['id']:x['name'] for x in inst.json()['data']}
    
    #Lanebreak classes have no instructor.  When I map later, I need a "No Instructor" key for lanebreak
    instructors_names['no instructor'] = 'no instructor'

    instructors_names['5a19bfe66e644a2fa3e6387a91ebc5ce'] = 'Christine Dercole'
    
    for workout in workout_ids:

        #endpoint for a singular workout
        workout_url = f'https://api.onepeloton.com/api/workout/{workout}'
        workout_request = session.get(workout_url)
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

    if not return_workout_id:
        collection_df = collection_df.drop('workout_id', axis = 1)
    
    return collection_df  

def write_to_rds(df: pd.DataFrame, host: str,database: str, port: str, user: str, password: str, create_table= False ):
    conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    if create_table:
        try:
            
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS peloton (
            workout_id varchar(45) PRIMARY KEY,
            date timestamp NOT NULL,
            class_title varchar(100) NOT NULL,
            instructor varchar(50) NOT NULL,
            class_type varchar(30) NOT NULL,
            length_min integer NOT NULL,
            total_output_kj float,
            rider_name varchar(30)
            )
            """)
            conn.commit()
            # cursor.close()

        except Exception as e:
            print(f"Database connection failed due to: {e}") 

    try:
        cursor = conn.cursor()
        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            header = tuple(df.head(0))
            formatted_header = ", ".join(str(x) for x in header)
            cursor.execute(f"""INSERT INTO peloton ({formatted_header}) VALUES {tuple(row)} ON CONFLICT ON CONSTRAINT peloton_pkey DO NOTHING """)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Database connection failed due to: {e}") 
        

if __name__=='__main__':
    password_config = dotenv_values("/home/x7824/.env")

    bryan_pass = password_config['PELOTON_PASS_BRYAN']
    hillary_pass = password_config['PELOTON_PASS_HILLARY']

    
    user_dict = {
        'x78240@yahoo.com': bryan_pass,
        'hillary.scherer@gmail.com': hillary_pass
    }

    all_historical_rides = pd.DataFrame()
    for email,password in user_dict.items():

        sesh = create_sesh(email=email, password= password)

        rider_name, all_ids = get_user_workout_ids(sesh )

        all_user_workouts = get_user_workout_details(sesh,rider_name = rider_name, workout_ids = all_ids, return_workout_id = True)

        all_historical_rides = pd.concat([all_historical_rides, all_user_workouts])

        sesh.close()

    all_historical_rides.to_csv('all_rides.csv', index = False)

    all_historical_rides['date'] = all_historical_rides['date'].astype(str)


    # postgres_pass = password_config['POSTGRES_PASS']

    # write_to_rds(df = all_historical_rides, 
    #             host = 'peloton-db.clki5gemvlvh.us-east-1.rds.amazonaws.com',
    #             database = 'peloton', 
    #             port = '5432', 
    #             user = 'postgres', 
    #             password = postgres_pass, 
    #             create_table= False )

