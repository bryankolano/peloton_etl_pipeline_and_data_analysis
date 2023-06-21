import json
import pandas as pd 
import boto3
import datetime
import urllib.parse
import psycopg2
import os


#RDS settings



rds_host  = "peloton-db.clki5gemvlvh.us-east-1.rds.amazonaws.com"
user_name = "postgres"
postgres_pass = os.environ['POSTGRES_PASS']
db_name = "peloton"
port = '5432'



s3 = boto3.client('s3')


def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding = 'utf-8')

    resp = s3.get_object(Bucket = bucket, Key = key)


    df = pd.read_csv(resp['Body'])
    
    try:
        conn = psycopg2.connect(host=rds_host, port=port, database=db_name, user=user_name, password=postgres_pass)
        
        cursor = conn.cursor()
        
          
        for index, row in df.iterrows():
            header = tuple(df.head(0))
            formatted_header = ", ".join(str(x) for x in header)
            cursor.execute(f"""
            INSERT INTO peloton ({formatted_header}) VALUES {tuple(row)}
            ON CONFLICT ON CONSTRAINT peloton_pkey DO NOTHING 
            """)
            
            
        conn.commit()
        cursor.close()
        

        
        return {
            'statusCode': 200,
            'body': f"{len(df)} workouts were added to the database."
        }   
        
    except Exception as e:
        print(f'Failed to connect to postgres database: {e}')
        return {
            'statusCode': 400,
            'body': f"Failed to upload any data to database."
        }   


    