# Peloton Workout Data Pipeline
## A collection of scripts and workflow that takes data from the Peloton API to an AWS SQL Database and then back for data analysis
## By: Bryan Kolano, Original repo creation: June 21st, 2023


***

#### Background

As I got more into my data engineering journey, I wanted to get some experience with one of the most common ETL orchestration tools: Apache Airflow.  

A year or so ago, I thought it would be fun to grab peloton workout data and then do analysis on it.  Now that I am making more of a transition to data engineering, I thought it would be more fun to turn this project into a data pipeline.... and then do some analysis on it.

Although Peloton does not publish their API documentation, I was able to find some blogs and github pages that showed a little bit about how to interact with the Peloton API.  


#### Files in this repo

1.  historical_workouts.py: 
    - A python script that will grab all Peloton workout information for riders (Bryan and Hillary in this case).  
    - This script grabs all the workout identifiers for a given person, and then collects all the workout details associated with each workout identifier.
    - Finally, as an option, this script will then write the data to a Postgres SQL database in AWS.  
    - This script was used to pull all workout data until the Airflow DAG was up and running.  
    - This script was run last on May 25th, 2023, and sent all days' of data up to and including May 25th to the AWS Postgres database.  Once the Airflow pipeline was up and running, this script no longer needed to run and directly update the database; the DAG and lambda function now do it.

2.  dags/peloton_to_s3.py:
    - This is a Airflow Directed Acyclic Graph (DAG) to grab daily workouts and send them to a postgres SQL instance in AWS.
    - The DAG contains three tasks inside:
        <ol>
            <li>Grab Bryan workout(s) for a day, write to CSV locally</li>
            <li>Grab Hillary workout(s) for a day, append to same local CSV</li>
            <li>Grab the day's CSV and send to an S3 bucket in AWS. </li>
        </ol>
    - This DAG (and its three tasks run daily), and if there is no new data for the day, it doesn't send anything to the S3 bucket.

3. data folder:
    - This is where the daily CSVs are written to, before they are uploaded to S3 bucket.
    - There is just one example in the folder to show an example of what a daily S3 upload looks like.

4. all_rides.csv:
    - Output of historical_workouts.py.  When this python script runs, it produces this CSV with all Peloton workouts of Bryan and Hillary.  The current version of all_rides.csv goes through May 25th, 2023 because after that is when the pipeline was up and running.

5. lambda.py:
    - This is a AWS Lambda function.  It currently resides and runs in my AWS virtual private cloud.
    - When my Airflow DAG (peloton_to_s3.py) sends new daily data to my AWS S3 bucket, this new upload triggers the Lambda function to run.  
    - The lambda function reads the new data and then writes it to the AWS Postgres SQL database.
    - The lambda function required a few additional layers (aka python packages) added in the run properly: Pandas and psycopg2.

6. peloton_analysis.ipynb
    - Data analysis of Bryan and Hillary's Peloton workouts since we acquired the bike in late October, 2021.



#### The pipeline

See below for what the entire data pipeline looks like!

![pipeline](https://github.com/bryankolano/peloton_etl_pipeline_and_data_analysis/blob/master/peloton_flow.jpeg)

From Google Cloud Platform (GCP) virtual Machine running Python with an Apache Airflow webserver and scheduler, the following four steps occur in one Airflow DAG scheduled to run at the end of every day:
1. Authentication 
    - Authenticate into peloton API endpoint for both Bryan and Hillary credentials
2. Gather data
    - GET request to collect all workout IDs for Bryan and Hillary's workouts
    - With each workout ID, run it through the workout endpoint to gather specific details about each workout (date, length, class type, etc.)
3. Data cleaning/ transformation
    - Convert data types
    - Convert class length to minutes
    - Convert output to kilojoules 
    - Grab instructor IDs from instructor endpoint, and map them to instructor names
    - Write to local computer as CSV
4. After Bryan's data is written to local CSV and Hillary's data is appended to the CSV for each day, the third task in this DAG is to send the CSV to an S3 bucket in AWS.

Steps 5 and 6 occur inside of AWS virtual private cloud

5. Lambda trigger and function run 
    - New object uploaded to S3 bucket in step 4 causes the lambda function to activate.
    - The lambda function reads in the new CSV in the S3 bucket, and then writes the data to the AWS-hosted Postgres database in step 6.

6. Database
    - An AWS Postgres SQL database houses all the data in one table.
    - Two different options have been used to write to the database:
        1. historical_workouts.py has a section of code to write all historical workouts to the database.
        2. The AWS Lambda function writes all new workouts to the database.

The last step occurs on the local machine.

7. Data analysis
    - The last step is to read some or all data from the database and then do analysis on the riders and their workouts.


#### Future work

From a ETL and data analysis standpoint, this project is done.  However, I have been working on creating an API through FastAPI to so that an user could query out workout data.  Right now, there are only 700+ workouts in the database, so grabbing every obervation is not a huge issue.  

If the dataset were to grow large in time, the API would help serve up filtered responses based on user inputs.

Currently, the API is operational on local host.  The plan is to containerize the API and then deploy it to AWS Lightsail.  After deployment, a check will be performed to see if I (or any user) could make an API call just like we would with any other API endpoint.
