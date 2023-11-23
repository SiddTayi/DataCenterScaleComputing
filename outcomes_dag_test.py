# !pip install boto 3

import boto3
from datetime import datetime
import pandas as pd
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os

from etl_scripts.transform import transform_data
from etl_scripts.load import load_data


url = "https://data.austintexas.gov/resource/9t4d-g238.json"
data = pd.read_json('https://data.austintexas.gov/resource/9t4d-g238.json')


# Setting up AWS s3
s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id='AKIA2QMVGA2J22YVP777',
    aws_secret_access_key='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'
)

# Initialize the S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)


# Upload the JSON data to S3
s3_client.put_object(
    Bucket=s3_bucket_name,
    Key=s3_object_key,
    Body=json.dumps(data),  # Convert Python data to JSON string
    ContentType='application/json'  # Specify the content type
)

print("Data uploaded to S3 successfully.")

# # Setting up a common directory that is accessible to all the other processes (DAG)
# airflow_home = os.environ.get('AIRFLOW_HOME', "/opt/airflow") 

# # Create a target csv directory that is connected to airflow home dir. 
# ### CSV -> Cloud
# csv_target_dir = airflow_home + "/data/{{ ds }}/downloads"
# csv_target_file = csv_target_dir + "/outcomes_{{ ds }}.json"

# csv_target_dir = airflow_home + "/data/{{ ds }}/processed"




# with  DAG(
#     dag_id = 'outcomes_dag',
#     start_date = datetime(2023, 11, 19), 
#     schedule_interval = '@daily'
# ) as dag:
    
#     # Downloading the file + Creating directories
#     extract = BashOperator(
#         task_id="Extract",
#         bash_command= f"curl --create-dirs -o {csv_target_file} {url}",
#         #bash_command = "echo done extracting...",
#         #python_command = f"first 5 rows:\n{source_url.head()}"
#     )

#     # Transformation
#     transform = PythonOperator(
#         task_id="Transform",
#         python_callable= transform_data, #f"ls {csv_target_dir}",
#         op_kwargs = {
#             'url': csv_target_file,
#             'target_dir': csv_target_dir

#         }
#     )

#     # Load
#     load_animal_dim = PythonOperator(
#         task_id="Load_animal_dim",
#         python_callable= load_data,
#         op_kwargs = {
#             'table_file': csv_target_dir + '/dim_animals.csv', # (os.path.join(target_dir, 'dim_animals.csv')
#             'table_name': 'dim_animal',
#             'key' : 'animal_id'
#         }
#     )
    

#     # Connect Step1 and Step2
#     extract >> transform >> load_animal_dim

# # to run the file in cli: docker exec -it solution-airflow-worker-1 bash