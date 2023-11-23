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
source_url = pd.read_json('https://data.austintexas.gov/resource/9t4d-g238.json')

# Setting up a common directory that is accessible to all the other processes (DAG)
airflow_home = os.environ.get('AIRFLOW_HOME', "/opt/airflow") 

# Create a target csv directory that is connected to airflow home dir. 
### CSV -> Cloud
csv_target_dir = airflow_home + "/data/{{ ds }}/downloads"
csv_target_file = csv_target_dir + "/outcomes_{{ ds }}.json"

csv_target_dir = airflow_home + "/data/{{ ds }}/processed"




with  DAG(
    dag_id = 'outcomes_dag',
    start_date = datetime(2023, 11, 22), 
    schedule_interval = '@daily'
) as dag:
    
    # Downloading the file + Creating directories
    extract = BashOperator(
        task_id="Extract",
        bash_command= f"curl --create-dirs -o {csv_target_file} {url}",
        #bash_command = "echo done extracting...",
        #python_command = f"first 5 rows:\n{source_url.head()}"
    )

    # Transformation
    transform = PythonOperator(
        task_id="Transform",
        python_callable= transform_data, #f"ls {csv_target_dir}",
        op_kwargs = {
            'url': csv_target_file,
            'target_dir': csv_target_dir

        },
         provide_context=True,  # Add this line to provide the context to the callable
    dag=dag 
    )

    # Load
    load_animal_dim = PythonOperator(
        task_id="Load",
        python_callable= load_data,
        op_kwargs = {
                    'table_file': {
            'dim_animals': os.path.join(csv_target_dir, 'dim_animals.csv'),
            'dim_date': os.path.join(csv_target_dir, 'dim_date.csv'),
            'dim_breed': os.path.join(csv_target_dir, 'dim_breed.csv'),
            'dim_outcome': os.path.join(csv_target_dir, 'dim_outcome.csv'),
            'fact_animal': os.path.join(csv_target_dir, 'fact_animal.csv'),
                    }   
            #'table': source_url, #,
            #'key' : 'animal_id'
        },
         provide_context=True,  # Add this line to provide the context to the callable
    dag=dag 
    )
    

    # Connect Step1 and Step2
    extract >> transform >> load_animal_dim

# to run the file in cli: docker exec -it solution-airflow-worker-1 bash