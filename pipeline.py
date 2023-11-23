from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.transfers.http_to_s3 import SimpleHttpToS3Operator
from airflow.providers.amazon.transfers.s3_to_redshift import S3ToRedshiftOperator
from etl_scripts.transform import transform_data
from etl_scripts.load import load_data

# AWS S3 and Redshift Connection Information
AWS_CONN_ID = 'aws_default'
REDSHIFT_CONN_ID = 'redshift_default'
S3_BUCKET_NAME = 'your-s3-bucket-name'
S3_KEY_PREFIX = 'your-s3-key-prefix'
REDSHIFT_SCHEMA = 'public'
REDSHIFT_TABLE = 'your_redshift_table'

# URL for data extraction
url = "https://data.austintexas.gov/resource/9t4d-g238.json"

# Setting up a common directory that is accessible to all the other processes (DAG)
airflow_home = Path("/opt/airflow")

# Create a target CSV directory that is connected to the Airflow home directory
csv_target_dir = airflow_home / "data/{{ ds }}/processed"
csv_target_file = csv_target_dir / "outcomes_{{ ds }}.json"

# Define DAG parameters
dag_id = 'outcomes_dag'
start_date = datetime(2023, 11, 22)
schedule_interval = '@daily'

# Define DAG
dag = DAG(
    dag_id=dag_id,
    start_date=start_date,
    schedule_interval=schedule_interval,
)

# Downloading the file + Creating directories
extract = SimpleHttpToS3Operator(
    task_id="Extract",
    method="GET",
    endpoint=url,
    bucket_name=S3_BUCKET_NAME,
    aws_conn_id=AWS_CONN_ID,
    verify=None,
    key=S3_KEY_PREFIX + "/raw_data/outcomes_{{ ds }}.json",
    dag=dag,
)

# Transformation
transform = PythonOperator(
    task_id="Transform",
    python_callable=transform_data,
    op_kwargs={'url': csv_target_file, 'target_dir': csv_target_dir},
    provide_context=True,
    dag=dag,
)

# Load to S3 (processed directory)
load_to_s3 = PythonOperator(
    task_id="LoadToS3",
    python_callable=load_data,
    op_kwargs={'data': None},  # Replace None with the actual data to load
    provide_context=True,
    dag=dag,
)

# Load to Redshift
load_to_redshift = S3ToRedshiftOperator(
    task_id="LoadToRedshift",
    schema=REDSHIFT_SCHEMA,
    table=REDSHIFT_TABLE,
    s3_bucket=S3_BUCKET_NAME,
    s3_key=S3_KEY_PREFIX + "/raw_data/outcomes_{{ ds }}.json",
    copy_options=['json'],
    aws_conn_id=AWS_CONN_ID,
    redshift_conn_id=REDSHIFT_CONN_ID,
    dag=dag,
)

# Connect the tasks
extract >> transform >> [load_to_s3, load_to_redshift]
