from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import boto3
import datetime
import yaml
from pathlib import Path

# Load the YAML configuration
yaml_path = Path(__file__).parent / "config.yaml"
with open(yaml_path, "r") as yaml_file:
    config = yaml.safe_load(yaml_file)

# Extracting AWS credentials and other parameters from the YAML file
AWS_ACCESS_KEY_ID = config['aws']['access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws']['secret_access_key']
AWS_SESSION_TOKEN = config['aws']['session_token']

logging.info(f"AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID}")
logging.info(f"AWS_SECRET_ACCESS_KEY: {AWS_SECRET_ACCESS_KEY}")
logging.info(f"AWS_SESSION_TOKEN: {AWS_SESSION_TOKEN}")

dag = DAG('Example_s3_file_upload_dag_old',
          description='Upload files to S3',
          schedule_interval='@once',
          start_date=datetime.datetime.now())

def upload_file_to_s3(bucket_name, source_file, destination_key):

    logging.info(f"Uploading {source_file} to S3 bucket {bucket_name} as {destination_key}")

    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN
    )
    try:
        s3.upload_file(Filename=source_file, Bucket=bucket_name, Key=destination_key)
        logging.info(f"File {source_file} uploaded to bucket {bucket_name} as {destination_key}")
    except Exception as e:
        logging.error(f"Failed to upload {source_file} to S3: {e}")
        raise

upload_task = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=upload_file_to_s3,
    dag=dag,
    op_kwargs={
        'bucket_name': 'temporallambalayers',
        'source_file': '/Users/jose.mancera/airflow/dags/exercises/data_sample/work_status.csv',
        'destination_key': 'work_status.csv'
    })

upload_task