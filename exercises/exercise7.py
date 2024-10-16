from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
import logging
from airflow.hooks.S3_hook import S3Hook


dag = DAG('Exercise_7_s3_file_upload_dag',
          description='Upload files to S3',
          schedule_interval='@once',
          start_date=datetime.now())

def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'bucketdatalakes'
    source_file = '/Users/jose.mancera/Desktop/Lectures/work_status.csv'
    destination_file = 'work_status.csv'

    credentials = s3_hook.get_credentials()
    logging.info(credentials)

    s3_hook.load_file(filename=source_file, key=destination_file, bucket_name=bucket_name, replace=True)

upload_task = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=upload_to_s3,
    dag=dag)

upload_task