import datetime as dt
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('S3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

with DAG(
    dag_id='session2.exercise6',
    schedule_interval='@once',
    start_date=datetime(2022, 3, 1),
    catchup=False

) as dag:
    # Upload the file
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/Users/jose.mancera/Desktop/Lectures/work_status.csv',
            'key': 'work_status_{}.csv'.format(dt.datetime.now())\
                       .replace(".", "_")\
                       .replace(":", "_")\
                       .replace(" ", ""),
            # 'key': 'work_status.csv',
            'bucket_name': 'bucketdatalakes'
        }
    )
