import boto3
import logging
import os
import datetime

# AWS credentials should be managed via environment variables for better security
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')

# Function to upload a file to S3
def upload_file_to_s3(bucket_name, source_file, destination_key):
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

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bucket_name = 'temporallambalayers'
    source_file = '/Users/jose.mancera/airflow/dags/exercises/data_sample/work_status.csv'
    destination_key = 'work_status_{}.csv'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))

    upload_file_to_s3(bucket_name, source_file, destination_key)
