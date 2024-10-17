import boto3
import logging
import psycopg2
import yaml
from pathlib import Path

'''
https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.S3Import.InstallExtension.html
'''

# Load the YAML configuration
yaml_path = Path(__file__).parent / "config.yaml"
with open(yaml_path, "r") as yaml_file:
    config = yaml.safe_load(yaml_file)

# Extracting AWS credentials and other parameters from the YAML file
AWS_ACCESS_KEY_ID = config['aws']['access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws']['secret_access_key']
AWS_SESSION_TOKEN = config['aws']['session_token']
AWS_REGION = config['aws']['region']

# Database credentials

DB_HOST = config['database']['endpoint']
DB_NAME = config['database']['db_name']
DB_USER = config['database']['username']
DB_PASSWORD = config['database']['password']

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

def create_table_in_rds():
    try:
        # Establish a connection to the RDS instance
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Create table query
        create_table_query = """
        CREATE TABLE IF NOT EXISTS job_titles (
            job_title VARCHAR(100),
            location VARCHAR(100),
            suspended VARCHAR(100)
        );
        """

        # Execute the query
        cursor.execute(create_table_query)
        conn.commit()

        logging.info("Table 'job_titles' created successfully (if it did not exist).")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Failed to create table in RDS: {e}")
        raise


# Function to load data from S3 to RDS using aws_s3.table_import_from_s3
def load_data_to_rds_from_s3(table_name, bucket_name, object_key):
    try:
        # Establish a connection to the RDS instance
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Execute the aws_s3.table_import_from_s3 function
        copy_sql = f"""
        SELECT aws_s3.table_import_from_s3(
            '{table_name}', 'job_title,location,suspended', '(format csv, header true)',
            '{bucket_name}',
            '/work_status.csv',
            '{AWS_REGION}',
            '{AWS_ACCESS_KEY_ID}','{AWS_SECRET_ACCESS_KEY}', '{AWS_SESSION_TOKEN}'
        );
        """

        logging.info(copy_sql)

        cursor.execute(copy_sql)
        conn.commit()
        logging.info(f"Data copied from S3 to RDS table {table_name}")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Failed to copy data to RDS: {e}")
        raise

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bucket_name = 'temporallambalayers'
    source_file = '/Users/jose.mancera/airflow/dags/exercises/data_sample/work_status.csv'
    destination_key = 'work_status.csv'
    table_name = 'job_titles'

    # Step 1: Upload file to S3
    upload_file_to_s3(bucket_name, source_file, destination_key)

    # Step 2: Create the table in RDS
    create_table_in_rds()

    # Step 3: Load the data into RDS from S3
    load_data_to_rds_from_s3(table_name, bucket_name, destination_key)
