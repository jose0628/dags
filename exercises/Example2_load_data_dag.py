import datetime
import psycopg2
import logging
import yaml # pip install pyyaml
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Get the directory of the current script
current_directory = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to the YAML file
yaml_path = os.path.join(current_directory, "config.yaml") #todo: change to your own yaml file

# Load the YAML configuration file
with open(yaml_path, "r") as yaml_file:
    config = yaml.safe_load(yaml_file)

# Extract the values
ENDPOINT = config['database']['endpoint']
DB_NAME = config['database']['db_name']
USERNAME = config['database']['username']
PASSWORD = config['database']['password']


def start_process():
    logging.info("Starting the DAG")

def load_data():

    try:
        logging.info("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))

    except psycopg2.Error as e:
        logging.info("Error: Could not make connection to the Postgres database")
        logging.info(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        logging.info("Error: Could not get curser to the Database")
        logging.info(e)

    # Auto commit is very important
    conn.set_session(autocommit=True)

    cur.execute("CREATE TABLE IF NOT EXISTS test (col1 int, col2 int, col3 int);")

    try:
        cur.execute("INSERT INTO test (col1, col2, col3) \
                 VALUES (%s, %s, %s)", \
                    (666666, 666666, 6666666))
    except psycopg2.Error as e:
        logging.info("Error: Inserting Rows")
        logging.info(e)


    try:
        cur.execute("SELECT * FROM test;")
    except psycopg2.Error as e:
        logging.info("Error: select *")
        logging.info(e)

    row = cur.fetchone()
    while row:
        logging.info(row)
        row = cur.fetchone()

    cur.close()
    conn.close()


dag = DAG(
    "Example_2_load_data_dag",
    schedule_interval='@once',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1))

start_process = PythonOperator(
    task_id="starting_dag",
    python_callable=start_process,
    dag=dag)

load_data = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag)

# Configure Task Dependencies
start_process >> load_data