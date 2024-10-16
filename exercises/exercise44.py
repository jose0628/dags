import datetime
import psycopg2
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def start_process():
    logging.info("Starting the DAG")

def load_data():

    ENDPOINT = 'datalake1.cr6fdztzvvs4.us-east-1.rds.amazonaws.com'
    DB_NAME = 'datalake1'
    USERNAME = 'pepe'
    PASSWORD = '1234567890'
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
    "example_load_data_dag",
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