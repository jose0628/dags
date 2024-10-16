from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import psycopg2
import logging


def greet():
    logging.info('Starting!')


def second_greet():
    logging.info('Finished!')

def update_rds_table():
    ENDPOINT = 'datalake1.c186ick0ma1q.us-east-1.rds.amazonaws.com'
    DB_NAME = 'datalake1'
    USERNAME = 'pepe'
    PASSWORD = '123456789'
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
                    (55555, 55555, 5555))
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


dag = DAG('Exercise_9_RDS_dag',
          description='Update RDS table with Airflow',
          schedule_interval='@once',
          start_date=datetime.datetime.now() - datetime.timedelta(days=1))

upload_task = PythonOperator(
    task_id='update_rds',
    python_callable=update_rds_table,
    dag=dag
)

greet_task = PythonOperator(
    task_id="greet_task",
    python_callable=greet,
    dag=dag
)

second_greet_task = PythonOperator(
    task_id="second_greet",
    python_callable=second_greet,
    dag=dag
)

greet_task >> upload_task >> second_greet_task