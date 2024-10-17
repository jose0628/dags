import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook



def start():
    logging.info('Starting the DAG')

def get_records():
    request = "SELECT * FROM test"
    pg_hook = PostgresHook(postgres_conn_id="rds_pepe", schema="datalake1")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for source in sources:
        logging.info(source)
    return sources

def get_pandas():
    db_hook = PostgresHook(postgres_conn_id="rds_pepe", schema="datalake1")
    df = db_hook.get_pandas_df('SELECT * FROM test')
    logging.info(f'Successfully used PostgresHook to return {len(df)} records')
    logging.info(df)

dag = DAG(
    'session1.exercise2_old',
    schedule_interval='@once',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1)
)

greet_task = PythonOperator(
    task_id="start_task",
    python_callable=start,
    dag=dag
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="rds_pepe",
    sql='''
            CREATE TABLE IF NOT EXISTS test (col1 int, col2 int, col3 int);
        '''
)

insert_values = PostgresOperator(
    task_id="insert_values",
    dag=dag,
    postgres_conn_id="rds_pepe",
    sql='''
            INSERT INTO test(col1, col2, col3) VALUES (555555, 555555, 555555);
        '''
)

get_records = PythonOperator(
    task_id="get_records",
    python_callable=get_records,
    dag=dag
)

get_pandas = PythonOperator(
    task_id='get_pandas',
    python_callable=get_pandas,
    dag=dag
)

greet_task >> create_table >> insert_values >> get_records >> get_pandas
