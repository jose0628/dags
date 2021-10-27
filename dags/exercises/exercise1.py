import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


#
# TODO: Define a function for the PythonOperator to call and have it log something
#
def greet():
    logging.info('Hello World Engineers!')


dag = DAG(
        'session1.exercise1.sol',
        start_date=datetime.datetime.now())

#
# TODO: Uncomment the operator below and replace the arguments labeled <REPLACE> below
#

greet_task = PythonOperator(
   task_id="greet_task",
   python_callable=greet,
   dag=dag
)

