from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.version import version
from datetime import datetime, timedelta
from airflow.models import Variable
import requests
headers = {"Content-Type": "application/json",
           "Accept": "*/*",
           }
AIRFLOW_USERNAME = 'admin'
AIRFLOW_PASSWORD = 'admin'
AIRFLOW_WEBSERVER_HOST = 'webserver'
AIRFLOW_WEBSERVER_PORT = '8080'

def print_message( **kwargs):

    #This pulls the varible where we stored the kafka message
    print(f"Message:{kwargs['dag_run'].conf['message']}")



# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG('example_dag',
         start_date=datetime(2021, 10, 31),
         max_active_runs=3,
         schedule_interval=None,  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False  # enable if you don't want historical dag runs to run
         ) as dag:



    t1 = PythonOperator(
        task_id='python_ex',
        python_callable=print_message,
        provide_context=True,

    )
    t2= BashOperator(
        task_id="bash_ex",
        bash_command='echo "{{ dag_run.conf["message"] }}"'

    )


    t1>>t2