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

def get_varible(run_id , **kwargs):

    #This pulls the varible where we stored the kafka message
    print("Variable: "+Variable.get(run_id))
    print(kwargs)

def my_clean_up_function(run_id , **kwargs):
    #This deletes the Variable after we have pulled the kafka message from it in previous task.

    var_return = requests.delete(
        f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/variables/{run_id}",
        headers=headers,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    )
    print(var_return)


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
         schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False  # enable if you don't want historical dag runs to run
         ) as dag:



    t1 = PythonOperator(
        task_id='get_message',
        python_callable=get_varible,
        provide_context=True,

    )

    t2 = PythonOperator(
        task_id='clean_up',
        python_callable=my_clean_up_function,
        provide_context=True,
    )


    t1>>t2