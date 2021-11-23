from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.version import version
from datetime import datetime, timedelta
from kafka import TopicPartition, KafkaProducer




def my_kafka_function(ts, **kwargs):
    """
   This looks at a kafka topic get all the messages from the earliest point
    """


    print(kwargs)
    KAFKA_SSL_CAFILE = None
    KAFKA_SSL_CERTFILE = None
    KAFKA_SSL_KEYFILE = None
    KAFKA_BOOTSTRAPSERVERS = ['broker:29092']
    DEFAULT_TOPIC = 'users'

    producer = KafkaProducer(
                             bootstrap_servers=KAFKA_BOOTSTRAPSERVERS,
                             ssl_cafile=KAFKA_SSL_CAFILE,
                             ssl_certfile=KAFKA_SSL_CERTFILE,
                             ssl_keyfile=KAFKA_SSL_KEYFILE
                             )

    producer.send(DEFAULT_TOPIC,key=b'foo',value=b'bar')




# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task

with DAG('kafka_produce',
         start_date=datetime(2021, 10, 31),
         max_active_runs=3,
         schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False  # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = DummyOperator(
        task_id='start'
    )

    t1 = PythonOperator(
        task_id=f'send_kafka_msg',
        python_callable=my_kafka_function,
    )

    t0 >> t1
