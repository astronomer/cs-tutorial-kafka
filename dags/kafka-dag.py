from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.version import version
from datetime import datetime, timedelta
from kafka import TopicPartition, KafkaConsumer


def my_custom_function(ts, **kwargs):
    """
    This can be any python code you want and is called from the python operator. The code is not executed until
    the task is run by the airflow scheduler.
    """
    print(
        f"I am task number {kwargs['task_number']}. This DAG Run execution date is {ts} and the current time is {datetime.now()}")
    print('Here is the full DAG Run context. It is available because provide_context=True')
    print(kwargs)
    print(f'ddd:{os.path.dirname(sys.executable)}')


def my_kafka_function(ts, **kwargs):
    """
    This can be any python code you want and is called from the python operator. The code is not executed until
    the task is run by the airflow scheduler.
    """
    print(
        f"I am task number {kwargs['message']}. This DAG Run execution date is {ts} and the current time is {datetime.now()}")
    print('Here is the full DAG Run context. It is available because provide_context=True')
    print(kwargs)
    print("%s:%d:%d: key=%s value=%s" % (kwargs['message'].topic, kwargs['message'].partition,
                                         kwargs['message'].offset, kwargs['message'].key,
                                         kwargs['message'].value))


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
DEFAULT_HOST = 'broker'
DEFAULT_PORT = 29092
DEFAULT_TOPIC = 'users'
timeout_ms = 5000
# Using a DAG context manager, you don't have to specify the dag property of each task
consumer = KafkaConsumer(DEFAULT_TOPIC,
                               auto_offset_reset='earliest',
                               bootstrap_servers = [f"{DEFAULT_HOST}:{DEFAULT_PORT}"],
                               enable_auto_commit=False,
                                ssl_cafile ="",
                                ssl_certfile ="",
                                ssl_keyfile =""
                               )
with DAG('kafka_dag',
         start_date=datetime(2021, 10, 31),
         max_active_runs=3,
         schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False  # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = DummyOperator(
        task_id='start'
    )

    messages = consumer.poll(timeout_ms)

    for message in messages[TopicPartition(topic=DEFAULT_TOPIC, partition=0)]:
        tn = PythonOperator(
            task_id=f'kafka_msg_{message.offset}',
            python_callable=my_kafka_function,
            op_kwargs={'message': message},
        )

        t0 >> tn

