import kafka
import requests
import json
import uuid

# To consume latest messages and auto-commit offsets
AIRFLOW_USERNAME = 'admin'
AIRFLOW_PASSWORD = 'admin'
AIRFLOW_WEBSERVER_HOST = 'localhost'
AIRFLOW_WEBSERVER_PORT = '8080'
KAFKA_BOOTSTRAPSERVERS = ['localhost:9092']
KAFKA_SSL_CAFILE = None
KAFKA_SSL_CERTFILE = None
KAFKA_SSL_KEYFILE = None
KAFKA_TOPIC = 'users'
consumer = kafka.KafkaConsumer(KAFKA_TOPIC,
                               bootstrap_servers=KAFKA_BOOTSTRAPSERVERS,
                               ssl_cafile=KAFKA_SSL_CAFILE,
                               ssl_certfile=KAFKA_SSL_CERTFILE,
                               ssl_keyfile=KAFKA_SSL_KEYFILE
                               )
try:
    headers = {"Content-Type": "application/json",
               "Accept": "*/*",
               }

    for message in consumer:
        # This id is attached to the Airflow Varible and Airflow DAGRun, so they can be attached to each other in airflow
        id = str(uuid.uuid1())
        message_info = f"message_value:{message.value}"
        var_data = {"key": id,
                    "value": message_info}
        #This REST Call creates a varible in Airflow with the kafka message value. Key of the varible is the id created above
        var_return = requests.post(
            f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/variables",
            headers=headers,
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD), data=json.dumps(var_data))
        data = {
            "dag_run_id": id,
            "conf":{"message":message_info}
        }
        #This REST Call triggers the DAG Run, The run id is the uuid generated above.
        # This allows us to us the run_id to pull the varible during the DAGRun
        dagrun_return = requests.post(
            f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/api/v1/dags/example_dag/dagRuns",
            headers=headers,
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD), data=json.dumps(data))

        print(dagrun_return)

except Exception as e:
    print(e)


finally:
    print('closing')
    consumer.close()
