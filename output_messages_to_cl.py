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



    for message in consumer:

        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))

except Exception as e:
    print(e)


finally:
    print('closing')
    consumer.close()
