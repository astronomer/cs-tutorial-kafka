# cs-tutorial-kafka

## Prereqs
astro cli 

python3 environment


## Setup

pull git repo 

```cd cs-tutorial-kafka ```

run:

```
astro dev start
```
run this to create the "users" topic in kafka:
```
docker-compose exec broker kafka-topics \                                               
  --create \
  --bootstrap-server broker:29092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic users
```
Run this to install the nessacery local packages
```
pip install -r pip install -r local_req.txt
```

start python script with(this script will continueally run, to stop press ctrl+c) This needs to be running for the process to work:
```
python output_messages.py
```
navigate in you web browser to see the Kafka Control Center:
http://localhost:9021/clusters

select the controlcenter.cluster


select topics


select the "users" topic


select "messages"


select  "Produce a new message to this topic"


set key and value values- Thes must both be in JSON form


select produce

you should see out from your output_message.py script that is running 
example DAG is trigger and you see the message out in the log of the get message task
