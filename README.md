# cs-tutorial-kafka


pull git repo && cd cs-tutorial-kafka 


run:


astro dev start

run this to create the "users" topic in kafka:

docker-compose exec broker kafka-topics \                                               
  --create \
  --bootstrap-server broker:29092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic users


then activate venv with 

source venv/bin/activate 	

start python script with(this script will continueally run, to stop press ctrl+c):

python output_messages.py

navigate in you web browser to:
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
