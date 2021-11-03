from kafka import TopicPartition, KafkaConsumer

brokers=['broker:9092']
topics='users'
#
# TOPIC = 'users'
# GROUP = 'MYGROUP'
# BOOTSTRAP_SERVERS = ['broker:9092']
#
# consumer = KafkaConsumer(
#         bootstrap_servers=BOOTSTRAP_SERVERS,
#         group_id=GROUP,
#         enable_auto_commit=False
#     )
#
#
# for p in consumer.partitions_for_topic(TOPIC):
#     tp = TopicPartition(TOPIC, p)
#     consumer.assign([tp])
#     committed = consumer.committed(tp)
#     consumer.seek_to_end(tp)
#     last_offset = consumer.position(tp)
#     print("topic: %s partition: %s committed: %s last: %s lag: %s" % (TOPIC, p, committed, last_offset, (last_offset - committed)))
#
# consumer.close(autocommit=False)

# To consume latest messages and auto-commit offsets
print(1)
consumer = KafkaConsumer(topics,
                       #  group_id='my-group',
                        auto_offset_reset='earliest',
                         bootstrap_servers=brokers,
                         enable_auto_commit=False)
print(2)


try:
    print(3)
    messages = consumer.poll(500)
    print(4)
finally:
    print(5)
    consumer.close()
    print(6)
#print(messages)
for x in messages[TopicPartition(topic='users',partition=0)]:
    print(7)
    print(x)
    print(x.offset)

    # for y in x:
    #     print(y)

# try:
#     # message=consumer.poll(50000)
#     # print(message)
#     for message in consumer:
#         # message value and key are raw bytes -- decode if necessary!
#         # e.g., for unicode: `message.value.decode('utf-8')`
#         print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                               message.offset, message.key,
#                                               message.value))
# finally:
#     print('closing')
#     consumer.close()