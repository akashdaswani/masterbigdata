from kafka import KafkaConsumer

consumer = KafkaConsumer ('test',
                          group_id='my_consumer',
                          bootstrap_servers=['localhost:9092'])

for message in consumer:
    print message