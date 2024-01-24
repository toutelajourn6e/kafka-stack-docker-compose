from time import sleep
from json import dumps
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

for i in range(101, 151):
    print("Iteration", i)
    data = {'counter': i}
    producer.send('topic_test', value=data)
    sleep(0.3)
