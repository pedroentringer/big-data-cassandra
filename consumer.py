from json import loads
from kafka import KafkaConsumer

topic = "topic_sensors"
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', enable_auto_commit=True, value_deserializer = lambda x: loads (x.decode( 'utf-8')))

for message in consumer:
    print(message.value)