from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv

from transform import transformation
from .insert_data_hbase import insert_dataHbase

load_dotenv()

def consum():
    bootstrap_servers = os.getenv('KAFKA_BROKER_INTERNAL', 'kafka:9092')
    topic = os.getenv('KAFKA_SMARTPHONE_TOPIC', 'smartphoneTopic')

    # Create a Kafka consumer
    consumer = KafkaConsumer(topic,
                             group_id=os.getenv('ML_CONSUMER_GROUP_ID', 'ml_consumer_group'),
                             auto_offset_reset='latest',
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    print(f"ML Consumer: Connecting to Kafka {bootstrap_servers}, topic {topic}")

    for message in consumer:
        try:
            data = message.value
            print('before ml operation')
            res = transformation(data)
            print(res)
            insert_dataHbase(res)

            print("-------------------")
        except Exception as e:
            print(f"Error processing message in ML consumer: {e}")
            continue


