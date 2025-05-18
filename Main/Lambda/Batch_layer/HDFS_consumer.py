import os
from kafka import KafkaConsumer
import json
from put_data_hdfs import store_data_in_hdfs
from dotenv import load_dotenv

load_dotenv()

def consum_hdfs():
    # Kafka broker configuration
    bootstrap_servers = os.getenv('KAFKA_BROKER_INTERNAL', 'kafka:9092')
    topic = os.getenv('KAFKA_SMARTPHONE_TOPIC', 'smartphoneTopic')

    # Create a Kafka consumer
    consumer = KafkaConsumer(topic,
                             group_id=os.getenv('HDFS_CONSUMER_GROUP_ID', 'hdfs_consumer_group'),
                             auto_offset_reset='latest',
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    print(f"HDFS Consumer: Connecting to Kafka {bootstrap_servers}, topic {topic}")

    for message in consumer:
        try:
            data = message.value
            store_data_in_hdfs(data)

            print("-------------------")
        except Exception as e:
            print(f"Error processing message in HDFS consumer: {e}")
            continue


