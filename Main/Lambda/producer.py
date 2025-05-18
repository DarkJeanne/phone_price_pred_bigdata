import os
from kafka import KafkaProducer
from dotenv import load_dotenv
import json

load_dotenv()
bootstrap_servers = os.getenv('KAFKA_BROKER_INTERNAL', 'kafka:9092')
topic = os.getenv('KAFKA_SMARTPHONE_TOPIC', 'smartphoneTopic')

print(f"Connecting to Kafka at {bootstrap_servers}, topic: {topic}")

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_message(message):
    try:
        producer.send(topic, message)
        print(f"Produced: {message} to Kafka topic: {topic}")
    except Exception as error:
        print(f"Error: {error}")

