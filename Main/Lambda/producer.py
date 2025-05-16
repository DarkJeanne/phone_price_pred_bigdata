import os
from kafka import KafkaProducer
from dotenv import load_dotenv
import json

load_dotenv() # Tải biến từ .env file (nếu có, hữu ích khi chạy local ngoài Docker)

bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
topic = os.getenv('KAFKA_SMARTPHONE_TOPIC', 'smartphoneTopic')

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

