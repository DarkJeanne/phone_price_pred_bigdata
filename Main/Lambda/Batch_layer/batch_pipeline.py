import time
import os
from producer import send_message

from HDFS_consumer import consum_hdfs
import threading
from Stream_data.stream_data import generate_real_time_data


def producer_thread():
    while True:
        try:
            file_path = '/app/Stream_data/stream_data.csv'
            if not os.path.exists(file_path):
                print(f"Error: File {file_path} not found")
                print(f"Current working directory: {os.getcwd()}")
                print(f"Files in /app/Stream_data/: {os.listdir('/app/Stream_data')}")
                time.sleep(5)
                continue
                
            message = generate_real_time_data(file_path)

            send_message(message)
            print("Message sent to Kafka topic")

            time.sleep(5)

        except Exception as e:
            print(f"Error in producer_thread: {str(e)}")

def consumer_thread():
    while True:
        try:
            consum_hdfs()
            time.sleep(3)
        except Exception as e:
            print(f"Error in consumer_thread: {str(e)}")

producer_thread = threading.Thread(target=producer_thread)
consumer_thread = threading.Thread(target=consumer_thread)

producer_thread.start()
consumer_thread.start()

producer_thread.join()
consumer_thread.join()
