import time
import os
from producer import send_message
from Stream_layer.ML_consumer import consum
import threading
from Stream_data.stream_data import generate_real_time_data


def producer_thread():
    while True:
        try:
            file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Stream_data', 'stream_data.csv')
            message = generate_real_time_data(file_path)

            send_message(message)
            print("Message sent to Kafka topic")

            time.sleep(5)

        except Exception as e:
            print(f"Error in producer_thread: {str(e)}")

def consumer_thread():
    while True:
        try:
            consum()
            time.sleep(3)
        except Exception as e:
            print(f"Error in consumer_thread: {str(e)}")

producer_thread = threading.Thread(target=producer_thread)
consumer_thread = threading.Thread(target=consumer_thread)

producer_thread.start()
consumer_thread.start()

producer_thread.join()
consumer_thread.join()
