import time
import os
from producer import send_message

from HDFS_consumer import consum_hdfs
import threading
from Stream_data.stream_data import generate_real_time_data


def producer_thread():
    while True:
        try:
            # Fix path issue - Use absolute path to Stream_data/stream_data.csv
            file_path = '/app/Stream_data/stream_data.csv'
            # Check if file exists before trying to use it
            if not os.path.exists(file_path):
                print(f"Error: File {file_path} not found")
                print(f"Current working directory: {os.getcwd()}")
                print(f"Files in /app/Stream_data/: {os.listdir('/app/Stream_data')}")
                time.sleep(5)
                continue
                
            message = generate_real_time_data(file_path)

            send_message(message)
            print("Message sent to Kafka topic")

            # Sleep for 5 seconds before collecting and sending the next set of data
            time.sleep(5)

        except Exception as e:
            print(f"Error in producer_thread: {str(e)}")

def consumer_thread():
    while True:
        try:
            consum_hdfs()
            # Sleep for a short interval before consuming the next message
            time.sleep(3)
        except Exception as e:
            print(f"Error in consumer_thread: {str(e)}")

# Create separate threads for producer and consumer
producer_thread = threading.Thread(target=producer_thread)
consumer_thread = threading.Thread(target=consumer_thread)

# Start the threads
producer_thread.start()
consumer_thread.start()

# Wait for the threads to finish (which will never happen in this case as they run infinitely)
producer_thread.join()
consumer_thread.join()
