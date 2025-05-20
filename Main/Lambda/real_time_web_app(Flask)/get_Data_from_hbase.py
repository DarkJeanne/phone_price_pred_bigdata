import os
import happybase
import time
from dotenv import load_dotenv

load_dotenv()

HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', '9090'))
HBASE_TABLE_NAME = os.getenv('HBASE_SMARTPHONE_TABLE', 'smartphone')
MAX_RETRIES = 5  # Increased retries
RETRY_DELAY = 5  # Increased delay

def decode_dict(d):
    return {k.decode('utf-8').split(':', 1)[1]: v.decode('utf-8') for k, v in d.items()}

def get_last_record_from_hbase():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            # Explicitly print connection details for debugging
            print(f"Connecting to HBase at {HBASE_HOST}:{HBASE_PORT} (attempt {retries+1}/{MAX_RETRIES})")
            
            # Establish a connection to HBase with longer timeout
            connection = happybase.Connection(
                host=HBASE_HOST, 
                port=HBASE_PORT, 
                timeout=20000,  # 20 seconds timeout
                autoconnect=False
            )
            connection.open()
            
            # Check if table exists
            tables = [t.decode('utf-8') for t in connection.tables()]
            if HBASE_TABLE_NAME not in tables:
                print(f"Table {HBASE_TABLE_NAME} doesn't exist in HBase")
                connection.close()
                return provide_sample_data()

            # Open the table
            table = connection.table(HBASE_TABLE_NAME)

            # Initialize the scanner
            scanner = table.scan(limit=5, reverse=True)  # Get last 5 records to have more options

            # Get the last record
            last_record = {}
            for key, data in scanner:
                last_record = decode_dict(data)
                if last_record:  # If we have data, break the loop
                    break

            # Close the connection
            connection.close()

            if not last_record:
                # If no records found, provide sample data
                print("No records found in HBase, using sample data")
                last_record = provide_sample_data()
            else:
                print(f"Successfully retrieved record from HBase: {last_record}")
            
            return last_record
        except Exception as e:
            print(f"Error connecting to HBase (attempt {retries+1}/{MAX_RETRIES}): {str(e)}")
            retries += 1
            if retries < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print("Max retries reached, using sample data")
                return provide_sample_data()

def provide_sample_data():
    """Provide sample smartphone data when HBase connection fails"""
    return {
        "Brand": "Sample Brand",
        "Model": "Sample Model",
        "Storage": "128GB",
        "RAM": "8GB",
        "Screen_size": "6.5 inches",
        "Battery": "4500mAh",
        "Sim_type": "Dual SIM",
        "Price": "12000000"
    }


