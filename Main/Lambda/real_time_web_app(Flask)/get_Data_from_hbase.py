import os
import happybase
import time
from dotenv import load_dotenv

load_dotenv()

HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', '9090'))
HBASE_TABLE_NAME = os.getenv('HBASE_SMARTPHONE_TABLE', 'smartphone')
MAX_RETRIES = 5
RETRY_DELAY = 5

def decode_dict(d):
    return {k.decode('utf-8').split(':', 1)[1]: v.decode('utf-8') for k, v in d.items()}

def get_last_record_from_hbase():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            print(f"Connecting to HBase at {HBASE_HOST}:{HBASE_PORT} (attempt {retries+1}/{MAX_RETRIES})")
            
            connection = happybase.Connection(
                host=HBASE_HOST, 
                port=HBASE_PORT, 
                timeout=20000,
                autoconnect=False
            )
            connection.open()
            
            tables = [t.decode('utf-8') for t in connection.tables()]
            if HBASE_TABLE_NAME not in tables:
                print(f"Table {HBASE_TABLE_NAME} doesn't exist in HBase")
                connection.close()
                return provide_sample_data()

            table = connection.table(HBASE_TABLE_NAME)

            scanner = table.scan(limit=5, reverse=True)

            last_record = {}
            for key, data in scanner:
                last_record = decode_dict(data)
                if last_record:
                    break

            connection.close()

            if not last_record:
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


