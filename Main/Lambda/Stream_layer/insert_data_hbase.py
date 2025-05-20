import os
from datetime import datetime
import happybase
import time
from dotenv import load_dotenv

load_dotenv()

HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', '9090'))
HBASE_TABLE_NAME = os.getenv('HBASE_SMARTPHONE_TABLE', 'smartphone')
HBASE_CF_INFO = os.getenv('HBASE_SMARTPHONE_CF', 'info')
MAX_RETRIES = 5
RETRY_DELAY = 5

def insert_dataHbase(data):
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
                print(f"Table {HBASE_TABLE_NAME} not found, creating it")
                families = {HBASE_CF_INFO: dict()}
                connection.create_table(HBASE_TABLE_NAME, families)
                print(f"Table {HBASE_TABLE_NAME} created")
            
            table = connection.table(HBASE_TABLE_NAME)

            row_key = datetime.now().strftime('%Y%m%d%H%M%S%f')

            data_to_insert = {
                f"{HBASE_CF_INFO}:date".encode('utf-8'): datetime.now().strftime('%Y-%m-%d %H:%M:%S').encode('utf-8'),
                f"{HBASE_CF_INFO}:Brand".encode('utf-8'): str(data[0]).encode('utf-8'),
                f"{HBASE_CF_INFO}:Screen_size".encode('utf-8'): str(data[1]).encode('utf-8'),
                f"{HBASE_CF_INFO}:RAM".encode('utf-8'): str(data[2]).encode('utf-8'),
                f"{HBASE_CF_INFO}:Storage".encode('utf-8'): str(data[3]).encode('utf-8'),
                f"{HBASE_CF_INFO}:Sim_type".encode('utf-8'): str(data[4]).encode('utf-8'),
                f"{HBASE_CF_INFO}:Battery".encode('utf-8'): str(data[5]).encode('utf-8'),
                f"{HBASE_CF_INFO}:Price".encode('utf-8'): str(data[6]).encode('utf-8')
            }

            table.put(row_key.encode('utf-8'), data_to_insert)

            print(f"Data successfully inserted into HBase table '{HBASE_TABLE_NAME}': {data}")
            connection.close()
            return True
            
        except Exception as e:
            print(f"Error inserting data into HBase (attempt {retries+1}/{MAX_RETRIES}): {str(e)}")
            retries += 1
            if retries < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print("Max retries reached, HBase insertion failed")
                return False





