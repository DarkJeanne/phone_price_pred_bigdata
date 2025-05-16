import os
import happybase
from dotenv import load_dotenv

load_dotenv()

HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', '9090'))
HBASE_TABLE_NAME = os.getenv('HBASE_SMARTPHONE_TABLE', 'smartphone') # Ở đây không cần .encode() vì tên bảng dùng trong table()

def decode_dict(d):
    return {k.decode('utf-8').split(':', 1)[1]: v.decode('utf-8') for k, v in d.items()}

def get_last_record_from_hbase():
    # Establish a connection to HBase
    # connection = happybase.Connection('localhost')  # Assuming HBase is running locally
    connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, autoconnect=False)
    connection.open()

    # Open the 'smartphone' table
    # table = connection.table('smartphone')
    table = connection.table(HBASE_TABLE_NAME)

    # Initialize the scanner
    scanner = table.scan(limit=1, reverse=True)

    # Get the last record
    last_record = {}
    try:
        for key, data in scanner:
            last_record = decode_dict(data)
            break  # Exit the loop after fetching the first (last) record
    finally:
        # Close the scanner
        scanner.close()

    # Close the connection
    connection.close()

    return last_record


