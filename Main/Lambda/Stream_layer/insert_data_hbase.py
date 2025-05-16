import os
from datetime import datetime
import happybase
from dotenv import load_dotenv

load_dotenv()

HBASE_HOST = os.getenv('HBASE_HOST', 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT', '9090')) # happybase cần port là integer
HBASE_TABLE_NAME = os.getenv('HBASE_SMARTPHONE_TABLE', 'smartphone').encode('utf-8') # Tên bảng cần là bytes
HBASE_CF_INFO = os.getenv('HBASE_SMARTPHONE_CF', 'info').encode('utf-8') # Tên CF cần là bytes

def insert_dataHbase(data):
    # connection = happybase.Connection('localhost')
    connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, autoconnect=False) # Thêm autoconnect=False để mở sau
    connection.open()

    # if b'smartphone' not in connection.tables():
    if HBASE_TABLE_NAME not in connection.tables():
        connection.create_table(
            HBASE_TABLE_NAME,
            {
                HBASE_CF_INFO: dict()  # Column family 'info'
            }
        )

    table = connection.table(HBASE_TABLE_NAME)

    row_key = datetime.now().strftime('%Y%m%d%H%M%S%f')

    # Tạo key cho column family một cách động
    # Ví dụ: b'info:date' sẽ thành HBASE_CF_INFO + b':date'
    data_to_insert = {
        HBASE_CF_INFO + b':date': bytes(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'utf-8'),
        HBASE_CF_INFO + b':Brand': bytes(data[0], 'utf-8'),
        HBASE_CF_INFO + b':Screen_size': bytes(str(data[1]), 'utf-8'),
        HBASE_CF_INFO + b':RAM': bytes(str(data[2]), 'utf-8'),
        HBASE_CF_INFO + b':Storage': bytes(str(data[3]), 'utf-8'),
        HBASE_CF_INFO + b':Sim_type': bytes(data[4], 'utf-8'),
        HBASE_CF_INFO + b':Battery': bytes(str(data[5]), 'utf-8'),
        HBASE_CF_INFO + b':Price': bytes(str(data[6]), 'utf-8')
    }

    table.put(bytes(row_key, 'utf-8'), data_to_insert)

    print(f"Data inserted into HBase table {HBASE_TABLE_NAME.decode('utf-8')}: ", data)

    connection.close()





