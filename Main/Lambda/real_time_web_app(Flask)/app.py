import os
from flask import Flask, render_template
# Giả sử get_Data_from_hbase.py cũng sẽ được cập nhật để dùng biến môi trường cho HBase
from get_Data_from_hbase import get_last_record_from_hbase 
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)


@app.route('/')
def index():
    last_record = get_last_record_from_hbase()
    return render_template('index.html',last_record=last_record)


if __name__ == '__main__':
    host = os.getenv('FLASK_APP_HOST', '127.0.0.1')
    port = int(os.getenv('FLASK_APP_PORT', '5000'))
    debug_mode = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    app.run(host=host, port=port, debug=debug_mode)