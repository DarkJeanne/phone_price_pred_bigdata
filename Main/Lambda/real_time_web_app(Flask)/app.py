import os
import logging
from flask import Flask, render_template, jsonify
# Giả sử get_Data_from_hbase.py cũng sẽ được cập nhật để dùng biến môi trường cho HBase
from get_Data_from_hbase import get_last_record_from_hbase 
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = Flask(__name__)

@app.route('/')
def index():
    try:
        logger.info("Fetching last record from HBase")
        last_record = get_last_record_from_hbase()
        logger.info(f"Successfully fetched record: {last_record}")
        return render_template('index.html', last_record=last_record)
    except Exception as e:
        logger.error(f"Error in index route: {str(e)}")
        error_message = {"error": str(e)}
        return render_template('index.html', last_record=error_message)

@app.route('/health')
def health():
    return jsonify({"status": "healthy"}), 200


if __name__ == '__main__':
    host = os.getenv('FLASK_APP_HOST', '0.0.0.0')  # Default to 0.0.0.0 to be accessible from outside container
    port = int(os.getenv('FLASK_APP_PORT', '5001'))
    debug_mode = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    
    logger.info(f"Starting Flask app on {host}:{port}, debug={debug_mode}")
    app.run(host=host, port=port, debug=debug_mode)