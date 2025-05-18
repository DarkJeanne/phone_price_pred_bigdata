import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_mongo_client():
    mongo_connection_uri_env = os.getenv('MONGO_CONNECTION_URI')
    # MONGO_DB_NAME will be used to select the database after connection
    mongo_db_name_for_operation = os.getenv('MONGO_DB_NAME', 'phone_price_pred') 

    if mongo_connection_uri_env:
        print(f"Connecting to MongoDB with provided URI: [SENSITIVE]")
        # The URI might contain a default database, but we will operate on mongo_db_name_for_operation
        client = MongoClient(mongo_connection_uri_env)
        return client, mongo_db_name_for_operation
    else:
        # Fallback to host/port construction (for local non-SRV setup)
        print(f"Constructing MongoDB URI from MONGO_HOST, MONGO_PORT, etc.")
        mongo_host = os.getenv('MONGO_HOST', 'localhost')
        mongo_port = int(os.getenv('MONGO_PORT', '27017'))
        mongo_user = os.getenv('MONGO_USER')
        mongo_password = os.getenv('MONGO_PASSWORD')
        
        if mongo_user and mongo_password:
            # Include database name in URI for authentication if authSource is not specified, 
            # or rely on authSource=admin if that's how users are set up.
            # For simplicity, we can connect to the server and then select the DB.
            mongo_uri_constructed = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin"
        else:
            mongo_uri_constructed = f"mongodb://{mongo_host}:{mongo_port}/"
        
        client = MongoClient(mongo_uri_constructed)
        return client, mongo_db_name_for_operation

def save_data_to_mongodb(data_df: pd.DataFrame, collection_name: str = None):
    if not isinstance(data_df, pd.DataFrame):
        print("Error: Input data must be a Pandas DataFrame.")
        return
    if data_df.empty:
        print("Info: DataFrame is empty. Nothing to save.")
        return

    client, db_name = get_mongo_client() # db_name here is from MONGO_DB_NAME
    db = client[db_name] # Select the target database
    
    target_collection_name = collection_name if collection_name else os.getenv('MONGO_COLLECTION_NAME', 'smartphones')
    collection = db[target_collection_name]

    try:
        records = data_df.to_dict(orient='records')
        collection.drop() 
        result = collection.insert_many(records)
        print(f"Successfully inserted {len(result.inserted_ids)} records into MongoDB collection '{target_collection_name}' in database '{db_name}'.")
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")
    finally:
        if client:
            client.close()

if __name__ == '__main__':
    # Ví dụ cách sử dụng (chạy độc lập để test)
    # Tạo một DataFrame mẫu
    sample_data = {
        'Brand': ['Samsung', 'Apple', 'Xiaomi'],
        'Model': ['Galaxy S23', 'iPhone 15', 'Redmi Note 12'],
        'Price': [800, 999, 300],
        'RAM_GB': [8, 6, 4]
    }
    sample_df = pd.DataFrame(sample_data)
    
    print("Attempting to save sample data to MongoDB (check .env for MONGO_CONNECTION_URI or other MONGO_* vars)...")
    # Cần có file .env ở cùng thư mục hoặc các biến MONGO_* được set trong môi trường
    # để script này chạy đúng khi không có Docker.
    save_data_to_mongodb(sample_df, "test_phones_collection") 