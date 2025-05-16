import os
import pandas as pd
from hdfs import InsecureClient
from dotenv import load_dotenv

load_dotenv()

def store_data_in_hdfs(transaction_data):
    columns = ['id','brand','model_name','screen_size','ram','rom','cams','sim_type','battary',
               'sale_percentage','product_rating','seller_name','seller_score','seller_followers','Reviews']
    transaction_df = pd.DataFrame([transaction_data], columns=columns)

    hdfs_namenode_webui_host = os.getenv('HDFS_NAMENODE_WEBUI_HOST_INTERNAL', 'namenode')
    hdfs_namenode_webui_port = os.getenv('HDFS_NAMENODE_WEBUI_PORT', '9870')
    hdfs_base_path = os.getenv('HDFS_BATCH_LAYER_PATH', '/batch-layer')
    hdfs_file_path = f"{hdfs_base_path}/raw_data.csv"

    client = InsecureClient(f'http://{hdfs_namenode_webui_host}:{hdfs_namenode_webui_port}')
    print(f"HDFS Put Data: Connecting to WebHDFS at http://{hdfs_namenode_webui_host}:{hdfs_namenode_webui_port}")

    if not client.status(hdfs_base_path, strict=False):
        print(f"HDFS Put Data: Creating directory {hdfs_base_path}")
        client.makedirs(hdfs_base_path)
    elif client.status(hdfs_base_path, strict=False)['type'] == 'FILE':
        print(f"Error: HDFS path {hdfs_base_path} is a file, not a directory.")
        return

    if not client.status(hdfs_file_path, strict=False):
        print(f"HDFS Put Data: File {hdfs_file_path} does not exist. Creating new file.")
        with client.write(hdfs_file_path, encoding='utf-8', overwrite=True) as writer:
            transaction_df.to_csv(writer, index=False, header=True)
        print(f"HDFS Put Data: Successfully wrote new file {hdfs_file_path}")
    else:
        try:
            print(f"HDFS Put Data: File {hdfs_file_path} exists. Appending data.")
            with client.read(hdfs_file_path, encoding='utf-8') as reader:
                existing_df = pd.read_csv(reader)
            combined_df = pd.concat([existing_df, transaction_df], ignore_index=True)
            with client.write(hdfs_file_path, encoding='utf-8', overwrite=True) as writer:
                combined_df.to_csv(writer, index=False, header=True)
            print(f"HDFS Put Data: Successfully appended to {hdfs_file_path}")
        except Exception as e:
            print(f"Error processing existing HDFS file {hdfs_file_path}: {e}")
            try:
                print(f"HDFS Put Data: Fallback - Overwriting file {hdfs_file_path} due to previous error.")
                with client.write(hdfs_file_path, encoding='utf-8', overwrite=True) as writer:
                    transaction_df.to_csv(writer, index=False, header=True)
            except Exception as e2:
                print(f"HDFS Put Data: Error during fallback overwrite of {hdfs_file_path}: {e2}")







