import pandas as pd
import pickle
import os
from dotenv import load_dotenv
from xgboost import XGBRegressor
from transform import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import IntegerType, StringType, FloatType
from pyspark.ml.feature import VectorAssembler

load_dotenv()

def get_spark_session():
    """Initializes and returns a Spark session."""
    # TODO: Add Spark master from env var if not running with spark-submit configuring it
    # spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")
    return SparkSession.builder \
        .appName("SmartphoneBatchTransformation") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()
        # Ensure mongo-spark-connector version matches Spark/Scala version

def read_data_from_hdfs(spark, hdfs_uri, file_path):
    """Reads data from HDFS into a Spark DataFrame."""
    full_path = f"{hdfs_uri}{file_path}"
    print(f"Reading data from HDFS: {full_path}")
    try:
        # Spark can read CSV directly from HDFS if configured correctly
        df = spark.read.csv(full_path, header=True, inferSchema=True)
        # If direct read fails or specific hdfs lib is needed:
        # hdfs_host = os.getenv('HDFS_NAMENODE_WEBUI_HOST_INTERNAL', 'namenode')
        # hdfs_port = os.getenv('HDFS_NAMENODE_WEBUI_PORT', '9870')
        # client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')
        # with client.read(file_path.replace(hdfs_uri, '')) as reader: # Adjust path for client
        #     pandas_df = pd.read_csv(reader)
        # df = spark.createDataFrame(pandas_df)
        return df
    except Exception as e:
        print(f"Error reading from HDFS: {e}")
        return None

def transform_data(spark_df):
    """Transforms the Spark DataFrame."""
    if spark_df is None:
        return None

    print("Initial schema:")
    spark_df.printSchema()
    print(f"Initial count: {spark_df.count()}")

    # Handle potential missing columns if inferSchema didn't pick them all up or they are named differently
    expected_cols = ["brand", "screen_size", "ram", "rom", "sim_type", "battary", "cams", "id", "model_name", "sale_percentage", "product_rating", "seller_name", "seller_score", "seller_followers", "Reviews"]
    for c in expected_cols:
        if c not in spark_df.columns:
            print(f"Warning: Column '{c}' not found in input DataFrame. It will be missing in output or cause errors.")
            # Optionally add it as null: spark_df = spark_df.withColumn(c, lit(None))


    # Drop rows with nulls in key columns needed for transformation/prediction
    # Adjust based on which columns are absolutely essential
    essential_cols_for_transform = ["brand", "screen_size", "ram", "rom", "sim_type", "battary"]
    spark_df = spark_df.dropna(subset=essential_cols_for_transform)
    print(f"Count after dropping NA in essential columns: {spark_df.count()}")

    # Convert columns to numeric types
    # Ensure columns exist before casting
    numeric_cols_to_cast = {
        "screen_size": FloatType(),
        "ram": FloatType(),
        "rom": FloatType(),
        "battary": FloatType()
    }
    for c_name, c_type in numeric_cols_to_cast.items():
        if c_name in spark_df.columns:
            spark_df = spark_df.withColumn(c_name, col(c_name).cast(c_type))
        else:
            print(f"Warning: Column {c_name} not found for casting.")


    # Define UDFs for mapping functions from transform.py
    # Ensure these functions handle None or unexpected values gracefully
    map_brand_udf = udf(lambda brand: map_brand_to_numeric(brand) if brand else None, IntegerType())
    map_sim_type_udf = udf(lambda sim_type: map_sim_type_to_numeric(sim_type) if sim_type else None, IntegerType())

    # Apply mapping functions
    if "brand" in spark_df.columns:
        spark_df = spark_df.withColumn("brand_numeric", map_brand_udf(col("brand")))
    if "sim_type" in spark_df.columns:
        spark_df = spark_df.withColumn("sim_type_numeric", map_sim_type_udf(col("sim_type")))
    
    print("Schema after type conversion and UDF mapping:")
    spark_df.printSchema()

    # Load pre-trained XGBoost model
    model_path = "xgb_model.pkl" # Assume it's in CWD or accessible path
    try:
        with open(model_path, "rb") as f:
            model = pickle.load(f)
    except FileNotFoundError:
        print(f"Error: Model file '{model_path}' not found.")
        return None
    except Exception as e:
        print(f"Error loading model: {e}")
        return None

    # Assemble features for prediction
    # Use the newly created numeric columns
    feature_cols = ["brand_numeric", "screen_size", "ram", "rom", "sim_type_numeric", "battary"]
    
    # Check if all feature columns exist
    missing_feature_cols = [fc for fc in feature_cols if fc not in spark_df.columns]
    if missing_feature_cols:
        print(f"Error: Missing feature columns for prediction: {missing_feature_cols}")
        return None
        
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip") # Added handleInvalid
    assembled_df = assembler.transform(spark_df)
    
    print("Schema after feature assembly:")
    assembled_df.printSchema()
    print(f"Count before prediction: {assembled_df.count()}")


    # Predict prices using the loaded XGBoost model
    # XGBoost model.predict expects a Pandas DataFrame or NumPy array
    # This step can be a bottleneck for very large data.
    # Consider alternatives like distributed XGBoost (xgboost.spark) for large datasets.
    
    # Select only necessary columns for toPandas()
    pandas_df_for_prediction = assembled_df.select(feature_cols).toPandas()
    
    if pandas_df_for_prediction.empty:
        print("No data available for prediction after transformations.")
        # Create an empty DataFrame with price column to avoid errors later
        final_df_with_price = assembled_df.withColumn("predicted_price", lit(None).cast(FloatType()))

    else:
        print(f"Pandas DataFrame for prediction shape: {pandas_df_for_prediction.shape}")
        # Ensure columns are in the same order as expected by the model if it's sensitive
        predictions = model.predict(pandas_df_for_prediction[feature_cols])
        
        # Add predicted prices as a new column
        # Need to join predictions back to the Spark DataFrame. This is tricky due to order.
        # A more robust way is to use a UDF if possible, or add a unique ID before toPandas and join on it.
        # For simplicity here, assuming order is maintained and compatible row count.
        
        # This approach is fragile if rows were dropped by handleInvalid="skip" or if order is not guaranteed
        # A safer way: add predictions to pandas_df_for_prediction, then convert back to Spark and join
        pandas_df_for_prediction['predicted_price'] = predictions
        
        # Select original id columns if available to join back
        # For example, if 'id' is a unique identifier from the original data
        # id_df = assembled_df.select("id", *feature_cols) # Assuming 'id' exists
        # pandas_df_with_ids_and_features = id_df.toPandas()
        # pandas_df_with_ids_and_features['predicted_price'] = model.predict(pandas_df_with_ids_and_features[feature_cols])
        # spark_predictions = spark.createDataFrame(pandas_df_with_ids_and_features[['id', 'predicted_price']])
        # final_df_with_price = assembled_df.join(spark_predictions, "id", "left_outer")

        # Simpler approach for now: add predictions back to the *original* assembled_df requires it to be converted to pandas
        # This is inefficient for large data.
        assembled_pandas_df = assembled_df.toPandas() # Potentially large
        
        # Check if lengths match before assigning. Could mismatch if assembler dropped rows.
        if len(assembled_pandas_df) == len(predictions):
            assembled_pandas_df["predicted_price"] = predictions
            final_df_with_price = spark.createDataFrame(assembled_pandas_df)
        else:
            print(f"Warning: Length mismatch between data ({len(assembled_pandas_df)}) and predictions ({len(predictions)}). Price prediction will be skipped.")
            # Fallback: add a null price column
            final_df_with_price = assembled_df.withColumn("predicted_price", lit(None).cast(FloatType()))


    print("Schema after prediction:")
    final_df_with_price.printSchema()
    final_df_with_price.show(5)
    return final_df_with_price

def save_data_to_mongodb(df, mongo_uri_to_use, db_name_to_use, collection_name_to_use):
    """Saves the Spark DataFrame to MongoDB."""
    if df is None:
        print("No data to save to MongoDB.")
        return

    print(f"Saving data to MongoDB. URI: [SENSITIVE], DB: {db_name_to_use}, Collection: {collection_name_to_use}")
    try:
        df.write.format("mongo") \
            .mode("append") \
            .option("uri", mongo_uri_to_use) \
            .option("database", db_name_to_use) \
            .option("collection", collection_name_to_use) \
            .save()
        print("Data saved to MongoDB successfully.")
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")

if __name__ == "__main__":
    spark = get_spark_session()

    # Get HDFS connection details from environment variables
    hdfs_namenode_host = os.getenv('HDFS_NAMENODE_HOST_INTERNAL', 'namenode') # e.g., namenode or localhost
    # Spark typically uses hdfs://<namenode>:<port>/path, not WebHDFS http for direct read
    # Default HDFS port is often 8020 or 9000 for the RPC interface
    hdfs_namenode_port = os.getenv('HDFS_NAMENODE_RPC_PORT', '9000') # Check your HDFS setup
    hdfs_base_uri = f"hdfs://{hdfs_namenode_host}:{hdfs_namenode_port}"
    
    hdfs_batch_path = os.getenv('HDFS_BATCH_LAYER_PATH', '/batch-layer') # e.g., /batch-layer
    hdfs_raw_file = os.getenv('HDFS_RAW_DATA_FILE', 'raw_data.csv') # e.g., raw_data.csv
    hdfs_full_path = f"{hdfs_batch_path}/{hdfs_raw_file}"

    # MongoDB connection logic updated
    mongo_connection_uri_from_env = os.getenv('MONGO_CONNECTION_URI')
    mongo_db_target = os.getenv('MONGO_DB_NAME', 'phone_price_pred') # DB to write to
    mongo_collection_target = os.getenv('MONGO_TRANSFORMED_SMARTPHONE_COLLECTION', 'smartphones')

    actual_mongo_uri_to_use = ""

    if mongo_connection_uri_from_env:
        print(f"Using MONGO_CONNECTION_URI from environment for Spark job.")
        actual_mongo_uri_to_use = mongo_connection_uri_from_env
    else:
        print(f"Constructing MongoDB URI from individual MONGO_HOST, PORT, etc. for Spark job.")
        mongo_host_local = os.getenv('MONGO_HOST_INTERNAL', 'mongodb')
        mongo_port_local = os.getenv('MONGO_PORT', '27017')
        mongo_user_local = os.getenv('MONGO_USERNAME', '')
        mongo_pass_local = os.getenv('MONGO_PASSWORD', '')
        # For constructed URI, the database name is often part of it or default connection db
        if mongo_user_local and mongo_pass_local:
            actual_mongo_uri_to_use = f"mongodb://{mongo_user_local}:{mongo_pass_local}@{mongo_host_local}:{mongo_port_local}/{mongo_db_target}?authSource=admin"
        else:
            actual_mongo_uri_to_use = f"mongodb://{mongo_host_local}:{mongo_port_local}/{mongo_db_target}"

    # Pipeline
    raw_spark_df = read_data_from_hdfs(spark, hdfs_base_uri, hdfs_full_path)
    
    if raw_spark_df:
        raw_spark_df.show(5)
        transformed_df = transform_data(raw_spark_df)
        if transformed_df:
            # Pass the determined URI, target DB, and target collection to save function
            save_data_to_mongodb(transformed_df, actual_mongo_uri_to_use, mongo_db_target, mongo_collection_target)
    else:
        print("Failed to read data from HDFS. Terminating job.")

    spark.stop()









