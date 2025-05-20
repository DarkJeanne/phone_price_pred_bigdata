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
    return SparkSession.builder \
        .appName("SmartphoneBatchTransformation") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

def read_data_from_hdfs(spark, hdfs_uri, file_path):
    """Reads data from HDFS into a Spark DataFrame."""
    full_path = f"{hdfs_uri}{file_path}"
    print(f"Reading data from HDFS: {full_path}")
    try:
        df = spark.read.csv(full_path, header=True, inferSchema=True)
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

    expected_cols = ["brand", "screen_size", "ram", "rom", "sim_type", "battary", "cams", "id", "model_name", "sale_percentage", "product_rating", "seller_name", "seller_score", "seller_followers", "Reviews"]
    for c in expected_cols:
        if c not in spark_df.columns:
            print(f"Warning: Column '{c}' not found in input DataFrame. It will be missing in output or cause errors.")

    essential_cols_for_transform = ["brand", "screen_size", "ram", "rom", "sim_type", "battary"]
    spark_df = spark_df.dropna(subset=essential_cols_for_transform)
    print(f"Count after dropping NA in essential columns: {spark_df.count()}")

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

    map_brand_udf = udf(lambda brand: map_brand_to_numeric(brand) if brand else None, IntegerType())
    map_sim_type_udf = udf(lambda sim_type: map_sim_type_to_numeric(sim_type) if sim_type else None, IntegerType())

    if "brand" in spark_df.columns:
        spark_df = spark_df.withColumn("brand_numeric", map_brand_udf(col("brand")))
    if "sim_type" in spark_df.columns:
        spark_df = spark_df.withColumn("sim_type_numeric", map_sim_type_udf(col("sim_type")))
    
    print("Schema after type conversion and UDF mapping:")
    spark_df.printSchema()

    model_path = None
    possible_paths = [
        "xgb_model.pkl",
        "/opt/spark/apps/ml_ops/xgb_model.pkl",
        "/opt/spark/apps/batch_layer/xgb_model.pkl",
        os.path.join(os.path.dirname(__file__), "xgb_model.pkl"),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "ML_operations", "xgb_model.pkl"),
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            model_path = path
            print(f"Found model at: {model_path}")
            break
    
    if model_path is None:
        print(f"Error: Model file not found. Searched paths: {possible_paths}")
        return None
        
    try:
        with open(model_path, "rb") as f:
            model = pickle.load(f)
            print(f"Successfully loaded model from {model_path}")
    except FileNotFoundError:
        print(f"Error: Model file '{model_path}' not found.")
        return None
    except Exception as e:
        print(f"Error loading model: {e}")
        return None

    feature_cols = ["brand_numeric", "screen_size", "ram", "rom", "sim_type_numeric", "battary"]
    
    missing_feature_cols = [fc for fc in feature_cols if fc not in spark_df.columns]
    if missing_feature_cols:
        print(f"Error: Missing feature columns for prediction: {missing_feature_cols}")
        return None
        
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    assembled_df = assembler.transform(spark_df)
    
    print("Schema after feature assembly:")
    assembled_df.printSchema()
    print(f"Count before prediction: {assembled_df.count()}")

    pandas_df_for_prediction = assembled_df.select(feature_cols).toPandas()
    
    if pandas_df_for_prediction.empty:
        print("No data available for prediction after transformations.")
        final_df_with_price = assembled_df.withColumn("predicted_price", lit(None).cast(FloatType()))
    else:
        print(f"Pandas DataFrame for prediction shape: {pandas_df_for_prediction.shape}")
        
        column_mapping = {
            'brand_numeric': 'brand',
            'sim_type_numeric': 'sim_type'
        }
        
        prediction_df = pandas_df_for_prediction.rename(columns=column_mapping)
        
        model_feature_cols = ['brand', 'screen_size', 'ram', 'rom', 'sim_type', 'battary']
        print(f"Using features with correct names: {model_feature_cols}")
        
        predictions = model.predict(prediction_df[model_feature_cols])
        
        prediction_df['predicted_price'] = predictions
        
        assembled_pandas_df = assembled_df.toPandas()
        
        if len(assembled_pandas_df) == len(predictions):
            assembled_pandas_df["predicted_price"] = predictions
            final_df_with_price = spark.createDataFrame(assembled_pandas_df)
        else:
            print(f"Warning: Length mismatch between data ({len(assembled_pandas_df)}) and predictions ({len(predictions)}). Price prediction will be skipped.")
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

    # Drop the features column if it exists, as it's not BSON compatible
    df_to_save = df
    if "features" in df.columns:
        print("Dropping 'features' column before saving to MongoDB.")
        df_to_save = df.drop("features")

    print(f"Saving data to MongoDB. URI: [SENSITIVE], DB: {db_name_to_use}, Collection: {collection_name_to_use}")
    try:
        df_to_save.write.format("mongo") \
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

    hdfs_namenode_host = os.getenv('HDFS_NAMENODE_HOST_INTERNAL', 'namenode')
    hdfs_namenode_port = os.getenv('HDFS_NAMENODE_RPC_PORT', '9000')
    hdfs_base_uri = f"hdfs://{hdfs_namenode_host}:{hdfs_namenode_port}"
    
    hdfs_batch_path = os.getenv('HDFS_BATCH_LAYER_PATH', '/batch-layer')
    hdfs_raw_file = os.getenv('HDFS_RAW_DATA_FILE', 'raw_data.csv')
    hdfs_full_path = f"{hdfs_batch_path}/{hdfs_raw_file}"

    mongo_connection_uri_from_env = os.getenv('MONGO_CONNECTION_URI')
    mongo_db_target = os.getenv('MONGO_DB_NAME', 'phone_price_pred')
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
        if mongo_user_local and mongo_pass_local:
            actual_mongo_uri_to_use = f"mongodb://{mongo_user_local}:{mongo_pass_local}@{mongo_host_local}:{mongo_port_local}/{mongo_db_target}?authSource=admin"
        else:
            actual_mongo_uri_to_use = f"mongodb://{mongo_host_local}:{mongo_port_local}/{mongo_db_target}"

    raw_spark_df = read_data_from_hdfs(spark, hdfs_base_uri, hdfs_full_path)
    
    if raw_spark_df:
        raw_spark_df.show(5)
        transformed_df = transform_data(raw_spark_df)
        if transformed_df:
            save_data_to_mongodb(transformed_df, actual_mongo_uri_to_use, mongo_db_target, mongo_collection_target)
    else:
        print("Failed to read data from HDFS. Terminating job.")

    spark.stop()









