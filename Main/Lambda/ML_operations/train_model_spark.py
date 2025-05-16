import pickle
import re # For cleaning strings
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType, StringType, IntegerType
from pyspark.ml.feature import VectorAssembler
from xgboost.spark import XGBoostRegressor
import os

# --- Configuration & Mappings (mirrored from transform.py) ---
# We will use Company Name directly, then map to numeric if we stick to the old approach
# or use StringIndexer for Spark ML if preferred (more robust for new brands).
# For now, sticking to explicit mapping to align with transform.py
BRAND_TO_NUMERIC = {
    'Maxfone': 1, 'Infinix': 2, 'Freeyond': 3, 'XIAOMI': 4,
    'Tecno': 5, 'Oppo': 6, 'Nokia': 7, 'Samsung': 8,
    'Huawei': 9, 'Vivo': 10, 'Realme': 11, 'Sowhat': 12,
    'Apple': 13 
    # Add more brands from the new dataset. If a brand is not here, it will get DEFAULT_BRAND_NUMERIC
}
DEFAULT_BRAND_NUMERIC = 100 # A new default for unmapped brands in this dataset

# SIM Type is not in the new dataset, so we will remove it from features for now.
# If SIM type becomes available, it can be re-added.

# --- UDFs for Spark DataFrame transformations ---
@udf(returnType=IntegerType())
def map_brand_to_numeric_func(brand):
    return BRAND_TO_NUMERIC.get(brand, DEFAULT_BRAND_NUMERIC)

@udf(returnType=FloatType())
def clean_ram_func(ram_str):
    if ram_str is None: return None
    match = re.search(r'(\d+)(?:GB)?'i, str(ram_str))
    return float(match.group(1)) if match else None

@udf(returnType=FloatType())
def clean_battery_func(battery_str):
    if battery_str is None: return None
    match = re.search(r'([\d,]+)(?:mAh)?'i, str(battery_str))
    return float(match.group(1).replace(',', '')) if match else None

@udf(returnType=FloatType())
def clean_screen_size_func(screen_str):
    if screen_str is None: return None
    match = re.search(r'(\d+\.?\d*)\s*(?:inches|inch)?'i, str(screen_str))
    return float(match.group(1)) if match else None

@udf(returnType=FloatType())
def clean_price_usd_func(price_str):
    if price_str is None: return None
    # Handles "USD 799", "799", etc.
    match = re.search(r'(?:USD\s*)?([\d,]+\.?\d*)'i, str(price_str))
    return float(match.group(1).replace(',', '')) if match else None

def train_model():
    spark = SparkSession.builder \
        .appName("XGBoostTrainingMobilePrice") \
        .master("local[*]") \
        .config("spark.sql.legacy.setCommandReplaced", "true") \
        .getOrCreate()

    data_path = "mobiles_dataset_2025.csv" # Updated dataset path
    
    print(f"Loading data from {data_path}")
    try:
        df = spark.read.csv(data_path, header=True, inferSchema=False) # inferSchema=False to apply custom cleaning
    except Exception as e:
        print(f"Error loading CSV: {e}")
        spark.stop()
        return

    print("Original DataFrame schema:")
    df.printSchema()
    df.show(3, truncate=False)

    # Define column names from the CSV
    COMPANY_NAME_COL = "Company Name"
    RAM_COL_RAW = "RAM"
    BATTERY_COL_RAW = "Battery Capacity"
    SCREEN_SIZE_COL_RAW = "Screen Size"
    # Choose one price column as the target. Using USA price as example.
    PRICE_COL_RAW = "Launched Price (USA)" 

    required_raw_cols = [COMPANY_NAME_COL, RAM_COL_RAW, BATTERY_COL_RAW, SCREEN_SIZE_COL_RAW, PRICE_COL_RAW]
    missing_cols = [col_name for col_name in required_raw_cols if col_name not in df.columns]
    if missing_cols:
        print(f"Error: Missing required columns in the CSV: {missing_cols}")
        print(f"Available columns: {df.columns}")
        spark.stop()
        return

    print("Preprocessing data...")
    df_processed = df.withColumn("brand_numeric", map_brand_to_numeric_func(col(COMPANY_NAME_COL))) \
                     .withColumn("ram_float", clean_ram_func(col(RAM_COL_RAW))) \
                     .withColumn("battery_float", clean_battery_func(col(BATTERY_COL_RAW))) \
                     .withColumn("screen_size_float", clean_screen_size_func(col(SCREEN_SIZE_COL_RAW))) \
                     .withColumn("label", clean_price_usd_func(col(PRICE_COL_RAW)))
    
    # Features for the model (SIM type removed as it's not in this dataset)
    feature_cols_for_assembler = [
        "brand_numeric", "screen_size_float", "ram_float", "battery_float"
        # Removed "storage_float" as "Storage" or "ROM" is not in the provided CSV header snippet.
        # If you have a storage column (e.g., from 'Mobile Weight' if it implies storage, or another col),
        # add it here and create a cleaning UDF for it.
    ]
    
    # Check if all assembler input columns are present after UDFs
    missing_feature_cols = [c for c in feature_cols_for_assembler if c not in df_processed.columns]
    if missing_feature_cols:
        print(f"Error: Columns needed for VectorAssembler are missing: {missing_feature_cols}")
        df_processed.printSchema()
        spark.stop()
        return

    df_processed = df_processed.select(feature_cols_for_assembler + ["label"]).dropna()
    
    if df_processed.count() == 0:
        print("Error: No data remaining after preprocessing and dropping nulls. Check data quality, UDFs, and column names.")
        df_processed.printSchema()
        spark.stop()
        return

    print("Assembling features...")
    assembler = VectorAssembler(
        inputCols=feature_cols_for_assembler,
        outputCol="features",
        handleInvalid="skip" # Changed from "error" to "skip" for robustness, or use "keep" / "error"
    )
    df_assembled = assembler.transform(df_processed)

    if df_assembled.count() == 0:
        print("Error: No data remaining after VectorAssembler. Check feature columns and handleInvalid strategy.")
        spark.stop()
        return
        
    final_df = df_assembled.select("features", "label")
    print("Final DataFrame for training (sample):")
    final_df.show(5, truncate=False)
    
    (train_df, test_df) = final_df.randomSplit([0.8, 0.2], seed=42)

    print("Training XGBoostRegressor model...")
    xgboost_regressor = XGBoostRegressor(
        featuresCol="features",
        labelCol="label",
        maxDepth=5 
        # Add other XGBoost parameters as needed
    )

    model = xgboost_regressor.fit(train_df)

    try:
        booster = model.get_booster()
    except AttributeError as e:
        print(f"Could not get booster directly via model.get_booster(): {e}")
        print("This might require accessing an internal attribute or a different method depending on XGBoost version.")
        spark.stop()
        return

    model_path = "xgb_model.pkl"
    print(f"Saving model to {model_path}...")
    with open(model_path, "wb") as f:
        pickle.dump(booster, f)
    
    print(f"Model saved successfully to {model_path}")

    # Optional: Evaluation
    # print("Evaluating model on test data...")
    # predictions = model.transform(test_df)
    # from pyspark.ml.evaluation import RegressionEvaluator
    # evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    # rmse = evaluator_rmse.evaluate(predictions)
    # print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")
    # evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
    # r2 = evaluator_r2.evaluate(predictions)
    # print(f"R^2 on test data = {r2}")

    spark.stop()

if __name__ == "__main__":
    if not os.path.exists("."):
        os.makedirs(".")
    train_model() 