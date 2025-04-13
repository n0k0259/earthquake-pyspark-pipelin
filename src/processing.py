# src/processing.py
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Configuration
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed/earthquakes" # Output directory for Parquet files

def get_latest_raw_file(directory):
    """Finds the most recently created JSON file in the directory."""
    list_of_files = glob.glob(os.path.join(directory, '*.json'))
    if not list_of_files:
        return None
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

def create_spark_session(app_name="EarthquakeProcessing"):
    """Creates and returns a SparkSession."""
    # Running locally using all available cores
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
    .getOrCreate()
    print(f"SparkSession created. Spark version: {spark.version}")
    return spark

    print(f"SparkSession created. Spark version: {spark.version}")
    return spark

def process_earthquake_data(spark, input_path, output_path):
    """Reads raw JSON, processes it, and writes to Parquet."""
    if not input_path:
        print("Error: No input JSON file found.")
        return

    print(f"Processing file: {input_path}")
    try:
        # Read the entire JSON structure (GeoJSON format)
        # Since we need the 'features' array, we'll read it and then explode
        # Spark might struggle inferring schema for complex nested JSON sometimes.
        # It's often better to read as text and parse JSON string, or provide schema.
        # Let's try inferSchema first for simplicity here.
        raw_df = spark.read.option("multiline", "true").json(input_path)
        print("Schema inferred by Spark:")
        raw_df.printSchema()

        # Check if 'features' array exists
        if 'features' not in raw_df.columns:
             print("Error: 'features' field not found in the JSON root.")
             raw_df.show(5, truncate=False) # Show what was loaded
             return

        # Explode the 'features' array to get one row per earthquake
        # Select only the features column before exploding for efficiency
        features_df = raw_df.select(F.explode("features").alias("feature"))
        # features_df.printSchema()
        # features_df.show(5, truncate=False)


        # Extract relevant fields from the nested structure
        processed_df = features_df.select(
            F.col("feature.id").alias("earthquake_id"),
            F.col("feature.properties.mag").alias("magnitude"),
            F.col("feature.properties.place").alias("place"),
            # Time is often in milliseconds since epoch, convert to timestamp
            (F.col("feature.properties.time") / 1000).cast(TimestampType()).alias("timestamp_utc"),
            F.col("feature.properties.tz").alias("timezone_offset"), # Timezone offset from UTC in minutes
            F.col("feature.properties.url").alias("details_url"),
            F.col("feature.properties.status").alias("status"),
            F.col("feature.properties.tsunami").alias("tsunami_alert"), # 0 or 1
            F.col("feature.properties.sig").alias("significance"), # 0-1000
            F.col("feature.properties.net").alias("network_id"),
            F.col("feature.properties.code").alias("event_code"),
            F.col("feature.properties.magType").alias("magnitude_type"),
            F.col("feature.properties.type").alias("event_type"), # e.g., 'earthquake'
            F.col("feature.geometry.coordinates").getItem(0).alias("longitude"),
            F.col("feature.geometry.coordinates").getItem(1).alias("latitude"),
            F.col("feature.geometry.coordinates").getItem(2).alias("depth_km")
        )

        # Add processing timestamp
        processed_df = processed_df.withColumn("processing_time_utc", F.current_timestamp())

        print("Processed DataFrame Schema:")
        processed_df.printSchema()
        print("Sample of processed data:")
        processed_df.show(10, truncate=False)

        # Write the processed data to Parquet format
        # Using overwrite mode for simplicity in this local demo
        # Partitioning can be useful for larger datasets (e.g., by date)
        print(f"Writing processed data to: {output_path}")
        processed_df.write \
            .mode("overwrite") \
            .parquet(output_path)

        print("Processing completed successfully.")

    except Exception as e:
        print(f"An error occurred during Spark processing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    spark = None # Ensure spark context is stopped even if it fails early
    try:
        latest_file = get_latest_raw_file(RAW_DATA_DIR)
        if latest_file:
            spark = create_spark_session()
            process_earthquake_data(spark, latest_file, PROCESSED_DATA_DIR)
        else:
            print("No raw data file found to process.")
    finally:
        if spark:
            print("Stopping SparkSession.")
            spark.stop()