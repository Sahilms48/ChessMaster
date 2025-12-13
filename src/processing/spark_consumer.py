import os
import sys

# Add project root to Python path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

# ============================================
# CRITICAL: Unset SPARK_HOME to use venv's PySpark only
# This prevents conflict with system Spark installation
# ============================================
if 'SPARK_HOME' in os.environ:
    del os.environ['SPARK_HOME']

# Fix for Windows - force localhost binding
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'

# Set Python path for Spark workers - use current Python executable
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path
print(f"Using Python: {python_path}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_CASUAL,
    TOPIC_PROFESSIONAL,
    PROCESSED_DATA_DIR,
    SPARK_APP_NAME,
    SPARK_MASTER
)
from src.processing.feature_extraction import add_features

def create_spark_session():
    """Create Spark session with Kafka support and Python UDF configuration"""
    return SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.executor.pyspark.memory", "512m") \
        .getOrCreate()

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("Spark session created successfully!")
    print(f"Spark version: {spark.version}")
    
    # Schema for JSON data from Kafka
    schema = StructType([
        StructField("source", StringType()),
        StructField("game_type", StringType()),
        StructField("event", StringType()),
        StructField("white", StringType()),
        StructField("black", StringType()),
        StructField("result", StringType()),
        StructField("eco", StringType()),
        StructField("opening", StringType()),
        StructField("time_control", StringType()),
        StructField("moves", StringType()),
        StructField("num_moves", IntegerType()),
        StructField("date", StringType())
    ])
    
    # Subscribe to both topics
    topics = f"{TOPIC_CASUAL},{TOPIC_PROFESSIONAL}"
    
    # Read Stream from Kafka
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ",".join(KAFKA_BOOTSTRAP_SERVERS)) \
            .option("subscribe", topics) \
            .option("startingOffsets", "earliest") \
            .load()
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return
        
    # Parse JSON from Kafka messages
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Feature Extraction using native Spark SQL functions (no Python workers needed)
    processed_df = add_features(parsed_df)
    
    # Output paths
    checkpoint_dir = os.path.join(PROCESSED_DATA_DIR, "checkpoints")
    output_path = os.path.join(PROCESSED_DATA_DIR, "games")
    
    # Ensure output directories exist
    os.makedirs(checkpoint_dir, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)
    
    # Write stream to Parquet
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime='10 seconds') \
        .start()
        
    print(f"Streaming to {output_path}...")
    print("Using native Spark SQL functions for feature extraction")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping stream...")
        query.stop()

if __name__ == "__main__":
    process_stream()
