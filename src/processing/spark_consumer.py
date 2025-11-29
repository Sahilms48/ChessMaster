import os
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
from src.processing.feature_extraction import get_opening_family_udf, get_game_phase_udf

def create_spark_session():
    # Note: You might need to adjust the spark-sql-kafka version based on your PySpark version
    return SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Schema for JSON data
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
    
    # Subscribe to multiple topics
    topics = f"{TOPIC_CASUAL},{TOPIC_PROFESSIONAL}"
    
    # Read Stream
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
        
    # Parse JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Feature Extraction
    processed_df = parsed_df \
        .withColumn("opening_family", get_opening_family_udf(col("eco"))) \
        .withColumn("game_phase", get_game_phase_udf(col("moves"), col("num_moves")))
    
    # Write to Parquet (Append mode)
    # Note: Structured Streaming to Parquet requires checkpointing
    checkpoint_dir = os.path.join(PROCESSED_DATA_DIR, "checkpoints")
    output_path = os.path.join(PROCESSED_DATA_DIR, "games")
    
    # Ensure output directories exist
    os.makedirs(checkpoint_dir, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)
    
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime='10 seconds') \
        .start()
        
    print(f"Streaming to {output_path}...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping stream...")
        query.stop()

if __name__ == "__main__":
    process_stream()

