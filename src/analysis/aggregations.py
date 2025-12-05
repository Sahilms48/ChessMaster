import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, countDistinct, when
from config.settings import PROCESSED_DATA_DIR, SPARK_APP_NAME, SPARK_MASTER

def create_spark_session():
    return SparkSession.builder \
        .appName(f"{SPARK_APP_NAME}_Aggregator") \
        .master(SPARK_MASTER) \
        .getOrCreate()

def run_aggregations():
    spark = create_spark_session()
    
    games_path = os.path.join(PROCESSED_DATA_DIR, "games")
    agg_base_path = os.path.join(PROCESSED_DATA_DIR, "aggregations")
    
    if not os.path.exists(games_path):
        print(f"No data directory found at {games_path}")
        spark.stop()
        return

    try:
        print("Reading games data...")
        df = spark.read.parquet(games_path)
        
        if df.rdd.isEmpty():
            print("No data in games parquet files yet.")
            spark.stop()
            return
            
        df.cache()
        print(f"Total games: {df.count()}")
        
        # 1. Opening Stats (By ECO instead of Name)
        print("Computing Top ECO Codes...")
        # We group by ECO and Game Type
        opening_stats = df.groupBy("game_type", "eco", "opening_family") \
            .agg(
                count("*").alias("count"),
                avg("num_moves").alias("avg_moves")
            ) \
            .orderBy(col("count").desc())
            
        opening_stats.write.mode("overwrite").parquet(os.path.join(agg_base_path, "opening_stats"))
        
        # 2. Game Length Stats (Raw data for histogram)
        print("Computing Game Length Stats...")
        length_stats = df.select("game_type", "num_moves")
        length_stats.write.mode("overwrite").parquet(os.path.join(agg_base_path, "game_length_stats"))

        # 3. Game Phase Distribution (For Endgame Rate)
        print("Computing Game Phases...")
        phase_stats = df.groupBy("game_type", "game_phase") \
            .agg(count("*").alias("count"))
        phase_stats.write.mode("overwrite").parquet(os.path.join(agg_base_path, "phase_stats"))
        
        # 4. Comparison Metrics (Overview)
        print("Computing Comparison Metrics...")
        comparison = df.groupBy("game_type") \
            .agg(
                count("*").alias("total_games"),
                avg("num_moves").alias("avg_moves"),
                countDistinct("eco").alias("unique_ecos")
            )
        comparison.write.mode("overwrite").parquet(os.path.join(agg_base_path, "comparison_metrics"))
        
        print("Aggregations completed.")
        
    except Exception as e:
        print(f"Error during aggregation: {e}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_aggregations()
