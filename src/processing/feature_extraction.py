from pyspark.sql.functions import col, when, substring, lit

def add_features(df):
    """
    Add opening_family and game_phase columns using native Spark SQL functions.
    This avoids Python UDFs which have issues on Windows.
    """
    # Opening family: first character of ECO code (A-E)
    df = df.withColumn(
        "opening_family",
        when(col("eco").isNull() | (col("eco") == ""), lit("Unknown"))
        .otherwise(substring(col("eco"), 1, 1))
    )
    
    # Game phase based on number of moves
    df = df.withColumn(
        "game_phase",
        when(col("num_moves").isNull(), lit("Unknown"))
        .when(col("num_moves") < 15, lit("Opening"))
        .when(col("num_moves") < 40, lit("Middlegame"))
        .otherwise(lit("Endgame"))
    )
    
    return df
