import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
sys.path.insert(0, os.path.dirname(__file__))
from spark_session import get_spark
from dotenv import load_dotenv

# Config
load_dotenv(dotenv_path="config/keys.env")
BUCKET    = os.getenv("S3_BUCKET_NAME")
PROCESSED = f"s3a://{BUCKET}/processed"
STATS_OUT = f"s3a://{BUCKET}/stats/team_game_stats.parquet"
BIG_PLAY_YARDS = 20


def map_plays(df: DataFrame) -> DataFrame:
    # Filter only for actual plays from scrimmage
    scrimmage = df.filter(F.col("down").isNotNull() & F.col("distance").isNotNull())

    return scrimmage.select(
        "gameId", # Unique game id
        "season",
        "week",
        F.col("offense").alias("team"),
        F.col("defense").alias("opponent"),
        (F.col("home") == F.col("offense")).alias("is_home"),

        "yardsGained",
        "ppa",
        "scoring",
        "down",
        "distance",

        # Calculate play success based on down and percentage of distance gained
        F.when(
            (F.col("down") == 1) & (F.col("yardsGained") >= F.col("distance") * 0.5), True
        ).when(
            (F.col("down") == 2) & (F.col("yardsGained") >= F.col("distance") * 0.7), True
        ).when(
            (F.col("down").isin(3, 4)) & (F.col("yardsGained") >= F.col("distance")), True
        ).otherwise(False).alias("is_success"),

        (F.col("down") == 3).alias("is_third_down"),
        (
            (F.col("down") == 3) & (F.col("yardsGained") >= F.col("distance"))
        ).alias("is_third_down_conversion"),

        (F.col("yardsGained") >= BIG_PLAY_YARDS).alias("is_big_play"),
    )

# Group by game and team details to calculate game level stats
def reduce_to_game_stats(mapped: DataFrame) -> DataFrame:
    agg = mapped.groupBy(
        "gameId", "season", "week", "team", "opponent", "is_home"
    ).agg(
        F.count("*").alias("plays"),
        F.sum("yardsGained").alias("total_yards"),
        F.sum(F.col("is_success").cast("int")).alias("successful_plays"),
        F.sum(F.col("is_third_down").cast("int")).alias("third_down_attempts"),
        F.sum(F.col("is_third_down_conversion").cast("int")).alias("third_down_conv"),
        F.sum(F.col("is_big_play").cast("int")).alias("big_plays"),
        F.sum(F.col("scoring").cast("int")).alias("scoring_plays"),
        F.round(F.avg("ppa"), 4).alias("avg_ppa"),
    )
    
    return agg.withColumns({
        "yards_per_play": F.round(F.col("total_yards") / F.col("plays"), 3),
        "success_rate": F.round(F.col("successful_plays") / F.col("plays"), 3),
        "third_down_pct": F.round(
            F.when(F.col("third_down_attempts") > 0,
                   F.col("third_down_conv") / F.col("third_down_attempts")
            ).otherwise(None), 3
        ),
        "big_play_pct": F.round(F.col("big_plays") / F.col("plays"), 3),
    }).select(
        "gameId", "season", "week", "team", "opponent", "is_home",
        "plays", "total_yards", "yards_per_play",
        "success_rate",
        "third_down_attempts", "third_down_conv", "third_down_pct",
        "big_plays", "big_play_pct",
        "scoring_plays", "avg_ppa",
    ).orderBy("season", "week", "team")

if __name__ == "__main__":
    # Makes sure bucket exists 
    if not BUCKET:
        sys.exit("ERROR: S3_BUCKET_NAME not set. Create config/keys.env from config/example.env.")

    spark = get_spark("MapReduce_Stats")
    spark.sparkContext.setLogLevel("WARN") # Reduce console log spam

    print(f"\nReading processed Parquet from: {PROCESSED}")
    plays_df = spark.read.parquet(PROCESSED) # Load processed play by play data

    print("MAP phase: tagging scrimmage plays")
    mapped = map_plays(plays_df) # Apply map logic to raw plays
    print(f"Scrimmage plays: {mapped.count():,}  (non-scrimmage plays excluded)")

    print("REDUCE phase: aggregating to game stats")
    stats = reduce_to_game_stats(mapped) # Apply aggregation logic

    row_count = stats.count()
    print(f"Team-game rows: {row_count:,}")

    print(f"\nWriting to: {STATS_OUT}")
    (
        stats.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(STATS_OUT)
    )

    print("\nVerification: reading output back")
    out = spark.read.parquet(STATS_OUT)
    out.printSchema()
    # Display a small sample
    print("\nSample (10 rows):")
    out.show(10, truncate=False)



    # Calculate and display averages
    print("\nLeague-wide averages across all team-games:")
    out.select(
        F.round(F.avg("yards_per_play"),  2).alias("avg_ypp"),
        F.round(F.avg("success_rate"),    3).alias("avg_success_rate"),
        F.round(F.avg("third_down_pct"),  3).alias("avg_3rd_pct"),
        F.round(F.avg("big_play_pct"),    3).alias("avg_big_play_pct"),
        F.round(F.avg("avg_ppa"),         4).alias("avg_ppa"),
    ).show()

    spark.stop()
    print("\nStage 3 complete.")
