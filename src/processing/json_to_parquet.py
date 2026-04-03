import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, BooleanType, DoubleType, TimestampType,
)

# Config 
sys.path.insert(0, os.path.dirname(__file__))
from spark_session import get_spark
from dotenv import load_dotenv
load_dotenv(dotenv_path="config/keys.env")
BUCKET      = os.getenv("S3_BUCKET_NAME")
RAW_BASE    = f"s3a://{BUCKET}/raw"
PROCESSED   = f"s3a://{BUCKET}/processed"

# Schema for the game clock
CLOCK_SCHEMA = StructType([
    StructField("minutes", IntegerType(), nullable=True),
    StructField("seconds", IntegerType(), nullable=True),
])

# Explicit schema definition for play by play data
PLAYS_SCHEMA = StructType([
    StructField("gameId",             LongType(),      nullable=False),
    StructField("driveId",            StringType(),    nullable=True),
    StructField("id",                 StringType(),    nullable=False),
    StructField("driveNumber",        IntegerType(),   nullable=True),
    StructField("playNumber",         IntegerType(),   nullable=True),
    StructField("offense",            StringType(),    nullable=True),
    StructField("offenseConference",  StringType(),    nullable=True),
    StructField("offenseScore",       IntegerType(),   nullable=True),
    StructField("defense",            StringType(),    nullable=True),
    StructField("defenseConference",  StringType(),    nullable=True),
    StructField("defenseScore",       IntegerType(),   nullable=True),
    StructField("home",               StringType(),    nullable=True),
    StructField("away",               StringType(),    nullable=True),
    StructField("period",             IntegerType(),   nullable=True),
    StructField("clock",              CLOCK_SCHEMA,    nullable=True),
    StructField("offenseTimeouts",    IntegerType(),   nullable=True),
    StructField("defenseTimeouts",    IntegerType(),   nullable=True),
    StructField("yardline",           IntegerType(),   nullable=True),
    StructField("yardsToGoal",        IntegerType(),   nullable=True),
    StructField("down",               IntegerType(),   nullable=True),
    StructField("distance",           IntegerType(),   nullable=True),   
    StructField("yardsGained",        IntegerType(),   nullable=True),
    StructField("scoring",            BooleanType(),   nullable=True),
    StructField("playType",           StringType(),    nullable=True),
    StructField("playText",           StringType(),    nullable=True),
    StructField("ppa",                DoubleType(),    nullable=True),  
    StructField("wallclock",          StringType(),    nullable=True),
])

def convert(spark: SparkSession, season: str = "*") -> int:
    raw_glob = f"{RAW_BASE}/year={season}/week=*/*.json"
    print(f"\nReading: {raw_glob}")

    # Read raw JSON data using the schema
    df = (
        spark.read
        .option("basePath", RAW_BASE)       
        .option("mode", "PERMISSIVE")       
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(PLAYS_SCHEMA)
        .json(raw_glob)
    )
    # Standardize column names and cast string times to Timestamps
    df = (
        df
        .withColumnRenamed("year", "season")
        .withColumn("wallclock", F.to_timestamp("wallclock"))
    )

    row_count = df.count()
    print(f"Rows read: {row_count:,}")
    # Identify and count records that failed schema check
    corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count() if "_corrupt_record" in df.columns else 0
    if corrupt:
        print(f"WARNING: {corrupt:,} corrupt/unparseable records skipped")

    if "_corrupt_record" in df.columns:
        df = df.drop("_corrupt_record")
    # Write cleaned dataframe back to S3 as Parquet files
    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("season", "week")
        .parquet(PROCESSED)
    )

    print(f"Written to: {PROCESSED}  (partitioned by season/week, Snappy)")
    return row_count

def verify(spark: SparkSession):
    # Load the Parquet files
    print(f"\nVerifying: {PROCESSED}")
    df = spark.read.parquet(PROCESSED)

    print("\nSchema:")
    df.printSchema()

    total = df.count()
    print(f"Total rows: {total:,}")

    # Display an aggregated count of plays to verify 
    print("\nRow counts by season and week:")
    (
        df.groupBy("season", "week")
        .count()
        .orderBy("season", "week")
        .show(50, truncate=False)
    )
    # Sample 
    print("Sample plays (5 rows):")
    df.select("season", "week", "gameId", "offense", "defense",
              "playType", "yardsGained", "ppa").show(5, truncate=False)

if __name__ == "__main__":
    # Checks if bucket exists 
    if not BUCKET:
        sys.exit("ERROR: S3_BUCKET_NAME not set. Create config/keys.env from config/example.env.")

    # Does all seasons
    season = "*"

    spark = get_spark("JSON_to_Parquet")
    spark.sparkContext.setLogLevel("WARN")

    convert(spark, season)
    verify(spark)

    spark.stop()
    print("\nStage 2 complete.")
