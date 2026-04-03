import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, BooleanType, DoubleType, TimestampType,
)

sys.path.insert(0, os.path.dirname(__file__))
from spark_session import get_spark

from dotenv import load_dotenv
load_dotenv(dotenv_path="config/keys.env")

BUCKET      = os.getenv("S3_BUCKET_NAME")
RAW_BASE    = f"s3a://{BUCKET}/raw"
PROCESSED   = f"s3a://{BUCKET}/processed"


CLOCK_SCHEMA = StructType([
    StructField("minutes", IntegerType(), nullable=True),
    StructField("seconds", IntegerType(), nullable=True),
])

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
    StructField("down",               IntegerType(),   nullable=True),   # null for special teams
    StructField("distance",           IntegerType(),   nullable=True),   # null for special teams
    StructField("yardsGained",        IntegerType(),   nullable=True),
    StructField("scoring",            BooleanType(),   nullable=True),
    StructField("playType",           StringType(),    nullable=True),
    StructField("playText",           StringType(),    nullable=True),
    StructField("ppa",                DoubleType(),    nullable=True),   # null for non-scrimmage plays
    StructField("wallclock",          StringType(),    nullable=True),
])

def convert(spark: SparkSession, season: str = "*") -> int:
    raw_glob = f"{RAW_BASE}/year={season}/week=*/*.json"
    print(f"\nReading: {raw_glob}")

    df = (
        spark.read
        .option("basePath", RAW_BASE)       
        .option("mode", "PERMISSIVE")       
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(PLAYS_SCHEMA)
        .json(raw_glob)
    )

    df = (
        df
        .withColumnRenamed("year", "season")
        .withColumn("wallclock", F.to_timestamp("wallclock"))
    )

    row_count = df.count()
    print(f"Rows read: {row_count:,}")

    corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count() if "_corrupt_record" in df.columns else 0
    if corrupt:
        print(f"WARNING: {corrupt:,} corrupt/unparseable records skipped")

    if "_corrupt_record" in df.columns:
        df = df.drop("_corrupt_record")

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
    print(f"\nVerifying: {PROCESSED}")
    df = spark.read.parquet(PROCESSED)

    print("\nSchema:")
    df.printSchema()

    total = df.count()
    print(f"Total rows: {total:,}")

    print("\nRow counts by season and week:")
    (
        df.groupBy("season", "week")
        .count()
        .orderBy("season", "week")
        .show(50, truncate=False)
    )

    print("Sample plays (5 rows):")
    df.select("season", "week", "gameId", "offense", "defense",
              "playType", "yardsGained", "ppa").show(5, truncate=False)

if __name__ == "__main__":
    if not BUCKET:
        sys.exit("ERROR: S3_BUCKET_NAME not set. Create config/keys.env from config/example.env.")

    season = sys.argv[1] if len(sys.argv) > 1 else "*"

    spark = get_spark("JSON_to_Parquet")
    spark.sparkContext.setLogLevel("WARN")

    convert(spark, season)
    verify(spark)

    spark.stop()
    print("\nStage 2 complete.")
