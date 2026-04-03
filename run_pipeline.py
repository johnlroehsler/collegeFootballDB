import logging
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv(dotenv_path="config/keys.env")

# Configure the root logger to output to both a local file and the console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
# Initialize a specific logger for this module
log = logging.getLogger(__name__)

# Data structure to track the execution metrics of each pipeline stage
@dataclass
class StageResult:
    name: str
    status: str = "PENDING"      
    duration_s: float = 0.0
    rows: Optional[int] = None
    error: Optional[str] = None

# Helper function to execute a pipeline stage and record its success or failure
def _run_stage(name: str, fn, results: list) -> bool:
    result = StageResult(name=name)
    results.append(result)
    log.info("=" * 60)
    log.info(f"Starting stage: {name}")
    t0 = time.time()
    try:
        rows = fn()
        result.status = "OK"
        result.rows = rows
        log.info(f"Stage {name} completed OK  ({time.time() - t0:.1f}s)"
                 + (f"  rows={rows:,}" if rows is not None else ""))
        return True
    except Exception as exc:
        result.status = "FAILED"
        result.error = str(exc)
        result.duration_s = time.time() - t0
        log.error(f"Stage {name} FAILED after {result.duration_s:.1f}s: {exc}", exc_info=True)
        return False
    finally:
        result.duration_s = time.time() - t0

# Helper function to log and record a stage that was intentionally bypassed
def _skip_stage(name: str, reason: str, results: list):
    r = StageResult(name=name, status="SKIPPED")
    results.append(r)
    log.info(f"Stage {name} SKIPPED — {reason}")

# Helper function to print a formatted table of all stage execution results
def _print_summary(results: list):
    log.info("=" * 60)
    log.info("PIPELINE SUMMARY")
    log.info("=" * 60)
    for r in results:
        rows_str = f"  rows={r.rows:,}" if r.rows is not None else ""
        err_str  = f"  error={r.error}" if r.error else ""
        log.info(f"  {r.status:<8}  {r.name:<30}  {r.duration_s:>6.1f}s{rows_str}{err_str}")
    log.info("=" * 60)

def main():
    # Ensure the required S3 bucket exists
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        log.error("S3_BUCKET_NAME not set — create config/keys.env from config/example.env")
        sys.exit(1)

    # Initialize the list to store results for all following stages
    results: list[StageResult] = []
    spark = None

    # Define the data ingestion stage logic
    def run_ingest():
        from src.ingestion.ingest import main as ingest_main
        ingest_main()
        return None

    _run_stage("1-Ingest", run_ingest, results)

    try:
        sys.path.insert(0, "src/processing")
        from spark_session import get_spark
        spark = get_spark("Pipeline")
        spark.sparkContext.setLogLevel("WARN")
    except Exception as exc:
        log.error(f"Could not start SparkSession — stages 2 and 3 will be skipped: {exc}",
                  exc_info=True)
        _skip_stage("2-Convert",   "SparkSession failed to start", results)
        _skip_stage("3-Aggregate", "SparkSession failed to start", results)
        _print_summary(results)
        sys.exit(1)

    try:
        # Define the JSON to Parquet file conversion stage logic
        def run_convert():
            from json_to_parquet import convert
            return convert(spark)

        _run_stage("2-Convert", run_convert, results)

        def run_aggregate():
            from aggregate_stats import map_plays, reduce_to_game_stats
            from pyspark.sql import functions as F

            processed = f"s3a://{bucket}/processed"
            stats_out = f"s3a://{bucket}/stats/team_game_stats.parquet"

            plays_df = spark.read.parquet(processed)
            mapped   = map_plays(plays_df)
            stats    = reduce_to_game_stats(mapped)
            row_count = stats.count()
            # Write the aggregated statistics back to S3
            (
                stats.write
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet(stats_out)
            )
            log.info(f"Stats written to: {stats_out}")

            out = spark.read.parquet(stats_out)
            avgs = out.select(
                F.round(F.avg("yards_per_play"), 2).alias("avg_ypp"),
                F.round(F.avg("success_rate"),   3).alias("avg_success_rate"),
                F.round(F.avg("third_down_pct"), 3).alias("avg_3rd_pct"),
                F.round(F.avg("avg_ppa"),        4).alias("avg_ppa"),
            ).collect()[0]
            # Log the computed league averages
            log.info(
                f"League averages — ypp={avgs.avg_ypp}  "
                f"success={avgs.avg_success_rate}  "
                f"3rd%={avgs.avg_3rd_pct}  "
                f"ppa={avgs.avg_ppa}"
            )
            return row_count

        # Execute the final aggregation stage
        _run_stage("3-Aggregate", run_aggregate, results)

    finally:
        # Ensure the Spark session is stopped
        if spark:
            spark.stop()
            log.info("SparkSession stopped.")

    # Output the final pipeline execution summary to the logs
    _print_summary(results)

    # Determine exit status: fail the script if any stage failed
    failed = [r for r in results if r.status == "FAILED"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
