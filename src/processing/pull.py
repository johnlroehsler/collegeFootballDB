import os
import json
import argparse
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Config
env_path = 'config/keys.env'
load_dotenv(dotenv_path=env_path)
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')


# Get Raw data
def fetch_raw(year, week, team):
    s3_key = f"raw/year={year}/week={week}/{team}_plays.json"
    print(f"Fetching s3://{BUCKET_NAME}/{s3_key}\n")
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        print(json.dumps(data[:3], indent=2))
        print(f"\n... {len(data)} total plays")
    except ClientError as e:
        code = e.response['Error']['Code']
        if code == 'NoSuchKey':
            print(f"Not found: {s3_key}")
        else:
            print(f"S3 error: {e}")


# Get processed data
def fetch_processed(season=None, week=None):
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from spark_session import get_spark

    path = f"s3a://{BUCKET_NAME}/processed/"
    print(f"Reading {path}\n")
    spark = get_spark("pull_processed")
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(path)

    if season:
        df = df.filter(df.season == int(season))
    if week:
        df = df.filter(df.week == int(week))

    total = df.count()
    print(f"Total rows (filtered): {total:,}\n")
    df.show(10, truncate=False)
    spark.stop()

# Get stats
def fetch_stats(team=None, season=None):
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from spark_session import get_spark

    path = f"s3a://{BUCKET_NAME}/stats/team_game_stats.parquet"
    print(f"Reading {path}\n")
    spark = get_spark("pull_stats")
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(path)

    if team:
        df = df.filter(df.team == team)
    if season:
        df = df.filter(df.season == int(season))

    total = df.count()
    print(f"Total rows (filtered): {total:,}\n")
    df.show(10, truncate=False)
    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Pull data from S3")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # raw
    p_raw = sub.add_parser("raw", help="Fetch a raw JSON file")
    p_raw.add_argument("year",  type=int)
    p_raw.add_argument("week",  type=int)
    p_raw.add_argument("team",  type=str)

    # processed
    p_proc = sub.add_parser("processed", help="Sample processed Parquet")
    p_proc.add_argument("--season", type=int, default=None)
    p_proc.add_argument("--week",   type=int, default=None)

    # stats
    p_stats = sub.add_parser("stats", help="Sample team_game_stats Parquet")
    p_stats.add_argument("--team",   type=str, default=None)
    p_stats.add_argument("--season", type=int, default=None)

    args = parser.parse_args()

    if args.cmd == "raw":
        fetch_raw(args.year, args.week, args.team)
    elif args.cmd == "processed":
        fetch_processed(args.season, args.week)
    elif args.cmd == "stats":
        fetch_stats(args.team, args.season)


if __name__ == "__main__":
    main()
