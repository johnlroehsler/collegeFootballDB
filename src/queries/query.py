"""
query.py — Spark SQL query interface for CollegeFootballDB.

Reads team_game_stats.parquet from S3 and runs analytical queries
against the processed stats using Spark SQL with predicate pushdown.

Usage:
    python src/queries/query.py                  # all queries
    python src/queries/query.py --team Georgia   # filter to one team
    python src/queries/query.py --season 2024    # filter to one season
"""

import argparse
import os
import sys

from dotenv import load_dotenv

load_dotenv(dotenv_path="config/keys.env")

sys.path.insert(0, "src/processing")
from spark_session import get_spark

BUCKET    = os.getenv("S3_BUCKET_NAME")
STATS_PATH = f"s3a://{BUCKET}/stats/team_game_stats.parquet"

DIVIDER = "-" * 60


def run_queries(team: str = None, season: str = None):
    if not BUCKET:
        sys.exit("ERROR: S3_BUCKET_NAME not set. Create config/keys.env from config/example.env.")

    spark = get_spark("Queries")
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nReading: {STATS_PATH}")
    df = spark.read.parquet(STATS_PATH)

    # Apply optional filters before registering the view
    if team:
        df = df.filter(df.team == team)
    if season:
        df = df.filter(df.season == int(season))

    df.createOrReplaceTempView("stats")
    print(f"Rows loaded: {df.count():,}")
    if team:
        print(f"Filter: team = {team}")
    if season:
        print(f"Filter: season = {season}")

    # ------------------------------------------------------------------
    # Q1: Average offensive stats per team per season
    # ------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("Q1: Avg offensive stats by team and season")
    print(DIVIDER)
    spark.sql("""
        SELECT
            team,
            season,
            ROUND(AVG(yards_per_play),  2) AS avg_ypp,
            ROUND(AVG(success_rate),    3) AS avg_success_rate,
            ROUND(AVG(third_down_pct),  3) AS avg_3rd_pct,
            ROUND(AVG(avg_ppa),         4) AS avg_ppa,
            COUNT(*)                        AS games
        FROM stats
        GROUP BY team, season
        ORDER BY season, avg_ypp DESC
    """).show(50, truncate=False)

    # ------------------------------------------------------------------
    # Q2: Year-over-year offensive trends across the SEC
    # ------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("Q2: SEC-wide offensive trends by season")
    print(DIVIDER)
    spark.sql("""
        SELECT
            season,
            ROUND(AVG(yards_per_play),  2) AS league_avg_ypp,
            ROUND(AVG(success_rate),    3) AS league_success_rate,
            ROUND(AVG(third_down_pct),  3) AS league_3rd_pct,
            ROUND(AVG(avg_ppa),         4) AS league_avg_ppa,
            SUM(plays)                      AS total_plays
        FROM stats
        GROUP BY season
        ORDER BY season
    """).show(truncate=False)

    # ------------------------------------------------------------------
    # Q3: Top 10 single-game offensive performances (yards per play)
    # ------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("Q3: Top 10 single-game offensive performances (yards per play)")
    print(DIVIDER)
    spark.sql("""
        SELECT
            team,
            opponent,
            season,
            week,
            yards_per_play,
            total_yards,
            plays,
            avg_ppa
        FROM stats
        ORDER BY yards_per_play DESC
        LIMIT 10
    """).show(truncate=False)

    # ------------------------------------------------------------------
    # Q4: Third-down conversion rate by team (career average)
    # ------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("Q4: Third-down conversion rate by team (all seasons)")
    print(DIVIDER)
    spark.sql("""
        SELECT
            team,
            ROUND(AVG(third_down_pct),  3) AS avg_3rd_pct,
            SUM(third_down_conv)            AS total_conversions,
            SUM(third_down_attempts)        AS total_attempts,
            COUNT(*)                        AS games
        FROM stats
        WHERE third_down_attempts > 0
        GROUP BY team
        ORDER BY avg_3rd_pct DESC
    """).show(truncate=False)

    # ------------------------------------------------------------------
    # Q5: Home vs. away performance split
    # ------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("Q5: Home vs. away offensive splits")
    print(DIVIDER)
    spark.sql("""
        SELECT
            is_home,
            ROUND(AVG(yards_per_play),  2) AS avg_ypp,
            ROUND(AVG(success_rate),    3) AS avg_success_rate,
            ROUND(AVG(third_down_pct),  3) AS avg_3rd_pct,
            ROUND(AVG(avg_ppa),         4) AS avg_ppa,
            COUNT(*)                        AS games
        FROM stats
        GROUP BY is_home
        ORDER BY is_home DESC
    """).show(truncate=False)

    # ------------------------------------------------------------------
    # Q6: Big-play rate by team and season
    # ------------------------------------------------------------------
    print(f"\n{DIVIDER}")
    print("Q6: Big-play rate (>=20 yards) by team and season")
    print(DIVIDER)
    spark.sql("""
        SELECT
            team,
            season,
            ROUND(AVG(big_play_pct), 3) AS avg_big_play_pct,
            SUM(big_plays)               AS total_big_plays,
            COUNT(*)                     AS games
        FROM stats
        GROUP BY team, season
        ORDER BY avg_big_play_pct DESC
        LIMIT 20
    """).show(truncate=False)

    spark.stop()
    print("\nQueries complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CollegeFootballDB query interface")
    parser.add_argument("--team",   default=None, help="Filter to a single team (e.g. Georgia)")
    parser.add_argument("--season", default=None, help="Filter to a single season year (e.g. 2024)")
    args = parser.parse_args()

    run_queries(team=args.team, season=args.season)
