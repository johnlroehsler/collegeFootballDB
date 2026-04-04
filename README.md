# CollegeFootballDB

A distributed analytics platform for processing and querying SEC football play-by-play data (2021–2025). Built for CS 4265: Big Data Analytics.

## Overview

Ingests play-by-play data from the CollegeFootballData.org API for all 14 SEC teams across 5 seasons, stores raw JSON in S3, converts to Parquet via Apache Spark, aggregates per-game offensive statistics using a MapReduce pattern, and exposes results through a Spark SQL query interface.

**Data source:** [CollegeFootballData.org API](https://api.collegefootballdata.com)  
**Teams:** 14 SEC teams (2021–2025, weeks 1–15)  
**Final output:** `s3://{bucket}/stats/team_game_stats.parquet`

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/johnlroehsler/collegeFootballDB.git
cd collegeFootballDB
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

Requires Python 3.9+ and Java 17.

On macOS with Homebrew:

```bash
brew install openjdk@17
```

On Windows, install [Eclipse Adoptium JDK 17](https://adoptium.net/) and ensure the `hadoop/` directory (included in the repo) is present at the project root -- it provides the Hadoop native binaries (`winutils.exe`) needed for Spark's S3A filesystem.

### 3. Configure credentials

Copy the example env file and fill in your credentials:

```bash
cp config/example.env config/keys.env
```

Edit `config/keys.env`:

```
CFB_API_KEY=your_collegefootballdata_api_key
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your_bucket_name
```

---

## Running the Pipeline

```bash
python run_pipeline.py
```

The pipeline runs three stages in sequence:

| Stage | Script | Description |
|-------|--------|-------------|
| 1 — Ingest | `src/ingestion/ingest.py` | Fetches play-by-play JSON from API, uploads to `s3://{bucket}/raw/` |
| 2 — Convert | `src/processing/json_to_parquet.py` | Converts raw JSON to Snappy Parquet, partitioned by season/week |
| 3 — Aggregate | `src/processing/aggregate_stats.py` | MapReduce aggregation to per-game team stats |

A summary is printed at the end:

```
============================================================
PIPELINE SUMMARY
============================================================
  OK        1-Ingest                          142.3s
  OK        2-Convert                          18.7s  rows=148,432
  OK        3-Aggregate                        11.2s  rows=1,260
============================================================
```

Logs are written to `pipeline.log`.

---

## Querying the Output

```bash
python src/queries/query.py                   # run all queries
python src/queries/query.py --team Georgia    # filter to one team
python src/queries/query.py --season 2024     # filter to one season
```

Runs 6 Spark SQL queries against `team_game_stats.parquet`:

- Avg yards/play, success rate, 3rd-down %, and PPA by team and season
- SEC-wide year-over-year offensive trends
- Top 10 single-game offensive performances
- Third-down conversion rate by team
- Home vs. away performance split
- Big-play rate (≥20 yards) by team and season

---

## Individual Scripts

| Script | Purpose |
|--------|---------|
| `scripts/info.py` | Check API quota and patron level |
| `src/processing/smoke_spark.py` | Verify Spark + S3 connectivity |
| `src/processing/pull.py` | Fetch and print a single raw JSON file from S3 |

```bash
python scripts/info.py
python src/processing/smoke_spark.py 2025 1 Georgia
python src/processing/pull.py
```

---

## Project Structure

```
collegeFootballDB/
├── config/
│   └── example.env               # credential template
├── data/
│   └── sample/                   # sample raw JSON for local testing
├── src/
│   ├── ingestion/
│   │   └── ingest.py             # Stage 1: API → S3
│   ├── processing/
│   │   ├── spark_session.py      # SparkSession factory (S3A + Java 17)
│   │   ├── json_to_parquet.py    # Stage 2: JSON → Parquet
│   │   ├── aggregate_stats.py    # Stage 3: MapReduce aggregation
│   │   ├── smoke_spark.py        # Spark/S3 connectivity test
│   │   └── pull.py               # S3 fetch utility (debug)
│   └── queries/
│       └── query.py              # Spark SQL query interface
├── scripts/
│   └── info.py                   # API quota checker
├── run_pipeline.py               # End-to-end pipeline orchestrator
├── requirements.txt
└── pipeline.log                  # Runtime log (generated on first run)
```

---

## S3 Data Layout

```
s3://{bucket}/
├── raw/
│   └── year=YYYY/
│       └── week=W/
│           └── {Team}_plays.json
├── processed/
│   └── season=YYYY/
│       └── week=W/
│           └── *.parquet
└── stats/
    └── team_game_stats.parquet
```
