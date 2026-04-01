import os
import json
import time
import logging
import requests
import boto3
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, ClientError
from typing import Optional


# Config
env_path = 'config/keys.env'
load_dotenv(dotenv_path=env_path)

API_KEY     = os.getenv('CFB_API_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
BASE_URL    = "https://api.collegefootballdata.com/plays"

# Teams
SEC_TEAMS = [
    "Alabama", "Arkansas", "Auburn", "Florida", "Georgia",
    "Kentucky", "LSU", "Mississippi State", "Missouri",
    "Ole Miss", "South Carolina", "Tennessee",
    "Texas A&M", "Vanderbilt",
]
SEASONS      = range(2021, 2026)   
WEEKS        = range(1, 16) 
MAX_RETRIES  = 5
BACKOFF_BASE = 2
REQUEST_DELAY = 0.25 

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ingest.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__) 


# API
def get_plays(year: int, week: int, team: str) -> Optional[list]:
    headers = {"Authorization": f"Bearer {API_KEY}"}
    params  = {"seasonType": "regular", "year": year, "week": week, "team": team}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"GET {team} | {year} W{week}  (attempt {attempt})")
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)

            #if rate limited
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", BACKOFF_BASE ** attempt))
                log.warning(f"Rate limited. Sleeping {retry_after}s …")
                time.sleep(retry_after)
                continue

            # if game or data does not exist
            if response.status_code == 404:
                log.info(f"No game found (404)")
                return []

            response.raise_for_status()
            data = response.json()

            if not data:
                log.info(f"API returned empty list")
                return []

            log.info(f"{len(data)} plays retrieved.")
            return data

        except requests.exceptions.Timeout:
            wait = BACKOFF_BASE ** attempt
            log.warning(f"Timeout. Retrying in {wait}s...")
            time.sleep(wait)

        except requests.exceptions.HTTPError as e:
            # Handle errors
            if response.status_code >= 500:
                wait = BACKOFF_BASE ** attempt
                log.warning(f"Server error {response.status_code}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"Client error {response.status_code}: {e}, skipping.")
                return []

        except requests.exceptions.RequestException as e:
            wait = BACKOFF_BASE ** attempt
            log.warning(f"Request error: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    log.error(f"All {MAX_RETRIES} retries exhausted for {team} {year} W{week}.")
    return None   # signals a hard failure

# Upload to S3
def upload_to_s3(data: list, year: int, week: int, team: str, s3_client) -> bool:
    s3_key = f"raw/year={year}/week={week}/{team.replace(' ', '_')}_plays.json"
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType="application/json",
        )
        log.info(f"Uploaded to s3://{BUCKET_NAME}/{s3_key}")
        return True
    except (BotoCoreError, ClientError) as e:
        log.error(f"S3 upload failed: {e}")
        return False


# Main
def main():

    s3_client = boto3.client("s3")

    stats = {"success": 0, "empty": 0, "failed": 0}

    total = len(SEC_TEAMS) * len(SEASONS) * len(WEEKS)
    log.info(f"Starting SEC ingestion: {len(SEC_TEAMS)} teams"
             f"{len(list(SEASONS))} seasons × {len(list(WEEKS))} weeks = {total} requests\n")

    for year in SEASONS:
        for week in WEEKS:
            for team in SEC_TEAMS:
                plays = get_plays(year, week, team)

                if plays is None:
                    # Hard failure after all retries
                    stats["failed"] += 1
                    continue

                if not plays:
                    # No game this week
                    stats["empty"] += 1
                    continue

                # Persist
                upload_to_s3(plays, year, week, team, s3_client)
                stats["success"] += 1

                time.sleep(REQUEST_DELAY)

    log.info(
        f"\nIngestion completed \n"
        f"Success  : {stats['success']}\n"
        f"Empty    : {stats['empty']}\n"
        f"Failed   : {stats['failed']}\n"
    )


if __name__ == "__main__":
    main()