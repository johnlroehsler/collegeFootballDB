import os
import json
import requests
import boto3
from dotenv import load_dotenv

env_path = 'config/keys.env'
load_dotenv(dotenv_path=env_path)


API_KEY = os.getenv('CFB_API_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
BASE_URL = "https://api.collegefootballdata.com/plays"

def get_plays(year, week, team):
    """Fetches play-by-play data from the API."""
    headers = {
        'Authorization': f'Bearer {API_KEY}',
    }
    params = {
        'seasonType': 'regular',
        'year': year,
        'week': week,
        'team': team
    }
    
    print(f"Fetching data for {team} - Year: {year}, Week: {week}...")
    response = requests.get(BASE_URL, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def upload_to_s3(data, year, week, team):
    """Uploads JSON data to S3 using the required partition strategy."""
    # Initialize the S3 client using boto3
    s3_client = boto3.client('s3')
    
    # Define the partition strategy path (M1 Requirement)
    s3_key = f"raw/year={year}/week={week}/{team}_plays.json"
    
    # Convert Python dictionary back to a JSON string for storage
    json_data = json.dumps(data)
    
    print(f"Uploading to S3: s3://{BUCKET_NAME}/{s3_key}")
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_data,
        ContentType='application/json'
    )
    print("Upload complete.")

def main():
    test_year = 2025
    test_week = 1
    test_team = "Georgia"
    
    try:
        # 1. Data Acquisition
        plays_data = get_plays(test_year, test_week, test_team)
        
        # 2. Basic Storage 
        if plays_data:
            upload_to_s3(plays_data, test_year, test_week, test_team)
        else:
            print("No data retrieved from API.")
            
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()