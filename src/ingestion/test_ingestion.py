import os
import json
import requests
import boto3
from dotenv import load_dotenv

# Config
env_path = 'config/keys.env'
load_dotenv(dotenv_path=env_path)
API_KEY = os.getenv('CFB_API_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
BASE_URL = "https://api.collegefootballdata.com/plays"

def get_plays(year, week, team):
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
    data = response.json()

    return data

#saves the data locally 
def save_to_local_sample(data, year, week, team):
    sample_dir = os.path.join("data", "sample")
    os.makedirs(sample_dir, exist_ok=True)
    
    file_name = f"{team}_plays_y{year}_w{week}.json"
    file_path = os.path.join(sample_dir, file_name)
    
    print(f"Writing local sample to: {file_path}")
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    print("Local save complete.")

#uploads to s3
def upload_to_s3(data, year, week, team):
    # Create the S3 client
    s3_client = boto3.client('s3')

    s3_key = f"raw/year={year}/week={week}/{team}_plays.json"
    
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
        plays_data = get_plays(test_year, test_week, test_team)
    
        if plays_data:
            # Saves locally
            save_to_local_sample(plays_data, test_year, test_week, test_team)
            
            # Upload to s3
            upload_to_s3(plays_data, test_year, test_week, test_team)
        else:
            print("No data retrieved from API.")
    
    # Error handling
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()