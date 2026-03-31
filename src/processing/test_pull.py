import os
import json
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Config
env_path = 'config/keys.env'
load_dotenv(dotenv_path=env_path)
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

def fetch_from_s3(year, week, team):
    s3_client = boto3.client('s3')
    
    s3_key = f"raw/year={year}/week={week}/{team}_plays.json"
    print(f"Attempting to fetch from S3: s3://{BUCKET_NAME}/{s3_key}\n")
    
    try:
        response = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=s3_key
        )
        
        file_content = response['Body'].read().decode('utf-8')
        data = json.loads(file_content)
        return data
    
    # Error handling
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Error: The file '{s3_key}' does not exist in bucket '{BUCKET_NAME}'.")
        else:
            print(f"An S3 error occurred: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def main():
    test_year = 2025
    test_week = 1
    test_team = "Georgia"
    
    # Fetch the data
    plays_data = fetch_from_s3(test_year, test_week, test_team)
    
    # Print the data if successfully retrieved
    if plays_data:
        print("Successfully retrieved data!")
        print("-" * 40)
        # Print a formatted JSON string
        print(json.dumps(plays_data, indent=4))
        print("-" * 40)
        print(f"Total plays retrieved: {len(plays_data)}")
    else:
        print("No data retrieved from S3.")

if __name__ == "__main__":
    main()