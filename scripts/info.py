import requests
import pandas as pd
from dotenv import load_dotenv
import os

# This script is mainly used for checking API usage and status

# Config
env_path = 'config/keys.env'
load_dotenv(dotenv_path=env_path)
API_KEY = os.getenv('CFB_API_KEY')
BASE_URL = "https://api.collegefootballdata.com"

headers = {
    "Authorization": f"Bearer {API_KEY}"
}

params = {

}

r = requests.get(f"{BASE_URL}/info", headers=headers, params=params)
r.raise_for_status()

info = r.json()
df = pd.DataFrame([info])

print(df[["patronLevel", "remainingCalls"]].head())