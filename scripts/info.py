import requests
import pandas as pd

API_KEY = "aD93zwvycv/KXaeinhzzWMo99xljl3y707e2wd0oIa1sWl8dC615g/Gb1rH2apT4"
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
