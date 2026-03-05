import requests
import pandas as pd

API_KEY = "aD93zwvycv/KXaeinhzzWMo99xljl3y707e2wd0oIa1sWl8dC615g/Gb1rH2apT4"
BASE_URL = "https://api.collegefootballdata.com"

headers = {
    "Authorization": f"Bearer {API_KEY}"
}

params = {
    "year": 2021,
    "week": 1,
    "team": "Georgia"
}

r = requests.get(f"{BASE_URL}/plays", headers=headers, params=params)
r.raise_for_status()

games = r.json()
df = pd.DataFrame(games)

print(f"Total games: {len(df)}")
print(df[["playNumber", "offense", "yardsGained", "scoring", "playType", "playText"]])
