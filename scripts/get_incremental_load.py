import os
import json
from datetime import datetime, timezone
import requests

API_KEY = os.getenv("COINGECKO_API_KEY")

BASE_URL = "https://api.coingecko.com/api/v3"

COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "binancecoin",
    "ripple"
]

OUTPUT_DIR = "data/samples/incremental_load"

os.makedirs(OUTPUT_DIR, exist_ok=True)

headers = {
    "x-cg-demo-api-key": os.getenv("COINGECKO_API_KEY")
}

params = {
    "vs_currency": "usd",
    "ids": ",".join(COINS)
}

url = f"{BASE_URL}/coins/markets"

print("Downloading incremental market snapshot...")

response = requests.get(
    url,
    headers=headers,
    params=params
)

print("Status:", response.status_code)

if response.status_code != 200:
    print("Error:", response.text)
    exit()

data = response.json()

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

output_file = f"{OUTPUT_DIR}/incremental_load_{today}.json"

with open(output_file, "w", encoding="utf-8") as file:
    json.dump(data, file, indent=2)

print(f"Saved: {output_file}")
print("Incremental load completed!")