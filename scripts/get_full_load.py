import os
import json
import time
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

OUTPUT_DIR = "data/samples/full_load"

os.makedirs(OUTPUT_DIR, exist_ok=True)

headers = {
    "x-cg-demo-api-key": os.getenv("COINGECKO_API_KEY")
}

for coin in COINS:

    print(f"Downloading full load for {coin}...")

    url = f"{BASE_URL}/coins/{coin}/market_chart"

    params = {
        "vs_currency": "usd",
        "days": "365"
    }

    response = requests.get(
        url,
        headers=headers,
        params=params
    )

    print("Status:", response.status_code)

    if response.status_code != 200:
        print("Error:", response.text)
        continue

    data = response.json()

    output_file = f"{OUTPUT_DIR}/{coin}.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    print(f"Saved: {output_file}")

    time.sleep(2)

print("Full load completed!")