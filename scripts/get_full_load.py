import os
import json
import time
import requests

from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv


# ============================================================
# PROJECT SETUP
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")

BASE_URL = "https://api.coingecko.com/api/v3"

API_KEY = os.getenv("COINGECKO_API_KEY")

if not API_KEY:
    raise ValueError(
        "COINGECKO_API_KEY is not set. "
        "Check your .env file."
    )


HEADERS = {
    "x-cg-demo-api-key": API_KEY
}


# ============================================================
# PARAMETERS
# ============================================================

NUMBER_OF_COINS = 250
HISTORICAL_DAYS = 90
INTERVAL = "hourly"

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "samples"
    / "full_load_250"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# API SESSION
# ============================================================

session = requests.Session()
session.headers.update(HEADERS)


# ============================================================
# GET TOP 250 COINS
# ============================================================

def get_top_coins():

    url = f"{BASE_URL}/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": NUMBER_OF_COINS,
        "page": 1,
        "sparkline": "false"
    }

    response = session.get(
        url,
        params=params,
        timeout=60
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# GET HISTORICAL DATA
# ============================================================

def get_historical_data(coin_id):

    url = (
        f"{BASE_URL}/coins/"
        f"{coin_id}/market_chart"
    )

    params = {
        "vs_currency": "usd",
        "days": HISTORICAL_DAYS,
        "interval": INTERVAL
    }

    response = session.get(
        url,
        params=params,
        timeout=60
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# START FULL LOAD
# ============================================================

run_timestamp = datetime.now(timezone.utc)

print("=" * 60)
print("CRYPTOCURRENCY FULL LOAD")
print("=" * 60)

print(
    f"Coins: {NUMBER_OF_COINS}"
)

print(
    f"History: {HISTORICAL_DAYS} days"
)

print(
    f"Granularity: {INTERVAL}"
)

print(
    f"Started: {run_timestamp.isoformat()}"
)

print("=" * 60)


# ============================================================
# GET TOP 250
# ============================================================

print("\nRetrieving top 250 cryptocurrencies...")

coins = get_top_coins()

print(
    f"Retrieved {len(coins)} coins."
)


# ============================================================
# SAVE COIN MANIFEST
# ============================================================

manifest_file = (
    OUTPUT_DIR
    / "coin_manifest.json"
)

with open(
    manifest_file,
    "w",
    encoding="utf-8"
) as file:

    json.dump(
        coins,
        file,
        indent=2
    )


# ============================================================
# DOWNLOAD HISTORICAL DATA
# ============================================================

successful = 0

failed = []


for index, coin in enumerate(
    coins,
    start=1
):

    coin_id = coin["id"]

    print(
        f"[{index}/{len(coins)}] "
        f"{coin_id}"
    )

    try:

        data = get_historical_data(
            coin_id
        )

        output_file = (
            OUTPUT_DIR
            / f"{coin_id}.json"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                data,
                file,
                indent=2
            )

        successful += 1

        print(
            f"  ✓ Saved {coin_id}.json"
        )

    except requests.RequestException as error:

        failed.append(
            {
                "coin_id": coin_id,
                "error": str(error)
            }
        )

        print(
            f"  ✗ Failed: {error}"
        )

    # Keep requests conservative
    time.sleep(2)


# ============================================================
# DOWNLOAD REPORT
# ============================================================

report = {

    "load_type": "FULL",

    "run_timestamp":
        run_timestamp.isoformat(),

    "requested_coins":
        len(coins),

    "historical_days":
        HISTORICAL_DAYS,

    "interval":
        INTERVAL,

    "successful_downloads":
        successful,

    "failed_downloads":
        failed
}


report_file = (
    OUTPUT_DIR
    / "download_report.json"
)


with open(
    report_file,
    "w",
    encoding="utf-8"
) as file:

    json.dump(
        report,
        file,
        indent=2
    )


# ============================================================
# FINISH
# ============================================================

print("\n" + "=" * 60)

print("FULL LOAD FINISHED")

print(
    f"Successful: {successful}"
)

print(
    f"Failed: {len(failed)}"
)

print(
    f"Manifest: {manifest_file}"
)

print(
    f"Report: {report_file}"
)

print("=" * 60)