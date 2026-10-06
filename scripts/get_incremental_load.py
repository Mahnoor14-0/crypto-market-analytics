import os
import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv


# ============================================================
# 1. PROJECT PATHS AND ENVIRONMENT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("COINGECKO_API_KEY")

if not API_KEY:
    raise ValueError(
        "COINGECKO_API_KEY not found. "
        "Check your .env file."
    )


# ============================================================
# 2. CONFIGURATION
# ============================================================

BASE_URL = "https://api.coingecko.com/api/v3"

MANIFEST_FILE = (
    PROJECT_ROOT
    / "data"
    / "samples"
    / "full_load_250"
    / "coin_manifest.json"
)

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "data"
    / "samples"
    / "incremental_load_250"
)

DAYS = 2

HEADERS = {
    "x-cg-demo-api-key": API_KEY
}


# ============================================================
# 3. COMMAND-LINE PARAMETERS
# ============================================================

parser = argparse.ArgumentParser(
    description="Download CoinGecko incremental hourly data."
)

parser.add_argument(
    "--run-date",
    type=str,
    default=None,
    help="Processing date in YYYY-MM-DD format."
)

args = parser.parse_args()


# ============================================================
# 4. DETERMINE RUN DATE
# ============================================================

if args.run_date:
    try:
        run_date = datetime.strptime(
            args.run_date,
            "%Y-%m-%d"
        ).date()
    except ValueError:
        raise ValueError(
            "run-date must be in YYYY-MM-DD format."
        )
else:
    run_date = datetime.now(timezone.utc).date()


# ============================================================
# 5. BATCH INFORMATION
# ============================================================

batch_id = f"incremental_{run_date}"

output_dir = OUTPUT_ROOT / str(run_date)

output_dir.mkdir(
    parents=True,
    exist_ok=True
)

print("=" * 60)
print("COINGECKO INCREMENTAL LOAD")
print("=" * 60)

print(f"Run date: {run_date}")
print(f"Batch ID: {batch_id}")
print(f"Historical window: {DAYS} days")
print(f"Output directory: {output_dir}")


# ============================================================
# 6. READ THE SAME 250-COIN MANIFEST USED BY FULL LOAD
# ============================================================

if not MANIFEST_FILE.exists():
    raise FileNotFoundError(
        f"Coin manifest not found:\n{MANIFEST_FILE}"
    )

with open(
    MANIFEST_FILE,
    "r",
    encoding="utf-8"
) as file:
    manifest = json.load(file)


COINS = [
    coin["id"]
    for coin in manifest
    if "id" in coin
]

print(f"Coins found in manifest: {len(COINS)}")

if len(COINS) == 0:
    raise ValueError(
        "No coin IDs found in coin_manifest.json."
    )


# ============================================================
# 7. CREATE REQUEST SESSION
# ============================================================

session = requests.Session()

session.headers.update(HEADERS)


# ============================================================
# 8. FUNCTION TO DOWNLOAD ONE COIN
# ============================================================

def download_coin(coin_id):

    url = f"{BASE_URL}/coins/{coin_id}/market_chart"

    params = {
        "vs_currency": "usd",
        "days": DAYS
    }

    max_attempts = 4

    for attempt in range(1, max_attempts + 1):

        try:

            response = session.get(
                url,
                params=params,
                timeout=60
            )

            # Successful request
            if response.status_code == 200:
                return response.json()

            # Rate limit
            if response.status_code == 429:

                wait_time = 10 * attempt

                print(
                    f"Rate limited for {coin_id}. "
                    f"Waiting {wait_time} seconds..."
                )

                time.sleep(wait_time)

                continue

            # Temporary server error
            if response.status_code >= 500:

                wait_time = 5 * attempt

                print(
                    f"Server error for {coin_id}. "
                    f"Retrying in {wait_time} seconds..."
                )

                time.sleep(wait_time)

                continue

            # Other HTTP error
            print(
                f"HTTP {response.status_code} "
                f"for {coin_id}:"
            )

            print(response.text)

            return None

        except requests.RequestException as error:

            wait_time = 5 * attempt

            print(
                f"Request error for {coin_id}: {error}"
            )

            if attempt < max_attempts:

                print(
                    f"Retrying in {wait_time} seconds..."
                )

                time.sleep(wait_time)

            else:

                return None

    return None


# ============================================================
# 9. DOWNLOAD ALL 250 COINS
# ============================================================

successful = 0

failed = []


for index, coin_id in enumerate(
    COINS,
    start=1
):

    print(
        f"\n[{index}/{len(COINS)}] "
        f"Downloading {coin_id}..."
    )

    data = download_coin(coin_id)

    if data is None:

        failed.append(coin_id)

        print(
            f"FAILED: {coin_id}"
        )

        continue


    # --------------------------------------------------------
    # Save raw response
    # --------------------------------------------------------

    output_file = (
        output_dir
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
        f"Saved: {output_file}"
    )


    # --------------------------------------------------------
    # Delay between API requests
    # --------------------------------------------------------

    time.sleep(2.5)


# ============================================================
# 10. DOWNLOAD REPORT
# ============================================================

report = {

    "batch_id": batch_id,

    "run_date": str(run_date),

    "load_type": "INCREMENTAL",

    "source": "CoinGecko",

    "endpoint": "/coins/{id}/market_chart",

    "requested_coins": len(COINS),

    "successful_downloads": successful,

    "failed_downloads": len(failed),

    "failed_coins": failed,

    "days_requested": DAYS,

    "download_timestamp": datetime.now(
        timezone.utc
    ).isoformat()
}


report_file = (
    output_dir
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
# 11. FINAL SUMMARY
# ============================================================

print("\n" + "=" * 60)

print("INCREMENTAL LOAD FINISHED")

print("=" * 60)

print(
    f"Requested coins: {len(COINS)}"
)

print(
    f"Successful downloads: {successful}"
)

print(
    f"Failed downloads: {len(failed)}"
)

print(
    f"Batch ID: {batch_id}"
)

print(
    f"Output directory: {output_dir}"
)

print("=" * 60)