import os
import json
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv


# --------------------------------------------------
# PROJECT SETUP
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("COINGECKO_API_KEY")

if not API_KEY:
    raise ValueError("CoinGecko API key not found. Check your .env file.")


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


# --------------------------------------------------
# PARAMETERS
# --------------------------------------------------

parser = argparse.ArgumentParser(
    description="Download parameterized incremental cryptocurrency data"
)

parser.add_argument(
    "--run-date",
    type=str,
    required=True,
    help="Data date in YYYY-MM-DD format"
)

args = parser.parse_args()

try:
    run_date = datetime.strptime(
        args.run_date,
        "%Y-%m-%d"
    ).date()
except ValueError:
    raise ValueError("run-date must be in YYYY-MM-DD format")


# --------------------------------------------------
# EXACT DATE RANGE
# --------------------------------------------------

# Include one previous day for overlap.
start_date = run_date - timedelta(days=1)

# End is exclusive.
end_date = run_date + timedelta(days=1)


start_datetime = datetime.combine(
    start_date,
    datetime.min.time(),
    tzinfo=timezone.utc
)

end_datetime = datetime.combine(
    end_date,
    datetime.min.time(),
    tzinfo=timezone.utc
)

from_timestamp = int(start_datetime.timestamp())
to_timestamp = int(end_datetime.timestamp())


# --------------------------------------------------
# OUTPUT DIRECTORY
# --------------------------------------------------

batch_id = f"incremental_{run_date}"

output_dir = OUTPUT_ROOT / str(run_date)

output_dir.mkdir(
    parents=True,
    exist_ok=True
)


# --------------------------------------------------
# LOAD COIN MANIFEST
# --------------------------------------------------

if not MANIFEST_FILE.exists():
    raise FileNotFoundError(
        f"Coin manifest not found: {MANIFEST_FILE}"
    )

with open(MANIFEST_FILE, "r", encoding="utf-8") as file:
    manifest = json.load(file)


COINS = [
    coin["id"]
    for coin in manifest
    if "id" in coin
]

print(f"Coins to download: {len(COINS)}")
print(f"Batch ID: {batch_id}")
print(f"Data range: {start_datetime} → {end_datetime}")
print(f"Output directory: {output_dir}")


# --------------------------------------------------
# API SETUP
# --------------------------------------------------

headers = {
    "x-cg-demo-api-key": API_KEY
}


# --------------------------------------------------
# DOWNLOAD
# --------------------------------------------------

download_report = []

run_started = datetime.now(timezone.utc).isoformat()

for index, coin_id in enumerate(COINS, start=1):

    print(
        f"[{index}/{len(COINS)}] Downloading {coin_id}..."
    )

    url = (
        f"{BASE_URL}/coins/"
        f"{coin_id}/market_chart/range"
    )

    params = {
        "vs_currency": "usd",
        "from": from_timestamp,
        "to": to_timestamp
    }

    success = False

    for attempt in range(1, 4):

        try:

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=60
            )

            if response.status_code == 200:

                data = response.json()

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

                download_report.append({
                    "coin_id": coin_id,
                    "status": "success",
                    "file": str(output_file),
                    "start_date": str(start_date),
                    "end_date": str(end_date)
                })

                success = True
                break

            elif response.status_code == 429:

                print(
                    f"Rate limited. Attempt {attempt}/3"
                )

                time.sleep(10)

            elif response.status_code >= 500:

                print(
                    f"Server error {response.status_code}. "
                    f"Attempt {attempt}/3"
                )

                time.sleep(5)

            else:

                print(
                    f"Failed: {response.status_code}"
                )

                print(response.text)

                break

        except requests.RequestException as error:

            print(
                f"Request error: {error}. "
                f"Attempt {attempt}/3"
            )

            time.sleep(5)

    if not success:

        download_report.append({
            "coin_id": coin_id,
            "status": "failed"
        })

    # Avoid aggressive API requests.
    time.sleep(2.5)


# --------------------------------------------------
# SAVE REPORT
# --------------------------------------------------

report = {
    "run_id": f"{batch_id}_{datetime.now(timezone.utc).strftime('%H%M%S')}",
    "batch_id": batch_id,
    "run_date": str(run_date),
    "start_date": str(start_date),
    "end_date": str(end_date),
    "from_timestamp": from_timestamp,
    "to_timestamp": to_timestamp,
    "number_of_coins": len(COINS),
    "run_started": run_started,
    "run_completed": datetime.now(timezone.utc).isoformat(),
    "files": download_report
}

report_file = output_dir / "download_report.json"

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


print("\nIncremental load completed.")
print(f"Batch: {batch_id}")
print(f"Report: {report_file}")