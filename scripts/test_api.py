import os
import requests
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

ENV_FILE = PROJECT_ROOT / ".env"

print("Project root:", PROJECT_ROOT)
print(".env location:", ENV_FILE)
print(".env exists:", ENV_FILE.exists())

load_dotenv(ENV_FILE)

API_KEY = os.getenv("COINGECKO_API_KEY")

print("API key loaded:", bool(API_KEY))

if API_KEY:
    print("API key starts with:", API_KEY[:3])
    print("API key length:", len(API_KEY))

if not API_KEY:
    raise ValueError("API key was not loaded from .env")

url = "https://api.coingecko.com/api/v3/ping"

headers = {
    "x-cg-demo-api-key": API_KEY
}

response = requests.get(
    url,
    headers=headers,
    timeout=30
)

print("Status code:", response.status_code)
print("Response:", response.text)