import os
import requests

API_KEY = os.getenv("COINGECKO_API_KEY")

url = "https://api.coingecko.com/api/v3/ping"

headers = {
    "x-cg-demo-api-key": os.getenv("COINGECKO_API_KEY")
}

response = requests.get(url, headers=headers)

print("Status code:", response.status_code)
print("Response:", response.text)