import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv


def main() -> None:
    load_dotenv()
    api_key = os.getenv("COINGLASS_API_KEY")

    if not api_key:
        print("ERROR: COINGLASS_API_KEY is not set in the environment/.env")
        return

    # Hardcode the exact missing temporal window (UTC)
    start_ts = int(datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_ts = int(datetime(2026, 3, 16, tzinfo=timezone.utc).timestamp() * 1000)

    url = "https://open-api-v4.coinglass.com/api/futures/funding-rate/history"
    headers = {"CG-API-KEY": api_key, "accept": "application/json"}
    params = {
        "symbol": "BTCUSDT",
        "exchange": "Binance",
        "interval": "1d",
        "startTime": start_ts,
        "endTime": end_ts,
    }

    print("Pinging CoinGlass for BTCUSDT (March 1 to March 16, 2026)...")
    response = requests.get(url, headers=headers, params=params, timeout=30)

    print(f"HTTP status: {response.status_code}")
    if response.status_code == 200:
        payload = response.json()
        data = payload.get("data", [])
        print(f"Success! Retrieved {len(data)} funding records.")
        if data:
            first = data[0]
            last = data[-1]
            first_dt = datetime.fromtimestamp(first["time"] / 1000, tz=timezone.utc)
            last_dt = datetime.fromtimestamp(last["time"] / 1000, tz=timezone.utc)
            print(f"First record: {first_dt.date()} -> {first.get('close')}")
            print(f"Last record: {last_dt.date()} -> {last.get('close')}")
        else:
            print("No records returned in 'data' array.")
    else:
        try:
            print("Response body:", response.text)
        except Exception:
            print("Non-text response body (unable to print).")


if __name__ == "__main__":
    main()

