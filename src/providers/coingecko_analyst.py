"""Analyst tier exclusive CoinGecko endpoints."""

import time
import requests
from datetime import date, datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import polars as pl

COINGECKO_BASE = "https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY = "CG-RhUWZY31TcDFBPfj4GWwcsMS"


def fetch_ohlc_range(
    coingecko_id: str,
    start_date: date,
    end_date: date,
    vs_currency: str = "usd",
    sleep_seconds: float = 0.12,
    max_retries: int = 5,
) -> List[Tuple[date, float, float, float, float]]:
    """
    Fetch OHLC data for a coin within a date range.
    
    CoinGecko limits:
    - Daily interval: up to 180 days per request
    - Hourly interval: up to 31 days per request
    
    This function automatically chunks large date ranges into multiple requests.
    
    Returns list of (date, open, high, low, close) tuples.
    """
    url = f"{COINGECKO_BASE}/coins/{coingecko_id}/ohlc/range"
    
    # CoinGecko limits: 180 days for daily, 31 days for hourly
    MAX_DAILY_DAYS = 180
    MAX_HOURLY_DAYS = 31
    
    # Use daily interval for historical data
    interval = "daily"
    max_days = MAX_DAILY_DAYS
    
    all_ohlc_data = []
    current_start = start_date
    
    while current_start <= end_date:
        # Calculate chunk end date (max 180 days)
        chunk_end = min(
            date(current_start.year, current_start.month, current_start.day) + timedelta(days=max_days - 1),
            end_date
        )
        
        # Convert dates to timestamps
        start_ts = int(datetime(current_start.year, current_start.month, current_start.day, tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime(chunk_end.year, chunk_end.month, chunk_end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        
        params = {
            "vs_currency": vs_currency,
            "from": start_ts,
            "to": end_ts,
            "interval": interval,
            "x_cg_pro_api_key": COINGECKO_API_KEY,
        }
        
        delay = sleep_seconds
        chunk_data = []
        
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(url, params=params, timeout=60, proxies={"http": None, "https": None})
                
                if resp.status_code == 200:
                    data = resp.json()
                    
                    # Data format: [[timestamp_ms, open, high, low, close], ...]
                    for row in data:
                        if len(row) >= 5:
                            ts_ms = row[0]
                            open_price = float(row[1])
                            high_price = float(row[2])
                            low_price = float(row[3])
                            close_price = float(row[4])
                            
                            d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
                            chunk_data.append((d, open_price, high_price, low_price, close_price))
                    
                    all_ohlc_data.extend(chunk_data)
                    time.sleep(sleep_seconds)
                    break  # Success, move to next chunk
                
                elif resp.status_code == 404:
                    # No data for this chunk, continue to next
                    break
                
                elif resp.status_code == 429:
                    print(f"[WARN] Rate limited for {coingecko_id} (429). Backing off for {delay:.1f}s...")
                    time.sleep(delay)
                    delay *= 2.0
                    continue
                
                elif resp.status_code == 401:
                    print(f"[ERROR] Unauthorized (401) for {coingecko_id}. Check API key.")
                    return []
                
                else:
                    error_text = resp.text[:200]
                    if "180 days" in error_text or "31 days" in error_text:
                        # Date range too large, but we're chunking - this shouldn't happen
                        print(f"[WARN] Date range limit hit for {coingecko_id}, skipping chunk")
                        break
                    else:
                        print(f"[ERROR] CoinGecko error for {coingecko_id}: {resp.status_code} {error_text}")
                        time.sleep(sleep_seconds)
                        break  # Skip this chunk and continue
                    
            except Exception as e:
                print(f"[ERROR] Request error for {coingecko_id}: {e}")
                if attempt < max_retries:
                    time.sleep(delay)
                    delay *= 2.0
                else:
                    break  # Skip this chunk and continue
        
        # Move to next chunk
        current_start = chunk_end + timedelta(days=1)
    
    return all_ohlc_data


def fetch_top_gainers_losers(
    duration: str = "24h",  # "1h", "24h", "7d", "14d", "30d", "200d", "1y"
    sleep_seconds: float = 0.12,
) -> Dict[str, List[Dict]]:
    """
    Fetch top 30 gainers and losers.
    
    Returns:
        {
            "gainers": [...],
            "losers": [...]
        }
    """
    url = f"{COINGECKO_BASE}/coins/top_gainers_losers"
    
    params = {
        "vs_currency": "usd",
        "duration": duration,
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        
        # API returns "top_gainers" and "top_losers"
        return {
            "gainers": data.get("top_gainers", []),
            "losers": data.get("top_losers", []),
        }
    except Exception as e:
        print(f"[ERROR] Failed to fetch top gainers/losers: {e}")
        return {"gainers": [], "losers": []}


def fetch_new_listings(
    sleep_seconds: float = 0.12,
) -> List[Dict]:
    """Fetch latest 200 newly listed coins."""
    url = f"{COINGECKO_BASE}/coins/list/new"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch new listings: {e}")
        return []


def fetch_exchange_volume_chart(
    exchange_id: str,
    days: int = 90,
    sleep_seconds: float = 0.12,
) -> List[Tuple[date, float, float]]:
    """
    Fetch exchange volume chart.
    
    Returns list of (date, volume_btc, volume_usd) tuples.
    """
    url = f"{COINGECKO_BASE}/exchanges/{exchange_id}/volume_chart"
    
    params = {
        "days": days,
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        volume_data = []
        for row in data:
            if len(row) >= 2:
                ts_ms = row[0]
                volume_btc = float(row[1]) if len(row) > 1 else 0.0
                volume_usd = float(row[2]) if len(row) > 2 else 0.0
                
                d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
                volume_data.append((d, volume_btc, volume_usd))
        
        time.sleep(sleep_seconds)
        return volume_data
    except Exception as e:
        print(f"[ERROR] Failed to fetch exchange volume for {exchange_id}: {e}")
        return []


def check_api_usage(
    sleep_seconds: float = 0.12,
) -> Dict:
    """Check API usage and rate limits."""
    url = f"{COINGECKO_BASE}/key"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data
    except Exception as e:
        print(f"[ERROR] Failed to check API usage: {e}")
        return {}


def fetch_trending_searches(
    sleep_seconds: float = 0.12,
) -> Dict:
    """
    Fetch trending search coins, NFTs, and categories on CoinGecko in the last 24 hours.
    
    Returns dict with 'coins', 'nfts', and 'categories' lists.
    """
    url = f"{COINGECKO_BASE}/search/trending"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch trending searches: {e}")
        return {}


def fetch_coins_categories(
    sleep_seconds: float = 0.12,
) -> List[Dict]:
    """
    Fetch all coin categories with market data (market cap, volume, etc.).
    
    Returns list of category dicts with market data.
    """
    url = f"{COINGECKO_BASE}/coins/categories"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[ERROR] Failed to fetch coin categories: {e}")
        return []


def fetch_coins_markets(
    vs_currency: str = "usd",
    order: str = "market_cap_desc",
    per_page: int = 250,
    page: int = 1,
    sparkline: bool = False,
    price_change_percentage: Optional[str] = None,
    sleep_seconds: float = 0.12,
) -> List[Dict]:
    """
    Fetch all supported coins with price, market cap, volume and market related data.
    
    Note: This endpoint is paginated (250 coins per page).
    For full market snapshot, may need multiple pages.
    
    Returns list of coin market data dicts.
    """
    url = f"{COINGECKO_BASE}/coins/markets"
    
    params = {
        "vs_currency": vs_currency,
        "order": order,
        "per_page": min(per_page, 250),  # Max 250 per page
        "page": page,
        "sparkline": str(sparkline).lower(),
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    if price_change_percentage:
        params["price_change_percentage"] = price_change_percentage
    
    try:
        resp = requests.get(url, params=params, timeout=60, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[ERROR] Failed to fetch coins markets (page {page}): {e}")
        return []


def fetch_exchange_volume_chart_range(
    exchange_id: str,
    start_date: date,
    end_date: date,
    sleep_seconds: float = 0.12,
    max_retries: int = 5,
) -> List[Tuple[date, float, float]]:
    """
    Fetch historical exchange volume chart data by date range (Analyst tier).
    
    CoinGecko limit: 'from' and 'to' parameters must be within 31 days of each other.
    This function automatically chunks large date ranges into multiple requests.
    
    Returns list of (date, volume_btc, volume_usd) tuples.
    """
    url = f"{COINGECKO_BASE}/exchanges/{exchange_id}/volume_chart/range"
    
    # CoinGecko limit: 31 days per request
    MAX_DAYS = 31
    
    all_volume_data = []
    current_start = start_date
    
    while current_start <= end_date:
        # Calculate chunk end date (max 31 days)
        chunk_end = min(
            date(current_start.year, current_start.month, current_start.day) + timedelta(days=MAX_DAYS - 1),
            end_date
        )
        
        # Convert dates to timestamps
        start_ts = int(datetime(current_start.year, current_start.month, current_start.day, tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime(chunk_end.year, chunk_end.month, chunk_end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        
        params = {
            "from": start_ts,
            "to": end_ts,
            "x_cg_pro_api_key": COINGECKO_API_KEY,
        }
        
        delay = sleep_seconds
        chunk_data = []
        
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(url, params=params, timeout=60, proxies={"http": None, "https": None})
                
                if resp.status_code == 200:
                    data = resp.json()
                    
                    # Data format: [[timestamp_ms, volume_btc, volume_usd], ...]
                    for row in data:
                        if len(row) >= 2:
                            ts_ms = row[0]
                            volume_btc = float(row[1]) if len(row) > 1 else 0.0
                            volume_usd = float(row[2]) if len(row) > 2 else 0.0
                            
                            d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
                            chunk_data.append((d, volume_btc, volume_usd))
                    
                    all_volume_data.extend(chunk_data)
                    time.sleep(sleep_seconds)
                    break  # Success, move to next chunk
                
                elif resp.status_code == 404:
                    # Exchange not found, skip this exchange entirely
                    return []
                
                elif resp.status_code == 429:
                    print(f"[WARN] Rate limited for {exchange_id} (429). Backing off for {delay:.1f}s...")
                    time.sleep(delay)
                    delay *= 2.0
                    continue
                
                elif resp.status_code == 401:
                    print(f"[ERROR] Unauthorized (401) for {exchange_id}. Check API key.")
                    return []
                
                else:
                    error_text = resp.text[:200]
                    if "31 days" in error_text:
                        # Date range too large, but we're chunking - this shouldn't happen
                        print(f"[WARN] Date range limit hit for {exchange_id}, skipping chunk")
                        break
                    else:
                        print(f"[ERROR] CoinGecko error for {exchange_id}: {resp.status_code} {error_text}")
                        time.sleep(sleep_seconds)
                        break  # Skip this chunk and continue
                    
            except Exception as e:
                print(f"[ERROR] Request error for {exchange_id}: {e}")
                if attempt < max_retries:
                    time.sleep(delay)
                    delay *= 2.0
                else:
                    break  # Skip this chunk and continue
        
        # Move to next chunk
        current_start = chunk_end + timedelta(days=1)
    
    return all_volume_data


def fetch_exchanges_list(
    sleep_seconds: float = 0.12,
) -> List[Dict]:
    """
    Fetch all supported exchanges with trading volumes and exchange data.
    
    Returns list of exchange dicts with ID, name, country, trading volumes, etc.
    """
    url = f"{COINGECKO_BASE}/exchanges"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=60, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[ERROR] Failed to fetch exchanges list: {e}")
        return []


def fetch_derivative_exchange_details(
    exchange_id: str,
    sleep_seconds: float = 0.12,
    max_retries: int = 5,
) -> Optional[Dict]:
    """
    Fetch specific derivative exchange details.
    
    Returns exchange dict with ID, name, open interest, volume, etc.
    """
    url = f"{COINGECKO_BASE}/derivatives/exchanges/{exchange_id}"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    delay = sleep_seconds
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
            
            if resp.status_code == 200:
                data = resp.json()
                time.sleep(sleep_seconds)
                return data
            
            elif resp.status_code == 404:
                print(f"[WARN] Derivative exchange {exchange_id} not found (404).")
                return None
            
            elif resp.status_code == 429:
                print(f"[WARN] Rate limited for {exchange_id} (429). Backing off for {delay:.1f}s...")
                time.sleep(delay)
                delay *= 2.0
                continue
            
            elif resp.status_code == 401:
                print(f"[ERROR] Unauthorized (401) for {exchange_id}. Check API key.")
                return None
            
            else:
                print(f"[ERROR] CoinGecko error for {exchange_id}: {resp.status_code} {resp.text[:200]}")
                time.sleep(sleep_seconds)
                return None
                
        except Exception as e:
            print(f"[ERROR] Request error for {exchange_id}: {e}")
            if attempt < max_retries:
                time.sleep(delay)
                delay *= 2.0
            else:
                return None
    
    return None


def fetch_categories_list(
    sleep_seconds: float = 0.12,
) -> List[Dict]:
    """
    Fetch all coin categories list (metadata only, no market data).
    
    Returns list of category dicts with ID, name, etc.
    """
    url = f"{COINGECKO_BASE}/coins/categories/list"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=60, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[ERROR] Failed to fetch categories list: {e}")
        return []


def fetch_exchange_details(
    exchange_id: str,
    sleep_seconds: float = 0.12,
    max_retries: int = 5,
) -> Optional[Dict]:
    """
    Fetch exchange details including tickers.
    
    Returns exchange dict with ID, name, tickers, etc.
    """
    url = f"{COINGECKO_BASE}/exchanges/{exchange_id}"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    delay = sleep_seconds
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=60, proxies={"http": None, "https": None})
            
            if resp.status_code == 200:
                data = resp.json()
                time.sleep(sleep_seconds)
                return data
            
            elif resp.status_code == 404:
                print(f"[WARN] Exchange {exchange_id} not found (404).")
                return None
            
            elif resp.status_code == 429:
                print(f"[WARN] Rate limited for {exchange_id} (429). Backing off for {delay:.1f}s...")
                time.sleep(delay)
                delay *= 2.0
                continue
            
            elif resp.status_code == 401:
                print(f"[ERROR] Unauthorized (401) for {exchange_id}. Check API key.")
                return None
            
            else:
                print(f"[ERROR] CoinGecko error for {exchange_id}: {resp.status_code} {resp.text[:200]}")
                time.sleep(sleep_seconds)
                return None
                
        except Exception as e:
            print(f"[ERROR] Request error for {exchange_id}: {e}")
            if attempt < max_retries:
                time.sleep(delay)
                delay *= 2.0
            else:
                return None
    
    return None


def fetch_derivatives_exchanges_list(
    sleep_seconds: float = 0.12,
) -> List[Dict]:
    """
    Fetch list of all derivative exchanges (metadata only).
    
    Returns list of exchange dicts with ID, name, etc.
    Note: This is different from /derivatives/exchanges which includes market data.
    """
    url = f"{COINGECKO_BASE}/derivatives/exchanges/list"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=60, proxies={"http": None, "https": None})
        resp.raise_for_status()
        data = resp.json()
        
        time.sleep(sleep_seconds)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[ERROR] Failed to fetch derivatives exchanges list: {e}")
        return []
