#!/usr/bin/env python3
"""
Long-term funding regime monitor for BTC-long / Alt-short strategy.

Fetches funding data from CoinGlass and computes a slow "funding regime" score.
"""

import os
import sys
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import requests
import pandas as pd
import numpy as np

# ============================================================================
# Configuration
# ============================================================================

COINGLASS_API_KEY = os.environ.get("COINGLASS_API_KEY", "")  # Set in env; never commit keys
COINGLASS_BASE = "https://open-api-v4.coinglass.com"
API_SLEEP_SECONDS = 3
HISTORICAL_CHUNK_DAYS = 90
CSV_FILENAME = "longterm_funding_history.csv"

# Alt basket symbols (~100-150 names)
ALT_SYMBOLS = [
    "ETH", "XRP", "BNB", "SOL", "TRX", "ADA", "DOGE", "BCH", "LINK", "AVAX",
    "ARB", "OP", "TIA", "INJ", "NEAR", "APT", "SUI", "JUP", "PYTH", "SEI",
    "MATIC", "DOT", "LTC", "UNI", "ATOM", "ETC", "XLM", "FIL", "ICP", "ALGO",
    "VET", "THETA", "EOS", "AAVE", "MKR", "GRT", "SNX", "COMP", "YFI", "SUSHI",
    "CRV", "1INCH", "BAL", "REN", "ZRX", "BAT", "ZEC", "DASH", "XMR", "ENJ",
    "MANA", "SAND", "AXS", "GALA", "CHZ", "FLOW", "ICP", "HBAR", "EGLD", "FTM",
    "ONE", "HARMONY", "LUNA", "UST", "WAVES", "KSM", "DOT", "ROSE", "CELO", "IOTA",
    "QTUM", "NEO", "ONT", "ZIL", "SC", "STORJ", "ANKR", "RUNE", "OCEAN", "ALPHA",
    "KAVA", "BAND", "CTSI", "OMG", "SKL", "LRC", "ZEN", "COTI", "FET", "RLC",
    "PERP", "UMA", "BADGER", "FIS", "BONK", "WIF", "PEPE", "FLOKI", "SHIB", "LUNC",
    "1000SATS", "ORDI", "RATS", "BOME", "MYRO", "POPCAT", "MEW", "GME", "TRUMP", "BIDEN"
]

L_LEVEL = 360  # Target lookback for level
L_TIME = 180   # Lookback for time in regime

# ============================================================================
# CoinGlass API Helper
# ============================================================================

def coinglass_get(url: str, params: Optional[Dict] = None, timeout: int = 10) -> Optional[Dict]:
    """
    Make a GET request to CoinGlass API with rate limiting and retry logic.
    
    Args:
        url: Full URL or path (will be joined with COINGLASS_BASE if relative)
        params: Query parameters
        timeout: Request timeout in seconds
        
    Returns:
        Parsed JSON dict or None on failure
    """
    # Sleep before each request to respect rate limits
    time.sleep(API_SLEEP_SECONDS)
    
    # Construct full URL
    if url.startswith("http"):
        full_url = url
    else:
        full_url = f"{COINGLASS_BASE}{url}"
    
    headers = {
        "CG-API-KEY": COINGLASS_API_KEY
    }
    
    try:
        response = requests.get(full_url, headers=headers, params=params, timeout=timeout)
        
        # Handle 429 with retry
        if response.status_code == 429:
            print(f"Warning: Rate limited (429), retrying after longer sleep...")
            time.sleep(API_SLEEP_SECONDS * 3)
            response = requests.get(full_url, headers=headers, params=params, timeout=timeout)
        
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from {url}: {e}")
        return None


# ============================================================================
# Live Funding Snapshot
# ============================================================================

def fetch_live_funding(symbol: str) -> Optional[float]:
    """
    Fetch today's funding rate snapshot for a symbol.
    
    Returns the arithmetic mean of funding rates across exchanges, or None if failed.
    """
    url = "/api/futures/funding-rate/exchange-list"
    params = {"symbol": symbol}
    
    data = coinglass_get(url, params)
    if not data or not isinstance(data, dict):
        return None
    
    # Look for the symbol entry
    if "data" not in data:
        return None
    
    data_list = data["data"]
    if not isinstance(data_list, list):
        return None
    
    # Find matching symbol entry
    symbol_entry = None
    for entry in data_list:
        if isinstance(entry, dict) and entry.get("symbol") == symbol:
            symbol_entry = entry
            break
    
    if not symbol_entry:
        return None
    
    # Extract stablecoin_margin_list
    stablecoin_list = symbol_entry.get("stablecoin_margin_list", [])
    if not isinstance(stablecoin_list, list) or len(stablecoin_list) == 0:
        return None
    
    # Collect funding rates
    funding_rates = []
    for exchange in stablecoin_list:
        if isinstance(exchange, dict):
            rate = exchange.get("funding_rate")
            if rate is not None:
                try:
                    funding_rates.append(float(rate))
                except (ValueError, TypeError):
                    pass
    
    if len(funding_rates) == 0:
        return None
    
    return np.mean(funding_rates)


def fetch_live_snapshot() -> Tuple[Optional[float], Dict[str, Optional[float]], float]:
    """
    Fetch live funding snapshot for BTC and all alts.
    
    Returns:
        (btc_funding, alt_funding_dict, alt_coverage)
    """
    print("Fetching live funding snapshot...")
    
    # Fetch BTC
    btc_funding = fetch_live_funding("BTC")
    if btc_funding is None:
        print("Warning: Failed to fetch BTC funding")
    
    # Fetch alts
    alt_funding = {}
    valid_count = 0
    
    for symbol in ALT_SYMBOLS:
        rate = fetch_live_funding(symbol)
        alt_funding[symbol] = rate
        if rate is not None:
            valid_count += 1
        print(f"  {symbol}: {rate if rate is not None else 'N/A'}")
    
    alt_coverage = valid_count / len(ALT_SYMBOLS) if len(ALT_SYMBOLS) > 0 else 0.0
    
    # Compute average alt funding
    valid_rates = [r for r in alt_funding.values() if r is not None]
    avg_alt_funding = np.mean(valid_rates) if len(valid_rates) > 0 else None
    
    return btc_funding, avg_alt_funding, alt_coverage


# ============================================================================
# Historical Funding (Backfill)
# ============================================================================

def fetch_historical_funding_chunk(symbol: str, start_date: str, end_date: str) -> Dict[str, float]:
    """
    Fetch historical funding for a symbol in a date range.
    
    Args:
        symbol: Symbol to fetch
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD, inclusive)
        
    Returns:
        Dict mapping date (YYYY-MM-DD) to daily average funding rate
    """
    url = "/api/futures/funding-rate/oi-weight-history"
    
    # Convert dates to timestamps (milliseconds)
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        # End date should be end of day
        end_dt = end_dt.replace(hour=23, minute=59, second=59)
        
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
    except ValueError as e:
        print(f"Error parsing dates {start_date} to {end_date}: {e}")
        return {}
    
    params = {
        "symbol": symbol,
        "interval": "8h",
        "startTime": start_ts,
        "endTime": end_ts
    }
    
    data = coinglass_get(url, params)
    if not data or not isinstance(data, dict):
        return {}
    
    if "data" not in data:
        return {}
    
    candles = data["data"]
    if not isinstance(candles, list):
        return {}
    
    # Group 8h candles by UTC date and compute daily average
    daily_rates = {}
    
    for candle in candles:
        if not isinstance(candle, dict):
            continue
        
        # Extract timestamp and close (funding rate)
        try:
            ts_ms = candle.get("time")
            close_rate = candle.get("close")
            
            if ts_ms is None or close_rate is None:
                continue
            
            # Convert timestamp to date
            ts_sec = ts_ms / 1000
            dt = datetime.utcfromtimestamp(ts_sec)
            date_str = dt.strftime("%Y-%m-%d")
            
            # Accumulate rates per date
            if date_str not in daily_rates:
                daily_rates[date_str] = []
            
            daily_rates[date_str].append(float(close_rate))
            
        except (ValueError, TypeError, KeyError) as e:
            continue
    
    # Compute daily averages
    result = {}
    for date_str, rates in daily_rates.items():
        if len(rates) > 0:
            result[date_str] = np.mean(rates)
    
    return result


def fetch_historical_funding(symbol: str, num_days: int) -> Dict[str, float]:
    """
    Fetch historical funding for a symbol, chunking requests if needed.
    
    Args:
        symbol: Symbol to fetch
        num_days: Number of days to fetch (up to yesterday)
        
    Returns:
        Dict mapping date (YYYY-MM-DD) to daily average funding rate
    """
    # Calculate date range (up to yesterday)
    end_date = datetime.utcnow() - timedelta(days=1)
    start_date = end_date - timedelta(days=num_days - 1)
    
    # Chunk into HISTORICAL_CHUNK_DAYS periods
    all_dates = {}
    current_start = start_date
    
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=HISTORICAL_CHUNK_DAYS - 1), end_date)
        
        start_str = current_start.strftime("%Y-%m-%d")
        end_str = current_end.strftime("%Y-%m-%d")
        
        print(f"  Fetching {symbol} from {start_str} to {end_str}...")
        chunk_data = fetch_historical_funding_chunk(symbol, start_str, end_str)
        
        # Merge (later dates override earlier if duplicate)
        for date, rate in chunk_data.items():
            all_dates[date] = rate
        
        current_start = current_end + timedelta(days=1)
    
    return all_dates


def backfill_data(num_days: int) -> pd.DataFrame:
    """
    Backfill historical funding data for BTC and alts.
    
    Returns:
        DataFrame with columns: date, f_btc, f_alt, alt_coverage
    """
    print(f"Backfilling {num_days} days of historical data...")
    
    # Fetch BTC
    print("Fetching BTC historical funding...")
    btc_data = fetch_historical_funding("BTC", num_days)
    
    # Fetch alts
    print(f"Fetching historical funding for {len(ALT_SYMBOLS)} alt symbols...")
    alt_data_by_symbol = {}
    
    for symbol in ALT_SYMBOLS:
        alt_data_by_symbol[symbol] = fetch_historical_funding(symbol, num_days)
        print(f"  {symbol}: {len(alt_data_by_symbol[symbol])} days")
    
    # Build date range
    end_date = datetime.utcnow() - timedelta(days=1)
    start_date = end_date - timedelta(days=num_days - 1)
    
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    # Build DataFrame
    rows = []
    for date in dates:
        f_btc = btc_data.get(date)
        
        # Collect alt rates for this date
        alt_rates = []
        for symbol in ALT_SYMBOLS:
            rate = alt_data_by_symbol[symbol].get(date)
            if rate is not None:
                alt_rates.append(rate)
        
        f_alt = np.mean(alt_rates) if len(alt_rates) > 0 else None
        alt_coverage = len(alt_rates) / len(ALT_SYMBOLS) if len(ALT_SYMBOLS) > 0 else 0.0
        
        rows.append({
            "date": date,
            "f_btc": f_btc,
            "f_alt": f_alt,
            "alt_coverage": alt_coverage
        })
    
    df = pd.DataFrame(rows)
    return df


def backfill_missing_dates(end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Backfill only missing dates in the existing CSV file.
    This is more efficient than full backfill as it only fetches missing data.
    
    Args:
        end_date: End date to check up to (YYYY-MM-DD), defaults to yesterday
        
    Returns:
        Updated DataFrame with missing dates filled
    """
    print("=== Long-term Funding Monitor (BACKFILL MISSING) ===")
    
    # Load existing data
    existing_df = load_csv()
    
    # Find missing dates
    last_date, missing_dates = find_missing_dates(existing_df, end_date)
    
    if not missing_dates:
        print("[INFO] No missing dates to backfill.")
        return existing_df
    
    if last_date is None:
        print(
            "[INFO] No existing data found. "
            "Use 'backfill N' mode for initial backfill."
        )
        return existing_df
    
    # Calculate how many days we need to fetch
    # We need from earliest missing date to latest missing date
    try:
        earliest_missing = min(missing_dates)
        latest_missing = max(missing_dates)
        earliest_d = datetime.strptime(earliest_missing, "%Y-%m-%d").date()
        latest_d = datetime.strptime(latest_missing, "%Y-%m-%d").date()
        
        # Calculate days from today to earliest missing date
        today = datetime.utcnow().date()
        days_to_fetch = (today - earliest_d).days + 1
        
        print(f"Fetching data for {len(missing_dates)} missing dates "
              f"(from {earliest_missing} to {latest_missing}, {days_to_fetch} days from today)...")
        
        # Fetch historical data (this will fetch more than needed, but that's okay)
        # We'll filter to only missing dates
        print("Fetching BTC historical funding...")
        btc_data = fetch_historical_funding("BTC", days_to_fetch)
        
        print(f"Fetching historical funding for {len(ALT_SYMBOLS)} alt symbols...")
        alt_data_by_symbol = {}
        
        for symbol in ALT_SYMBOLS:
            alt_data_by_symbol[symbol] = fetch_historical_funding(symbol, days_to_fetch)
        
        # Build DataFrame with only missing dates
        rows = []
        for date in missing_dates:
            f_btc = btc_data.get(date)
            
            # Collect alt rates for this date
            alt_rates = []
            for symbol in ALT_SYMBOLS:
                rate = alt_data_by_symbol[symbol].get(date)
                if rate is not None:
                    alt_rates.append(rate)
            
            f_alt = np.mean(alt_rates) if len(alt_rates) > 0 else None
            alt_coverage = len(alt_rates) / len(ALT_SYMBOLS) if len(ALT_SYMBOLS) > 0 else 0.0
            
            rows.append({
                "date": date,
                "f_btc": f_btc,
                "f_alt": f_alt,
                "alt_coverage": alt_coverage
            })
        
        new_df = pd.DataFrame(rows)
        
        if new_df.empty:
            print("[WARN] No data fetched for missing dates.")
            return existing_df
        
        # Merge with existing data
        merged_df = merge_dataframes(existing_df, new_df)
        
        # Recompute regime metrics for all data (force recompute to fill in missing dates)
        print("Recomputing regime metrics...")
        merged_df = compute_regime_metrics(merged_df, force_recompute=True)
        
        # Verify regime_bucket was computed for new dates
        new_dates_with_regime = merged_df[merged_df["date"].isin(missing_dates) & merged_df["regime_bucket"].notna()]
        print(f"Computed regime_bucket for {len(new_dates_with_regime)} out of {len(missing_dates)} new dates")
        if len(new_dates_with_regime) < len(missing_dates):
            missing_with_data = merged_df[merged_df["date"].isin(missing_dates) & merged_df["f_alt"].notna()]
            print(f"  - {len(missing_with_data)} new dates have valid f_alt data")
            if len(missing_with_data) > 0:
                print(f"  - Note: Regime metrics require at least {L_LEVEL} days of history before a date")
        
        print(f"=== Backfill complete: added {len(missing_dates)} days ===")
        
        return merged_df
        
    except Exception as e:
        print(f"[ERROR] Error during backfill: {e}")
        import traceback
        traceback.print_exc()
        return existing_df


# ============================================================================
# CSV Management
# ============================================================================

def load_csv() -> pd.DataFrame:
    """Load existing CSV or return empty DataFrame."""
    try:
        df = pd.read_csv(CSV_FILENAME)
        # Ensure date is string
        df["date"] = df["date"].astype(str)
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=["date", "f_btc", "f_alt", "alt_coverage",
                                     "p_level", "frac_hot_180", "frac_cold_180",
                                     "struct_hot", "early_hot", "struct_cold",
                                     "score_raw", "funding_regime_long", "regime_bucket"])


def find_missing_dates(df: pd.DataFrame, end_date: Optional[str] = None) -> Tuple[Optional[str], List[str]]:
    """
    Find ALL missing dates in the DataFrame, including:
    - Gaps between consecutive dates in the middle of the data
    - Missing dates from the last entry to end_date (default: today)
    
    Args:
        df: DataFrame with 'date' column (YYYY-MM-DD format)
        end_date: End date to check up to (YYYY-MM-DD), defaults to yesterday
        
    Returns:
        (last_date_in_df, list_of_all_missing_dates)
        If no data exists, returns (None, [])
    """
    if df.empty or "date" not in df.columns:
        print(f"[INFO] No existing data found. All dates are missing.")
        return None, []
    
    # Parse all existing dates
    existing_dates = set()
    last_date = None
    first_date = None
    
    for date_str in df["date"].values:
        if pd.isna(date_str) or not date_str:
            continue
        try:
            d = datetime.strptime(str(date_str), "%Y-%m-%d").date()
            existing_dates.add(str(date_str))
            if first_date is None or str(date_str) < first_date:
                first_date = str(date_str)
            if last_date is None or str(date_str) > last_date:
                last_date = str(date_str)
        except (ValueError, TypeError):
            continue
    
    if not existing_dates:
        print(f"[WARN] DataFrame exists but no valid dates found. Treating as empty.")
        return None, []
    
    # Determine end date (default: yesterday UTC)
    if end_date is None:
        end_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Find all missing dates:
    # 1. Gaps between consecutive dates in the existing data
    # 2. Missing dates from last_date to end_date
    
    missing_dates = []
    
    # Sort existing dates to find gaps
    sorted_dates = sorted(existing_dates)
    
    # Check for gaps between consecutive dates
    for i in range(len(sorted_dates) - 1):
        current_date_str = sorted_dates[i]
        next_date_str = sorted_dates[i + 1]
        try:
            current_d = datetime.strptime(current_date_str, "%Y-%m-%d").date()
            next_d = datetime.strptime(next_date_str, "%Y-%m-%d").date()
            # If there's a gap (more than 1 day difference)
            gap = (next_d - current_d).days
            if gap > 1:
                # Add all dates in between
                for day_offset in range(1, gap):
                    missing_date = current_d + timedelta(days=day_offset)
                    missing_dates.append(missing_date.strftime("%Y-%m-%d"))
        except (ValueError, TypeError):
            continue
    
    # Add missing dates from last_date to end_date
    try:
        last_d = datetime.strptime(last_date, "%Y-%m-%d").date()
        end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        if last_d < end_d:
            current = last_d + timedelta(days=1)
            while current <= end_d:
                date_str = current.strftime("%Y-%m-%d")
                if date_str not in existing_dates:
                    missing_dates.append(date_str)
                current += timedelta(days=1)
    except (ValueError, TypeError):
        pass
    
    # Remove duplicates and sort
    missing_dates = sorted(list(set(missing_dates)))
    
    if missing_dates:
        print(
            f"[INFO] Found {len(missing_dates)} missing days total:\n"
            f"  - First existing date: {first_date}\n"
            f"  - Last existing date: {last_date}\n"
            f"  - Missing date range: {missing_dates[0]} to {missing_dates[-1]}"
        )
    else:
        if last_date and datetime.strptime(last_date, "%Y-%m-%d").date() >= datetime.strptime(end_date, "%Y-%m-%d").date():
            print(f"[INFO] Data is up to date. Last entry: {last_date}, End date: {end_date}")
        else:
            print(f"[INFO] No gaps found in existing data. Last entry: {last_date}, End date: {end_date}")
    
    return last_date, missing_dates


def save_csv(df: pd.DataFrame):
    """Save DataFrame to CSV."""
    df.to_csv(CSV_FILENAME, index=False)


def merge_dataframes(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """
    Merge new data into existing DataFrame.
    
    On date conflicts, keep the latest values from new.
    Regime columns are preserved from existing but will be recomputed.
    """
    if existing.empty:
        return new.copy()
    
    if new.empty:
        return existing.copy()
    
    # Merge on date, keeping new values on conflict
    merged = pd.merge(existing, new, on="date", how="outer", suffixes=("_old", "_new"))
    
    # For columns that exist in both, prefer _new
    result_cols = ["date"]
    for col in ["f_btc", "f_alt", "alt_coverage"]:
        if f"{col}_new" in merged.columns:
            merged[col] = merged[f"{col}_new"].fillna(merged.get(f"{col}_old", np.nan))
        elif f"{col}_old" in merged.columns:
            merged[col] = merged[f"{col}_old"]
        else:
            merged[col] = np.nan
        result_cols.append(col)
    
    # Initialize regime columns (they will be recomputed by compute_regime_metrics)
    regime_cols = ["p_level", "frac_hot_180", "frac_cold_180", "struct_hot",
                   "early_hot", "struct_cold", "score_raw", "funding_regime_long", "regime_bucket"]
    for col in regime_cols:
        if col in existing.columns:
            # For existing dates, keep old regime values (will be recomputed anyway)
            if f"{col}_old" in merged.columns:
                merged[col] = merged[f"{col}_old"]
            else:
                merged[col] = np.nan
        else:
            # Initialize new column
            merged[col] = np.nan
        result_cols.append(col)
    
    merged = merged[result_cols]
    
    # Sort by date
    merged = merged.sort_values("date").reset_index(drop=True)
    
    return merged


# ============================================================================
# Regime Calculation
# ============================================================================

def compute_regime_metrics(df: pd.DataFrame, force_recompute: bool = False) -> pd.DataFrame:
    """
    Compute long-term funding regime metrics for all dates.
    
    Only computes metrics for dates where f_alt is not NaN and sufficient history exists.
    
    Args:
        df: DataFrame with funding data
        force_recompute: If True, recompute all metrics even if they already exist
    """
    df = df.copy()
    
    # Initialize regime columns if they don't exist
    regime_cols = ["p_level", "frac_hot_180", "frac_cold_180", "struct_hot",
                   "early_hot", "struct_cold", "score_raw", "funding_regime_long", "regime_bucket"]
    for col in regime_cols:
        if col not in df.columns:
            df[col] = np.nan
    
    # Filter to rows with valid f_alt
    valid_mask = df["f_alt"].notna()
    valid_df = df[valid_mask].copy().reset_index(drop=True)
    
    n_valid = len(valid_df)
    
    if n_valid < 60:
        print(f"Warning: Only {n_valid} valid days (need at least 60) - skipping regime calculation")
        return df
    
    # Use L_LEVEL as target, but allow computing with less history if we don't have enough
    # Minimum history required: 60 days (for basic metrics)
    # Preferred: L_LEVEL (360 days) for full accuracy
    L_eff = min(L_LEVEL, n_valid)
    min_history = 60  # Minimum days needed to compute metrics
    
    # Count how many dates will get computed
    # We can compute for dates where we have at least min_history days of history
    dates_to_compute = max(0, n_valid - min_history + 1)
    print(f"Computing regime metrics for {dates_to_compute} dates (using {L_eff} days lookback, minimum {min_history} days required)")
    
    # First pass: compute all funding_regime_long values and store them
    computed_values = []  # List of (date_val, all_metrics_dict)
    
    for i in range(n_valid):
        # Use min_history as minimum, but prefer L_eff if available
        if i < min_history - 1:
            continue  # Skip early dates without minimum history
        
        # Determine actual lookback window for this date
        actual_lookback = min(L_eff, i + 1)
        
        # Level window (last actual_lookback days, up to L_eff)
        level_start = max(0, i - actual_lookback + 1)
        W_level = valid_df.loc[level_start:i+1, "f_alt"].values
        
        # Percentile of current level
        current_f_alt = valid_df.loc[i, "f_alt"]
        if len(W_level) > 1:
            rank = np.sum(W_level <= current_f_alt)
            p_level = max(0.0, min(1.0, (rank - 1) / (len(W_level) - 1)))
        else:
            p_level = 0.5
        
        # Quartiles
        if len(W_level) > 0:
            q25 = np.percentile(W_level, 25)
            q75 = np.percentile(W_level, 75)
        else:
            q25 = current_f_alt
            q75 = current_f_alt
        
        # Time-in-regime window (last L_TIME days, or available history)
        time_lookback = min(L_TIME, i + 1)
        time_start = max(0, i - time_lookback + 1)
        W_time = valid_df.loc[time_start:i+1, "f_alt"].values
        
        # Fractions
        frac_hot_180 = np.sum(W_time >= q75) / len(W_time)
        frac_cold_180 = np.sum(W_time <= q25) / len(W_time)
        
        # Structural features
        struct_hot = 0.5 * p_level + 0.5 * frac_hot_180
        early_hot = max(0.0, p_level - frac_hot_180)
        struct_cold = frac_cold_180
        
        # Raw score
        score_raw = 0.6 * struct_hot - 0.3 * struct_cold - 0.3 * early_hot
        
        # Normalize to [0,1] and flip to risk
        F_fav = (score_raw + 0.6) / 1.2
        F_fav = max(0.0, min(1.0, F_fav))
        funding_regime_long = 1.0 - F_fav
        
        date_val = valid_df.loc[i, "date"]
        if not isinstance(date_val, str):
            date_val = str(date_val)
        
        computed_values.append({
            "date": date_val,
            "p_level": p_level,
            "frac_hot_180": frac_hot_180,
            "frac_cold_180": frac_cold_180,
            "struct_hot": struct_hot,
            "early_hot": early_hot,
            "struct_cold": struct_cold,
            "score_raw": score_raw,
            "funding_regime_long": funding_regime_long,
        })
    
    if len(computed_values) == 0:
        print("No dates computed")
        return df
    
    # Calculate percentile-based thresholds from actual distribution
    funding_regime_vals = [v["funding_regime_long"] for v in computed_values]
    threshold_low = np.percentile(funding_regime_vals, 33.33)   # 33rd percentile
    threshold_high = np.percentile(funding_regime_vals, 66.67)  # 67th percentile
    
    print(f"Using adaptive thresholds: GREEN < {threshold_low:.4f}, AMBER < {threshold_high:.4f}, RED >= {threshold_high:.4f}")
    
    # Second pass: assign buckets and store back
    computed_count = 0
    for metrics in computed_values:
        date_val = metrics["date"]
        funding_regime_long = metrics["funding_regime_long"]
        
        # Assign bucket based on percentile thresholds
        if funding_regime_long < threshold_low:
            regime_bucket = "GREEN"
        elif funding_regime_long < threshold_high:
            regime_bucket = "AMBER"
        else:
            regime_bucket = "RED"
        
        # Find matching rows in original df
        mask = df["date"].astype(str) == date_val
        
        if mask.any():
            # Only update if force_recompute is True or if the value is currently NaN
            if force_recompute or pd.isna(df.loc[mask, "regime_bucket"].iloc[0]):
                df.loc[mask, "p_level"] = metrics["p_level"]
                df.loc[mask, "frac_hot_180"] = metrics["frac_hot_180"]
                df.loc[mask, "frac_cold_180"] = metrics["frac_cold_180"]
                df.loc[mask, "struct_hot"] = metrics["struct_hot"]
                df.loc[mask, "early_hot"] = metrics["early_hot"]
                df.loc[mask, "struct_cold"] = metrics["struct_cold"]
                df.loc[mask, "score_raw"] = metrics["score_raw"]
                df.loc[mask, "funding_regime_long"] = funding_regime_long
                df.loc[mask, "regime_bucket"] = regime_bucket
                computed_count += 1
    
    print(f"Computed regime metrics for {computed_count} dates")
    return df


# ============================================================================
# Output Formatting
# ============================================================================

def get_interpretation(bucket: str) -> str:
    """Get interpretation string for regime bucket."""
    if bucket == "GREEN":
        return "structurally favourable: alts have been positively funded and crowded for a long time."
    elif bucket == "AMBER":
        return "mixed/neutral: funding not clearly supportive or dangerous."
    elif bucket == "RED":
        return "structurally risky: alts have been cheap on funding or you are early in a hot regime."
    else:
        return "unknown regime"


def compute_size_multiplier(funding_regime_long: float) -> float:
    """Compute size multiplier from regime score."""
    return 0.5 + (1.0 - funding_regime_long)


def print_summary(df: pd.DataFrame, mode: str = "LIVE"):
    """Print summary of latest regime metrics."""
    if df.empty:
        print("No data available.")
        return
    
    # Get latest row with regime data
    latest = df[df["funding_regime_long"].notna()].tail(1)
    
    if latest.empty:
        print("No regime data computed yet (need at least 60 valid days).")
        return
    
    row = latest.iloc[0]
    date = row["date"]
    regime_score = row["funding_regime_long"]
    bucket = row["regime_bucket"]
    
    size_mult = compute_size_multiplier(regime_score)
    interpretation = get_interpretation(bucket)
    
    print("\n" + "=" * 60)
    print(f"LONG-TERM FUNDING REGIME ({mode})")
    print("=" * 60)
    print(f"Date: {date}")
    print(f"Long-term funding regime score: {regime_score:.2f} ({bucket})")
    print(f"Interpretation: {interpretation}")
    print(f"Suggested size multiplier vs base: {size_mult:.2f}x")
    print("=" * 60 + "\n")


# ============================================================================
# Main CLI
# ============================================================================

def main():
    """Main entry point."""
    args = sys.argv[1:]
    
    if len(args) == 0 or (len(args) == 1 and args[0] == "live"):
        # LIVE mode
        print("Running in LIVE mode...")
        
        # Fetch live snapshot
        btc_funding, alt_funding, alt_coverage = fetch_live_snapshot()
        
        if btc_funding is None and alt_funding is None:
            print("Error: Failed to fetch any funding data")
            return
        
        # Get today's date
        today = datetime.utcnow().strftime("%Y-%m-%d")
        
        # Create new row
        new_row = pd.DataFrame([{
            "date": today,
            "f_btc": btc_funding,
            "f_alt": alt_funding,
            "alt_coverage": alt_coverage
        }])
        
        # Load existing CSV
        existing_df = load_csv()
        
        # Merge
        merged_df = merge_dataframes(existing_df, new_row)
        
        # Compute regime metrics (force recompute to ensure all dates are computed)
        merged_df = compute_regime_metrics(merged_df, force_recompute=True)
        
        # Save
        save_csv(merged_df)
        
        # Print summary
        print_summary(merged_df, "LIVE")
        
    elif len(args) == 2 and args[0] == "backfill":
        # BACKFILL mode (full backfill for N days)
        try:
            num_days = int(args[1])
        except ValueError:
            print(f"Error: Invalid number of days: {args[1]}")
            return
        
        print(f"Running in BACKFILL mode for {num_days} days...")
        
        # Fetch historical data
        new_df = backfill_data(num_days)
        
        if new_df.empty:
            print("Error: No historical data fetched")
            return
        
        # Load existing CSV
        existing_df = load_csv()
        
        # Merge
        merged_df = merge_dataframes(existing_df, new_df)
        
        # Compute regime metrics (force recompute to ensure all dates are computed)
        merged_df = compute_regime_metrics(merged_df, force_recompute=True)
        
        # Save
        save_csv(merged_df)
        
        print(f"Saved {len(merged_df)} total days to {CSV_FILENAME}")
        
        # Print summary
        print_summary(merged_df, "BACKFILL")
        
    elif len(args) == 1 and (args[0] == "backfill" or args[0] == "update"):
        # BACKFILL MISSING mode (only fills gaps)
        existing_df = load_csv()
        
        # Backfill missing dates
        merged_df = backfill_missing_dates()
        
        # Save
        save_csv(merged_df)
        
        print(f"Saved {len(merged_df)} total days to {CSV_FILENAME}")
        
        # Print summary
        print_summary(merged_df, "BACKFILL MISSING")
        
    else:
        print("Usage:")
        print("  python longterm_monitor.py          → live mode")
        print("  python longterm_monitor.py live     → live mode")
        print("  python longterm_monitor.py backfill N → backfill last N days (full)")
        print("  python longterm_monitor.py backfill  → backfill only missing dates")
        print("  python longterm_monitor.py update    → backfill only missing dates")
        return


if __name__ == "__main__":
    main()

