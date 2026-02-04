"""
Backfill ETH price data for 2024.

This script:
1. Downloads ETH data for 2024 from CoinGecko
2. Adds it to the wide format prices_daily.parquet
3. Converts and appends to fact_price.parquet
"""
import sys
from pathlib import Path
from datetime import date
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from src.providers.coingecko import fetch_price_history
from src.data_lake.mapping import generate_asset_id

print("=" * 80)
print("BACKFILL ETH DATA FOR 2024")
print("=" * 80)

# Step 1: Download ETH data for 2024
# Basic plan allows 2 years historical, so we can query 2024 directly
# But to be safe and respect rate limits, we'll query in chunks
print("\n[Step 1] Downloading ETH data for 2024 from CoinGecko...")
print("  Note: Respecting 250 calls/min rate limit (0.25s between calls)")

# Basic plan allows "within the past 2 years" - as of 2026-01-27, that's from ~2024-01-27
# The fetch_price_history function uses offset_days=-2, so we need to account for that
# Let's use 2024-02-01 as a safe start date (well within 2-year window)
from datetime import datetime, timedelta
two_years_ago = datetime.now().date() - timedelta(days=730)  # ~2 years
# Add 3 days buffer to account for the -2 day offset in fetch_price_history
effective_start = max(date(2024, 2, 1), two_years_ago + timedelta(days=3))

print(f"  Note: Basic plan limits to past 2 years (from {two_years_ago})")
print(f"  Will query from {effective_start} to {date(2024, 12, 31)}")

# Query in 6-month chunks to be safe with API limits
chunks = [
    (effective_start, date(2024, 6, 30)),
    (date(2024, 7, 1), date(2024, 12, 31)),
]

all_prices = {}
all_mcaps = {}
all_volumes = {}

for chunk_start, chunk_end in chunks:
    print(f"  Fetching {chunk_start} to {chunk_end}...")
    prices, mcaps, vols = fetch_price_history(
        coingecko_id="ethereum",
        start_date=chunk_start,
        end_date=chunk_end,
        sleep_seconds=0.25,  # Respect 250 calls/min rate limit
    )
    
    if prices:
        all_prices.update(prices)
        all_mcaps.update(mcaps)
        all_volumes.update(vols)
        print(f"    [OK] Downloaded {len(prices)} days")
    else:
        print(f"    [SKIP] No data returned for this chunk")

if len(all_prices) == 0:
    print("ERROR: No ETH data returned from CoinGecko. Check API key or network.")
    sys.exit(1)

print(f"\n  Total downloaded: {len(all_prices)} days of ETH price data")
print(f"  Date range: {min(all_prices.keys())} to {max(all_prices.keys())}")

# Step 2: Add to wide format
print("\n[Step 2] Adding ETH to wide format prices_daily.parquet...")
prices_path = Path("data/curated/prices_daily.parquet")
mcaps_path = Path("data/curated/marketcap_daily.parquet")
volumes_path = Path("data/curated/volume_daily.parquet")

# Load existing wide format
if prices_path.exists():
    prices_wide = pd.read_parquet(prices_path)
else:
    prices_wide = pd.DataFrame()

if mcaps_path.exists():
    mcaps_wide = pd.read_parquet(mcaps_path)
else:
    mcaps_wide = pd.DataFrame()

if volumes_path.exists():
    volumes_wide = pd.read_parquet(volumes_path)
else:
    volumes_wide = pd.DataFrame()

# Convert ETH data to Series
eth_prices_series = pd.Series(all_prices, name='ETH')
eth_mcaps_series = pd.Series(all_mcaps, name='ETH')
eth_volumes_series = pd.Series(all_volumes, name='ETH')

# Create DataFrames with date index
eth_prices_df = pd.DataFrame({'ETH': eth_prices_series})
eth_mcaps_df = pd.DataFrame({'ETH': eth_mcaps_series})
eth_volumes_df = pd.DataFrame({'ETH': eth_volumes_series})

# Merge with existing (combine_first prefers existing, so we'll update)
if not prices_wide.empty:
    # Update existing with new ETH data
    prices_wide['ETH'] = eth_prices_df['ETH']
    prices_wide = prices_wide.sort_index()
else:
    prices_wide = eth_prices_df

if not mcaps_wide.empty:
    mcaps_wide['ETH'] = eth_mcaps_df['ETH']
    mcaps_wide = mcaps_wide.sort_index()
else:
    mcaps_wide = eth_mcaps_df

if not volumes_wide.empty:
    volumes_wide['ETH'] = eth_volumes_df['ETH']
    volumes_wide = volumes_wide.sort_index()
else:
    volumes_wide = eth_volumes_df

# Save updated wide format
prices_path.parent.mkdir(parents=True, exist_ok=True)
prices_wide.to_parquet(prices_path)
mcaps_wide.to_parquet(mcaps_path)
volumes_wide.to_parquet(volumes_path)
print(f"  Updated prices_daily.parquet: {len(prices_wide)} days, {len(prices_wide.columns)} coins")
print(f"  Updated marketcap_daily.parquet: {len(mcaps_wide)} days, {len(mcaps_wide.columns)} coins")
print(f"  Updated volume_daily.parquet: {len(volumes_wide)} days, {len(volumes_wide.columns)} coins")

# Step 3: Convert to fact tables and append
print("\n[Step 3] Converting to fact tables and appending...")
data_lake_dir = Path("data/curated/data_lake")
fact_price_path = data_lake_dir / "fact_price.parquet"
fact_mcap_path = data_lake_dir / "fact_marketcap.parquet"
fact_volume_path = data_lake_dir / "fact_volume.parquet"

from src.data_lake.schema import FACT_PRICE_SCHEMA, FACT_MARKETCAP_SCHEMA, FACT_VOLUME_SCHEMA

# Load existing fact tables
existing_fact_price = pd.read_parquet(fact_price_path) if fact_price_path.exists() else pd.DataFrame()
existing_fact_mcap = pd.read_parquet(fact_mcap_path) if fact_mcap_path.exists() else pd.DataFrame()
existing_fact_volume = pd.read_parquet(fact_volume_path) if fact_volume_path.exists() else pd.DataFrame()

# Convert ETH wide data to fact format
def convert_eth_to_fact(wide_series, fact_schema, value_col, source="coingecko"):
    rows = []
    asset_id = generate_asset_id(symbol="ETH")
    
    for date_val, value in wide_series.items():
        if pd.isna(value):
            continue
        
        if isinstance(date_val, pd.Timestamp):
            date_obj = date_val.date()
        elif isinstance(date_val, date):
            date_obj = date_val
        else:
            date_obj = pd.to_datetime(date_val).date()
        
        rows.append({
            "asset_id": asset_id,
            "date": date_obj,
            value_col: float(value),
            "source": source,
        })
    
    return pd.DataFrame(rows)

eth_fact_price = convert_eth_to_fact(eth_prices_series, FACT_PRICE_SCHEMA, "close")
eth_fact_mcap = convert_eth_to_fact(eth_mcaps_series, FACT_MARKETCAP_SCHEMA, "marketcap")
eth_fact_volume = convert_eth_to_fact(eth_volumes_series, FACT_VOLUME_SCHEMA, "volume")

print(f"  Converted ETH to fact format: {len(eth_fact_price)} price rows")

# Append to existing (deduplicate)
if not existing_fact_price.empty:
    # Remove existing ETH rows for 2024 dates
    eth_2024_dates = set(eth_fact_price['date'].unique())
    existing_fact_price = existing_fact_price[
        ~((existing_fact_price['asset_id'] == 'ETH') & 
          (existing_fact_price['date'].isin(eth_2024_dates)))
    ]
    combined_price = pd.concat([existing_fact_price, eth_fact_price], ignore_index=True)
    combined_price = combined_price.drop_duplicates(subset=['asset_id', 'date'], keep='last')
    combined_price = combined_price.sort_values(['date', 'asset_id'])
else:
    combined_price = eth_fact_price

if not existing_fact_mcap.empty:
    eth_2024_dates = set(eth_fact_mcap['date'].unique())
    existing_fact_mcap = existing_fact_mcap[
        ~((existing_fact_mcap['asset_id'] == 'ETH') & 
          (existing_fact_mcap['date'].isin(eth_2024_dates)))
    ]
    combined_mcap = pd.concat([existing_fact_mcap, eth_fact_mcap], ignore_index=True)
    combined_mcap = combined_mcap.drop_duplicates(subset=['asset_id', 'date'], keep='last')
    combined_mcap = combined_mcap.sort_values(['date', 'asset_id'])
else:
    combined_mcap = eth_fact_mcap

if not existing_fact_volume.empty:
    eth_2024_dates = set(eth_fact_volume['date'].unique())
    existing_fact_volume = existing_fact_volume[
        ~((existing_fact_volume['asset_id'] == 'ETH') & 
          (existing_fact_volume['date'].isin(eth_2024_dates)))
    ]
    combined_volume = pd.concat([existing_fact_volume, eth_fact_volume], ignore_index=True)
    combined_volume = combined_volume.drop_duplicates(subset=['asset_id', 'date'], keep='last')
    combined_volume = combined_volume.sort_values(['date', 'asset_id'])
else:
    combined_volume = eth_fact_volume

# Save updated fact tables
data_lake_dir.mkdir(parents=True, exist_ok=True)
combined_price.to_parquet(fact_price_path, index=False)
combined_mcap.to_parquet(fact_mcap_path, index=False)
combined_volume.to_parquet(fact_volume_path, index=False)

print(f"  Updated fact_price.parquet: {len(combined_price)} rows (+{len(eth_fact_price)} ETH rows)")
print(f"  Updated fact_marketcap.parquet: {len(combined_mcap)} rows (+{len(eth_fact_mcap)} ETH rows)")
print(f"  Updated fact_volume.parquet: {len(combined_volume)} rows (+{len(eth_fact_volume)} ETH rows)")

# Verify
eth_in_fact = combined_price[combined_price['asset_id'] == 'ETH']
eth_2024_in_fact = eth_in_fact[
    (eth_in_fact['date'] >= date(2024, 1, 1)) & 
    (eth_in_fact['date'] <= date(2024, 12, 31))
]
print(f"\n[Verification] ETH in fact_price for 2024: {len(eth_2024_in_fact)} rows")
if len(eth_2024_in_fact) > 0:
    print(f"  Date range: {eth_2024_in_fact['date'].min()} to {eth_2024_in_fact['date'].max()}")

print("\n" + "=" * 80)
print("BACKFILL COMPLETE")
print("=" * 80)
