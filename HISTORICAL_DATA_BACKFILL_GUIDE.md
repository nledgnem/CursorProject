# Historical Data Backfill Guide - Analyst Tier

## Important Clarification

**The upgrade to Analyst tier gives you ACCESS to 10 years of historical data via the API, but it does NOT automatically populate your existing parquet files.**

You need to:
1. ✅ **Write a backfill script** to fetch the additional historical data
2. ✅ **Run the script** to download 10 years of data for your assets
3. ✅ **Append the data** to your existing fact tables

---

## Current Situation

### Your Existing Data:
- **Date Range:** 2024-01-07 to 2026-01-05 (730 days, ~2 years)
- **Source:** Basic tier (limited to 2 years)
- **Files:** `fact_price.parquet`, `fact_marketcap.parquet`, `fact_volume.parquet`

### After Analyst Tier Upgrade:
- **API Access:** Can fetch data from 2013 onwards (10+ years)
- **Your Files:** Still only contain 2024-2026 data (until you backfill)
- **Action Required:** Run backfill script to extend to 10 years

---

## Backfill Strategy

### Option 1: Full Historical Backfill (Recommended)

Backfill all assets from 2013 (or earliest available) to 2024-01-06 (day before your current data starts).

**Benefits:**
- ✅ Complete 10-year history
- ✅ No gaps in data
- ✅ Better for long-term backtesting

**Considerations:**
- ⚠️ Takes time: ~2,718 assets × 0.12s = ~5.4 minutes minimum (plus API call time)
- ⚠️ Uses API credits: ~2,718 calls for full backfill
- ⚠️ Some assets may not have 10 years of history (newer coins)

### Option 2: Selective Backfill (Faster)

Only backfill top assets (BTC, ETH, major alts) for 10 years, keep others at 2 years.

**Benefits:**
- ✅ Faster execution
- ✅ Focuses on most important assets
- ✅ Lower API credit usage

**Use Case:** If you primarily backtest on BTC/ETH and top 50 alts

---

## Implementation: Backfill Script

### Step 1: Update Rate Limiting

After upgrading to Analyst tier, update `src/providers/coingecko.py`:

```python
# Change from:
sleep_seconds: float = 0.25,  # 250 calls/min

# To:
sleep_seconds: float = 0.12,  # 500 calls/min (Analyst tier)
```

### Step 2: Create Backfill Script

Create `scripts/backfill_historical_data.py`:

```python
#!/usr/bin/env python3
"""
Backfill historical data for all assets from 2013 to start of existing data.

This script:
1. Finds the earliest date in existing fact tables
2. Fetches historical data from 2013-01-01 to (earliest_date - 1 day)
3. Appends to existing fact tables
"""

import sys
from pathlib import Path
from datetime import date, timedelta
from typing import Optional
import polars as pl
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko import fetch_price_history, download_all_coins
from src.data_lake.mapping import generate_asset_id
from src.data_lake.schema import (
    FACT_PRICE_SCHEMA,
    FACT_MARKETCAP_SCHEMA,
    FACT_VOLUME_SCHEMA,
)


def find_earliest_date_in_fact_table(data_lake_dir: Path, table_name: str) -> Optional[date]:
    """Find the earliest date in an existing fact table."""
    filepath = data_lake_dir / f"{table_name}.parquet"
    
    if not filepath.exists():
        return None
    
    try:
        df = pl.read_parquet(str(filepath))
        if "date" in df.columns and len(df) > 0:
            min_date = df.select(pl.col("date").min()).item()
            return min_date
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    
    return None


def backfill_historical_data(
    data_lake_dir: Path,
    start_date: date = date(2013, 1, 1),  # CoinGecko data starts around 2013
    asset_allowlist_path: Optional[Path] = None,
    max_assets: Optional[int] = None,
):
    """
    Backfill historical data for all assets.
    
    Args:
        data_lake_dir: Path to data lake directory
        start_date: Start date for backfill (default: 2013-01-01)
        asset_allowlist_path: Optional path to CSV with assets to backfill
        max_assets: Optional limit on number of assets to backfill (for testing)
    """
    print("=" * 80)
    print("HISTORICAL DATA BACKFILL")
    print("=" * 80)
    
    # Find earliest date in existing data
    earliest_price_date = find_earliest_date_in_fact_table(data_lake_dir, "fact_price")
    earliest_mcap_date = find_earliest_date_in_fact_table(data_lake_dir, "fact_marketcap")
    earliest_vol_date = find_earliest_date_in_fact_table(data_lake_dir, "fact_volume")
    
    # Use the earliest date across all tables
    earliest_date = min(
        d for d in [earliest_price_date, earliest_mcap_date, earliest_vol_date] 
        if d is not None
    ) if any(d is not None for d in [earliest_price_date, earliest_mcap_date, earliest_vol_date]) else None
    
    if earliest_date is None:
        print("No existing data found. Starting from scratch.")
        end_date = date.today()
    else:
        print(f"Existing data starts at: {earliest_date}")
        end_date = earliest_date - timedelta(days=1)
        print(f"Backfilling from {start_date} to {end_date}")
    
    if end_date < start_date:
        print("No backfill needed - existing data already covers requested range.")
        return
    
    # Load asset list
    if asset_allowlist_path and asset_allowlist_path.exists():
        allowlist_df = pd.read_csv(asset_allowlist_path)
        print(f"Loaded {len(allowlist_df)} assets from allowlist")
    else:
        # Use dim_asset to get all assets
        dim_asset_path = data_lake_dir / "dim_asset.parquet"
        if dim_asset_path.exists():
            dim_asset = pl.read_parquet(str(dim_asset_path))
            # Get coingecko_id from mapping or use symbol
            allowlist_df = dim_asset.to_pandas()
            print(f"Loaded {len(allowlist_df)} assets from dim_asset")
        else:
            print("ERROR: No asset list found. Please provide allowlist_path or ensure dim_asset.parquet exists.")
            return
    
    # Limit assets if specified (for testing)
    if max_assets:
        allowlist_df = allowlist_df.head(max_assets)
        print(f"Limited to {max_assets} assets for testing")
    
    # Prepare output directory (use temp directory for new data)
    temp_dir = Path("data/curated/temp_backfill")
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\nBackfilling {len(allowlist_df)} assets from {start_date} to {end_date}")
    print(f"Estimated time: ~{len(allowlist_df) * 0.12 / 60:.1f} minutes (500 calls/min)")
    print()
    
    # Use existing download_all_coins function but with custom date range
    # We'll need to modify it or create a custom version
    
    # For now, use the existing function with modified date range
    download_all_coins(
        allowlist_path=asset_allowlist_path if asset_allowlist_path else None,
        start_date=start_date,
        end_date=end_date,
        output_dir=temp_dir,
    )
    
    # Convert wide format to fact tables
    print("\nConverting to fact table format...")
    from scripts.convert_to_fact_tables import convert_wide_to_fact_table
    
    # Load wide format files
    prices_wide = pd.read_parquet(temp_dir / "prices_daily.parquet")
    mcaps_wide = pd.read_parquet(temp_dir / "marketcap_daily.parquet")
    volumes_wide = pd.read_parquet(temp_dir / "volume_daily.parquet")
    
    # Convert to fact tables
    prices_fact = convert_wide_to_fact_table(
        prices_wide, FACT_PRICE_SCHEMA, "coingecko", "close"
    )
    mcaps_fact = convert_wide_to_fact_table(
        mcaps_wide, FACT_MARKETCAP_SCHEMA, "coingecko", "marketcap"
    )
    volumes_fact = convert_wide_to_fact_table(
        volumes_wide, FACT_VOLUME_SCHEMA, "coingecko", "volume"
    )
    
    # Load existing fact tables
    print("\nLoading existing fact tables...")
    existing_prices = pl.read_parquet(str(data_lake_dir / "fact_price.parquet")) if (data_lake_dir / "fact_price.parquet").exists() else None
    existing_mcaps = pl.read_parquet(str(data_lake_dir / "fact_marketcap.parquet")) if (data_lake_dir / "fact_marketcap.parquet").exists() else None
    existing_volumes = pl.read_parquet(str(data_lake_dir / "fact_volume.parquet")) if (data_lake_dir / "fact_volume.parquet").exists() else None
    
    # Convert new data to Polars
    prices_new = pl.from_pandas(prices_fact)
    mcaps_new = pl.from_pandas(mcaps_fact)
    volumes_new = pl.from_pandas(volumes_fact)
    
    # Combine with existing data (deduplicate)
    print("Merging with existing data...")
    
    if existing_prices is not None:
        prices_combined = pl.concat([prices_new, existing_prices]).unique(subset=["asset_id", "date"])
    else:
        prices_combined = prices_new
    
    if existing_mcaps is not None:
        mcaps_combined = pl.concat([mcaps_new, existing_mcaps]).unique(subset=["asset_id", "date"])
    else:
        mcaps_combined = mcaps_new
    
    if existing_volumes is not None:
        volumes_combined = pl.concat([volumes_new, existing_volumes]).unique(subset=["asset_id", "date"])
    else:
        volumes_combined = volumes_new
    
    # Sort by date
    prices_combined = prices_combined.sort(["asset_id", "date"])
    mcaps_combined = mcaps_combined.sort(["asset_id", "date"])
    volumes_combined = volumes_combined.sort(["asset_id", "date"])
    
    # Save updated fact tables
    print(f"\nSaving updated fact tables...")
    prices_combined.write_parquet(str(data_lake_dir / "fact_price.parquet"))
    mcaps_combined.write_parquet(str(data_lake_dir / "fact_marketcap.parquet"))
    volumes_combined.write_parquet(str(data_lake_dir / "fact_volume.parquet"))
    
    print(f"✅ Backfill complete!")
    print(f"  Prices: {len(prices_combined):,} records")
    print(f"  Market caps: {len(mcaps_combined):,} records")
    print(f"  Volumes: {len(volumes_combined):,} records")
    
    # Cleanup temp directory
    import shutil
    shutil.rmtree(temp_dir)
    print(f"\nCleaned up temporary files")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Backfill historical data")
    parser.add_argument(
        "--start-date",
        type=str,
        default="2013-01-01",
        help="Start date for backfill (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--allowlist",
        type=str,
        help="Path to asset allowlist CSV"
    )
    parser.add_argument(
        "--max-assets",
        type=int,
        help="Limit number of assets (for testing)"
    )
    
    args = parser.parse_args()
    
    start_date = date.fromisoformat(args.start_date)
    allowlist_path = Path(args.allowlist) if args.allowlist else None
    
    data_lake_dir = Path("data/curated/data_lake")
    
    backfill_historical_data(
        data_lake_dir=data_lake_dir,
        start_date=start_date,
        asset_allowlist_path=allowlist_path,
        max_assets=args.max_assets,
    )
```

---

## Usage Examples

### Full Backfill (All Assets, 10 Years)

```bash
# Backfill all assets from 2013 to start of existing data
python scripts/backfill_historical_data.py

# Or specify custom start date
python scripts/backfill_historical_data.py --start-date 2015-01-01
```

### Selective Backfill (Top Assets Only)

```bash
# Create a CSV with top assets (BTC, ETH, top 50 alts)
# Then backfill only those:
python scripts/backfill_historical_data.py --allowlist data/top_assets.csv
```

### Test Run (Limited Assets)

```bash
# Test with just 10 assets first
python scripts/backfill_historical_data.py --max-assets 10
```

---

## What Happens After Backfill

### Before Backfill:
```
fact_price.parquet: 2024-01-07 to 2026-01-05 (730 days)
fact_marketcap.parquet: 2024-01-07 to 2026-01-05 (730 days)
fact_volume.parquet: 2024-01-07 to 2026-01-05 (730 days)
```

### After Backfill:
```
fact_price.parquet: 2013-01-01 to 2026-01-05 (~4,750 days, 13 years)
fact_marketcap.parquet: 2013-01-01 to 2026-01-05 (~4,750 days, 13 years)
fact_volume.parquet: 2013-01-01 to 2026-01-05 (~4,750 days, 13 years)
```

**Note:** Not all assets will have full 13 years (newer coins like SOL, AVAX started later)

---

## Important Considerations

### 1. **API Credit Usage**
- Full backfill: ~2,718 assets × 1 call = ~2,718 calls
- Well within Analyst tier limit (500k/month)
- Can spread across multiple days if needed

### 2. **Data Availability**
- **BTC, ETH:** Full history from ~2013
- **Older alts (LTC, XRP, etc.):** Full history from ~2013-2014
- **Newer alts (SOL, AVAX, etc.):** History from their launch dates
- **Very new coins:** May only have recent data

### 3. **Storage Impact**
- Current: ~11-12 MB per fact table
- After backfill: ~75-80 MB per fact table (6-7x increase)
- Still manageable, but be aware of disk space

### 4. **Time Required**
- Full backfill: ~5-10 minutes (depending on API response times)
- Can run overnight or during low-usage periods

---

## Verification After Backfill

Run the inspection script to verify:

```bash
python inspect_data_lake.py
```

Check that:
- ✅ Date ranges extend to 2013 (or earliest available)
- ✅ Row counts increased significantly
- ✅ No duplicate records
- ✅ Data quality is maintained

---

## Summary

**Answer to your question:**

> "For the existing parquet files, the data will be populated for the past 10 years right?"

**Short Answer:** Not automatically. You need to run a backfill script.

**Long Answer:**
1. ✅ Analyst tier gives you **API access** to 10 years of data
2. ❌ Your existing parquet files **won't automatically update**
3. ✅ You need to **write and run a backfill script** to fetch the historical data
4. ✅ The script will **append historical data** to your existing fact tables
5. ✅ After backfill, your files will contain **10+ years of data** (where available)

**The good news:** Your existing `fetch_price_history` function already supports date ranges, so you just need to:
- Update the rate limit (0.25s → 0.12s)
- Run a backfill script with start_date=2013-01-01
- Append the results to your existing fact tables

I can create the complete backfill script for you if you'd like!
