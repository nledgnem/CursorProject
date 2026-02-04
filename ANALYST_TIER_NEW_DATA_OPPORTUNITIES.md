# New Data Opportunities with Analyst Tier

## Understanding Your Data Lake

**Yes, you're understanding this correctly!** 

Your data lake (`data/curated/data_lake/`) is **YOUR storage** - it's where you store parquet files that you create from various data sources (CoinGecko, CoinGlass, Binance, etc.). 

**Key Points:**
- ✅ CoinGecko API provides the **raw data** via API calls
- ✅ You write scripts to **fetch and transform** that data
- ✅ You save it as **parquet files** in your data lake
- ✅ These parquet files follow your **standardized schema** (fact tables, dimension tables)

---

## Current Data Lake Structure

### Existing Fact Tables (from Basic Tier):
- `fact_price.parquet` - Daily closing prices
- `fact_marketcap.parquet` - Daily market capitalization  
- `fact_volume.parquet` - Daily trading volume
- `fact_funding.parquet` - Funding rates (from CoinGlass)
- `fact_open_interest.parquet` - Open interest (from CoinGlass)

### Existing Dimension Tables:
- `dim_asset.parquet` - Asset metadata
- `dim_instrument.parquet` - Trading instrument metadata

---

## New Parquet Files You Can Add with Analyst Tier

### 1. **OHLC Data (Open, High, Low, Close)**

**Endpoint:** `/coins/{id}/ohlc` (Analyst tier exclusive for time-range queries)

**New Fact Table:** `fact_ohlc.parquet`

**Schema:**
```python
FACT_OHLC_SCHEMA = {
    "asset_id": str,
    "date": date,
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "source": str,  # "coingecko"
}
```

**Use Cases:**
- ✅ **Better price analysis** - High/low ranges for volatility analysis
- ✅ **Candlestick patterns** - Technical analysis indicators
- ✅ **Drawdown calculations** - More accurate max drawdown using high/low
- ✅ **Gap analysis** - Identify price gaps between days
- ✅ **MSM v0 enhancement** - Better volatility spread calculations

**Example Data:**
```
asset_id  date       open    high    low     close   source
BTC       2024-01-01 42000   43500   41800   43000   coingecko
ETH       2024-01-01 2400    2500    2380    2450    coingecko
```

---

### 2. **Top Gainers & Losers (Market Breadth)**

**Endpoint:** `/coins/top_gainers_losers` (Analyst tier exclusive)

**New Fact Table:** `fact_market_breadth.parquet`

**Schema:**
```python
FACT_MARKET_BREADTH_SCHEMA = {
    "date": date,
    "asset_id": str,
    "rank": int,  # Rank in top gainers/losers
    "price_change_24h": float,  # Percentage change
    "price_change_7d": float,   # Percentage change
    "price_change_14d": float,  # Percentage change
    "price_change_30d": float,  # Percentage change
    "category": str,  # "gainer" or "loser"
    "source": str,  # "coingecko"
}
```

**Use Cases:**
- ✅ **Regime Detection** - Market breadth is a key regime feature!
- ✅ **ALT Breadth Feature** - Count how many alts are gaining vs losing
- ✅ **Momentum Analysis** - Identify trending assets
- ✅ **Market Sentiment** - Gauge overall market direction

**Example Data:**
```
date       asset_id  rank  price_change_24h  category  source
2024-01-01 SOL       1     15.5              gainer    coingecko
2024-01-01 AVAX      2     12.3              gainer    coingecko
2024-01-01 BTC       1     -3.2              loser     coingecko
```

**Integration with MSM v0:**
This could directly feed your **ALT Breadth** feature in the regime monitor!

---

### 3. **Recently Added Coins (New Listings)**

**Endpoint:** `/coins/list/new` (Analyst tier exclusive)

**New Dimension Table:** `dim_new_listings.parquet`

**Schema:**
```python
DIM_NEW_LISTINGS_SCHEMA = {
    "asset_id": str,
    "symbol": str,
    "name": str,
    "listing_date": date,  # When it was listed on CoinGecko
    "coingecko_id": str,
    "source": str,  # "coingecko"
}
```

**Use Cases:**
- ✅ **Universe Expansion** - Automatically discover new assets
- ✅ **Early Entry Opportunities** - Track newly listed coins
- ✅ **Universe Eligibility** - Add to eligibility checks (age requirements)
- ✅ **Data Pipeline Automation** - Auto-add new coins to tracking

**Example Data:**
```
asset_id  symbol  name              listing_date  coingecko_id      source
NEWCOIN   NEW     New Coin Token    2024-01-15    new-coin-token    coingecko
```

---

### 4. **Historical OHLC (Extended Time Range)**

**Endpoint:** `/coins/{id}/ohlc` with time range (Analyst tier exclusive)

**New Fact Table:** `fact_ohlc_historical.parquet`

**Benefits:**
- ✅ **10 years of OHLC data** (vs 2 years on Basic)
- ✅ **Hourly OHLC** for recent periods (vs daily only)
- ✅ **5-minute OHLC** for intraday analysis

**Use Cases:**
- ✅ **Extended backtesting** - 10 years of OHLC for strategy validation
- ✅ **Intraday analysis** - 5-minute candles for entry/exit timing
- ✅ **Volatility modeling** - More granular volatility calculations

---

### 5. **Exchange Volume Data**

**Endpoint:** `/exchanges/{id}/volume_chart` (Available on Analyst tier)

**New Fact Table:** `fact_exchange_volume.parquet`

**Schema:**
```python
FACT_EXCHANGE_VOLUME_SCHEMA = {
    "exchange_id": str,  # "binance", "coinbase", etc.
    "date": date,
    "volume_btc": float,
    "volume_usd": float,
    "source": str,  # "coingecko"
}
```

**Use Cases:**
- ✅ **Liquidity Analysis** - Track exchange-level liquidity
- ✅ **Market Structure** - Understand volume concentration
- ✅ **Regime Detection** - Exchange volume patterns may indicate regimes

---

### 6. **Derivative Data (Futures/Perpetuals)**

**Endpoint:** `/derivatives` and related endpoints (Analyst tier)

**New Fact Tables:**
- `fact_derivative_volume.parquet`
- `fact_derivative_open_interest.parquet`

**Use Cases:**
- ✅ **Complement CoinGlass data** - Additional source for funding/OI
- ✅ **Cross-validation** - Verify CoinGlass data quality
- ✅ **Broader coverage** - More exchanges than CoinGlass

---

## Implementation Guide

### Step 1: Create New Schema Definitions

Add to `src/data_lake/schema.py`:

```python
# OHLC Fact Table
FACT_OHLC_SCHEMA = {
    "asset_id": str,
    "date": date,
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "source": str,
}

# Market Breadth Fact Table
FACT_MARKET_BREADTH_SCHEMA = {
    "date": date,
    "asset_id": str,
    "rank": int,
    "price_change_24h": float,
    "price_change_7d": float,
    "price_change_14d": float,
    "price_change_30d": float,
    "category": str,  # "gainer" or "loser"
    "source": str,
}

# New Listings Dimension Table
DIM_NEW_LISTINGS_SCHEMA = {
    "asset_id": str,
    "symbol": str,
    "name": str,
    "listing_date": date,
    "coingecko_id": str,
    "source": str,
}
```

### Step 2: Create Fetch Scripts

Create `src/providers/coingecko_analyst.py`:

```python
"""Analyst tier exclusive CoinGecko endpoints."""

import requests
import time
from datetime import date, datetime, timezone
from typing import Dict, List
import polars as pl

COINGECKO_BASE = "https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY = "CG-RhUWZY31TcDFBPfj4GWwcsMS"


def fetch_top_gainers_losers(
    duration: str = "24h",  # "1h", "24h", "7d", "14d", "30d", "200d", "1y"
    sleep_seconds: float = 0.12,  # 500 calls/min
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
    
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    
    time.sleep(sleep_seconds)
    
    return {
        "gainers": data.get("gainers", []),
        "losers": data.get("losers", []),
    }


def fetch_new_listings(
    sleep_seconds: float = 0.12,
) -> List[Dict]:
    """Fetch latest 200 newly listed coins."""
    url = f"{COINGECKO_BASE}/coins/list/new"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    
    time.sleep(sleep_seconds)
    
    return data


def fetch_ohlc_range(
    coingecko_id: str,
    start_date: date,
    end_date: date,
    vs_currency: str = "usd",
    sleep_seconds: float = 0.12,
) -> List[Dict]:
    """
    Fetch OHLC data for a coin within a date range.
    
    Returns list of [timestamp_ms, open, high, low, close]
    """
    url = f"{COINGECKO_BASE}/coins/{coingecko_id}/ohlc"
    
    # Convert dates to timestamps
    start_ts = int(datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    
    params = {
        "vs_currency": vs_currency,
        "from": start_ts,
        "to": end_ts,
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }
    
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    
    time.sleep(sleep_seconds)
    
    return data
```

### Step 3: Create Conversion Scripts

Create `scripts/fetch_analyst_tier_data.py`:

```python
#!/usr/bin/env python3
"""
Fetch and save Analyst tier exclusive data to data lake.
"""

import sys
from pathlib import Path
from datetime import date, timedelta
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko_analyst import (
    fetch_top_gainers_losers,
    fetch_new_listings,
    fetch_ohlc_range,
)
from src.data_lake.mapping import generate_asset_id


def save_market_breadth(data_lake_dir: Path):
    """Fetch and save top gainers/losers data."""
    print("Fetching top gainers/losers...")
    
    # Fetch for different durations
    durations = ["24h", "7d", "14d", "30d"]
    all_data = []
    
    for duration in durations:
        result = fetch_top_gainers_losers(duration=duration)
        
        today = date.today()
        
        # Process gainers
        for coin in result["gainers"]:
            all_data.append({
                "date": today,
                "asset_id": generate_asset_id(coin.get("symbol", "")),
                "rank": coin.get("rank", 0),
                "price_change_24h": coin.get("price_change_percentage_24h", 0.0),
                "price_change_7d": coin.get("price_change_percentage_7d", 0.0),
                "price_change_14d": coin.get("price_change_percentage_14d", 0.0),
                "price_change_30d": coin.get("price_change_percentage_30d", 0.0),
                "category": "gainer",
                "source": "coingecko",
            })
        
        # Process losers
        for coin in result["losers"]:
            all_data.append({
                "date": today,
                "asset_id": generate_asset_id(coin.get("symbol", "")),
                "rank": coin.get("rank", 0),
                "price_change_24h": coin.get("price_change_percentage_24h", 0.0),
                "price_change_7d": coin.get("price_change_percentage_7d", 0.0),
                "price_change_14d": coin.get("price_change_percentage_14d", 0.0),
                "price_change_30d": coin.get("price_change_percentage_30d", 0.0),
                "category": "loser",
                "source": "coingecko",
            })
    
    # Convert to DataFrame and save
    df = pl.DataFrame(all_data)
    output_path = data_lake_dir / "fact_market_breadth.parquet"
    df.write_parquet(output_path)
    print(f"Saved {len(df)} records to {output_path}")


def save_new_listings(data_lake_dir: Path):
    """Fetch and save newly listed coins."""
    print("Fetching new listings...")
    
    listings = fetch_new_listings()
    
    all_data = []
    for coin in listings:
        all_data.append({
            "asset_id": generate_asset_id(coin.get("symbol", "")),
            "symbol": coin.get("symbol", ""),
            "name": coin.get("name", ""),
            "listing_date": date.today(),  # Approximate
            "coingecko_id": coin.get("id", ""),
            "source": "coingecko",
        })
    
    df = pl.DataFrame(all_data)
    output_path = data_lake_dir / "dim_new_listings.parquet"
    df.write_parquet(output_path)
    print(f"Saved {len(df)} new listings to {output_path}")


if __name__ == "__main__":
    data_lake_dir = Path("data/curated/data_lake")
    data_lake_dir.mkdir(parents=True, exist_ok=True)
    
    save_market_breadth(data_lake_dir)
    save_new_listings(data_lake_dir)
```

---

## Integration with MSM v0 Strategy

### Enhanced Regime Monitor Features:

1. **ALT Breadth (Improved)**
   - Current: Count alts moving up/down (from price data)
   - Enhanced: Use `fact_market_breadth.parquet` for more accurate breadth metrics
   - Benefit: More reliable regime detection

2. **Volatility Spread (Enhanced)**
   - Current: Uses close prices only
   - Enhanced: Use OHLC high/low for true volatility ranges
   - Benefit: More accurate volatility calculations

3. **Momentum (Enhanced)**
   - Current: Calculated from price returns
   - Enhanced: Use top gainers/losers rankings
   - Benefit: Cross-validate momentum signals

---

## Data Lake Structure After Upgrade

```
data/curated/data_lake/
├── dim_asset.parquet                    [EXISTING]
├── dim_instrument.parquet               [EXISTING]
├── dim_new_listings.parquet             [NEW - Analyst tier]
├── fact_price.parquet                   [EXISTING]
├── fact_marketcap.parquet               [EXISTING]
├── fact_volume.parquet                  [EXISTING]
├── fact_ohlc.parquet                    [NEW - Analyst tier]
├── fact_market_breadth.parquet          [NEW - Analyst tier]
├── fact_funding.parquet                 [EXISTING - CoinGlass]
├── fact_open_interest.parquet           [EXISTING - CoinGlass]
├── fact_exchange_volume.parquet         [NEW - Analyst tier]
└── map_provider_asset.parquet           [EXISTING]
```

---

## Summary

**Yes, you understand correctly!**

1. ✅ **Analyst tier unlocks new API endpoints**
2. ✅ **You write scripts to fetch this data**
3. ✅ **You save it as parquet files in YOUR data lake**
4. ✅ **These files follow your existing schema patterns**
5. ✅ **They integrate with your existing fact/dimension tables**

**Key New Tables You Can Add:**
- `fact_ohlc.parquet` - OHLC data for better price analysis
- `fact_market_breadth.parquet` - Top gainers/losers (directly useful for MSM v0!)
- `dim_new_listings.parquet` - Newly listed coins for universe expansion
- `fact_exchange_volume.parquet` - Exchange-level volume data

**Most Valuable for MSM v0:**
- ⭐⭐⭐⭐⭐ `fact_market_breadth.parquet` - Directly enhances ALT Breadth feature
- ⭐⭐⭐⭐ `fact_ohlc.parquet` - Better volatility calculations
- ⭐⭐⭐ `dim_new_listings.parquet` - Universe expansion

All of these will be part of your standardized data lake and can be queried alongside your existing tables!
