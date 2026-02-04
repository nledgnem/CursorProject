# Instrument to Asset Linkage

**Date:** 2025-12-29  
**Status:** ✅ **COMPLETE**

---

## What Was Done

Added `asset_id` column to `dim_instrument` table to create a direct link between instruments and assets. This enables universal queries across instrument types for the same underlying asset.

---

## The Linkage

### Schema Update

**Before:**
```python
DIM_INSTRUMENT_SCHEMA = {
    "instrument_id": str,        # e.g., "binance_perp_BTCUSDT"
    "base_asset_symbol": str,    # e.g., "BTC"
    # ... other fields ...
    # ❌ No direct link to asset_id
}
```

**After:**
```python
DIM_INSTRUMENT_SCHEMA = {
    "instrument_id": str,        # e.g., "binance_perp_BTCUSDT"
    "base_asset_symbol": str,    # e.g., "BTC"
    "asset_id": Optional[str],   # e.g., "BTC" ✅ NEW!
    # ... other fields ...
}
```

### Example Linkage

| instrument_id | base_asset_symbol | asset_id |
|---------------|-------------------|----------|
| `binance_perp_BTCUSDT` | BTC | BTC |
| `binance_perp_ETHUSDT` | ETH | ETH |
| `binance_perp_SOLUSDT` | SOL | SOL |

---

## Benefits

### 1. Universal Queries

**Query all instruments for an asset:**
```sql
SELECT * FROM dim_instrument 
WHERE asset_id = 'BTC'
```

Returns all BTC instruments:
- `binance_perp_BTCUSDT` (perpetual)
- `binance_spot_BTCUSDT` (spot - if exists)
- `binance_futures_BTCUSDT_20251231` (futures - if exists)

### 2. Direct Joins

**Join instruments with asset fact tables:**
```sql
SELECT 
    di.instrument_id,
    di.instrument_type,
    fp.date,
    fp.close
FROM dim_instrument di
JOIN fact_price fp ON di.asset_id = fp.asset_id
WHERE di.instrument_id = 'binance_perp_BTCUSDT'
```

### 3. Aggregations by Asset

**Get all funding rates for an asset across all instruments:**
```sql
SELECT 
    ff.date,
    ff.funding_rate,
    di.instrument_id,
    di.venue
FROM fact_funding ff
JOIN dim_instrument di ON ff.instrument_id = di.instrument_id
WHERE di.asset_id = 'BTC'
ORDER BY ff.date DESC
```

### 4. Cross-Instrument Analysis

**Compare prices across different instrument types for the same asset:**
```sql
SELECT 
    di.instrument_type,
    AVG(fp.close) as avg_price
FROM dim_instrument di
JOIN fact_price fp ON di.asset_id = fp.asset_id
WHERE di.asset_id = 'BTC'
GROUP BY di.instrument_type
```

---

## How It Works

### Linking Logic

The `asset_id` is populated by joining `dim_instrument.base_asset_symbol` with `dim_asset.symbol` or `dim_asset.asset_id`:

```python
# Create lookup: symbol -> asset_id
symbol_to_asset_id = dict(zip(dim_asset["symbol"], dim_asset["asset_id"]))

# Map base_asset_symbol to asset_id
dim_instrument["asset_id"] = dim_instrument["base_asset_symbol"].map(symbol_to_asset_id)
```

### Matching Examples

| base_asset_symbol | Matches dim_asset.symbol? | asset_id Result |
|-------------------|---------------------------|-----------------|
| BTC | ✅ Yes | BTC |
| ETH | ✅ Yes | ETH |
| SOL | ✅ Yes | SOL |
| MKR | ❌ Not in dim_asset | None |
| DEFI | ❌ Not in dim_asset | None |

**Note:** Unmatched instruments have `asset_id = None`. This is expected for:
- New assets not yet in `dim_asset`
- Special instruments (indices, baskets)
- Assets that exist on exchanges but not in our asset universe

---

## Current Status

### Linkage Statistics

- **Total instruments:** 605
- **Linked to asset_id:** 381 (63%)
- **Unlinked:** 224 (37%)

**Why some are unlinked:**
- Asset not in `dim_asset` table (likely not in our universe)
- Symbol mismatch (e.g., different symbol format)
- Special instruments (indices, DEFI baskets, etc.)

### Linked Examples

✅ Successfully linked:
- BTC, ETH, SOL, DOT, YFI, BAL, CRV, TRB, RUNE, SUSHI, EGLD, etc.

⚠️ Not linked (examples):
- MKR, DEFI, STORJ, BLZ, FTM, ALPHA, BEL, REN, FLM, OMG

---

## Usage

### Adding asset_id to Existing dim_instrument

If you have an existing `dim_instrument.parquet` file without `asset_id`, run:

```bash
python scripts/add_asset_id_to_instruments.py
```

This will:
1. Load `dim_instrument.parquet` and `dim_asset.parquet`
2. Create symbol → asset_id lookup
3. Add `asset_id` column to `dim_instrument`
4. Save updated `dim_instrument.parquet`

### Building New dim_instrument with asset_id

When building `dim_instrument` from scratch (via `convert_to_fact_tables.py`), `asset_id` is automatically populated if `dim_asset.parquet` exists:

```bash
python scripts/convert_to_fact_tables.py \
  --curated-dir data/curated \
  --data-lake-dir data/curated/data_lake \
  --perp-listings data/curated/perp_listings_binance_aligned.parquet
```

The script will:
1. Load `dim_asset` if available
2. Pass it to `build_dim_instrument_from_binance_perps()`
3. Automatically link `asset_id` during construction

---

## Code Changes

### Schema (`src/data_lake/schema.py`)

```python
DIM_INSTRUMENT_SCHEMA = {
    # ... existing fields ...
    "asset_id": Optional[str],  # ✅ NEW: Links to dim_asset.asset_id
}
```

### Building Function (`src/data_lake/mapping.py`)

```python
def build_dim_instrument_from_binance_perps(
    perp_listings: pd.DataFrame,
    dim_asset: Optional[pd.DataFrame] = None,  # ✅ NEW parameter
) -> pd.DataFrame:
    # ... creates lookup from dim_asset ...
    # ... maps base_asset_symbol → asset_id ...
    rows.append({
        # ... existing fields ...
        "asset_id": asset_id,  # ✅ NEW field
    })
```

### Conversion Script (`scripts/convert_to_fact_tables.py`)

```python
# Load dim_asset for linkage
dim_asset_for_link = pd.read_parquet(dim_asset_path)

# Build dim_instrument with asset_id
dim_instrument = build_dim_instrument_from_binance_perps(
    perp_listings,
    dim_asset=dim_asset_for_link,  # ✅ Pass dim_asset
)
```

### New Utility Script (`scripts/add_asset_id_to_instruments.py`)

Script to add `asset_id` to existing `dim_instrument` files.

---

## Query Examples

### Example 1: All BTC Instruments

```python
import pandas as pd
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")
dim_instrument = pd.read_parquet(data_lake_dir / "dim_instrument.parquet")

btc_instruments = dim_instrument[dim_instrument["asset_id"] == "BTC"]
print(btc_instruments[["instrument_id", "instrument_type", "venue"]])
```

### Example 2: Join Instruments with Prices

```python
dim_instrument = pd.read_parquet(data_lake_dir / "dim_instrument.parquet")
fact_price = pd.read_parquet(data_lake_dir / "fact_price.parquet")

# Get BTC perp instrument
btc_perp = dim_instrument[
    (dim_instrument["asset_id"] == "BTC") & 
    (dim_instrument["instrument_type"] == "perpetual")
]["instrument_id"].iloc[0]

# Join to get prices
# Note: fact_price uses asset_id, so we can join directly
btc_prices = fact_price[fact_price["asset_id"] == "BTC"]
```

### Example 3: All Instruments for Top Assets

```python
dim_asset = pd.read_parquet(data_lake_dir / "dim_asset.parquet")
dim_instrument = pd.read_parquet(data_lake_dir / "dim_instrument.parquet")
fact_marketcap = pd.read_parquet(data_lake_dir / "fact_marketcap.parquet")

# Get top 10 assets by market cap
latest_date = fact_marketcap["date"].max()
top_assets = (
    fact_marketcap[fact_marketcap["date"] == latest_date]
    .nlargest(10, "marketcap")["asset_id"]
    .tolist()
)

# Get all instruments for these assets
top_instruments = dim_instrument[dim_instrument["asset_id"].isin(top_assets)]
print(top_instruments[["asset_id", "instrument_id", "instrument_type"]])
```

---

## Summary

✅ **Schema updated:** `dim_instrument` now includes `asset_id` column  
✅ **Building updated:** New instruments automatically get `asset_id` if `dim_asset` available  
✅ **Existing tables updated:** Script provided to add `asset_id` to existing tables  
✅ **381/605 instruments linked:** 63% successfully linked to assets  

**Key Benefit:** You can now query "all instruments for BTC" or join instruments directly with asset fact tables using `asset_id` as the universal link!

---

## Future Enhancements

1. **Better Matching:** Improve matching logic for unmatched instruments
2. **Manual Overrides:** Allow manual mapping for special cases
3. **Multiple Assets:** Handle instruments tied to multiple assets (baskets)
4. **Historical Tracking:** Track when asset_id mappings change over time

