# Standardized Data Lake Format - Explanation

## What is the Standardized Data Lake Format?

The **standardized data lake format** is a **normalized, relational database-style structure** that uses:

1. **Canonical IDs** - Standardized identifiers (`asset_id`, `instrument_id`) that are consistent across all tables
2. **Long format** - Each row represents one observation (vs. wide format where columns = assets)
3. **Dimension tables** - Master data (assets, instruments)
4. **Fact tables** - Time-series observations (prices, marketcaps, volumes)
5. **Mapping tables** - Provider-specific IDs mapped to canonical IDs

This is similar to a **star schema** or **data warehouse** approach used in traditional databases.

## Key Principles

### 1. Canonical IDs

**Core Identifiers:**
- `asset_id` - Canonical asset identifier (e.g., `"BTC"`, `"ETH"`, `"1INCH"`)
- `instrument_id` - Canonical instrument identifier (e.g., `"binance_perp_BTCUSDT"`)

**Why this matters:**
- All fact tables use the same `asset_id` format
- Joins are standardized: `fact_price.asset_id = dim_asset.asset_id`
- No ambiguity about which identifier to use

### 2. Long Format (Normalized)

**Data Lake Format (Long):**
```python
# fact_price.parquet
asset_id  date       close
BTC       2024-01-01 42000
ETH       2024-01-01 2400
BTC       2024-01-02 43000
ETH       2024-01-02 2500
```

**Legacy Format (Wide):**
```python
# prices_daily.parquet
date       BTC    ETH    SOL    ...
2024-01-01 42000  2400   95     ...
2024-01-02 43000  2500   98     ...
```

**Benefits of Long Format:**
- ✅ Easy to add new assets (just add rows)
- ✅ Standardized schema (same columns for all assets)
- ✅ Easy to join with dimension tables
- ✅ Efficient filtering and querying
- ✅ Works well with SQL databases/DuckDB

### 3. Table Types

#### Dimension Tables
**Purpose:** Master data - one row per entity

**Examples:**
- `dim_asset.parquet` - One row per asset (939 rows)
  - Columns: `asset_id`, `symbol`, `name`, `chain`, `contract_address`, `coingecko_id`, etc.
- `dim_instrument.parquet` - One row per instrument (605 rows)
  - Columns: `instrument_id`, `instrument_symbol`, `base_asset_symbol`, `venue`, `instrument_type`, etc.

#### Fact Tables
**Purpose:** Time-series observations - many rows per entity

**Examples:**
- `fact_price.parquet` - Daily prices (478,834 rows)
  - Columns: `asset_id`, `date`, `close`, `source`
  - One row per asset per date
- `fact_marketcap.parquet` - Daily market caps (475,656 rows)
  - Columns: `asset_id`, `date`, `marketcap`, `source`
- `fact_volume.parquet` - Daily volumes (476,969 rows)
  - Columns: `asset_id`, `date`, `volume`, `source`
- `fact_funding.parquet` - Funding rates (204,361 rows)
  - Columns: `asset_id`, `instrument_id`, `date`, `funding_rate`, `exchange`, `source`

#### Mapping Tables
**Purpose:** Map provider-specific IDs to canonical IDs (with temporal validity)

**Examples:**
- `map_provider_asset.parquet` - Maps CoinGecko IDs to `asset_id`
  - Columns: `provider`, `provider_asset_id`, `asset_id`, `valid_from`, `valid_to`, `mapping_method`, `confidence`
- `map_provider_instrument.parquet` - Maps Binance symbols to `instrument_id`
  - Columns: `provider`, `provider_instrument_id`, `instrument_id`, `valid_from`, `valid_to`, `mapping_method`, `confidence`

#### Output Tables
**Purpose:** Results from pipeline processing

**Examples:**
- `universe_eligibility.parquet` - Eligibility status for each candidate
  - Should have: `asset_id`, `rebalance_date`, `symbol`, `exclusion_reason`, etc.
- `universe_snapshots.parquet` - Selected basket members
  - Should have: `asset_id`, `rebalance_date`, `symbol`, `weight`, `rank`, etc.

## Schema Definitions

### Fact Table Schema Example

```python
FACT_PRICE_SCHEMA = {
    "asset_id": str,      # Canonical ID (required)
    "date": date,         # Observation date (required)
    "close": float,       # Value (required)
    "source": str,        # Data source (required)
}
```

### Dimension Table Schema Example

```python
DIM_ASSET_SCHEMA = {
    "asset_id": str,           # Canonical ID (required)
    "symbol": str,             # Human-readable symbol
    "name": Optional[str],     # Full name
    "chain": Optional[str],    # Blockchain
    "contract_address": Optional[str],  # Contract address
    "coingecko_id": Optional[str],      # Provider ID
    "is_stable": bool,         # Flags
    "is_wrapped_stable": bool,
    "metadata_json": Optional[str],     # Additional metadata
}
```

### Mapping Table Schema Example

```python
MAP_PROVIDER_ASSET_SCHEMA = {
    "provider": str,                    # "coingecko", "binance", etc.
    "provider_asset_id": str,           # Provider's ID (e.g., "bitcoin")
    "asset_id": str,                    # Our canonical ID (e.g., "BTC")
    "valid_from": date,                 # When mapping becomes valid
    "valid_to": Optional[date],         # When mapping expires (None = current)
    "mapping_method": str,              # "exact_match", "manual_override", etc.
    "confidence": float,                # 0.0-1.0 confidence score
}
```

## Files Aligned to Data Lake Format

### ✅ Fully Aligned (Data Lake Directory)

**Location:** `data/curated/data_lake/`

**Dimension Tables:**
- ✅ `dim_asset.parquet` - 939 assets
- ✅ `dim_instrument.parquet` - 605 instruments

**Mapping Tables:**
- ✅ `map_provider_asset.parquet` - CoinGecko → asset_id mappings
- ✅ `map_provider_instrument.parquet` - Binance → instrument_id mappings

**Fact Tables:**
- ✅ `fact_price.parquet` - 478,834 price observations
- ✅ `fact_marketcap.parquet` - 475,656 marketcap observations
- ✅ `fact_volume.parquet` - 476,969 volume observations
- ✅ `fact_funding.parquet` - 204,361 funding rate observations

**Other:**
- ✅ `perp_listings_binance_aligned.parquet` - Recently aligned (has `instrument_id`)
- ✅ `fact_funding.parquet` - Has both `asset_id` and `instrument_id` (fully aligned)

## Files NOT Aligned to Data Lake Format

### ❌ Wide Format Files (Legacy)

**Location:** `data/curated/` and `data/raw/`

**Legacy Wide Format Files:**
- ❌ `prices_daily.parquet` - Wide format (one column per asset)
- ❌ `marketcap_daily.parquet` - Wide format (one column per asset)
- ❌ `volume_daily.parquet` - Wide format (one column per asset)

**Characteristics:**
- One column per asset symbol (e.g., `BTC`, `ETH`, `SOL`)
- Date column as index or first column
- 940+ columns total
- Not normalized - can't easily add new assets
- Can't join directly with dimension tables

**Why they exist:**
- Original format from early development
- Still used by some legacy scripts
- Maintained for backward compatibility

### ⚠️ Output Files (Partially Aligned)

**Location:** `data/curated/`

**Files that may need alignment:**
- ⚠️ `universe_eligibility.parquet` - May not have `asset_id` column yet
- ⚠️ `universe_snapshots.parquet` - May not have `asset_id` column yet

**Status:**
- Schema allows `asset_id` (it's Optional in the schema)
- But may currently only have `symbol`
- Should be updated to include `asset_id` for full alignment

### ❌ Raw Provider Files

**Location:** `data/raw/`

**Raw Format Files:**
- ❌ `perp_listings_binance.parquet` - Uses `symbol` instead of `instrument_id`
  - **Note:** An aligned version exists: `perp_listings_binance_aligned.parquet`

## Comparison: Wide vs Long Format

### Wide Format (Legacy)

```python
# prices_daily.parquet
date       BTC     ETH     SOL     ... (940 columns)
2024-01-01 42000   2400    95      ...
2024-01-02 43000   2500    98      ...
```

**Problems:**
- ❌ Schema changes when assets are added
- ❌ Hard to join with dimension tables
- ❌ Inefficient for filtering by asset
- ❌ Many columns (940+)
- ❌ Not SQL-friendly

### Long Format (Data Lake)

```python
# fact_price.parquet
asset_id  date       close    source
BTC       2024-01-01 42000    coingecko
ETH       2024-01-01 2400     coingecko
SOL       2024-01-01 95       coingecko
BTC       2024-01-02 43000    coingecko
ETH       2024-01-02 2500     coingecko
SOL       2024-01-02 98       coingecko
```

**Benefits:**
- ✅ Fixed schema (always same columns)
- ✅ Easy to join: `fact_price.asset_id = dim_asset.asset_id`
- ✅ Easy to filter: `WHERE asset_id = 'BTC'`
- ✅ SQL-friendly
- ✅ Scales to any number of assets
- ✅ Can add metadata columns easily

## How to Join Tables (Data Lake Format)

### Example: Get prices with asset metadata

```python
import pandas as pd

# Load fact table
prices = pd.read_parquet('data/curated/data_lake/fact_price.parquet')

# Load dimension table
assets = pd.read_parquet('data/curated/data_lake/dim_asset.parquet')

# Join using canonical ID
result = prices.merge(
    assets[['asset_id', 'symbol', 'name', 'chain']],
    on='asset_id',
    how='left'
)

# Result has: asset_id, date, close, source, symbol, name, chain
```

### Example: Get funding rates with instrument and asset info

```python
# Load fact table
funding = pd.read_parquet('data/curated/data_lake/fact_funding.parquet')

# Load dimension tables
instruments = pd.read_parquet('data/curated/data_lake/dim_instrument.parquet')
assets = pd.read_parquet('data/curated/data_lake/dim_asset.parquet')

# Join
result = funding.merge(
    instruments[['instrument_id', 'instrument_symbol', 'base_asset_symbol']],
    on='instrument_id',
    how='left'
).merge(
    assets[['asset_id', 'symbol', 'name']],
    on='asset_id',
    how='left'
)
```

## Migration Path

### Current State

1. **Data Lake Files:** ✅ Fully aligned and operational
2. **Wide Format Files:** ❌ Still exist for backward compatibility
3. **Output Files:** ⚠️ May need updates to include `asset_id`

### Recommended Actions

1. **Use Data Lake Files** for all new code
   - Prefer `fact_*.parquet` over `*_daily.parquet`
   - Use `asset_id` for joins

2. **Update Output Files** to include `asset_id`
   - Add `asset_id` column to `universe_eligibility.parquet`
   - Add `asset_id` column to `universe_snapshots.parquet`

3. **Deprecate Wide Format** (eventually)
   - Keep for backward compatibility
   - Don't use in new code
   - Can remove once all code is migrated

## Summary

**Standardized Data Lake Format =**
- ✅ Canonical IDs (`asset_id`, `instrument_id`)
- ✅ Long format (normalized)
- ✅ Dimension + Fact + Mapping tables
- ✅ Consistent schema across all files
- ✅ Easy joins and queries

**Files Aligned:** ✅ All files in `data/curated/data_lake/`

**Files Not Aligned:** 
- ❌ Wide format files (`*_daily.parquet`) - Legacy format
- ❌ Output files (`universe_eligibility.parquet`, `universe_snapshots.parquet`) - Missing `asset_id`

---

**Key Takeaway:** The data lake format uses **normalized relational tables** with **canonical IDs** for consistent joins across all data sources. This is the modern, scalable approach vs. the legacy wide format.

