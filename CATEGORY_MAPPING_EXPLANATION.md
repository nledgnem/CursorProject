# Asset-to-Category Mapping Explanation

## Current State

### ❌ `dim_categories.parquet` - Category Metadata Only

**What it contains:**
- `category_id` - Category identifier (e.g., "proof-of-work", "layer-1")
- `category_name` - Category display name (e.g., "Proof of Work (PoW)", "Layer 1 (L1)")
- `source` - Data source ("coingecko")

**What it does NOT contain:**
- ❌ Which assets belong to each category
- ❌ Asset-to-category mappings

**Example:**
```
category_id: proof-of-work
category_name: Proof of Work (PoW)
source: coingecko
```

This tells you the category exists, but NOT which coins are in it.

---

## ✅ Solution: `map_category_asset.parquet` - Asset-to-Category Mappings

**What it contains:**
- `asset_id` - Canonical asset identifier (e.g., "BTC", "ETH")
- `category_id` - Category ID from dim_categories (e.g., "proof-of-work")
- `category_name` - Category name from API (for reference)
- `source` - Data source ("coingecko")

**Example:**
```
asset_id: BTC
category_id: proof-of-work
category_name: Proof of Work (PoW)
source: coingecko
```

This tells you **BTC belongs to the "proof-of-work" category**.

---

## How to Use

### Example 1: Find which categories BTC belongs to

```python
import polars as pl

# Load mapping table
mappings = pl.read_parquet('data/curated/data_lake/map_category_asset.parquet')

# Get categories for BTC
btc_categories = mappings.filter(pl.col('asset_id') == 'BTC')
print(btc_categories.select(['asset_id', 'category_id', 'category_name']))
```

**Result:**
- BTC belongs to: "Proof of Work (PoW)", "Layer 1 (L1)", "Bitcoin Ecosystem", etc.

### Example 2: Find all assets in "proof-of-work" category

```python
# Get all assets in proof-of-work category
pow_assets = mappings.filter(pl.col('category_id') == 'proof-of-work')
print(pow_assets.select(['asset_id', 'category_id']))
```

### Example 3: Join with dim_asset for full details

```python
# Load dimension tables
assets = pl.read_parquet('data/curated/data_lake/dim_asset.parquet')
categories = pl.read_parquet('data/curated/data_lake/dim_categories.parquet')
mappings = pl.read_parquet('data/curated/data_lake/map_category_asset.parquet')

# Get BTC with its categories
btc_with_categories = (
    assets.filter(pl.col('asset_id') == 'BTC')
    .join(
        mappings.select(['asset_id', 'category_id']),
        on='asset_id',
        how='left'
    )
    .join(
        categories.select(['category_id', 'category_name']),
        on='category_id',
        how='left'
    )
)
```

---

## Current Status

### ✅ Implemented:
- `dim_categories.parquet` - 739 categories (metadata only)
- `map_category_asset.parquet` - Asset-to-category mappings (sample: 27 mappings for 4 assets)

### ⚠️ Partial:
- **Sample data only** - Currently has mappings for 20 test assets
- **Full fetch needed** - To get mappings for all ~2,700 assets, need to run full fetch

---

## To Get Complete Mappings

Run the full fetch (will make ~2,700 API calls):

```python
# In scripts/fetch_asset_categories.py, change:
fetch_and_save_asset_categories(max_assets=None)  # Remove limit
```

Or modify the script to process all assets.

---

## Summary

**Question:** Does `dim_categories` tell you which assets belong to each category?

**Answer:** ❌ **No.** `dim_categories` only has category metadata.

**Solution:** ✅ Use `map_category_asset.parquet` which contains the asset-to-category mappings.

**Example:**
- `dim_categories` tells you "proof-of-work" category exists
- `map_category_asset` tells you BTC belongs to "proof-of-work"

---

## Data Lake Structure

```
dim_categories.parquet          → Category metadata (ID, name)
         ↓
map_category_asset.parquet      → Asset-to-category mappings
         ↓
dim_asset.parquet               → Asset metadata
```

**Join path:**
```
dim_asset.asset_id = map_category_asset.asset_id
map_category_asset.category_id = dim_categories.category_id
```

This allows you to:
1. Find which categories an asset belongs to
2. Find which assets belong to a category
3. Get full asset and category details via joins
