# Full Category Mapping Fetch - Status

## Current Status

The full asset-to-category mapping fetch has been started. This process will:

- **Process:** ~2,717 assets (all assets with CoinGecko IDs)
- **API Calls:** ~2,700 calls (one per asset)
- **Estimated Time:** 5-10 minutes
- **Rate Limit:** 0.12 seconds between calls (500 calls/min limit)

## What's Being Fetched

For each asset, the script:
1. Calls `/coins/{id}` endpoint
2. Extracts the `categories` field (list of category names)
3. Maps category names to `category_id` from `dim_categories`
4. Saves to `map_category_asset.parquet`

## Output File

**File:** `data/curated/data_lake/map_category_asset.parquet`

**Schema:**
- `asset_id` - Canonical asset ID (e.g., "BTC")
- `category_id` - Category ID from dim_categories (e.g., "proof-of-work")
- `category_name` - Category name from API (for reference)
- `source` - "coingecko"

## How to Check Progress

Run:
```bash
python check_category_fetch_progress.py
```

This will show:
- Current number of mappings
- Unique assets processed
- Estimated progress percentage
- Sample mappings

## Example Usage After Completion

### Find which categories BTC belongs to:
```python
import polars as pl

mappings = pl.read_parquet('data/curated/data_lake/map_category_asset.parquet')
btc_categories = mappings.filter(pl.col('asset_id') == 'BTC')
print(btc_categories.select(['asset_id', 'category_id', 'category_name']))
```

### Find all assets in "proof-of-work" category:
```python
pow_assets = mappings.filter(pl.col('category_id') == 'proof-of-work')
print(pow_assets.select(['asset_id', 'category_id']))
```

## Notes

- The script runs in the background
- It will continue even if the terminal is closed
- Progress is saved incrementally (file is updated as assets are processed)
- If interrupted, you can re-run and it will skip already-processed assets
