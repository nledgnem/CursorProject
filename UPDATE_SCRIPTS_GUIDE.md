# Data Update Scripts Guide

## Current Update Behavior

### ❌ **FULL REPLACE (Truncate-and-Reload)**

**Current Scripts**:
1. `scripts/download_data.py` - **FULL REPLACE**
2. `scripts/convert_to_fact_tables.py` - **FULL REPLACE**
3. `scripts/fetch_coinglass_funding.py` - **FULL REPLACE** (but can be made incremental)

**Problem**: These scripts **overwrite** existing files, causing:
- Re-downloading entire datasets (e.g., 2 years of data)
- Wasting API calls and time
- No incremental updates

---

## ✅ **Solution: Incremental Update Script**

### New Script: `scripts/incremental_update.py`

**Behavior**: **INCREMENTAL (Upsert/Append)**

This script:
1. ✅ Checks existing fact tables to find latest dates
2. ✅ Downloads only missing date ranges
3. ✅ Merges new data with existing (deduplicates)
4. ✅ Appends to fact tables (no full replacement)

### Usage

```bash
# Auto-detect latest date and update from there
python scripts/incremental_update.py

# Update last 7 days (if no existing data)
python scripts/incremental_update.py --days-back 7

# Update specific date range
python scripts/incremental_update.py --start-date 2025-12-20 --end-date 2025-12-22
```

### How It Works

1. **Checks Existing Data**:
   - Reads `fact_price.parquet`, `fact_marketcap.parquet`, `fact_volume.parquet`
   - Finds latest date per asset
   - Determines overall latest date

2. **Downloads Only New Data**:
   - Downloads from `latest_date + 1` to `today`
   - Merges with existing wide format files

3. **Appends to Fact Tables**:
   - Converts only new wide format data to fact format
   - Appends to existing fact tables
   - Deduplicates on `(asset_id, date)` - keeps newest value

4. **Saves**:
   - Updates both wide format and fact tables
   - Preserves all existing data

---

## Which Script to Use?

### For Daily Updates (Recommended) ✅

**Use**: `scripts/incremental_update.py`

```bash
# Daily update (auto-detects what's needed)
python scripts/incremental_update.py
```

**Benefits**:
- ✅ Only downloads new data (e.g., last 3 days)
- ✅ Fast (minutes instead of hours)
- ✅ API-friendly (fewer calls)
- ✅ Preserves existing data

### For Full Refresh (Initial Setup or After Long Gap)

**Use**: `scripts/run_pipeline.py` (full pipeline)

```bash
# Full pipeline (downloads everything)
python scripts/run_pipeline.py --config configs/golden.yaml
```

**When to use**:
- Initial setup
- After long gap (e.g., >30 days)
- When you want to refresh everything
- When data quality issues detected

---

## Update Behavior Comparison

| Script | Behavior | Speed | API Calls | Use Case |
|--------|----------|-------|-----------|----------|
| `incremental_update.py` | ✅ **Incremental** | Fast (minutes) | Few (only new dates) | **Daily updates** |
| `download_data.py` | ❌ Full replace | Slow (hours) | Many (all dates) | Initial setup |
| `convert_to_fact_tables.py` | ❌ Full replace | Medium | None | After download |
| `run_pipeline.py` | ❌ Full replace | Very slow | Many | Full refresh |

---

## Recommended Workflow

### Daily/Regular Updates

```bash
# Step 1: Incremental update (downloads only new data)
python scripts/incremental_update.py

# Step 2: Update funding (if needed)
python scripts/fetch_coinglass_funding.py --api-key YOUR_KEY

# Step 3: Rebuild snapshots (if needed)
python scripts/run_pipeline.py --config configs/golden.yaml --skip-download --skip-qc
```

### Weekly/Monthly Full Refresh

```bash
# Full pipeline refresh
python scripts/run_pipeline.py --config configs/golden.yaml --coinglass-api-key YOUR_KEY
```

---

## Implementation Details

### Incremental Update Logic

```python
# 1. Check existing fact tables
existing_fact_price = pd.read_parquet("fact_price.parquet")
latest_date = existing_fact_price["date"].max()  # e.g., 2025-12-19

# 2. Download only new dates
start_date = latest_date + timedelta(days=1)  # 2025-12-20
end_date = date.today()  # 2025-12-22
download_data(start_date, end_date)  # Only 3 days!

# 3. Merge with existing
new_wide = load_new_wide_format()
existing_wide = load_existing_wide_format()
merged_wide = existing_wide.combine_first(new_wide)

# 4. Append to fact tables
new_fact = convert_to_fact(new_wide)
merged_fact = pd.concat([existing_fact_price, new_fact])
merged_fact = merged_fact.drop_duplicates(subset=["asset_id", "date"], keep="last")
merged_fact.to_parquet("fact_price.parquet")
```

---

## Testing Incremental Update

```bash
# Test with small date range first
python scripts/incremental_update.py --start-date 2025-12-20 --end-date 2025-12-20

# Verify data was appended (not replaced)
python scripts/check_data_freshness.py
```

---

## Summary

**✅ Use `incremental_update.py` for daily updates** - It's incremental, fast, and efficient.

**❌ Avoid `download_data.py` + `convert_to_fact_tables.py` for regular updates** - They do full replace.

**The incremental script automatically**:
- Detects what data you already have
- Downloads only missing dates
- Merges and deduplicates
- Preserves all existing data

This is exactly what you need for efficient daily updates! 🎉
