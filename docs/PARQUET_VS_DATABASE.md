# Parquet Files vs. Single Database: Best Practices

## Current Setup (Recommended ✅)

Your pipeline uses **separate parquet files** with **DuckDB views** - this is actually the **optimal approach** for analytics workloads.

### How It Works Now

1. **Parquet Files** (separate files):
   - `prices_daily.parquet` - 723 days × 939 symbols
   - `marketcap_daily.parquet` - 723 days × 939 symbols  
   - `volume_daily.parquet` - 723 days × 939 symbols
   - `universe_snapshots.parquet` - basket selections
   - `universe_eligibility.parquet` - all candidates
   - `perp_listings_binance.parquet` - perp metadata

2. **DuckDB Views** (in `research.duckdb`):
   - Creates SQL views that point to parquet files
   - **Does NOT copy data** - queries parquet files directly
   - Uses `read_parquet()` function

### Why This Is Good ✅

**Advantages:**
- ✅ **No duplication**: Data lives in one place (parquet files)
- ✅ **Efficient**: DuckDB only reads columns/dates you query
- ✅ **Fast**: Parquet is columnar (perfect for analytics)
- ✅ **Flexible**: Easy to update individual files
- ✅ **Small DB file**: `research.duckdb` is just metadata (~KB, not GB)
- ✅ **Version control friendly**: Parquet files are immutable snapshots

**Example Query:**
```sql
-- DuckDB queries parquet directly, doesn't load everything
SELECT symbol, price 
FROM prices_daily 
WHERE date = '2024-01-01' 
  AND symbol IN ('BTC', 'ETH', 'BNB')
```

## Alternative: Import into Single Database

If you want a **single consolidated database** (all data imported), you can do this:

### Option 1: Import Parquet as Tables (Not Recommended)

```python
# This would COPY all data into DuckDB
conn.execute("CREATE TABLE prices_daily AS SELECT * FROM read_parquet('prices_daily.parquet')")
conn.execute("CREATE TABLE marketcap_daily AS SELECT * FROM read_parquet('marketcap_daily.parquet')")
# ... etc
```

**Disadvantages:**
- ❌ **Duplicates data**: Now you have parquet + database copies
- ❌ **Large DB file**: Database becomes GB-sized
- ❌ **Slower updates**: Must re-import when parquet files change
- ❌ **Memory intensive**: Loads everything into DuckDB

### Option 2: Keep Current Approach (Recommended) ✅

**Your current setup is optimal!** DuckDB views give you:
- Single query interface (SQL)
- No data duplication
- Efficient columnar reads
- Easy updates (just regenerate parquet files)

## When to Use Each Approach

| Use Case | Parquet + Views (Current) | Imported Tables |
|----------|---------------------------|-----------------|
| Analytics queries | ✅ Perfect | ⚠️ Works but slower |
| Frequent updates | ✅ Easy | ❌ Must re-import |
| Disk space | ✅ Efficient | ❌ Duplicates data |
| Query performance | ✅ Fast (columnar) | ✅ Fast (but duplicates) |
| Version control | ✅ Immutable files | ❌ Large binary DB |

## Recommendation

**Keep your current setup!** It's the industry-standard approach:
- **Parquet files** = immutable, versioned data snapshots
- **DuckDB views** = unified SQL query interface
- **Best of both worlds**: No duplication + SQL convenience

## If You Still Want Consolidation

If you really want a single database file, I can create a script that:
1. Imports all parquet files as tables
2. Creates a consolidated `research_full.duckdb` file
3. Keeps the original parquet files (for versioning)

But honestly, **your current approach is better** for most use cases.

