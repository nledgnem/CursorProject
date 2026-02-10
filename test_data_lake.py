#!/usr/bin/env python3
"""Test data lake implementation."""

import sys
import pandas as pd
from pathlib import Path
import duckdb

# Fix Windows encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Test fact tables
print("=" * 70)
print("TESTING DATA LAKE FACT TABLES")
print("=" * 70)

data_lake_dir = Path("data/curated/data_lake")

# Test fact_price
fact_price = pd.read_parquet(data_lake_dir / "fact_price.parquet")
print(f"\n[OK] fact_price: {len(fact_price):,} rows")
print(f"   Columns: {list(fact_price.columns)}")
print(f"   Date range: {fact_price['date'].min()} to {fact_price['date'].max()}")
print(f"   Unique assets: {fact_price['asset_id'].nunique()}")
print(f"   Sample:")
print(fact_price.head(5))

# Test dim_asset
dim_asset = pd.read_parquet(data_lake_dir / "dim_asset.parquet")
print(f"\n[OK] dim_asset: {len(dim_asset)} rows")
print(f"   Columns: {list(dim_asset.columns)}")
print(f"   Sample:")
print(dim_asset.head(5))

# Test DuckDB views
print("\n" + "=" * 70)
print("TESTING DUCKDB VIEWS")
print("=" * 70)

db_path = Path("outputs/runs/20251222_020959_33c6310e263588f2/research.duckdb")
if not db_path.exists():
    print(f"[FAIL] Database not found: {db_path}")
    print("   Run pipeline first to create database")
else:
    conn = duckdb.connect(str(db_path))
    
    # Test if data lake views exist
    try:
        result = conn.execute("SELECT COUNT(*) FROM fact_price").fetchone()
        print(f"\n[OK] fact_price view: {result[0]:,} rows")
    except Exception as e:
        print(f"\n[FAIL] fact_price view not found: {e}")
    
    try:
        result = conn.execute("SELECT COUNT(*) FROM dim_asset").fetchone()
        print(f"[OK] dim_asset view: {result[0]:,} rows")
    except Exception as e:
        print(f"[FAIL] dim_asset view not found: {e}")
    
    # Test a join query
    try:
        query = """
        SELECT 
            da.symbol,
            COUNT(DISTINCT fp.date) as days_with_price,
            AVG(fp.close) as avg_price
        FROM fact_price fp
        JOIN dim_asset da ON fp.asset_id = da.asset_id
        WHERE da.symbol IN ('BTC', 'ETH', 'BNB', 'XRP', 'ADA')
        GROUP BY da.symbol
        ORDER BY avg_price DESC
        """
        result = conn.execute(query).fetchdf()
        print(f"\n[OK] Join query (fact_price + dim_asset):")
        print(result)
    except Exception as e:
        print(f"\n[FAIL] Join query failed: {e}")
    
    conn.close()

print("\n" + "=" * 70)
print("TEST COMPLETE")
print("=" * 70)
