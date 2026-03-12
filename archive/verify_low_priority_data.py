#!/usr/bin/env python3
"""Verify low-priority data fetch results and schema compliance."""

import polars as pl
from pathlib import Path

data_lake_dir = Path("data/curated/data_lake")

print("=" * 80)
print("LOW-PRIORITY DATA VERIFICATION & SCHEMA COMPLIANCE")
print("=" * 80)
print()

files = {
    "dim_categories.parquet": "Categories List",
    "fact_exchange_tickers.parquet": "Exchange Tickers",
}

for filename, description in files.items():
    filepath = data_lake_dir / filename
    if not filepath.exists():
        print(f"[ERROR] {filename} - NOT FOUND")
        continue
    
    try:
        df = pl.read_parquet(str(filepath))
        print(f"[OK] {filename} - {description}")
        print(f"   Rows: {len(df):,}")
        print(f"   Columns ({len(df.columns)}): {', '.join(df.columns)}")
        
        if "date" in df.columns:
            print(f"   Date range: {df['date'].min()} to {df['date'].max()}")
        
        if "category_id" in df.columns:
            print(f"   Unique categories: {df['category_id'].n_unique()}")
            if len(df) <= 20:
                print(f"   Sample categories: {df['category_id'].unique()[:10].to_list()}")
        
        if "exchange_id" in df.columns:
            print(f"   Unique exchanges: {df['exchange_id'].n_unique()}")
            print(f"   Exchanges: {df['exchange_id'].unique().to_list()}")
        
        if "ticker_pair" in df.columns:
            print(f"   Unique trading pairs: {df['ticker_pair'].n_unique()}")
            print(f"   Sample pairs: {df['ticker_pair'].unique()[:10].to_list()}")
        
        # Check for required schema fields
        if filename == "dim_categories.parquet":
            required = ["category_id", "category_name", "source"]
            missing = [col for col in required if col not in df.columns]
            if missing:
                print(f"   [WARN] Missing required columns: {missing}")
            else:
                print(f"   [OK] All required schema columns present")
        
        if filename == "fact_exchange_tickers.parquet":
            required = ["date", "exchange_id", "ticker_pair", "source"]
            missing = [col for col in required if col not in df.columns]
            if missing:
                print(f"   [WARN] Missing required columns: {missing}")
            else:
                print(f"   [OK] All required schema columns present")
        
        # Show sample data
        print(f"   Sample row:")
        sample = df.head(1).to_dicts()[0]
        for key, value in list(sample.items())[:5]:
            print(f"     {key}: {value}")
        
        print()

    except Exception as e:
        print(f"[ERROR] {filename} - ERROR: {e}")
        print()

print("=" * 80)
print("SCHEMA COMPLIANCE CHECK")
print("=" * 80)
print()

# Check against schema definitions
try:
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from src.data_lake.schema import DIM_CATEGORIES_SCHEMA, FACT_EXCHANGE_TICKERS_SCHEMA
    
    # Check dim_categories
    if (data_lake_dir / "dim_categories.parquet").exists():
        df = pl.read_parquet(str(data_lake_dir / "dim_categories.parquet"))
        schema_cols = set(DIM_CATEGORIES_SCHEMA.keys())
        df_cols = set(df.columns)
        
        missing = schema_cols - df_cols
        extra = df_cols - schema_cols
        
        if missing:
            print(f"[WARN] dim_categories missing columns: {missing}")
        if extra:
            print(f"[INFO] dim_categories extra columns: {extra}")
        if not missing and not extra:
            print(f"[OK] dim_categories matches schema exactly")
    
    # Check fact_exchange_tickers
    if (data_lake_dir / "fact_exchange_tickers.parquet").exists():
        df = pl.read_parquet(str(data_lake_dir / "fact_exchange_tickers.parquet"))
        schema_cols = set(FACT_EXCHANGE_TICKERS_SCHEMA.keys())
        df_cols = set(df.columns)
        
        missing = schema_cols - df_cols
        extra = df_cols - schema_cols
        
        if missing:
            print(f"[WARN] fact_exchange_tickers missing columns: {missing}")
        if extra:
            print(f"[INFO] fact_exchange_tickers extra columns: {extra}")
        if not missing and not extra:
            print(f"[OK] fact_exchange_tickers matches schema exactly")
    
except Exception as e:
    print(f"[WARN] Could not verify schema compliance: {e}")

print()
print("=" * 80)
