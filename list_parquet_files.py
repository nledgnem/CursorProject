#!/usr/bin/env python3
"""List all parquet files with details."""

from pathlib import Path
import polars as pl
from datetime import date

data_lake_dir = Path("data/curated/data_lake")
parquet_files = sorted([f for f in data_lake_dir.glob("*.parquet")])

print("=" * 80)
print("DATA LAKE PARQUET FILES INVENTORY")
print("=" * 80)
print()

# Group by type
dimension_files = []
fact_files = []
mapping_files = []
test_files = []

for f in parquet_files:
    name = f.name
    if name.startswith("dim_"):
        dimension_files.append(f)
    elif name.startswith("fact_"):
        if "test" in name:
            test_files.append(f)
        else:
            fact_files.append(f)
    elif name.startswith("map_"):
        mapping_files.append(f)
    else:
        fact_files.append(f)

print("DIMENSION TABLES (Metadata):")
print("-" * 80)
for f in dimension_files:
    try:
        df = pl.read_parquet(str(f))
        print(f"  {f.name:40s} | {len(df):,} rows | {len(df.columns)} columns")
    except Exception as e:
        print(f"  {f.name:40s} | ERROR: {e}")

print()
print("FACT TABLES (Time-series data):")
print("-" * 80)
for f in fact_files:
    try:
        df = pl.read_parquet(str(f))
        if "date" in df.columns:
            date_range = f"{df['date'].min()} to {df['date'].max()}"
            print(f"  {f.name:40s} | {len(df):,} rows | Date: {date_range}")
        else:
            print(f"  {f.name:40s} | {len(df):,} rows | {len(df.columns)} columns")
    except Exception as e:
        print(f"  {f.name:40s} | ERROR: {e}")

print()
print("MAPPING TABLES:")
print("-" * 80)
for f in mapping_files:
    try:
        df = pl.read_parquet(str(f))
        print(f"  {f.name:40s} | {len(df):,} rows | {len(df.columns)} columns")
    except Exception as e:
        print(f"  {f.name:40s} | ERROR: {e}")

if test_files:
    print()
    print("TEST FILES (can be removed):")
    print("-" * 80)
    for f in test_files:
        try:
            df = pl.read_parquet(str(f))
            print(f"  {f.name:40s} | {len(df):,} rows")
        except Exception as e:
            print(f"  {f.name:40s} | ERROR: {e}")

print()
print("=" * 80)
print(f"TOTAL: {len(parquet_files)} parquet files")
print("=" * 80)
