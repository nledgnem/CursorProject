#!/usr/bin/env python3
"""Inspect marketcap_daily parquet file."""

import pandas as pd
import sys

# Set encoding for Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

df = pd.read_parquet('data/curated/marketcap_daily.parquet')

print("=" * 60)
print("MARKETCAP_DAILY.PARQUET SUMMARY")
print("=" * 60)
print(f"\nShape: {df.shape[0]} rows (dates) x {df.shape[1]} columns (symbols)")
print(f"\nDate range: {df.index.min()} to {df.index.max()}")
print(f"Number of trading days: {len(df)}")
print(f"Number of symbols: {len(df.columns)}")

print(f"\nFirst 5 dates:")
print(df.head(5).to_string())

print(f"\nLast 5 dates:")
print(df.tail(5).to_string())

print(f"\nSample symbols (first 20):")
print(list(df.columns[:20]))

print(f"\nData types:")
print(df.dtypes.value_counts())

print(f"\nMissing data summary:")
missing = df.isna().sum()
print(f"  Total missing values: {missing.sum():,}")
print(f"  Symbols with any missing: {(missing > 0).sum()}")
print(f"  Symbols with all missing: {(missing == len(df)).sum()}")

print(f"\nSample market cap values (first date, first 10 symbols):")
first_date = df.index[0]
sample_symbols = df.columns[:10]
print(f"Date: {first_date}")
for sym in sample_symbols:
    val = df.loc[first_date, sym]
    if pd.notna(val):
        print(f"  {sym}: ${val:,.0f}")
    else:
        print(f"  {sym}: NaN")

print(f"\nLargest market caps on first date:")
first_date_data = df.loc[first_date].dropna().sort_values(ascending=False)
print(first_date_data.head(10).to_string())


