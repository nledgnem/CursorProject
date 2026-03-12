#!/usr/bin/env python3
"""Inspect marketcap_daily parquet file - focused summary."""

import pandas as pd
import sys

# Set encoding for Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

df = pd.read_parquet('data/curated/marketcap_daily.parquet')

print("=" * 70)
print("MARKETCAP_DAILY.PARQUET - DATA SUMMARY")
print("=" * 70)

print(f"\n📊 DIMENSIONS:")
print(f"   • Rows (dates): {df.shape[0]:,}")
print(f"   • Columns (symbols): {df.shape[1]:,}")
print(f"   • Date range: {df.index.min().date()} to {df.index.max().date()}")
print(f"   • Total days: {len(df)}")

print(f"\n📈 DATA QUALITY:")
missing = df.isna().sum()
print(f"   • Total missing values: {missing.sum():,}")
print(f"   • Symbols with any missing: {(missing > 0).sum()}")
print(f"   • Symbols with all missing: {(missing == len(df)).sum()}")
print(f"   • Symbols with complete data: {(missing == 0).sum()}")

print(f"\n💰 SAMPLE VALUES (First date: {df.index[0].date()}):")
first_date = df.index[0]
first_date_data = df.loc[first_date].dropna().sort_values(ascending=False)
print(f"\n   Top 20 by market cap:")
for i, (sym, val) in enumerate(first_date_data.head(20).items(), 1):
    print(f"   {i:2d}. {sym:12s} ${val:>15,.0f}")

print(f"\n📅 SAMPLE VALUES (Last date: {df.index[-1].date()}):")
last_date = df.index[-1]
last_date_data = df.loc[last_date].dropna().sort_values(ascending=False)
print(f"\n   Top 20 by market cap:")
for i, (sym, val) in enumerate(last_date_data.head(20).items(), 1):
    print(f"   {i:2d}. {sym:12s} ${val:>15,.0f}")

print(f"\n🔍 SAMPLE SYMBOLS (first 30):")
print(f"   {', '.join(df.columns[:30])}")

print(f"\n📊 STATISTICS:")
print(f"   • Min market cap: ${df.min().min():,.0f}")
print(f"   • Max market cap: ${df.max().max():,.0f}")
print(f"   • Mean market cap (non-null): ${df.stack().mean():,.0f}")
print(f"   • Median market cap (non-null): ${df.stack().median():,.0f}")

print("\n" + "=" * 70)


