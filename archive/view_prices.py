#!/usr/bin/env python3
"""View prices_daily parquet file in readable format."""

import pandas as pd
import sys

# Set encoding for Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

df = pd.read_parquet('data/curated/prices_daily.parquet')

print("=" * 80)
print("PRICES_DAILY.PARQUET - DATA VIEWER")
print("=" * 80)

print(f"\n📊 DIMENSIONS:")
print(f"   • Rows (dates): {df.shape[0]:,}")
print(f"   • Columns (symbols): {df.shape[1]:,}")
print(f"   • Date range: {df.index.min().date()} to {df.index.max().date()}")

print(f"\n📈 DATA QUALITY:")
missing = df.isna().sum()
print(f"   • Total missing values: {missing.sum():,}")
print(f"   • Symbols with any missing: {(missing > 0).sum()}")
print(f"   • Symbols with complete data: {(missing == 0).sum()}")

print(f"\n💰 FIRST 10 DATES - SAMPLE PRICES (showing first 20 symbols):")
print("-" * 80)
sample_symbols = df.columns[:20].tolist()
sample_dates = df.head(10)

# Create a formatted display
for date in sample_dates.index:
    print(f"\n{date.date()}:")
    for sym in sample_symbols:
        val = df.loc[date, sym]
        if pd.notna(val):
            print(f"   {sym:12s} ${val:>12,.4f}")
        else:
            print(f"   {sym:12s} {'NaN':>12s}")

print(f"\n\n📅 LAST 10 DATES - SAMPLE PRICES (showing first 20 symbols):")
print("-" * 80)
last_dates = df.tail(10)

for date in last_dates.index:
    print(f"\n{date.date()}:")
    for sym in sample_symbols:
        val = df.loc[date, sym]
        if pd.notna(val):
            print(f"   {sym:12s} ${val:>12,.4f}")
        else:
            print(f"   {sym:12s} {'NaN':>12s}")

print(f"\n\n🔍 TOP 20 SYMBOLS BY PRICE (on first date):")
print("-" * 80)
first_date = df.index[0]
first_date_prices = df.loc[first_date].dropna().sort_values(ascending=False)
for i, (sym, price) in enumerate(first_date_prices.head(20).items(), 1):
    print(f"   {i:2d}. {sym:12s} ${price:>15,.4f}")

print(f"\n\n📊 STATISTICS:")
print(f"   • Min price: ${df.min().min():,.4f}")
print(f"   • Max price: ${df.max().max():,.4f}")
print(f"   • Mean price (non-null): ${df.stack().mean():,.4f}")
print(f"   • Median price (non-null): ${df.stack().median():,.4f}")

print(f"\n🔍 ALL SYMBOLS ({len(df.columns)} total):")
print(f"   {', '.join(df.columns.tolist())}")

print("\n" + "=" * 80)
print("\n💡 TIP: To see specific symbols or dates, modify this script or use:")
print("   df = pd.read_parquet('data/curated/prices_daily.parquet')")
print("   df.loc['2024-01-01', ['BTC', 'ETH', 'BNB']]  # Example query")

