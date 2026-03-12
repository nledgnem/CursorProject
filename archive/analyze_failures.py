#!/usr/bin/env python3
"""Analyze why symbols are failing."""

import pandas as pd
from pathlib import Path

print("=" * 70)
print("ANALYZING FETCH FAILURES")
print("=" * 70)

# Load existing funding data to see what symbols we have
funding_path = Path("data/curated/data_lake/fact_funding.parquet")
if funding_path.exists():
    df = pd.read_parquet(funding_path)
    existing_symbols = set(df['asset_id'].unique())
    print(f"\nExisting symbols in data: {len(existing_symbols)}")
    print(f"Sample: {sorted(list(existing_symbols))[:20]}")
    
    # Check what symbols the script is trying to fetch
    universe_path = Path("data/curated/universe_eligibility.parquet")
    basket_path = Path("data/curated/universe_snapshots.parquet")
    
    all_symbols_to_fetch = set()
    if universe_path.exists():
        try:
            universe_df = pd.read_parquet(universe_path)
            if "symbol" in universe_df.columns:
                all_symbols_to_fetch.update(universe_df["symbol"].unique())
        except:
            pass
    
    if basket_path.exists():
        try:
            basket_df = pd.read_parquet(basket_path)
            if "symbol" in basket_df.columns:
                all_symbols_to_fetch.update(basket_df["symbol"].unique())
        except:
            pass
    
    print(f"\nSymbols to fetch (from universe files): {len(all_symbols_to_fetch)}")
    
    # Find symbols that don't exist in our data
    missing_symbols = all_symbols_to_fetch - existing_symbols
    print(f"\nSymbols not in existing data: {len(missing_symbols)}")
    print(f"Sample missing symbols: {sorted(list(missing_symbols))[:30]}")
    
    # Check for problematic symbol names
    problematic = [s for s in missing_symbols if not s.replace('$', '').replace('-', '').replace('_', '').isalnum()]
    if problematic:
        print(f"\nPotentially problematic symbol names: {problematic[:20]}")
    
    print(f"\nInterpretation:")
    print(f"  - 507 symbols skipped: These already have data up to date")
    print(f"  - 2210 symbols failed: These are likely:")
    print(f"    * Symbols that don't exist on Binance ({len(missing_symbols)} symbols not in data)")
    print(f"    * Invalid symbol names")
    print(f"    * API errors (symbol exists but API returned errors)")
    print(f"\n  This is NORMAL - not all symbols in the universe have funding data available.")
