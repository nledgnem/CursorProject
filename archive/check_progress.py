#!/usr/bin/env python3
"""Check progress of data fetch script."""

import pandas as pd
from pathlib import Path
import time
import os

print("=" * 70)
print("DATA FETCH PROGRESS CHECK")
print("=" * 70)

# Check funding data
funding_path = Path("data/curated/data_lake/fact_funding.parquet")
if funding_path.exists():
    df = pd.read_parquet(funding_path)
    mtime = funding_path.stat().st_mtime
    age_minutes = (time.time() - mtime) / 60
    
    print(f"\n[OK] Funding file exists")
    print(f"  Records: {len(df):,}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Assets: {df['asset_id'].nunique()}")
    print(f"  Last modified: {age_minutes:.1f} minutes ago")
    if age_minutes < 5:
        print("  Status: [ACTIVE] Recently updated - script is active")
    elif age_minutes < 30:
        print("  Status: [SLOW] Not updated recently - may be processing")
    else:
        print("  Status: [STUCK?] Not updated for a while - may be stuck")
else:
    print("\n[NO FILE] Funding file does not exist yet")

# Check OI data
oi_path = Path("data/curated/data_lake/fact_open_interest.parquet")
if oi_path.exists():
    df2 = pd.read_parquet(oi_path)
    mtime2 = oi_path.stat().st_mtime
    age_minutes2 = (time.time() - mtime2) / 60
    
    print(f"\n[OK] OI file exists")
    print(f"  Records: {len(df2):,}")
    print(f"  Date range: {df2['date'].min()} to {df2['date'].max()}")
    print(f"  Assets: {df2['asset_id'].nunique()}")
    print(f"  Last modified: {age_minutes2:.1f} minutes ago")
    if age_minutes2 < 5:
        print("  Status: [ACTIVE] Recently updated - script is active")
    elif age_minutes2 < 30:
        print("  Status: [SLOW] Not updated recently - may be processing")
    else:
        print("  Status: [STUCK?] Not updated for a while - may be stuck")
else:
    print("\n[NO FILE] OI file does not exist yet")

print("\n" + "=" * 70)
