#!/usr/bin/env python3
"""Check data freshness and date ranges for all data tables."""

import sys
import io
from pathlib import Path
from datetime import date, datetime
import pandas as pd
import json

# Windows encoding fix
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def check_data_freshness():
    """Check freshness of all data tables."""
    repo_root = Path(__file__).parent.parent
    data_lake_dir = repo_root / "data" / "curated" / "data_lake"
    curated_dir = repo_root / "data" / "curated"
    
    print("=" * 80)
    print("DATA TABLE FRESHNESS CHECK")
    print("=" * 80)
    print(f"\nCurrent Date: {date.today()}")
    print(f"Check Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Check data lake tables
    print("\n" + "=" * 80)
    print("DATA LAKE TABLES (data/curated/data_lake/)")
    print("=" * 80)
    
    data_lake_tables = {
        "dim_asset": "Dimension: Assets",
        "dim_instrument": "Dimension: Instruments",
        "map_provider_asset": "Mapping: Provider → Asset",
        "map_provider_instrument": "Mapping: Provider → Instrument",
        "fact_price": "Fact: Prices",
        "fact_marketcap": "Fact: Market Cap",
        "fact_volume": "Fact: Volume",
        "fact_funding": "Fact: Funding Rates",
    }
    
    for table_name, description in data_lake_tables.items():
        filepath = data_lake_dir / f"{table_name}.parquet"
        if filepath.exists():
            # Get file modification time
            mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
            file_age_days = (datetime.now() - mtime).days
            
            # Read data and check date range
            try:
                df = pd.read_parquet(filepath)
                row_count = len(df)
                
                # Check for date column
                date_info = ""
                if "date" in df.columns:
                    date_col = pd.to_datetime(df["date"])
                    min_date = date_col.min().date()
                    max_date = date_col.max().date()
                    days_old = (date.today() - max_date).days
                    date_info = f" | Date Range: {min_date} to {max_date} | Latest: {days_old} days ago"
                elif "valid_from" in df.columns:
                    date_col = pd.to_datetime(df["valid_from"])
                    min_date = date_col.min().date()
                    max_date = date_col.max().date()
                    date_info = f" | Valid From Range: {min_date} to {max_date}"
                
                # Check for asset_id or symbol count
                asset_info = ""
                if "asset_id" in df.columns:
                    asset_count = df["asset_id"].nunique()
                    asset_info = f" | Assets: {asset_count}"
                elif "symbol" in df.columns:
                    asset_count = df["symbol"].nunique()
                    asset_info = f" | Symbols: {asset_count}"
                
                status = "✅" if file_age_days <= 1 else "⚠️" if file_age_days <= 7 else "❌"
                print(f"{status} {description:30} | Rows: {row_count:>8,} | Updated: {mtime.strftime('%Y-%m-%d %H:%M')} ({file_age_days} days ago){date_info}{asset_info}")
                
            except Exception as e:
                print(f"❌ {description:30} | ERROR: {e}")
        else:
            print(f"[MISSING] {description:30} | FILE NOT FOUND")
    
    # Check metadata files
    print("\n" + "=" * 80)
    print("METADATA FILES")
    print("=" * 80)
    
    metadata_files = [
        (data_lake_dir / "funding_metadata.json", "Funding Metadata"),
        (data_lake_dir / "mapping_validation.json", "Mapping Validation"),
        (curated_dir / "run_metadata_download.json", "Download Metadata"),
        (curated_dir / "run_metadata_snapshots.json", "Snapshots Metadata"),
    ]
    
    for filepath, description in metadata_files:
        if filepath.exists():
            mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
            file_age_days = (datetime.now() - mtime).days
            
            try:
                with open(filepath) as f:
                    metadata = json.load(f)
                
                timestamp = metadata.get("timestamp", metadata.get("validation_timestamp", ""))
                if timestamp:
                    try:
                        meta_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        file_age_days = (datetime.now() - meta_time.replace(tzinfo=None)).days
                    except:
                        pass
                
                status = "✅" if file_age_days <= 1 else "⚠️" if file_age_days <= 7 else "❌"
                print(f"{status} {description:30} | Updated: {mtime.strftime('%Y-%m-%d %H:%M')} ({file_age_days} days ago)")
                
                # Show key metadata
                if "date_range" in metadata:
                    dr = metadata["date_range"]
                    print(f"   Date Range: {dr.get('start', 'N/A')} to {dr.get('end', 'N/A')}")
                if "row_count" in metadata:
                    print(f"   Row Count: {metadata['row_count']:,}")
                    
            except Exception as e:
                print(f"❌ {description:30} | ERROR: {e}")
        else:
            print(f"⚠️  {description:30} | FILE NOT FOUND")
    
    # Check pipeline outputs
    print("\n" + "=" * 80)
    print("PIPELINE OUTPUTS (data/curated/)")
    print("=" * 80)
    
    output_tables = {
        "universe_eligibility.parquet": "Universe Eligibility",
        "universe_snapshots.parquet": "Basket Snapshots",
        "prices_daily.parquet": "Prices (Wide Format)",
        "marketcap_daily.parquet": "Market Cap (Wide Format)",
        "volume_daily.parquet": "Volume (Wide Format)",
    }
    
    for filename, description in output_tables.items():
        filepath = curated_dir / filename
        if filepath.exists():
            mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
            file_age_days = (datetime.now() - mtime).days
            
            try:
                df = pd.read_parquet(filepath)
                row_count = len(df)
                
                date_info = ""
                if "date" in df.columns or "rebalance_date" in df.columns:
                    date_col_name = "date" if "date" in df.columns else "rebalance_date"
                    date_col = pd.to_datetime(df[date_col_name])
                    min_date = date_col.min().date()
                    max_date = date_col.max().date()
                    days_old = (date.today() - max_date).days
                    date_info = f" | Date Range: {min_date} to {max_date} | Latest: {days_old} days ago"
                
                status = "✅" if file_age_days <= 1 else "⚠️" if file_age_days <= 7 else "❌"
                print(f"{status} {description:30} | Rows: {row_count:>8,} | Updated: {mtime.strftime('%Y-%m-%d %H:%M')} ({file_age_days} days ago){date_info}")
                
            except Exception as e:
                print(f"❌ {description:30} | ERROR: {e}")
        else:
            print(f"⚠️  {description:30} | FILE NOT FOUND")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("\n[OK] = Updated within last 24 hours")
    print("[WARN] = Updated within last 7 days")
    print("[OLD] = Older than 7 days (may need refresh)")
    print("\nTo refresh data, run:")
    print("  python scripts/run_pipeline.py --config configs/golden.yaml")


if __name__ == "__main__":
    check_data_freshness()
