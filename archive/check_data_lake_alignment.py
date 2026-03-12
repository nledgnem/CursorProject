"""Check which parquet files are aligned to the standardized data lake format."""

import pandas as pd
from pathlib import Path
from collections import defaultdict

def check_file_alignment(file_path: Path, expected_format: str):
    """Check if a file is aligned to data lake format."""
    try:
        df = pd.read_parquet(file_path)
        cols = df.columns.tolist()
        
        info = {
            'file': str(file_path),
            'rows': len(df),
            'columns': cols,
            'format': 'unknown'
        }
        
        # Check for data lake format indicators
        has_asset_id = 'asset_id' in cols
        has_instrument_id = 'instrument_id' in cols
        has_date_col = 'date' in cols or 'rebalance_date' in cols
        
        # Determine format
        if expected_format == 'fact':
            # Fact tables should have: asset_id, date, and a value column
            if has_asset_id and has_date_col and len(cols) <= 5:
                info['format'] = 'data_lake_fact'
                info['aligned'] = True
            else:
                info['format'] = 'wide' if len(cols) > 100 else 'other'
                info['aligned'] = False
        elif expected_format == 'dim':
            if has_asset_id or has_instrument_id:
                info['format'] = 'data_lake_dimension'
                info['aligned'] = True
            else:
                info['format'] = 'other'
                info['aligned'] = False
        elif expected_format == 'map':
            if (has_asset_id or has_instrument_id) and 'provider' in cols:
                info['format'] = 'data_lake_mapping'
                info['aligned'] = True
            else:
                info['format'] = 'other'
                info['aligned'] = False
        elif expected_format == 'output':
            # Output tables may or may not have asset_id yet
            if has_asset_id:
                info['format'] = 'data_lake_output'
                info['aligned'] = True
            else:
                info['format'] = 'legacy_output'
                info['aligned'] = False
        else:
            info['format'] = 'unknown'
            info['aligned'] = None
        
        return info
    except Exception as e:
        return {'file': str(file_path), 'error': str(e)}

def main():
    print("=" * 80)
    print("Data Lake Format Alignment Check")
    print("=" * 80)
    print()
    
    repo_root = Path(__file__).parent
    
    # Define files to check
    files_to_check = [
        # Data Lake files (should be aligned)
        ('data/curated/data_lake/dim_asset.parquet', 'dim'),
        ('data/curated/data_lake/dim_instrument.parquet', 'dim'),
        ('data/curated/data_lake/map_provider_asset.parquet', 'map'),
        ('data/curated/data_lake/map_provider_instrument.parquet', 'map'),
        ('data/curated/data_lake/fact_price.parquet', 'fact'),
        ('data/curated/data_lake/fact_marketcap.parquet', 'fact'),
        ('data/curated/data_lake/fact_volume.parquet', 'fact'),
        ('data/curated/data_lake/fact_funding.parquet', 'fact'),
        
        # Wide format files (legacy, not aligned)
        ('data/curated/prices_daily.parquet', 'wide'),
        ('data/curated/marketcap_daily.parquet', 'wide'),
        ('data/curated/volume_daily.parquet', 'wide'),
        ('data/raw/prices_daily.parquet', 'wide'),
        ('data/raw/marketcap_daily.parquet', 'wide'),
        ('data/raw/volume_daily.parquet', 'wide'),
        
        # Output files (may or may not be aligned)
        ('data/curated/universe_eligibility.parquet', 'output'),
        ('data/curated/universe_snapshots.parquet', 'output'),
        
        # Other files
        ('data/raw/perp_listings_binance.parquet', 'other'),
        ('data/curated/perp_listings_binance_aligned.parquet', 'other'),
    ]
    
    results = defaultdict(list)
    
    print("Checking files...")
    print()
    
    for file_path_str, expected_format in files_to_check:
        file_path = repo_root / file_path_str
        if not file_path.exists():
            print(f"[SKIP] {file_path_str} (not found)")
            continue
        
        info = check_file_alignment(file_path, expected_format)
        
        if 'error' in info:
            print(f"[ERROR] {file_path_str}: {info['error']}")
            results['error'].append(info)
        else:
            status = "OK" if info.get('aligned') else "NOT ALIGNED" if info.get('aligned') == False else "?"
            print(f"[{status:12}] {file_path_str}")
            print(f"           Format: {info['format']}, Rows: {info['rows']:,}, Columns: {len(info['columns'])}")
            if 'asset_id' in info['columns']:
                print(f"           Has asset_id: YES")
            if 'instrument_id' in info['columns']:
                print(f"           Has instrument_id: YES")
            print()
            results[info['format']].append(info)
    
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print()
    
    print("FILES ALIGNED TO DATA LAKE FORMAT:")
    for info in results.get('data_lake_fact', []) + results.get('data_lake_dimension', []) + results.get('data_lake_mapping', []):
        print(f"  [OK] {Path(info['file']).name}")
    
    print()
    print("OUTPUT FILES (may need alignment):")
    for info in results.get('data_lake_output', []):
        print(f"  [OK] {Path(info['file']).name} (has asset_id)")
    for info in results.get('legacy_output', []):
        print(f"  [NEEDS WORK] {Path(info['file']).name} (missing asset_id)")
    
    print()
    print("WIDE FORMAT FILES (legacy, not aligned):")
    for info in results.get('wide', []):
        print(f"  [LEGACY] {Path(info['file']).name} (wide format, not normalized)")
    
    print()
    print("OTHER FILES:")
    for info in results.get('other', []):
        file_name = Path(info['file']).name
        if 'aligned' in file_name:
            print(f"  [OK] {file_name} (aligned)")
        else:
            print(f"  [?] {file_name} (check manually)")

if __name__ == "__main__":
    main()

