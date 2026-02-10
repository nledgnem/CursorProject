"""
Check ID standardization across all parquet files.
Examines ID columns and compares them across different data sources.
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from collections import defaultdict
import json
import datetime
import numpy as np

def get_parquet_files(base_dir="data"):
    """Find all parquet files in data directory."""
    data_dir = Path(base_dir)
    parquet_files = []
    
    # Key directories to check
    for pattern in ["**/*.parquet"]:
        for file in data_dir.rglob(pattern):
            # Skip venv files
            if "venv" not in str(file):
                parquet_files.append(file)
    
    return sorted(parquet_files)

def analyze_parquet_file(file_path):
    """Analyze a single parquet file and extract ID information."""
    try:
        # Read schema without loading full data
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema_arrow
        
        # Get column names
        columns = [field.name for field in schema]
        
        # Identify ID columns (common patterns)
        id_columns = [col for col in columns if any(
            keyword in col.lower() for keyword in [
                'id', 'asset_id', 'instrument_id', 'canonical', 
                'symbol', 'ticker', 'provider_asset_id', 'provider_instrument_id'
            ]
        )]
        
        # Read full file to analyze
        df_full = pd.read_parquet(file_path)
        df_sample = df_full.head(1000)
        
        id_info = {}
        for col in id_columns:
            if col in df_full.columns:
                sample_values = df_sample[col].dropna().head(20).tolist()
                dtype = str(df_full[col].dtype)
                unique_count_sample = df_sample[col].nunique()
                unique_count_full = df_full[col].nunique()
                null_count_sample = df_sample[col].isna().sum()
                total_count_sample = len(df_sample)
                total_count_full = len(df_full)
                
                id_info[col] = {
                    'dtype': dtype,
                    'sample_values': sample_values,
                    'unique_count_sample': unique_count_sample,
                    'unique_count_full': unique_count_full,
                    'null_count_sample': null_count_sample,
                    'total_count_sample': total_count_sample,
                    'total_count_full': total_count_full
                }
        
        return {
            'file_path': str(file_path),
            'columns': columns,
            'id_columns': id_columns,
            'id_info': id_info,
            'num_rows_sample': len(df_sample)
        }
    
    except Exception as e:
        return {
            'file_path': str(file_path),
            'error': str(e)
        }

def compare_ids_across_files(analysis_results):
    """Compare ID formats and values across files."""
    # Group by ID column name
    id_column_groups = defaultdict(list)
    
    for result in analysis_results:
        if 'error' in result:
            continue
        
        file_path = Path(result['file_path']).name
        for col, info in result['id_info'].items():
            id_column_groups[col].append({
                'file': file_path,
                'full_path': result['file_path'],
                'info': info
            })
    
    comparisons = {}
    for col_name, files_with_col in id_column_groups.items():
        if len(files_with_col) < 2:
            continue  # Only compare if column exists in multiple files
        
        # Check data types
        dtypes = [f['info']['dtype'] for f in files_with_col]
        dtype_consistent = len(set(dtypes)) == 1
        
        # Check sample values for format consistency
        all_samples = []
        for f in files_with_col:
            all_samples.extend(f['info'].get('sample_values', []))
        
        # Analyze value formats
        value_patterns = analyze_value_patterns(all_samples)
        
        comparisons[col_name] = {
            'files': [f['file'] for f in files_with_col],
            'dtype_consistent': dtype_consistent,
            'dtypes': {f['file']: f['info']['dtype'] for f in files_with_col},
            'value_patterns': value_patterns,
            'file_details': {f['file']: {
                'unique_count': f['info'].get('unique_count_full', f['info'].get('unique_count_sample')),
                'total_count': f['info'].get('total_count_full', f['info'].get('total_count_sample')),
                'sample_values': f['info'].get('sample_values', [])[:5]
            } for f in files_with_col}
        }
    
    return comparisons

def analyze_value_patterns(values):
    """Analyze patterns in ID values."""
    if not values:
        return {}
    
    patterns = {
        'has_underscores': sum(1 for v in values if isinstance(v, str) and '_' in str(v)),
        'has_dashes': sum(1 for v in values if isinstance(v, str) and '-' in str(v)),
        'has_colons': sum(1 for v in values if isinstance(v, str) and ':' in str(v)),
        'is_lowercase': sum(1 for v in values if isinstance(v, str) and str(v).islower()),
        'is_uppercase': sum(1 for v in values if isinstance(v, str) and str(v).isupper()),
        'is_mixed_case': sum(1 for v in values if isinstance(v, str) and not str(v).islower() and not str(v).isupper()),
        'numeric_only': sum(1 for v in values if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit())),
        'avg_length': sum(len(str(v)) for v in values) / len(values) if values else 0,
        'min_length': min(len(str(v)) for v in values) if values else 0,
        'max_length': max(len(str(v)) for v in values) if values else 0,
    }
    
    return patterns

def find_id_overlaps(analysis_results):
    """Find overlapping ID values across different files."""
    id_value_sets = defaultdict(dict)
    
    for result in analysis_results:
        if 'error' in result:
            continue
        
        file_path = Path(result['file_path']).name
        for col, info in result['id_info'].items():
            try:
                df = pd.read_parquet(result['file_path'])
                if col in df.columns:
                    unique_values = set(df[col].dropna().unique())
                    id_value_sets[col][file_path] = unique_values
            except Exception as e:
                print(f"  Warning: Could not load {file_path} for overlap check: {e}")
    
    overlaps = {}
    for col_name, file_value_sets in id_value_sets.items():
        if len(file_value_sets) < 2:
            continue
        
        file_names = list(file_value_sets.keys())
        overlaps[col_name] = {}
        
        # Check pairwise overlaps
        for i, file1 in enumerate(file_names):
            for file2 in file_names[i+1:]:
                set1 = file_value_sets[file1]
                set2 = file_value_sets[file2]
                intersection = set1 & set2
                union = set1 | set2
                
                if len(union) > 0:
                    overlap_pct = len(intersection) / len(union) * 100
                else:
                    overlap_pct = 0
                
                overlaps[col_name][f"{file1} <-> {file2}"] = {
                    'intersection_size': len(intersection),
                    'union_size': len(union),
                    'overlap_percentage': overlap_pct,
                    'sample_intersection': list(intersection)[:10]
                }
    
    return overlaps

def main():
    print("=" * 80)
    print("ID Standardization Check Across Parquet Files")
    print("=" * 80)
    print()
    
    # Find all parquet files
    print("Scanning for parquet files...")
    parquet_files = get_parquet_files()
    print(f"Found {len(parquet_files)} parquet files")
    print()
    
    # Analyze each file
    print("Analyzing files...")
    analysis_results = []
    for file_path in parquet_files:
        print(f"  Analyzing: {file_path.name}")
        result = analyze_parquet_file(file_path)
        analysis_results.append(result)
        if 'error' in result:
            print(f"    ERROR: {result['error']}")
        else:
            print(f"    ID columns found: {result['id_columns']}")
    
    print()
    print("=" * 80)
    print("FILE-BY-FILE ANALYSIS")
    print("=" * 80)
    print()
    
    # Display file-by-file analysis
    for result in analysis_results:
        if 'error' in result:
            print(f"\n{Path(result['file_path']).name}: ERROR - {result['error']}")
            continue
        
        print(f"\n{Path(result['file_path']).name}:")
        print(f"  Columns: {len(result['columns'])} total")
        print(f"  ID columns: {result['id_columns']}")
        
        for col, info in result['id_info'].items():
            print(f"\n  {col}:")
            print(f"    Data type: {info['dtype']}")
            if 'unique_count_full' in info:
                print(f"    Unique values: {info['unique_count_full']:,} (full file)")
                print(f"    Total rows: {info['total_count_full']:,}")
            else:
                print(f"    Unique values: {info['unique_count_sample']:,} (sample)")
                print(f"    Total rows (sample): {info['total_count_sample']:,}")
            
            if info['sample_values']:
                print(f"    Sample values: {info['sample_values'][:5]}")
    
    print()
    print("=" * 80)
    print("CROSS-FILE COMPARISONS")
    print("=" * 80)
    print()
    
    # Compare IDs across files
    comparisons = compare_ids_across_files(analysis_results)
    
    if not comparisons:
        print("No ID columns found in multiple files for comparison.")
    else:
        for col_name, comp in comparisons.items():
            print(f"\n{col_name}:")
            print(f"  Found in {len(comp['files'])} files: {', '.join(comp['files'])}")
            print(f"  Data type consistent: {comp['dtype_consistent']}")
            if not comp['dtype_consistent']:
                print(f"    WARNING: Data types differ!")
                for file, dtype in comp['dtypes'].items():
                    print(f"      {file}: {dtype}")
            
            print(f"\n  Value patterns:")
            patterns = comp['value_patterns']
            print(f"    Contains underscores: {patterns.get('has_underscores', 0)}/{len(comp['file_details'][comp['files'][0]]['sample_values']) * len(comp['files'])}")
            print(f"    Contains colons: {patterns.get('has_colons', 0)}/{len(comp['file_details'][comp['files'][0]]['sample_values']) * len(comp['files'])}")
            print(f"    Average length: {patterns.get('avg_length', 0):.1f}")
            print(f"    Length range: {patterns.get('min_length', 0)}-{patterns.get('max_length', 0)}")
            
            print(f"\n  Per-file details:")
            for file, details in comp['file_details'].items():
                print(f"    {file}:")
                print(f"      Unique count: {details['unique_count']:,}")
                print(f"      Total count: {details['total_count']:,}")
                print(f"      Sample: {details['sample_values']}")
    
    print()
    print("=" * 80)
    print("ID VALUE OVERLAPS")
    print("=" * 80)
    print()
    
    # Find overlaps
    print("Checking for overlapping ID values across files...")
    overlaps = find_id_overlaps(analysis_results)
    
    if not overlaps:
        print("No overlapping ID columns found for overlap analysis.")
    else:
        for col_name, overlap_info in overlaps.items():
            print(f"\n{col_name}:")
            for pair, stats in overlap_info.items():
                print(f"  {pair}:")
                print(f"    Overlap: {stats['intersection_size']:,} common values")
                print(f"    Overlap %: {stats['overlap_percentage']:.1f}%")
                if stats['sample_intersection']:
                    print(f"    Sample common values: {stats['sample_intersection']}")
    
    # Save results to JSON
    output_file = "id_standardization_report.json"
    report = {
        'analysis_results': analysis_results,
        'comparisons': comparisons,
        'overlaps': overlaps
    }
    
    # Convert to JSON-serializable format
    def make_serializable(obj):
        if isinstance(obj, (dict, list)):
            if isinstance(obj, dict):
                return {k: make_serializable(v) for k, v in obj.items()}
            else:
                return [make_serializable(item) for item in obj]
        elif isinstance(obj, (pd.Timestamp, datetime.date, datetime.datetime)):
            return str(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif hasattr(obj, 'item'):  # numpy scalar types
            return obj.item()
        else:
            return obj
    
    report_serializable = make_serializable(report)
    
    with open(output_file, 'w') as f:
        json.dump(report_serializable, f, indent=2)
    
    print()
    print("=" * 80)
    print(f"Report saved to: {output_file}")
    print("=" * 80)

if __name__ == "__main__":
    main()

