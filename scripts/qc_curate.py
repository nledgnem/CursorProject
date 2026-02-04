#!/usr/bin/env python3
"""Quality control and curation pipeline for raw crypto data."""

import sys
import argparse
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import subprocess
import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.metadata import get_git_commit_hash, get_file_hash


# ============================================================================
# QC CONFIGURATION (configurable defaults)
# ============================================================================

QC_CONFIG = {
    # Non-negativity
    "prices_must_be_positive": True,
    "mcap_volume_allow_zero": True,  # Market cap/volume can be 0, but not negative
    
    # Outlier detection
    "RET_SPIKE": 5.0,  # 500% return spike threshold
    "MCAP_MULT": 20,  # Market cap spike = mcap > MCAP_MULT * 30d median
    "VOL_MULT": 50,  # Volume spike = vol > VOL_MULT * 30d median
    
    # Repairs
    # NOTE: Gap filling is now handled in the backtest layer, not QC.
    # QC should preserve missing values as NA to keep the data layer honest.
    "allow_ffill": False,  # Disabled: gap filling moved to backtest layer
    "max_ffill_days": 2,  # Not used when allow_ffill=False
    "allow_interpolate": False,  # Interpolation off by default
    "allow_post_align_ffill": False,  # Disabled: gap filling moved to backtest layer
}


def get_config_hash(config: Dict) -> str:
    """Get hash of config dict for metadata."""
    config_str = json.dumps(config, sort_keys=True)
    return hashlib.sha256(config_str.encode()).hexdigest()[:16]


# ============================================================================
# DATA LOADING
# ============================================================================

def load_raw_panels(raw_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load raw parquet panels from directory.
    
    Returns:
        Dict with keys: 'prices', 'marketcap', 'volume'
    """
    panels = {}
    
    prices_path = raw_dir / "prices_daily.parquet"
    mcaps_path = raw_dir / "marketcap_daily.parquet"
    volumes_path = raw_dir / "volume_daily.parquet"
    
    if not prices_path.exists():
        raise FileNotFoundError(f"Prices file not found: {prices_path}")
    if not mcaps_path.exists():
        raise FileNotFoundError(f"Market cap file not found: {mcaps_path}")
    if not volumes_path.exists():
        raise FileNotFoundError(f"Volume file not found: {volumes_path}")
    
    print(f"Loading raw panels from {raw_dir}")
    panels['prices'] = pd.read_parquet(prices_path)
    panels['marketcap'] = pd.read_parquet(mcaps_path)
    panels['volume'] = pd.read_parquet(volumes_path)
    
    # Ensure datetime index
    for name, df in panels.items():
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        df.index.name = 'date'
        # Sort by date
        panels[name] = df.sort_index()
    
    print(f"  Prices: {len(panels['prices'])} rows, {len(panels['prices'].columns)} symbols")
    print(f"  Market cap: {len(panels['marketcap'])} rows, {len(panels['marketcap'].columns)} symbols")
    print(f"  Volume: {len(panels['volume'])} rows, {len(panels['volume'].columns)} symbols")
    
    return panels


# ============================================================================
# SANITY CHECKS (non-negativity, duplicates, etc.)
# ============================================================================

def apply_sanity_checks(
    df: pd.DataFrame,
    dataset_name: str,
    repair_log: List[Dict],
    config: Dict,
) -> pd.DataFrame:
    """
    Apply basic sanity checks: non-negativity, duplicates.
    
    Modifies df in-place and appends to repair_log.
    """
    df = df.copy()
    original_shape = df.shape
    
    # Check for duplicate dates in index
    duplicate_dates = df.index[df.index.duplicated(keep=False)]
    if len(duplicate_dates) > 0:
        unique_dupes = duplicate_dates.unique()
        print(f"  [WARN] {len(unique_dupes)} duplicate dates found in {dataset_name}")
        # Keep last occurrence
        df = df[~df.index.duplicated(keep='last')]
        # Log summary (individual rows only if reasonable number)
        if len(unique_dupes) <= 100:
            for dup_date in unique_dupes:
                repair_log.append({
                    "dataset": dataset_name,
                    "symbol": "ALL",
                    "date": str(dup_date),
                    "action": "keep_last",
                    "rule": "duplicate_date",
                    "old_value": None,
                    "new_value": None,
                    "notes": f"Removed duplicate date, kept last occurrence",
                })
        else:
            repair_log.append({
                "dataset": dataset_name,
                "symbol": "ALL",
                "date": f"{len(unique_dupes)} dates",
                "action": "keep_last",
                "rule": "duplicate_date",
                "old_value": None,
                "new_value": None,
                "notes": f"{len(unique_dupes)} duplicate dates removed (kept last)",
            })
    
    # Non-negativity checks
    if dataset_name == "prices" and config.get("prices_must_be_positive", True):
        # Prices must be > 0
        mask_zero = (df == 0) & df.notna()
        mask_neg = (df < 0) & df.notna()
        
        for col in df.columns:
            # Zero prices
            zero_idx = df.index[mask_zero[col]]
            for idx in zero_idx:
                old_val = df.loc[idx, col]
                df.loc[idx, col] = np.nan
                repair_log.append({
                    "dataset": dataset_name,
                    "symbol": col,
                    "date": str(idx),
                    "action": "set_na",
                    "rule": "zero_price",
                    "old_value": float(old_val),
                    "new_value": None,
                    "notes": "",
                })
            
            # Negative prices
            neg_idx = df.index[mask_neg[col]]
            for idx in neg_idx:
                old_val = df.loc[idx, col]
                df.loc[idx, col] = np.nan
                repair_log.append({
                    "dataset": dataset_name,
                    "symbol": col,
                    "date": str(idx),
                    "action": "set_na",
                    "rule": "neg_price",
                    "old_value": float(old_val),
                    "new_value": None,
                    "notes": "",
                })
    
    elif dataset_name in ["marketcap", "volume"]:
        # Market cap/volume: allow zero, but not negative
        allow_zero = config.get("mcap_volume_allow_zero", True)
        mask_neg = (df < 0) & df.notna()
        
        for col in df.columns:
            neg_idx = df.index[mask_neg[col]]
            for idx in neg_idx:
                old_val = df.loc[idx, col]
                df.loc[idx, col] = np.nan
                repair_log.append({
                    "dataset": dataset_name,
                    "symbol": col,
                    "date": str(idx),
                    "action": "set_na",
                    "rule": "neg_price",  # Reuse rule name
                    "old_value": float(old_val),
                    "new_value": None,
                    "notes": "",
                })
    
    return df


# ============================================================================
# OUTLIER DETECTION
# ============================================================================

def apply_outlier_flags(
    df: pd.DataFrame,
    dataset_name: str,
    repair_log: List[Dict],
    config: Dict,
) -> pd.DataFrame:
    """
    Apply outlier detection rules and set flagged points to NA.
    
    For prices: return spikes, jump-and-revert
    For mcap/volume: rolling median spikes
    """
    df = df.copy()
    
    if dataset_name == "prices":
        # Calculate returns (fill_method=None prevents bridging across gaps)
        returns = df.pct_change(fill_method=None)
        
        # Return spike detection
        ret_spike_thresh = config.get("RET_SPIKE", 5.0)
        # Only flag spikes where return was computed (both t-1 and t must be non-NA)
        # returns.notna() ensures this (pct_change with fill_method=None returns NaN if t-1 or t is NaN)
        spike_mask = (returns.abs() > ret_spike_thresh) & returns.notna()
        
        for col in df.columns:
            spike_dates = list(returns.index[spike_mask[col]])
            handled_dates = set()  # Track dates already handled by jump_revert
            
            # First pass: Check for jump-and-revert patterns
            for i, date_idx in enumerate(spike_dates):
                if date_idx in handled_dates:
                    continue
                
                if i + 1 < len(spike_dates):
                    next_date = spike_dates[i + 1]
                    # Check if consecutive dates and opposite signs
                    if (next_date - date_idx).days == 1:
                        ret_t = returns.loc[date_idx, col]
                        ret_t1 = returns.loc[next_date, col]
                        if np.sign(ret_t) == -np.sign(ret_t1):
                            # Jump and revert - flag both
                            old_val_t = df.loc[date_idx, col]
                            old_val_t1 = df.loc[next_date, col]
                            
                            df.loc[date_idx, col] = np.nan
                            df.loc[next_date, col] = np.nan
                            
                            repair_log.append({
                                "dataset": dataset_name,
                                "symbol": col,
                                "date": str(date_idx),
                                "action": "set_na",
                                "rule": "jump_revert",
                                "old_value": float(old_val_t),
                                "new_value": None,
                                "notes": f"Jump: {ret_t:.2%}, Revert: {ret_t1:.2%}",
                            })
                            repair_log.append({
                                "dataset": dataset_name,
                                "symbol": col,
                                "date": str(next_date),
                                "action": "set_na",
                                "rule": "jump_revert",
                                "old_value": float(old_val_t1),
                                "new_value": None,
                                "notes": f"Jump: {ret_t:.2%}, Revert: {ret_t1:.2%}",
                            })
                            
                            handled_dates.add(date_idx)
                            handled_dates.add(next_date)
            
            # Second pass: Regular return spikes (not handled by jump_revert)
            for date_idx in spike_dates:
                if date_idx in handled_dates:
                    continue  # Already handled by jump_revert
                
                if pd.isna(df.loc[date_idx, col]):
                    continue  # Already set to NA somehow
                
                old_val = df.loc[date_idx, col]
                ret = returns.loc[date_idx, col]
                df.loc[date_idx, col] = np.nan
                repair_log.append({
                    "dataset": dataset_name,
                    "symbol": col,
                    "date": str(date_idx),
                    "action": "set_na",
                    "rule": "return_spike",
                    "old_value": float(old_val),
                    "new_value": None,
                    "notes": f"Return: {ret:.2%}",
                })
    
    elif dataset_name == "marketcap":
        # Rolling median spike detection
        mcap_mult = config.get("MCAP_MULT", 20)
        window = 30
        
        for col in df.columns:
            series = df[col]
            rolling_median = series.rolling(window=window, min_periods=1).median()
            threshold = rolling_median * mcap_mult
            
            spike_mask = (series > threshold) & series.notna()
            spike_dates = series.index[spike_mask]
            
            for date_idx in spike_dates:
                old_val = df.loc[date_idx, col]
                median_val = rolling_median.loc[date_idx]
                df.loc[date_idx, col] = np.nan
                repair_log.append({
                    "dataset": dataset_name,
                    "symbol": col,
                    "date": str(date_idx),
                    "action": "set_na",
                    "rule": "mcap_spike",
                    "old_value": float(old_val),
                    "new_value": None,
                    "notes": f"Value: {old_val:.0f}, Median: {median_val:.0f}",
                })
    
    elif dataset_name == "volume":
        # Rolling median spike detection
        vol_mult = config.get("VOL_MULT", 50)
        window = 30
        
        for col in df.columns:
            series = df[col]
            rolling_median = series.rolling(window=window, min_periods=1).median()
            threshold = rolling_median * vol_mult
            
            spike_mask = (series > threshold) & series.notna()
            spike_dates = series.index[spike_mask]
            
            for date_idx in spike_dates:
                old_val = df.loc[date_idx, col]
                median_val = rolling_median.loc[date_idx]
                df.loc[date_idx, col] = np.nan
                repair_log.append({
                    "dataset": dataset_name,
                    "symbol": col,
                    "date": str(date_idx),
                    "rule": "vol_spike",
                    "action": "set_na",
                    "old_value": float(old_val),
                    "new_value": None,
                    "notes": f"Value: {old_val:.0f}, Median: {median_val:.0f}",
                })
    
    return df


# ============================================================================
# REPAIRS (gap filling)
# ============================================================================

def apply_repairs(
    df: pd.DataFrame,
    dataset_name: str,
    repair_log: List[Dict],
    config: Dict,
) -> pd.DataFrame:
    """
    Apply repairs: forward fill for short gaps (prices only).
    
    Only fills 1-2 consecutive NA days if there is valid data before.
    """
    df = df.copy()
    
    # Only repair prices
    if dataset_name != "prices":
        return df
    
    if not config.get("allow_ffill", True):
        return df
    
    max_ffill = config.get("max_ffill_days", 2)
    
    for col in df.columns:
        series = df[col].copy()
        
        # Find gaps (consecutive NAs)
        is_na = series.isna()
        if not is_na.any():
            continue  # No gaps in this series
        
        # Use shift to identify gap boundaries
        # A gap starts when we transition from non-NA to NA
        gap_starts = (~is_na.shift(1, fill_value=False)) & is_na
        
        # Process each gap
        for gap_start_idx in series.index[gap_starts]:
            gap_start_pos = series.index.get_loc(gap_start_idx)
            
            # Count consecutive NAs from this start
            gap_size = 0
            gap_indices = []
            for i in range(gap_start_pos, len(series)):
                if pd.isna(series.iloc[i]):
                    gap_size += 1
                    gap_indices.append(series.index[i])
                else:
                    break
            
            if gap_size == 0 or gap_size > max_ffill:
                continue  # Skip large gaps or invalid
            
            # Check if there's valid data before the gap
            if gap_start_pos == 0:
                continue  # No data before
            
            # Get the last valid value before gap
            last_valid_idx = series.iloc[:gap_start_pos].last_valid_index()
            if last_valid_idx is None:
                continue  # No valid data before
            
            fill_value = series.loc[last_valid_idx]
            
            # Forward fill
            for fill_idx in gap_indices:
                df.loc[fill_idx, col] = fill_value
                repair_log.append({
                    "dataset": dataset_name,
                    "symbol": col,
                    "date": str(fill_idx),
                    "action": "ffill",
                    "rule": "missing_gap",
                    "old_value": None,
                    "new_value": float(fill_value),
                    "notes": f"Gap size: {gap_size}",
                })
    
    return df


# ============================================================================
# ALIGN DATASETS
# ============================================================================

def align_datasets(panels: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Align all datasets to a complete daily index (full date range).
    Preserves NA values.
    """
    # Get min and max dates across all datasets
    all_dates = []
    for df in panels.values():
        if len(df) > 0:
            all_dates.extend([df.index.min(), df.index.max()])
    
    if not all_dates:
        return panels
    
    min_date = min(all_dates)
    max_date = max(all_dates)
    
    # Create complete daily range
    common_index = pd.date_range(start=min_date, end=max_date, freq="D", name="date")
    
    aligned = {}
    for name, df in panels.items():
        aligned[name] = df.reindex(common_index)
    
    return aligned


# ============================================================================
# OUTPUT FUNCTIONS
# ============================================================================

def write_outputs(
    panels: Dict[str, pd.DataFrame],
    out_dir: Path,
) -> None:
    """Write curated parquet files."""
    out_dir.mkdir(parents=True, exist_ok=True)
    
    file_map = {
        "prices": "prices_daily.parquet",
        "marketcap": "marketcap_daily.parquet",
        "volume": "volume_daily.parquet",
    }
    
    for name, df in panels.items():
        output_path = out_dir / file_map[name]
        df.to_parquet(output_path)
        print(f"  Saved {name} to {output_path}")


def write_repair_log(repair_log: List[Dict], outputs_dir: Path) -> None:
    """Write repair log to parquet."""
    outputs_dir.mkdir(parents=True, exist_ok=True)
    
    if not repair_log:
        # Create empty DataFrame with correct schema
        df = pd.DataFrame(columns=[
            "dataset", "symbol", "date", "action", "rule",
            "old_value", "new_value", "notes"
        ])
    else:
        df = pd.DataFrame(repair_log)
        # Ensure date is string for parquet compatibility
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)
    
    output_path = outputs_dir / "repair_log.parquet"
    df.to_parquet(output_path, index=False)
    print(f"  Saved repair log ({len(df)} entries) to {output_path}")


def write_qc_report(
    raw_panels: Dict[str, pd.DataFrame],
    curated_panels: Dict[str, pd.DataFrame],
    repair_log: List[Dict],
    outputs_dir: Path,
    config: Dict,
) -> None:
    """Write human-readable QC report."""
    outputs_dir.mkdir(parents=True, exist_ok=True)
    
    report_lines = []
    report_lines.append("# QC Curation Report")
    report_lines.append("")
    report_lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    report_lines.append("")
    
    # Dataset summaries
    for dataset in ["prices", "marketcap", "volume"]:
        raw_df = raw_panels[dataset]
        curated_df = curated_panels[dataset]
        
        report_lines.append(f"## {dataset.upper()}")
        report_lines.append("")
        
        # Date range
        date_range = f"{raw_df.index.min()} to {raw_df.index.max()}"
        n_days = len(raw_df)
        n_symbols = len(raw_df.columns)
        report_lines.append(f"- Date range: {date_range}")
        report_lines.append(f"- Number of days: {n_days}")
        report_lines.append(f"- Number of symbols: {n_symbols}")
        
        # Missingness before/after (use each dataframe's own shape)
        raw_total = raw_df.shape[0] * raw_df.shape[1]
        curated_total = curated_df.shape[0] * curated_df.shape[1]
        raw_missing_pct = (raw_df.isna().sum().sum() / raw_total) * 100 if raw_total > 0 else 0.0
        curated_missing_pct = (curated_df.isna().sum().sum() / curated_total) * 100 if curated_total > 0 else 0.0
        report_lines.append(f"- Missingness before QC: {raw_missing_pct:.2f}%")
        report_lines.append(f"- Missingness after QC: {curated_missing_pct:.2f}%")
        report_lines.append("")
    
    # Repair log summary
    if repair_log:
        repair_df = pd.DataFrame(repair_log)
        report_lines.append("## Repair Summary")
        report_lines.append("")
        
        # Counts by rule
        rule_counts = repair_df["rule"].value_counts()
        report_lines.append("### Edits by Rule")
        for rule, count in rule_counts.items():
            report_lines.append(f"- {rule}: {count} edits")
        report_lines.append("")
        
        # Top symbols by edit count
        symbol_counts = repair_df["symbol"].value_counts().head(20)
        report_lines.append("### Top 20 Symbols by Number of Edits")
        for symbol, count in symbol_counts.items():
            report_lines.append(f"- {symbol}: {count} edits")
        report_lines.append("")
    else:
        report_lines.append("## Repair Summary")
        report_lines.append("")
        report_lines.append("No repairs applied.")
        report_lines.append("")
    
    # Config summary
    report_lines.append("## QC Configuration")
    report_lines.append("")
    for key, value in sorted(config.items()):
        report_lines.append(f"- {key}: {value}")
    report_lines.append("")
    
    # Write report
    output_path = outputs_dir / "qc_report.md"
    with open(output_path, "w") as f:
        f.write("\n".join(report_lines))
    print(f"  Saved QC report to {output_path}")


def write_run_metadata(
    raw_dir: Path,
    out_dir: Path,
    outputs_dir: Path,
    raw_panels: Dict[str, pd.DataFrame],
    curated_panels: Dict[str, pd.DataFrame],
    repair_log: List[Dict],
    config: Dict,
    repo_root: Path,
    config_path: Optional[Path] = None,
) -> None:
    """Write run metadata JSON."""
    outputs_dir.mkdir(parents=True, exist_ok=True)
    
    # File mappings
    raw_files = {
        "prices": raw_dir / "prices_daily.parquet",
        "marketcap": raw_dir / "marketcap_daily.parquet",
        "volume": raw_dir / "volume_daily.parquet",
    }
    
    curated_files = {
        "prices": out_dir / "prices_daily.parquet",
        "marketcap": out_dir / "marketcap_daily.parquet",
        "volume": out_dir / "volume_daily.parquet",
    }
    
    # Counts
    row_counts = {}
    symbol_counts = {}
    for dataset in ["prices", "marketcap", "volume"]:
        row_counts[f"{dataset}_rows"] = len(curated_panels[dataset])
        symbol_counts[f"{dataset}_symbols"] = len(curated_panels[dataset].columns)
    
    # Repair log stats
    if repair_log:
        repair_df = pd.DataFrame(repair_log)
        edits_by_rule = repair_df["rule"].value_counts().to_dict()
        total_edits = len(repair_df)
    else:
        edits_by_rule = {}
        total_edits = 0
    
    # Date ranges
    all_dates = set()
    for df in curated_panels.values():
        all_dates.update(df.index)
    if all_dates:
        date_range = {
            "start_date": str(min(all_dates)),
            "end_date": str(max(all_dates)),
        }
    else:
        date_range = {}
    
    metadata = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "script_name": "qc_curate.py",
        "git_commit_hash": get_git_commit_hash(repo_root),
        "config_hash": get_config_hash(config),
        "input_files": {},
        "output_files": {},
        "row_counts": row_counts,
        "symbol_counts": symbol_counts,
        "repair_stats": {
            "total_edits": total_edits,
            "edits_by_rule": edits_by_rule,
        },
        "date_range": date_range,
    }
    
    # Add config file info if provided
    if config_path:
        metadata["config_file"] = str(config_path.relative_to(repo_root)) if config_path.is_relative_to(repo_root) else str(config_path)
        metadata["config_file_hash"] = get_file_hash(config_path)
    
    # Add file hashes
    for name, path in raw_files.items():
        metadata["input_files"][name] = {
            "path": str(path.relative_to(repo_root)) if path.is_relative_to(repo_root) else str(path),
            "hash": get_file_hash(path),
        }
    
    for name, path in curated_files.items():
        metadata["output_files"][name] = {
            "path": str(path.relative_to(repo_root)) if path.is_relative_to(repo_root) else str(path),
            "hash": get_file_hash(path),
        }
    
    output_path = outputs_dir / "run_metadata_qc.json"
    with open(output_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved run metadata to {output_path}")


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_qc_pipeline(
    raw_dir: Path,
    out_dir: Path,
    outputs_dir: Path,
    config: Dict,
    repo_root: Path,
    config_path: Optional[Path] = None,
) -> None:
    """Run full QC pipeline."""
    print("=" * 60)
    print("QC Curation Pipeline")
    print("=" * 60)
    
    # Load raw panels
    raw_panels = load_raw_panels(raw_dir)
    
    # Initialize repair log
    repair_log = []
    
    # Process each dataset
    curated_panels = {}
    for dataset_name, raw_df in raw_panels.items():
        print(f"\nProcessing {dataset_name}...")
        
        # Apply sanity checks
        df = apply_sanity_checks(raw_df, dataset_name, repair_log, config)
        
        # Apply outlier flags
        df = apply_outlier_flags(df, dataset_name, repair_log, config)
        
        # Apply repairs (only if enabled - disabled by default to preserve NA values)
        # Gap filling is now handled in the backtest layer, not QC
        if config.get("allow_ffill", False):
            df = apply_repairs(df, dataset_name, repair_log, config)
        else:
            print(f"  Gap filling disabled (preserving NA values for backtest layer)")
        
        curated_panels[dataset_name] = df
    
    # Align all datasets to common index (full daily range)
    # This adds rows for missing calendar days but preserves NA values (no filling)
    print("\nAligning datasets to common daily index...")
    print("  Note: Missing calendar days will be added with NA values (no gap filling)")
    curated_panels = align_datasets(curated_panels)
    # Also align raw panels for fair comparison in report
    raw_panels_aligned = align_datasets(raw_panels)
    
    # Optional second-pass repair after alignment (for calendar-missing days)
    # Disabled by default - gap filling is now handled in backtest layer
    if config.get("allow_post_align_ffill", False) and config.get("allow_ffill", False):
        print("\nApplying post-alignment gap fill...")
        for dataset_name, df in curated_panels.items():
            curated_panels[dataset_name] = apply_repairs(df, dataset_name, repair_log, config)
    
    # Write outputs
    print("\nWriting outputs...")
    write_outputs(curated_panels, out_dir)
    write_repair_log(repair_log, outputs_dir)
    write_qc_report(raw_panels_aligned, curated_panels, repair_log, outputs_dir, config)
    write_run_metadata(
        raw_dir, out_dir, outputs_dir,
        raw_panels, curated_panels, repair_log, config, repo_root, config_path
    )
    
    print("\n" + "=" * 60)
    print("QC Pipeline Complete")
    print("=" * 60)
    print(f"Total repairs applied: {len(repair_log)}")


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QC and curate raw crypto data")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing raw parquet files",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/curated"),
        help="Directory for curated parquet output files",
    )
    parser.add_argument(
        "--outputs-dir",
        type=Path,
        default=Path("outputs"),
        help="Directory for QC report, repair log, and metadata",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to QC config YAML file (optional, overrides defaults)",
    )
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    
    # Resolve relative paths
    raw_dir = args.raw_dir if args.raw_dir.is_absolute() else repo_root / args.raw_dir
    out_dir = args.out_dir if args.out_dir.is_absolute() else repo_root / args.out_dir
    outputs_dir = args.outputs_dir if args.outputs_dir.is_absolute() else repo_root / args.outputs_dir
    
    # Load config (merge with defaults)
    config = QC_CONFIG.copy()
    config_path = None
    if args.config:
        config_path = args.config if args.config.is_absolute() else repo_root / args.config
        if config_path.exists():
            with open(config_path) as f:
                user_config = yaml.safe_load(f)
                if user_config:
                    config.update(user_config)
            print(f"Loaded QC config from {config_path}")
        else:
            print(f"[WARN] Config file not found: {config_path}, using defaults")
    
    run_qc_pipeline(raw_dir, out_dir, outputs_dir, config, repo_root, config_path)
