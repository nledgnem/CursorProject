#!/usr/bin/env python3
"""Run monitor evaluation: compute forward returns, bucket stats, edge stats, significance."""

import sys
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone, date
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import yaml
import hashlib

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.evaluation.forward_returns import compute_forward_returns, align_regime_and_returns
from src.evaluation.regime_eval import compute_bucket_stats, compute_edge_stats, block_bootstrap
from src.monitors.existing_monitor import ExistingMonitor
from src.utils.metadata import get_file_hash


def load_ls_returns(ls_returns_path: Path) -> pd.DataFrame:
    """Load LS returns from backtest results CSV."""
    df = pd.read_csv(ls_returns_path)
    
    # Ensure date column exists and is datetime
    if "date" not in df.columns:
        raise ValueError("LS returns CSV must have 'date' column")
    
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    
    # Find LS return column
    ls_col = None
    for col in ["r_ls_net", "r_ls", "ls_ret"]:
        if col in df.columns:
            ls_col = col
            break
    
    if ls_col is None:
        raise ValueError("No LS return column found (expected r_ls_net, r_ls, or ls_ret)")
    
    result = pd.DataFrame(index=df.index)
    result["ls_ret"] = df[ls_col]
    
    return result


def load_or_compute_regime(
    regime_path_or_compute: str,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """
    Load regime from CSV or compute using monitor.
    
    Args:
        regime_path_or_compute: Path to CSV or "compute"
        config: Config dict with monitor_name
        
    Returns:
        DataFrame with date index and regime_1_5 column
    """
    if regime_path_or_compute.lower() == "compute":
        # Compute using monitor
        monitor_name = config.get("monitor_name", "existing_regime_monitor")
        
        if monitor_name == "existing_regime_monitor":
            # Use existing monitor
            regime_csv_path = config.get("regime_csv_path")
            if regime_csv_path:
                regime_csv_path = Path(regime_csv_path)
            else:
                regime_csv_path = None  # Will use default
            
            monitor = ExistingMonitor(regime_csv_path=regime_csv_path)
            
            # Get date range from config (if available)
            date_range = config.get("date_range", {})
            start_date = date.fromisoformat(date_range["start"]) if date_range.get("start") else None
            end_date = date.fromisoformat(date_range["end"]) if date_range.get("end") else None
            
            regime_df = monitor.get_regime_series(start_date=start_date, end_date=end_date)
            return regime_df
        else:
            raise ValueError(f"Unknown monitor_name: {monitor_name}")
    else:
        # Load from CSV
        regime_path = Path(regime_path_or_compute)
        if not regime_path.exists():
            raise FileNotFoundError(f"Regime CSV not found: {regime_path}")
        
        df = pd.read_csv(regime_path)
        
        # Parse date column
        date_col = None
        for col in ["date", "date_iso"]:
            if col in df.columns:
                date_col = col
                break
        
        if date_col is None:
            raise ValueError("No date column found in regime CSV")
        
        # Convert to date index, handling various ISO8601 formats and timezones
        # Use errors='coerce' to handle inconsistent formats, then normalize to timezone-naive dates
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        # Remove timezone if present, then normalize to date-only
        if df[date_col].dt.tz is not None:
            df[date_col] = df[date_col].dt.tz_localize(None)
        df[date_col] = df[date_col].dt.normalize()
        df = df.set_index(date_col)
        df.index.name = "date"
        # Ensure index is timezone-naive DatetimeIndex
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        
        # Ensure regime_1_5 column exists
        if "regime_1_5" not in df.columns:
            # Try to convert from bucket or score
            from src.monitors.base import bucket_to_1_5, score_to_bucket_1_5
            if "bucket" in df.columns:
                df["regime_1_5"] = df["bucket"].apply(bucket_to_1_5)
            elif "regime_score" in df.columns:
                df["regime_1_5"] = df["regime_score"].apply(score_to_bucket_1_5)
            else:
                raise ValueError("No regime_1_5, bucket, or regime_score column found")
        
        result = pd.DataFrame(index=df.index)
        result["regime_1_5"] = df["regime_1_5"]
        if "score_raw" in df.columns:
            result["score_raw"] = df["score_raw"]
        if "monitor_name" in df.columns:
            result["monitor_name"] = df["monitor_name"]
        else:
            result["monitor_name"] = config.get("monitor_name", "unknown")
        
        return result


def print_summary(
    bucket_stats_per_horizon: Dict[int, pd.DataFrame],
    edge_stats_per_horizon: Dict[int, Dict[str, Any]],
    bootstrap_per_horizon: Dict[int, Dict[str, Any]],
    horizons: list,
):
    """Print one-page console summary."""
    print("\n" + "=" * 80)
    print("MONITOR EVALUATION SUMMARY")
    print("=" * 80)
    
    for horizon in horizons:
        print(f"\n--- Horizon H={horizon} days ---")
        
        # Bucket stats
        if horizon in bucket_stats_per_horizon:
            bucket_stats = bucket_stats_per_horizon[horizon]
            print("\nBucket Statistics:")
            print(f"  {'Regime':<8} {'n':<6} {'Mean':<10} {'Median':<10} {'Std':<10} {'Sharpe':<10}")
            for _, row in bucket_stats.iterrows():
                print(
                    f"  {row['regime']:<8} {int(row['n']):<6} "
                    f"{row['mean']:>9.4f} {row['median']:>9.4f} "
                    f"{row['std']:>9.4f} {row['sharpe_like']:>9.4f}"
                )
        
        # Edge stats
        if horizon in edge_stats_per_horizon:
            edge = edge_stats_per_horizon[horizon]
            print("\nEdge Statistics:")
            print(f"  Edge Best (regime 5=BEST vs all):  {edge['edge_best']:>8.4f}  (n5={edge['n5']}, n_all={edge['n_all']})")
            print(f"  Edge Worst (regime 1=WORST vs all): {edge['edge_worst']:>8.4f}  (n1={edge['n1']})")
            print(f"  Spread (regime 5=BEST - regime 1=WORST): {edge['spread_1_5']:>8.4f}")
        
        # Bootstrap significance
        if horizon in bootstrap_per_horizon:
            boot = bootstrap_per_horizon[horizon]
            print("\nSignificance (Block Bootstrap):")
            
            for stat_name, (p_value, (ci_lower, ci_upper)) in boot.items():
                stat_display = {
                    "edge_best": "Edge Best (regime 5=BEST)",
                    "edge_worst": "Edge Worst (regime 1=WORST)",
                    "spread_1_5": "Spread (5=BEST - 1=WORST)",
                }.get(stat_name, stat_name)
                
                ci_str = f"[{ci_lower:.4f}, {ci_upper:.4f}]" if not (np.isnan(ci_lower) or np.isnan(ci_upper)) else "[N/A]"
                p_str = f"{p_value:.4f}" if not np.isnan(p_value) else "N/A"
                print(f"  {stat_display}: p={p_str:>8}  CI95%={ci_str}")
    
    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate regime monitor using forward returns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to monitor evaluation config YAML",
    )
    parser.add_argument(
        "--ls-returns",
        type=Path,
        required=True,
        help="Path to backtest_results.csv with LS returns",
    )
    parser.add_argument(
        "--regime",
        type=str,
        required=True,
        help='Path to regime CSV or "compute" to compute using monitor',
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    
    # Load config
    with open(args.config) as f:
        config = yaml.safe_load(f)
    
    # Resolve paths
    ls_returns_path = args.ls_returns if args.ls_returns.is_absolute() else repo_root / args.ls_returns
    
    print("=" * 80)
    print("MONITOR EVALUATION")
    print("=" * 80)
    print(f"Config: {args.config}")
    print(f"LS Returns: {ls_returns_path}")
    print(f"Regime: {args.regime}")
    
    # Load data
    print("\n[Step 1] Loading data...")
    ls_returns_df = load_ls_returns(ls_returns_path)
    print(f"  Loaded {len(ls_returns_df)} LS return days")
    
    regime_df = load_or_compute_regime(args.regime, config)
    print(f"  Loaded/computed {len(regime_df)} regime days")
    
    # Compute forward returns
    print("\n[Step 2] Computing forward returns...")
    horizons = config.get("horizons", [5, 10, 20])
    fwd_returns_df = compute_forward_returns(ls_returns_df["ls_ret"], horizons)
    print(f"  Computed forward returns for horizons: {horizons}")
    
    # Combine LS returns and forward returns
    ls_with_fwd = ls_returns_df.join(fwd_returns_df, how="inner")
    
    # Align regime and returns
    print("\n[Step 3] Aligning regime and returns...")
    calendar_config = config.get("calendar", {})
    drop_missing = calendar_config.get("drop_missing", True)
    aligned_df = align_regime_and_returns(regime_df, ls_with_fwd, drop_missing=drop_missing)
    print(f"  Aligned {len(aligned_df)} days (common dates only)")
    
    # Check sample sizes
    n1 = len(aligned_df[aligned_df["regime_1_5"] == 1])
    n5 = len(aligned_df[aligned_df["regime_1_5"] == 5])
    if n1 < 30 or n5 < 30:
        print(f"\n[WARN] Small sample sizes: n1={n1}, n5={n5} (< 30)")
    
    # Create output directory
    output_dir_base = Path(config.get("output_dir_base", "outputs/monitor_eval"))
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = output_dir_base / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[Step 4] Output directory: {output_dir}")
    
    # Evaluate for each horizon
    bucket_stats_per_horizon = {}
    edge_stats_per_horizon = {}
    bootstrap_per_horizon = {}
    
    bootstrap_config = config.get("block_bootstrap", {})
    bootstrap_enabled = bootstrap_config.get("enabled", True)
    block_size = bootstrap_config.get("block_size", 10)
    n_boot = bootstrap_config.get("n_boot", 300)
    seed = config.get("seed", 42)
    
    print("\n[Step 5] Computing statistics...")
    for horizon in horizons:
        print(f"  Horizon H={horizon}...")
        
        # Bucket stats
        bucket_stats = compute_bucket_stats(aligned_df, horizon)
        bucket_stats_per_horizon[horizon] = bucket_stats
        
        # Edge stats
        edge_stats = compute_edge_stats(aligned_df, horizon)
        edge_stats_per_horizon[horizon] = edge_stats
        
        # Bootstrap
        if bootstrap_enabled:
            bootstrap_results = block_bootstrap(
                aligned_df, horizon,
                block_size=block_size,
                n_boot=n_boot,
                seed=seed,
            )
            bootstrap_per_horizon[horizon] = bootstrap_results
    
    # Save outputs
    print("\n[Step 6] Saving outputs...")
    
    # Bucket stats (combined across horizons)
    all_bucket_stats = []
    for horizon in horizons:
        if horizon in bucket_stats_per_horizon:
            all_bucket_stats.append(bucket_stats_per_horizon[horizon])
    if all_bucket_stats:
        bucket_stats_combined = pd.concat(all_bucket_stats, ignore_index=True)
        bucket_stats_path = output_dir / "regime_bucket_stats.csv"
        bucket_stats_combined.to_csv(bucket_stats_path, index=False)
        print(f"  Saved: {bucket_stats_path}")
    
    # Edge stats (combined)
    all_edge_stats = []
    for horizon in horizons:
        if horizon in edge_stats_per_horizon:
            all_edge_stats.append(edge_stats_per_horizon[horizon])
    if all_edge_stats:
        edge_stats_df = pd.DataFrame(all_edge_stats)
        
        # Add bootstrap results as columns
        if bootstrap_enabled:
            for horizon in horizons:
                if horizon in bootstrap_per_horizon:
                    boot = bootstrap_per_horizon[horizon]
                    for stat_name, (p_value, (ci_lower, ci_upper)) in boot.items():
                        row_idx = edge_stats_df[edge_stats_df["horizon"] == horizon].index
                        if len(row_idx) > 0:
                            edge_stats_df.loc[row_idx[0], f"{stat_name}_pvalue"] = p_value
                            edge_stats_df.loc[row_idx[0], f"{stat_name}_ci_lower"] = ci_lower
                            edge_stats_df.loc[row_idx[0], f"{stat_name}_ci_upper"] = ci_upper
        
        edge_stats_path = output_dir / "regime_edges.csv"
        edge_stats_df.to_csv(edge_stats_path, index=False)
        print(f"  Saved: {edge_stats_path}")
    
    # Print summary
    print_summary(bucket_stats_per_horizon, edge_stats_per_horizon, bootstrap_per_horizon, horizons)
    
    # Create run_receipt
    run_receipt = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "inputs": {
            "ls_returns_path": str(ls_returns_path),
            "ls_returns_hash": get_file_hash(ls_returns_path),
            "ls_returns_rows": len(ls_returns_df),
            "regime_source": args.regime,
            "regime_rows": len(regime_df),
        },
        "outputs": {
            "aligned_rows": len(aligned_df),
            "date_range": {
                "start": str(aligned_df.index.min().date()) if len(aligned_df) > 0 else None,
                "end": str(aligned_df.index.max().date()) if len(aligned_df) > 0 else None,
            },
            "sample_sizes": {
                "n1": n1,
                "n5": n5,
                "n_all": len(aligned_df),
            },
            "horizons": horizons,
        },
        "warnings": [],
    }
    
    if n1 < 30 or n5 < 30:
        run_receipt["warnings"].append(f"Small sample sizes: n1={n1}, n5={n5}")
    
    receipt_path = output_dir / "run_receipt.json"
    with open(receipt_path, "w") as f:
        json.dump(run_receipt, f, indent=2, default=str)
    print(f"\n  Saved: {receipt_path}")
    
    print("\n[SUCCESS] Evaluation complete!")
    print(f"  Results: {output_dir}")


if __name__ == "__main__":
    main()


