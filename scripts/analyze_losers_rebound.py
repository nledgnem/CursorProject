#!/usr/bin/env python3
"""
Analyze if 2024 "losers" (by return) have higher absolute returns in Jan1–Feb15 2025.

This script implements the full analysis spec:
- Universe: Top 150 by market cap at 2024-12-31, excluding ineligible tokens
- Formation window: 2024-01-01 to 2024-12-20
- Forward test window: 2025-01-01 to 2025-02-15
- Robustness: Forward-fill missing prices, drop coins with >5% missing days
- Output: Bucket table, plot, and diagnostics
"""

import sys
from pathlib import Path
from datetime import date, datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import spearmanr
from typing import Set, List, Tuple

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Parameters (hardcoded as per spec)
FORMATION_START = date(2024, 1, 1)
FORMATION_END = date(2024, 12, 20)
FORWARD_START = date(2025, 1, 1)
FORWARD_END = date(2025, 2, 15)
RANKING_DATE = date(2024, 12, 31)
TOP_N = 150
MISSING_THRESHOLD = 0.05  # 5%


def load_data_lake(data_lake_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load fact tables from data lake."""
    fact_price_path = data_lake_dir / "fact_price.parquet"
    fact_mcap_path = data_lake_dir / "fact_marketcap.parquet"
    dim_asset_path = data_lake_dir / "dim_asset.parquet"
    
    print(f"Loading fact_price from {fact_price_path}")
    fact_price = pd.read_parquet(fact_price_path)
    fact_price["date"] = pd.to_datetime(fact_price["date"]).dt.date
    
    print(f"Loading fact_marketcap from {fact_mcap_path}")
    fact_mcap = pd.read_parquet(fact_mcap_path)
    fact_mcap["date"] = pd.to_datetime(fact_mcap["date"]).dt.date
    
    print(f"Loading dim_asset from {dim_asset_path}")
    dim_asset = pd.read_parquet(dim_asset_path)
    
    return fact_price, fact_mcap, dim_asset


def build_blacklist(
    dim_asset: pd.DataFrame,
    stablecoins_path: Path,
    blacklist_path: Path,
) -> Set[str]:
    """
    Build blacklist of ineligible tokens.
    
    Includes:
    - Stablecoins (from stablecoins.csv and dim_asset.is_stable)
    - Wrapped tokens (WBTC, WETH, renBTC, etc.)
    - LST/receipt tokens (stETH, wstETH, cbETH, rETH, frxETH, sfrxETH)
    - Tokens from blacklist.csv
    - Exchange tokens (if identifiable)
    """
    blacklist = set()
    
    # Load stablecoins from CSV
    if stablecoins_path.exists():
        stablecoins_df = pd.read_csv(stablecoins_path)
        if "symbol" in stablecoins_df.columns:
            blacklist.update(stablecoins_df["symbol"].str.upper().str.strip())
    
    # Add stablecoins from dim_asset
    if "is_stable" in dim_asset.columns:
        stable_assets = dim_asset[dim_asset["is_stable"] == True]
        blacklist.update(stable_assets["asset_id"].str.upper())
        blacklist.update(stable_assets["symbol"].str.upper())
    
    # Load blacklist.csv
    if blacklist_path.exists():
        blacklist_df = pd.read_csv(blacklist_path)
        if "symbol" in blacklist_df.columns:
            blacklist.update(blacklist_df["symbol"].str.upper().str.strip())
    
    # Default wrapped/LST/receipt tokens (as per spec)
    default_excluded = {
        # Wrapped tokens
        "WBTC", "WETH", "RENBTC", "HBTC", "PBTC",
        # LST/receipt tokens
        "STETH", "WSTETH", "CBETH", "RETH", "FRXETH", "SFRXETH",
        "STSOL", "MSOL", "JITOSOL", "BSOL",
        # LP tokens (common patterns)
        "UNI-V2", "UNI-V3", "SLP", "CAKE-LP",
        # Vault/share tokens
        "YV", "CVX", "CRV", "BAL", "SUSHI",
    }
    blacklist.update(default_excluded)
    
    # Add any tokens with "wrapped" or "w" prefix in symbol (heuristic)
    if "symbol" in dim_asset.columns:
        for symbol in dim_asset["symbol"].str.upper():
            if pd.notna(symbol):
                if symbol.startswith("W") and len(symbol) > 1:
                    # Check if it's a wrapped version (e.g., WBTC, WETH)
                    base = symbol[1:]
                    if base in ["BTC", "ETH", "SOL", "AVAX", "MATIC", "BNB"]:
                        blacklist.add(symbol)
    
    return blacklist


def get_top_n_universe(
    fact_mcap: pd.DataFrame,
    ranking_date: date,
    top_n: int,
    blacklist: Set[str],
    dim_asset: pd.DataFrame,
) -> List[str]:
    """
    Get top N coin_ids by market cap at ranking_date, excluding blacklisted tokens.
    
    Returns:
        List of asset_ids (coin_ids)
    """
    # Get market cap snapshot at ranking_date
    mcap_snapshot = fact_mcap[fact_mcap["date"] == ranking_date].copy()
    
    if len(mcap_snapshot) == 0:
        raise ValueError(f"No market cap data found for {ranking_date}")
    
    # Merge with dim_asset to get symbol for blacklist matching
    mcap_snapshot = mcap_snapshot.merge(
        dim_asset[["asset_id", "symbol"]],
        on="asset_id",
        how="left"
    )
    
    # Apply blacklist (check both asset_id and symbol)
    mcap_snapshot = mcap_snapshot[
        ~mcap_snapshot["asset_id"].str.upper().isin(blacklist) &
        ~mcap_snapshot["symbol"].str.upper().isin(blacklist)
    ]
    
    # Rank by market cap descending
    mcap_snapshot = mcap_snapshot.sort_values("marketcap", ascending=False)
    
    # Select top N
    top_n_assets = mcap_snapshot.head(top_n)["asset_id"].tolist()
    
    print(f"Selected {len(top_n_assets)} assets from top {top_n} (after exclusions)")
    
    return top_n_assets


def get_price_series(
    fact_price: pd.DataFrame,
    asset_id: str,
    start_date: date,
    end_date: date,
) -> Tuple[pd.Series, float]:
    """
    Get price series for an asset in a date range.
    
    Returns:
        Tuple of (price_series with forward-filled values, missing_ratio)
    """
    # Filter for this asset and date range
    asset_prices = fact_price[
        (fact_price["asset_id"] == asset_id) &
        (fact_price["date"] >= start_date) &
        (fact_price["date"] <= end_date)
    ].copy()
    
    # Create full date range
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    date_range_dates = [d.date() for d in date_range]
    
    # Create series indexed by date
    price_series = pd.Series(index=date_range_dates, dtype=float)
    
    # Fill with actual prices
    for _, row in asset_prices.iterrows():
        price_series[row["date"]] = row["close"]
    
    # Count missing before forward-fill
    total_days = len(date_range_dates)
    missing_days = price_series.isna().sum()
    missing_ratio = missing_days / total_days if total_days > 0 else 1.0
    
    # Forward-fill
    price_series = price_series.ffill()
    
    return price_series, missing_ratio


def compute_returns(
    fact_price: pd.DataFrame,
    asset_ids: List[str],
    formation_start: date,
    formation_end: date,
    forward_start: date,
    forward_end: date,
    missing_threshold: float,
) -> pd.DataFrame:
    """
    Compute returns for all assets, filtering out those with too many missing days.
    
    Returns:
        DataFrame with columns: asset_id, return_2024, return_fwd
    """
    results = []
    excluded = []
    
    for asset_id in asset_ids:
        # Get formation window prices
        formation_prices, formation_missing = get_price_series(
            fact_price, asset_id, formation_start, formation_end
        )
        
        # Get forward window prices
        forward_prices, forward_missing = get_price_series(
            fact_price, asset_id, forward_start, forward_end
        )
        
        # Check missing threshold
        if formation_missing > missing_threshold or forward_missing > missing_threshold:
            excluded.append({
                "asset_id": asset_id,
                "reason": "missing_data",
                "formation_missing_ratio": formation_missing,
                "forward_missing_ratio": forward_missing,
            })
            continue
        
        # Check if we have prices at start and end of each window
        if pd.isna(formation_prices.iloc[0]) or pd.isna(formation_prices.iloc[-1]):
            excluded.append({
                "asset_id": asset_id,
                "reason": "missing_start_or_end",
                "formation_missing_ratio": formation_missing,
                "forward_missing_ratio": forward_missing,
            })
            continue
        
        if pd.isna(forward_prices.iloc[0]) or pd.isna(forward_prices.iloc[-1]):
            excluded.append({
                "asset_id": asset_id,
                "reason": "missing_start_or_end",
                "formation_missing_ratio": formation_missing,
                "forward_missing_ratio": forward_missing,
            })
            continue
        
        # Compute returns
        return_2024 = (formation_prices.iloc[-1] / formation_prices.iloc[0]) - 1.0
        return_fwd = (forward_prices.iloc[-1] / forward_prices.iloc[0]) - 1.0
        
        results.append({
            "asset_id": asset_id,
            "return_2024": return_2024,
            "return_fwd": return_fwd,
        })
    
    print(f"Computed returns for {len(results)} assets")
    print(f"Excluded {len(excluded)} assets due to missing data")
    
    return pd.DataFrame(results), pd.DataFrame(excluded)


def create_buckets(returns_df: pd.DataFrame) -> Tuple[int, pd.DataFrame]:
    """
    Create buckets from returns, ranked by 2024 return (worst to best).
    
    Returns:
        Tuple of (num_buckets, bucket_assignments_df)
    """
    N = len(returns_df)
    
    # Number of buckets: B = min(10, floor(N / 9))
    B = min(10, N // 9)
    
    if B < 3:
        raise ValueError(f"Sample too small after exclusions: N={N}, B={B}")
    
    # Rank by 2024 return ascending (worst to best)
    returns_df = returns_df.sort_values("return_2024", ascending=True).reset_index(drop=True)
    
    # Assign buckets with sizes ±1
    bucket_sizes = [N // B] * B
    remainder = N % B
    for i in range(remainder):
        bucket_sizes[i] += 1
    
    # Assign bucket numbers
    bucket_assignments = []
    idx = 0
    for bucket_num in range(1, B + 1):
        bucket_size = bucket_sizes[bucket_num - 1]
        for _ in range(bucket_size):
            bucket_assignments.append(bucket_num)
            idx += 1
    
    returns_df["bucket"] = bucket_assignments
    
    return B, returns_df


def trimmed_mean(values: List[float], m: int) -> float:
    """
    Compute trimmed mean based on bucket size.
    
    Rules:
    - If m >= 15: drop 2 lowest + 2 highest
    - If 9 <= m <= 14: drop 1 lowest + 1 highest
    - Otherwise: no trimming (shouldn't happen per spec)
    """
    if m < 9:
        # Shouldn't happen, but handle gracefully
        return np.mean(values)
    
    sorted_vals = sorted(values)
    
    if m >= 15:
        # Drop 2 lowest + 2 highest
        trimmed = sorted_vals[2:-2]
    else:  # 9 <= m <= 14
        # Drop 1 lowest + 1 highest
        trimmed = sorted_vals[1:-1]
    
    return np.mean(trimmed)


def compute_bucket_stats(bucketed_returns: pd.DataFrame, num_buckets: int) -> pd.DataFrame:
    """
    Compute trimmed mean forward return for each bucket.
    
    Returns:
        DataFrame with columns: bucket, count, trimmed_mean_fwd_return
    """
    bucket_stats = []
    
    for bucket_num in range(1, num_buckets + 1):
        bucket_data = bucketed_returns[bucketed_returns["bucket"] == bucket_num]
        m = len(bucket_data)
        
        if m == 0:
            continue
        
        forward_returns = bucket_data["return_fwd"].tolist()
        trimmed_mean_fwd = trimmed_mean(forward_returns, m)
        
        # Also compute untrimmed mean and median for sanity check
        untrimmed_mean = np.mean(forward_returns)
        median = np.median(forward_returns)
        
        bucket_stats.append({
            "bucket": bucket_num,
            "count": m,
            "trimmed_mean_fwd_return": trimmed_mean_fwd,
            "untrimmed_mean": untrimmed_mean,  # For sanity check
            "median": median,  # For sanity check
        })
    
    return pd.DataFrame(bucket_stats)


def compute_bucket_daily_returns(
    fact_price: pd.DataFrame,
    bucketed_returns: pd.DataFrame,
    forward_start: date,
    forward_end: date,
    num_buckets: int,
) -> pd.DataFrame:
    """
    Compute daily cumulative returns for each bucket over the forward test period.
    
    Returns:
        DataFrame with columns: date, bucket_1, bucket_2, ..., bucket_10 (cumulative returns)
    """
    # Create date range for forward window
    date_range = pd.date_range(start=forward_start, end=forward_end, freq="D")
    date_range_dates = [d.date() for d in date_range]
    
    # Initialize result DataFrame
    result_df = pd.DataFrame({"date": date_range_dates})
    
    # For each bucket, compute average cumulative return
    for bucket_num in range(1, num_buckets + 1):
        bucket_assets = bucketed_returns[bucketed_returns["bucket"] == bucket_num]["asset_id"].tolist()
        
        if len(bucket_assets) == 0:
            result_df[f"bucket_{bucket_num}"] = np.nan
            continue
        
        # Get daily prices for all assets in this bucket
        bucket_daily_returns = []
        
        for asset_id in bucket_assets:
            # Get forward window prices
            forward_prices, _ = get_price_series(
                fact_price, asset_id, forward_start, forward_end
            )
            
            # Compute daily returns (skip if missing first price)
            if pd.isna(forward_prices.iloc[0]):
                continue
            
            # Compute cumulative returns from start
            initial_price = forward_prices.iloc[0]
            cumulative_returns = (forward_prices / initial_price) - 1.0
            bucket_daily_returns.append(cumulative_returns)
        
        if len(bucket_daily_returns) == 0:
            result_df[f"bucket_{bucket_num}"] = np.nan
            continue
        
        # Average across assets in bucket (trimmed mean approach for each day)
        bucket_avg_returns = []
        for day_idx in range(len(date_range_dates)):
            day_returns = []
            for ret in bucket_daily_returns:
                if day_idx < len(ret) and pd.notna(ret.iloc[day_idx]):
                    day_returns.append(ret.iloc[day_idx])
            
            if len(day_returns) == 0:
                bucket_avg_returns.append(np.nan)
            else:
                # Use trimmed mean if we have enough observations
                m = len(day_returns)
                if m >= 15:
                    sorted_returns = sorted(day_returns)
                    trimmed = sorted_returns[2:-2]
                    bucket_avg_returns.append(np.mean(trimmed))
                elif m >= 9:
                    sorted_returns = sorted(day_returns)
                    trimmed = sorted_returns[1:-1]
                    bucket_avg_returns.append(np.mean(trimmed))
                else:
                    bucket_avg_returns.append(np.mean(day_returns))
        
        result_df[f"bucket_{bucket_num}"] = bucket_avg_returns
    
    return result_df


def compute_diagnostics(bucket_stats: pd.DataFrame) -> Tuple[float, int]:
    """
    Compute Spearman correlation and inversion count.
    
    Returns:
        Tuple of (spearman_correlation, inversion_count)
    """
    # Spearman correlation between bucket index and trimmed mean forward return
    spearman_corr, spearman_p = spearmanr(
        bucket_stats["bucket"],
        bucket_stats["trimmed_mean_fwd_return"]
    )
    
    # Inversion count: number of times bucket_k_return < bucket_(k+1)_return
    inversions = 0
    for i in range(len(bucket_stats) - 1):
        if bucket_stats.iloc[i]["trimmed_mean_fwd_return"] < bucket_stats.iloc[i + 1]["trimmed_mean_fwd_return"]:
            inversions += 1
    
    return spearman_corr, inversions


def main():
    """Main analysis function."""
    repo_root = Path(__file__).parent.parent
    data_lake_dir = repo_root / "data" / "curated" / "data_lake"
    stablecoins_path = repo_root / "data" / "stablecoins.csv"
    blacklist_path = repo_root / "data" / "blacklist.csv"
    outputs_dir = repo_root / "outputs"
    outputs_dir.mkdir(exist_ok=True)
    
    print("=" * 80)
    print("Losers Rebound Analysis")
    print("=" * 80)
    print(f"Formation window: {FORMATION_START} to {FORMATION_END}")
    print(f"Forward test window: {FORWARD_START} to {FORWARD_END}")
    print(f"Ranking date: {RANKING_DATE}")
    print(f"Top N: {TOP_N}")
    print(f"Missing threshold: {MISSING_THRESHOLD * 100}%")
    print()
    
    # Step 1: Load data
    print("Step 1: Loading data lake tables...")
    fact_price, fact_mcap, dim_asset = load_data_lake(data_lake_dir)
    print(f"  fact_price: {len(fact_price):,} rows, {fact_price['asset_id'].nunique()} assets")
    print(f"  fact_mcap: {len(fact_mcap):,} rows, {fact_mcap['asset_id'].nunique()} assets")
    print(f"  dim_asset: {len(dim_asset):,} assets")
    print()
    
    # Step 2: Build blacklist
    print("Step 2: Building blacklist...")
    blacklist = build_blacklist(dim_asset, stablecoins_path, blacklist_path)
    print(f"  Blacklist size: {len(blacklist)} tokens")
    print()
    
    # Step 3: Get top N universe
    print("Step 3: Selecting top N universe...")
    top_n_assets = get_top_n_universe(
        fact_mcap, RANKING_DATE, TOP_N, blacklist, dim_asset
    )
    print(f"  Selected {len(top_n_assets)} assets")
    print()
    
    # Step 4: Compute returns
    print("Step 4: Computing returns...")
    returns_df, excluded_df = compute_returns(
        fact_price,
        top_n_assets,
        FORMATION_START,
        FORMATION_END,
        FORWARD_START,
        FORWARD_END,
        MISSING_THRESHOLD,
    )
    print(f"  Final sample size: {len(returns_df)} assets")
    print()
    
    # Step 5: Create buckets
    print("Step 5: Creating buckets...")
    num_buckets, bucketed_returns = create_buckets(returns_df)
    print(f"  Created {num_buckets} buckets")
    print()
    
    # Step 6: Compute bucket stats
    print("Step 6: Computing bucket statistics...")
    bucket_stats = compute_bucket_stats(bucketed_returns, num_buckets)
    print()
    
    # Step 7: Compute diagnostics
    print("Step 7: Computing diagnostics...")
    spearman_corr, inversions = compute_diagnostics(bucket_stats)
    print()
    
    # Step 8: Output results
    print("Step 8: Outputting results...")
    
    # Save bucket table
    output_table = bucket_stats[["bucket", "count", "trimmed_mean_fwd_return"]].copy()
    output_table_path = outputs_dir / "losers_rebound_buckets.csv"
    output_table.to_csv(output_table_path, index=False)
    print(f"  Saved bucket table to: {output_table_path}")
    
    # Print bucket table
    print("\nBucket Table:")
    print("-" * 80)
    print(output_table.to_string(index=False))
    print()
    
    # Print diagnostics
    print("Diagnostics:")
    print("-" * 80)
    print(f"Spearman correlation (bucket vs trimmed_mean_fwd_return): {spearman_corr:.4f}")
    print(f"  (Negative = losers rebound more)")
    print(f"Inversion count: {inversions} / {num_buckets - 1}")
    print(f"  (Fewer inversions = more monotonic)")
    print()
    
    # Compute daily cumulative returns for each bucket
    print("Computing daily returns for each bucket...")
    daily_returns_df = compute_bucket_daily_returns(
        fact_price,
        bucketed_returns,
        FORWARD_START,
        FORWARD_END,
        num_buckets,
    )
    
    # Create line chart with 10 lines (one per bucket)
    print("Creating plot...")
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Convert dates to datetime for matplotlib
    dates = pd.to_datetime(daily_returns_df["date"])
    
    # Color palette for 10 buckets (using a distinct color scheme)
    colors = plt.cm.tab10(np.linspace(0, 1, num_buckets))
    
    # Plot each bucket as a line
    for bucket_num in range(1, num_buckets + 1):
        col_name = f"bucket_{bucket_num}"
        if col_name not in daily_returns_df.columns:
            continue
        
        # Get bucket info for labeling
        bucket_info = bucket_stats[bucket_stats["bucket"] == bucket_num].iloc[0]
        count = bucket_info["count"]
        final_return = bucket_info["trimmed_mean_fwd_return"]
        
        # Label: Bucket X (worst/best) with final return
        if bucket_num == 1:
            label = f"Bucket {bucket_num} (Worst 2024, n={count}, Final: {final_return:.1%})"
        elif bucket_num == num_buckets:
            label = f"Bucket {bucket_num} (Best 2024, n={count}, Final: {final_return:.1%})"
        else:
            label = f"Bucket {bucket_num} (n={count}, Final: {final_return:.1%})"
        
        # Plot line
        ax.plot(
            dates,
            daily_returns_df[col_name] * 100,  # Convert to percentage
            label=label,
            linewidth=2.5,
            alpha=0.8,
            color=colors[bucket_num - 1],
        )
    
    # Formatting
    ax.set_xlabel("Date", fontsize=13, fontweight='bold')
    ax.set_ylabel("Cumulative Return (%)", fontsize=13, fontweight='bold')
    ax.set_title(
        f"Daily Cumulative Returns by Bucket\n"
        f"Formation Period: {FORMATION_START} to {FORMATION_END} | "
        f"Forward Test: {FORWARD_START} to {FORWARD_END}",
        fontsize=14,
        fontweight='bold',
        pad=20
    )
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.axhline(y=0, color="k", linestyle="-", alpha=0.5, linewidth=1)
    
    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Legend - place outside plot area
    ax.legend(
        loc='center left',
        bbox_to_anchor=(1, 0.5),
        fontsize=9,
        framealpha=0.9,
        fancybox=True,
        shadow=True
    )
    
    # Add statistics text box
    stats_text = f"Spearman ρ: {spearman_corr:.3f}\nInversions: {inversions}/{num_buckets-1}"
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    plot_path = outputs_dir / "losers_rebound_plot.png"
    plt.savefig(plot_path, dpi=150, bbox_inches="tight")
    print(f"  Saved plot to: {plot_path}")
    plt.close()
    
    # Save intermediate lists
    eligible_path = outputs_dir / "losers_rebound_eligible_assets.csv"
    pd.DataFrame({"asset_id": top_n_assets}).to_csv(eligible_path, index=False)
    print(f"  Saved eligible assets to: {eligible_path}")
    
    if len(excluded_df) > 0:
        excluded_path = outputs_dir / "losers_rebound_excluded_assets.csv"
        excluded_df.to_csv(excluded_path, index=False)
        print(f"  Saved excluded assets to: {excluded_path}")
    
    print()
    print("=" * 80)
    print("Analysis complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()

