"""Regime evaluation: bucket stats, edge stats, and significance testing."""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from pathlib import Path


def compute_bucket_stats(
    aligned_df: pd.DataFrame,
    horizon: int,
    regime_col: str = "regime_1_5",
) -> pd.DataFrame:
    """
    Compute statistics for each regime bucket.
    
    Args:
        aligned_df: DataFrame with regime_1_5 and fwd_ret_H columns
        horizon: Horizon H (e.g., 5)
        regime_col: Name of regime column (default: regime_1_5)
        
    Returns:
        DataFrame with columns:
        - horizon, regime, n, mean, median, std, sharpe_like, min, max
    """
    fwd_col = f"fwd_ret_{horizon}"
    
    if fwd_col not in aligned_df.columns:
        raise ValueError(f"Forward return column {fwd_col} not found")
    
    stats = []
    
    for regime in sorted(aligned_df[regime_col].dropna().unique()):
        mask = aligned_df[regime_col] == regime
        returns = aligned_df.loc[mask, fwd_col].dropna()
        
        if len(returns) == 0:
            continue
        
        mean_ret = returns.mean()
        std_ret = returns.std()
        sharpe_like = mean_ret / std_ret if std_ret > 0 else np.nan
        
        stats.append({
            "horizon": horizon,
            "regime": regime,
            "n": len(returns),
            "mean": mean_ret,
            "median": returns.median(),
            "std": std_ret,
            "sharpe_like": sharpe_like,
            "min": returns.min(),
            "max": returns.max(),
        })
    
    return pd.DataFrame(stats)


def compute_edge_stats(
    aligned_df: pd.DataFrame,
    horizon: int,
    regime_col: str = "regime_1_5",
) -> Dict[str, float]:
    """
    Compute edge statistics comparing regime 1 and 5 to all.
    
    Note: Regime 5 = BEST (GREEN), Regime 1 = WORST (RED)
    
    Args:
        aligned_df: DataFrame with regime_1_5 and fwd_ret_H columns
        horizon: Horizon H (e.g., 5)
        regime_col: Name of regime column (default: regime_1_5)
        
    Returns:
        Dict with:
        - edge_best: mean(fwd_ret | regime=5) - mean(fwd_ret | ALL)  [regime 5 = best]
        - edge_worst: mean(fwd_ret | regime=1) - mean(fwd_ret | ALL)  [regime 1 = worst]
        - spread_1_5: mean(fwd_ret | regime=5) - mean(fwd_ret | regime=1)  [best - worst]
        - n1, n5, n_all: Sample sizes
    """
    fwd_col = f"fwd_ret_{horizon}"
    
    if fwd_col not in aligned_df.columns:
        raise ValueError(f"Forward return column {fwd_col} not found")
    
    # All returns
    all_returns = aligned_df[fwd_col].dropna()
    mean_all = all_returns.mean()
    n_all = len(all_returns)
    
    # Regime 1 = WORST (RED)
    regime_1_returns = aligned_df[aligned_df[regime_col] == 1][fwd_col].dropna()
    mean_1 = regime_1_returns.mean() if len(regime_1_returns) > 0 else np.nan
    n1 = len(regime_1_returns)
    
    # Regime 5 = BEST (GREEN)
    regime_5_returns = aligned_df[aligned_df[regime_col] == 5][fwd_col].dropna()
    mean_5 = regime_5_returns.mean() if len(regime_5_returns) > 0 else np.nan
    n5 = len(regime_5_returns)
    
    edge_best = mean_5 - mean_all if not np.isnan(mean_5) else np.nan
    edge_worst = mean_1 - mean_all if not np.isnan(mean_1) else np.nan
    spread_1_5 = mean_5 - mean_1 if not (np.isnan(mean_5) or np.isnan(mean_1)) else np.nan
    
    return {
        "horizon": horizon,
        "edge_best": edge_best,  # regime 5 (best) vs all
        "edge_worst": edge_worst,  # regime 1 (worst) vs all
        "spread_1_5": spread_1_5,  # regime 5 - regime 1
        "n1": n1,
        "n5": n5,
        "n_all": n_all,
        "mean_all": mean_all,
        "mean_1": mean_1,
        "mean_5": mean_5,
    }


def block_bootstrap(
    aligned_df: pd.DataFrame,
    horizon: int,
    regime_col: str = "regime_1_5",
    block_size: int = 10,
    n_boot: int = 300,
    seed: Optional[int] = None,
) -> Dict[str, Tuple[float, float]]:
    """
    Block bootstrap to compute confidence intervals for edge stats.
    
    Accounts for autocorrelation and overlapping windows by resampling blocks.
    
    Args:
        aligned_df: DataFrame with regime_1_5 and fwd_ret_H columns
        horizon: Horizon H
        regime_col: Name of regime column
        block_size: Size of blocks to resample (default: 10 trading days)
        n_boot: Number of bootstrap samples (default: 300 for speed)
        seed: Random seed for reproducibility
        
    Returns:
        Dict with keys edge_best, edge_worst, spread_1_5
        Values are tuples (p_value, (ci_lower, ci_upper))
        p_value is two-sided p-value (probability |stat| >= |observed|)
        ci_lower/ci_upper are 2.5th and 97.5th percentiles (95% CI)
    """
    if seed is not None:
        np.random.seed(seed)
    
    fwd_col = f"fwd_ret_{horizon}"
    
    # Compute observed statistics
    observed = compute_edge_stats(aligned_df, horizon, regime_col)
    observed_edge_best = observed["edge_best"]
    observed_edge_worst = observed["edge_worst"]
    observed_spread = observed["spread_1_5"]
    
    # Prepare data: we need to resample blocks of (regime, fwd_ret) pairs
    data = aligned_df[[regime_col, fwd_col]].dropna().copy()
    
    if len(data) < block_size:
        # Too few samples for block bootstrap
        return {
            "edge_best": (np.nan, (np.nan, np.nan)),
            "edge_worst": (np.nan, (np.nan, np.nan)),
            "spread_1_5": (np.nan, (np.nan, np.nan)),
        }
    
    # Number of blocks
    n_blocks = len(data) - block_size + 1
    
    # Bootstrap statistics
    boot_edge_best = []
    boot_edge_worst = []
    boot_spread = []
    
    for _ in range(n_boot):
        # Sample blocks with replacement
        n_blocks_to_sample = (len(data) // block_size) + 1
        block_indices = np.random.choice(n_blocks, size=n_blocks_to_sample, replace=True)
        
        # Construct resampled data
        resampled_rows = []
        for block_idx in block_indices:
            block = data.iloc[block_idx:block_idx + block_size]
            resampled_rows.append(block)
        
        if resampled_rows:
            resampled_df = pd.concat(resampled_rows, ignore_index=False)
            # Truncate to original length
            if len(resampled_df) > len(data):
                resampled_df = resampled_df.iloc[:len(data)]
            
            # Compute statistics on resampled data
            try:
                boot_stats = compute_edge_stats(resampled_df, horizon, regime_col)
                if not np.isnan(boot_stats["edge_best"]):
                    boot_edge_best.append(boot_stats["edge_best"])
                if not np.isnan(boot_stats["edge_worst"]):
                    boot_edge_worst.append(boot_stats["edge_worst"])
                if not np.isnan(boot_stats["spread_1_5"]):
                    boot_spread.append(boot_stats["spread_1_5"])
            except (ValueError, KeyError):
                # Skip if computation fails
                continue
    
    def compute_pvalue_and_ci(boot_values, observed_value):
        """Compute p-value and 95% CI from bootstrap samples."""
        if len(boot_values) == 0 or np.isnan(observed_value):
            return (np.nan, (np.nan, np.nan))
        
        boot_values = np.array(boot_values)
        ci_lower = np.percentile(boot_values, 2.5)
        ci_upper = np.percentile(boot_values, 97.5)
        
        # Two-sided p-value: P(|stat| >= |observed|)
        abs_observed = abs(observed_value)
        abs_boot = np.abs(boot_values)
        p_value = np.mean(abs_boot >= abs_observed)
        
        return (p_value, (ci_lower, ci_upper))
    
    return {
        "edge_best": compute_pvalue_and_ci(boot_edge_best, observed_edge_best),
        "edge_worst": compute_pvalue_and_ci(boot_edge_worst, observed_edge_worst),
        "spread_1_5": compute_pvalue_and_ci(boot_spread, observed_spread),
    }

