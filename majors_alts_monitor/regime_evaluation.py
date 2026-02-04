"""Regime-conditional forward returns evaluation."""

import polars as pl
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import date
import logging
from scipy import stats

logger = logging.getLogger(__name__)


def block_bootstrap(
    data: pl.DataFrame,
    block_size: int = 10,
    n_boot: int = 300,
    seed: Optional[int] = None,
) -> List[pl.DataFrame]:
    """
    Block bootstrap resampling to account for autocorrelation.
    
    Args:
        data: DataFrame to resample (must have 'date' column, sorted by date)
        block_size: Number of consecutive days per block
        n_boot: Number of bootstrap iterations
        seed: Random seed for reproducibility
    
    Returns:
        List of n_boot resampled DataFrames
    """
    if seed is not None:
        np.random.seed(seed)
    
    n_rows = len(data)
    if n_rows < block_size:
        logger.warning(f"Insufficient data for block bootstrap (n={n_rows}, block_size={block_size})")
        return [data] * n_boot
    
    # Number of blocks needed
    n_blocks = (n_rows + block_size - 1) // block_size  # Ceiling division
    
    bootstrap_samples = []
    for _ in range(n_boot):
        # Sample block start indices (with replacement)
        block_starts = np.random.randint(0, n_rows - block_size + 1, size=n_blocks)
        
        # Collect blocks
        resampled_rows = []
        for start_idx in block_starts:
            block = data.slice(start_idx, block_size)
            resampled_rows.append(block)
        
        # Concatenate blocks
        if resampled_rows:
            resampled = pl.concat(resampled_rows)
            # Sort by date to maintain temporal order (even though it's resampled)
            resampled = resampled.sort("date")
            bootstrap_samples.append(resampled)
        else:
            bootstrap_samples.append(data)
    
    return bootstrap_samples


def compute_regime_edges(
    forward_returns: pl.DataFrame,
    horizon_col: str,
) -> Dict[str, float]:
    """
    Compute regime edge statistics.
    
    Args:
        forward_returns: DataFrame with forward returns and regime column
        horizon_col: Column name for forward returns
    
    Returns:
        Dict with edge_best, edge_worst, spread_1_5, mean_all, mean_1, mean_5, n1, n5, n_all
    """
    # Compute overall mean
    mean_all = forward_returns[horizon_col].mean()
    n_all = len(forward_returns)
    
    # Get regime 1 (worst) and regime 5 (best) if they exist
    # Handle both numeric (1, 5) and string (STRONG_RISK_ON_ALTS, STRONG_RISK_ON_MAJORS) regimes
    regime_col = forward_returns["regime"]
    unique_regimes = regime_col.unique().to_list()
    
    # Try to find worst and best regimes
    # For numeric: 1 = worst, 5 = best
    # For string: look for patterns like "RISK_ON_ALTS" (worst) and "RISK_ON_MAJORS" (best)
    worst_regime = None
    best_regime = None
    
    # Check for numeric regimes
    numeric_regimes = [r for r in unique_regimes if isinstance(r, (int, float))]
    if numeric_regimes:
        worst_regime = min(numeric_regimes)
        best_regime = max(numeric_regimes)
    else:
        # Check for string regimes
        for regime in unique_regimes:
            if isinstance(regime, str):
                if "ALTS" in regime.upper() and (worst_regime is None or "STRONG" in regime.upper()):
                    worst_regime = regime
                if "MAJORS" in regime.upper() and (best_regime is None or "STRONG" in regime.upper()):
                    best_regime = regime
    
    # Compute regime-specific means
    mean_1 = 0.0
    mean_5 = 0.0
    n1 = 0
    n5 = 0
    
    if worst_regime is not None:
        worst_data = forward_returns.filter(pl.col("regime") == worst_regime)
        if len(worst_data) > 0:
            mean_1 = worst_data[horizon_col].mean()
            n1 = len(worst_data)
    
    if best_regime is not None:
        best_data = forward_returns.filter(pl.col("regime") == best_regime)
        if len(best_data) > 0:
            mean_5 = best_data[horizon_col].mean()
            n5 = len(best_data)
    
    # Compute edges
    edge_best = mean_5 - mean_all if n5 > 0 else 0.0
    edge_worst = mean_1 - mean_all if n1 > 0 else 0.0
    spread_1_5 = mean_5 - mean_1 if (n1 > 0 and n5 > 0) else 0.0
    
    return {
        "edge_best": float(edge_best),
        "edge_worst": float(edge_worst),
        "spread_1_5": float(spread_1_5),
        "mean_all": float(mean_all),
        "mean_1": float(mean_1),
        "mean_5": float(mean_5),
        "n1": int(n1),
        "n5": int(n5),
        "n_all": int(n_all),
    }


def evaluate_regime_edges(
    target_returns: pl.DataFrame,
    regime_series: pl.DataFrame,
    horizons_days: List[int] = [5, 10, 20, 40, 60],
    bootstrap: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Evaluate regime-conditional forward returns.
    
    Computes mean(y | regime), hit rate, count, t-stat for each regime and horizon.
    Optionally includes block bootstrap significance testing.
    
    Args:
        target_returns: DataFrame with (date, return) - the target series y_{t\to t+H}
        regime_series: DataFrame with (date, regime) - regime labels S_t
        horizons_days: List of forward return horizons to evaluate
        bootstrap: Optional dict with bootstrap config:
            - enabled: bool (default: False)
            - block_size: int (default: 10)
            - n_boot: int (default: 300)
            - seed: Optional[int] for reproducibility
    
    Returns:
        Dict with regime-conditional statistics for each horizon, including bootstrap results if enabled
    """
    # Validate inputs
    if len(target_returns) == 0:
        logger.warning("Target returns DataFrame is empty")
        return {}
    if len(regime_series) == 0:
        logger.warning("Regime series DataFrame is empty")
        return {}
    
    # Check required columns
    if "date" not in target_returns.columns:
        logger.warning("Target returns missing 'date' column")
        return {}
    if "date" not in regime_series.columns:
        logger.warning("Regime series missing 'date' column")
        return {}
    
    # Join target returns with regime series
    joined = target_returns.join(regime_series, on="date", how="inner").sort("date")
    
    if len(joined) == 0:
        logger.warning("No overlapping data between target returns and regime series")
        return {}
    
    results = {}
    
    for horizon in horizons_days:
        # Compute forward returns for this horizon
        # Shift returns forward by horizon days
        forward_returns = joined.with_columns([
            pl.col("return").shift(-horizon).alias(f"forward_return_{horizon}d")
        ]).drop_nulls(subset=[f"forward_return_{horizon}d"])
        
        if len(forward_returns) == 0:
            continue
        
        # Group by regime and compute statistics
        regime_stats = (
            forward_returns
            .group_by("regime")
            .agg([
                pl.col(f"forward_return_{horizon}d").mean().alias("mean_return"),
                pl.col(f"forward_return_{horizon}d").std().alias("std_return"),
                pl.col(f"forward_return_{horizon}d").count().alias("count"),
                (pl.col(f"forward_return_{horizon}d") > 0).sum().alias("positive_count"),
            ])
        )
        
        # Compute hit rate and t-stat for each regime
        regime_results = {}
        for row in regime_stats.iter_rows(named=True):
            regime = row["regime"]
            mean_ret = row["mean_return"]
            std_ret = row["std_return"]
            count = row["count"]
            positive_count = row["positive_count"]
            
            # Handle None values
            if mean_ret is None:
                mean_ret = 0.0
            if std_ret is None:
                std_ret = 0.0
            if count is None:
                count = 0
            
            hit_rate = positive_count / count if count > 0 else 0.0
            
            # T-statistic: mean / (std / sqrt(n))
            t_stat = (mean_ret / (std_ret / np.sqrt(count))) if (std_ret is not None and std_ret > 0 and count > 1) else 0.0
            
            # P-value (two-tailed t-test)
            if count > 1 and std_ret is not None and std_ret > 0:
                p_value = 2 * (1 - stats.t.cdf(abs(t_stat), count - 1))
            else:
                p_value = 1.0
            
            regime_results[regime] = {
                "mean_return": float(mean_ret),
                "std_return": float(std_ret),
                "count": int(count),
                "hit_rate": float(hit_rate),
                "t_stat": float(t_stat),
                "p_value": float(p_value),
            }
        
        # Compute regime edges and bootstrap if enabled
        edge_results = {}
        if bootstrap and bootstrap.get("enabled", False):
            # Compute observed edges
            observed_edges = compute_regime_edges(forward_returns, f"forward_return_{horizon}d")
            edge_results.update(observed_edges)
            
            # Run block bootstrap
            block_size = bootstrap.get("block_size", 10)
            n_boot = bootstrap.get("n_boot", 300)
            seed = bootstrap.get("seed", None)
            
            logger.info(f"Running block bootstrap for horizon {horizon}d: block_size={block_size}, n_boot={n_boot}")
            
            # Prepare data for bootstrap (join forward returns with regime)
            bootstrap_data = forward_returns.select(["date", f"forward_return_{horizon}d", "regime"])
            
            # Run bootstrap
            bootstrap_samples = block_bootstrap(bootstrap_data, block_size=block_size, n_boot=n_boot, seed=seed)
            
            # Compute edges for each bootstrap sample
            bootstrap_edges = []
            for sample in bootstrap_samples:
                sample_edges = compute_regime_edges(sample, f"forward_return_{horizon}d")
                bootstrap_edges.append(sample_edges)
            
            # Compute bootstrap statistics
            if len(bootstrap_edges) > 0:
                # Extract edge values
                edge_best_boot = [e["edge_best"] for e in bootstrap_edges]
                edge_worst_boot = [e["edge_worst"] for e in bootstrap_edges]
                spread_1_5_boot = [e["spread_1_5"] for e in bootstrap_edges]
                
                # Compute p-values: fraction of bootstrap samples with |edge| >= |observed_edge|
                edge_best_pvalue = np.mean(np.abs(edge_best_boot) >= abs(observed_edges["edge_best"]))
                edge_worst_pvalue = np.mean(np.abs(edge_worst_boot) >= abs(observed_edges["edge_worst"]))
                spread_1_5_pvalue = np.mean(np.abs(spread_1_5_boot) >= abs(observed_edges["spread_1_5"]))
                
                # Compute 95% confidence intervals (2.5th and 97.5th percentiles)
                edge_best_ci = np.percentile(edge_best_boot, [2.5, 97.5])
                edge_worst_ci = np.percentile(edge_worst_boot, [2.5, 97.5])
                spread_1_5_ci = np.percentile(spread_1_5_boot, [2.5, 97.5])
                
                edge_results.update({
                    "edge_best_pvalue": float(edge_best_pvalue),
                    "edge_best_ci_lower": float(edge_best_ci[0]),
                    "edge_best_ci_upper": float(edge_best_ci[1]),
                    "edge_worst_pvalue": float(edge_worst_pvalue),
                    "edge_worst_ci_lower": float(edge_worst_ci[0]),
                    "edge_worst_ci_upper": float(edge_worst_ci[1]),
                    "spread_1_5_pvalue": float(spread_1_5_pvalue),
                    "spread_1_5_ci_lower": float(spread_1_5_ci[0]),
                    "spread_1_5_ci_upper": float(spread_1_5_ci[1]),
                })
        else:
            # Compute edges without bootstrap
            observed_edges = compute_regime_edges(forward_returns, f"forward_return_{horizon}d")
            edge_results.update(observed_edges)
        
        results[f"horizon_{horizon}d"] = {
            "regime_stats": regime_results,
            "edge_stats": edge_results,
        }
    
    return results


def compute_target_returns(
    prices: pl.DataFrame,
    alt_weights: Dict[str, float],
    major_weights: Dict[str, float],
    start_date: date,
    end_date: date,
    horizon_days: int = 1,
) -> pl.DataFrame:
    """
    Compute target returns: y_{t\to t+H} = r_alts_index - r_BTC.
    
    Args:
        prices: (asset_id, date, close)
        alt_weights: ALT basket weights (negative for short)
        major_weights: Major weights (positive for long, typically {"BTC": 1.0})
        start_date: Start date
        end_date: End date
        horizon_days: Forward return horizon
    
    Returns:
        DataFrame with (date, return) where return = r_alts_index - r_BTC
    """
    # Filter prices to date range
    prices_filtered = prices.filter(
        (pl.col("date") >= pl.date(start_date.year, start_date.month, start_date.day)) &
        (pl.col("date") <= pl.date(end_date.year, end_date.month, end_date.day))
    ).sort("date")
    
    # Get BTC prices
    btc_prices = prices_filtered.filter(pl.col("asset_id") == "BTC").sort("date")
    
    if len(btc_prices) == 0:
        logger.warning("No BTC prices found")
        return pl.DataFrame({"date": [], "return": []})
    
    # Compute ALT index returns
    alt_returns_list = []
    dates_list = []
    
    for alt_id, weight in alt_weights.items():
        alt_prices = prices_filtered.filter(pl.col("asset_id") == alt_id).sort("date")
        if len(alt_prices) > 0:
            alt_returns = alt_prices.with_columns([
                (pl.col("close") / pl.col("close").shift(horizon_days) - 1.0).alias("return")
            ]).select(["date", "return"])
            alt_returns_list.append(alt_returns)
    
    if len(alt_returns_list) == 0:
        logger.warning("No ALT prices found")
        return pl.DataFrame({"date": [], "return": []})
    
    # Combine ALT returns (weighted average)
    alt_index = alt_returns_list[0]
    for i, alt_ret_df in enumerate(alt_returns_list[1:], 1):
        alt_index = alt_index.join(alt_ret_df, on="date", how="outer", suffix=f"_{i}")
    
    # Compute weighted ALT index return
    alt_cols = [col for col in alt_index.columns if col.startswith("return")]
    if len(alt_cols) == 0:
        return pl.DataFrame({"date": [], "return": []})
    
    # Simple approach: equal weight if weights not aligned
    alt_index = alt_index.with_columns([
        pl.sum_horizontal([pl.col(col) for col in alt_cols]) / len(alt_cols)
        .alias("alt_return")
    ]).select(["date", "alt_return"])
    
    # Compute BTC returns
    btc_returns = btc_prices.with_columns([
        (pl.col("close") / pl.col("close").shift(horizon_days) - 1.0).alias("btc_return")
    ]).select(["date", "btc_return"])
    
    # Join and compute target: r_alts - r_BTC
    target = alt_index.join(btc_returns, on="date", how="inner").with_columns([
        (pl.col("alt_return") - pl.col("btc_return")).alias("return")
    ]).select(["date", "return"]).drop_nulls()
    
    return target


def format_regime_evaluation_results(results: Dict[str, Any]) -> str:
    """Format regime evaluation results as a readable string."""
    lines = []
    lines.append("=" * 80)
    lines.append("REGIME-CONDITIONAL FORWARD RETURNS")
    lines.append("=" * 80)
    
    for horizon_key, horizon_data in results.items():
        horizon = horizon_key.replace("horizon_", "").replace("d", "")
        lines.append(f"\nHorizon: {horizon} days")
        lines.append("-" * 80)
        
        # Extract regime_stats and edge_stats
        if isinstance(horizon_data, dict) and "regime_stats" in horizon_data:
            regime_stats = horizon_data["regime_stats"]
            edge_stats = horizon_data.get("edge_stats", {})
        else:
            # Legacy format (backward compatibility)
            regime_stats = horizon_data
            edge_stats = {}
        
        # Regime statistics
        lines.append("Regime Statistics:")
        lines.append(f"{'Regime':<25} {'Mean Return':>15} {'Hit Rate':>12} {'Count':>8} {'T-Stat':>10} {'P-Value':>10}")
        lines.append("-" * 80)
        
        for regime, stats_dict in sorted(regime_stats.items()):
            lines.append(
                f"{regime:<25} "
                f"{stats_dict['mean_return']*100:>14.2f}% "
                f"{stats_dict['hit_rate']*100:>11.1f}% "
                f"{stats_dict['count']:>8} "
                f"{stats_dict['t_stat']:>10.2f} "
                f"{stats_dict['p_value']:>10.4f}"
            )
        
        # Edge statistics (if available)
        if edge_stats:
            lines.append("\nEdge Statistics:")
            lines.append(f"{'Metric':<20} {'Value':>15} {'P-Value':>12} {'CI Lower':>12} {'CI Upper':>12}")
            lines.append("-" * 80)
            
            if "edge_best" in edge_stats:
                lines.append(
                    f"{'Edge Best':<20} "
                    f"{edge_stats['edge_best']*100:>14.2f}% "
                    f"{edge_stats.get('edge_best_pvalue', 1.0):>11.4f} "
                    f"{edge_stats.get('edge_best_ci_lower', 0.0)*100:>11.2f}% "
                    f"{edge_stats.get('edge_best_ci_upper', 0.0)*100:>11.2f}%"
                )
            
            if "edge_worst" in edge_stats:
                lines.append(
                    f"{'Edge Worst':<20} "
                    f"{edge_stats['edge_worst']*100:>14.2f}% "
                    f"{edge_stats.get('edge_worst_pvalue', 1.0):>11.4f} "
                    f"{edge_stats.get('edge_worst_ci_lower', 0.0)*100:>11.2f}% "
                    f"{edge_stats.get('edge_worst_ci_upper', 0.0)*100:>11.2f}%"
                )
            
            if "spread_1_5" in edge_stats:
                lines.append(
                    f"{'Spread (5-1)':<20} "
                    f"{edge_stats['spread_1_5']*100:>14.2f}% "
                    f"{edge_stats.get('spread_1_5_pvalue', 1.0):>11.4f} "
                    f"{edge_stats.get('spread_1_5_ci_lower', 0.0)*100:>11.2f}% "
                    f"{edge_stats.get('spread_1_5_ci_upper', 0.0)*100:>11.2f}%"
                )
    
    lines.append("=" * 80)
    return "\n".join(lines)
