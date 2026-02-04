"""Regime-conditional forward returns evaluation."""

import polars as pl
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import date
import logging
from scipy import stats

logger = logging.getLogger(__name__)


def evaluate_regime_edges(
    target_returns: pl.DataFrame,
    regime_series: pl.DataFrame,
    horizons_days: List[int] = [5, 10, 20, 40, 60],
) -> Dict[str, Any]:
    """
    Evaluate regime-conditional forward returns.
    
    Computes mean(y | regime), hit rate, count, t-stat for each regime and horizon.
    
    Args:
        target_returns: DataFrame with (date, return) - the target series y_{t\to t+H}
        regime_series: DataFrame with (date, regime) - regime labels S_t
        horizons_days: List of forward return horizons to evaluate
    
    Returns:
        Dict with regime-conditional statistics for each horizon
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
        
        results[f"horizon_{horizon}d"] = regime_results
    
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
    
    for horizon_key, regime_stats in results.items():
        horizon = horizon_key.replace("horizon_", "").replace("d", "")
        lines.append(f"\nHorizon: {horizon} days")
        lines.append("-" * 80)
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
    
    lines.append("=" * 80)
    return "\n".join(lines)
