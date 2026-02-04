"""Feature computation for MSM v0: 7-day mean funding rate."""

import polars as pl
from typing import List, Optional, Tuple
from datetime import date, timedelta
import logging

logger = logging.getLogger(__name__)


def compute_7d_mean_funding(
    funding: pl.DataFrame,
    asset_ids: List[str],
    decision_date: date,
    lookback_days: int = 7,
) -> Tuple[Optional[float], float, int]:
    """
    Compute 7-day mean funding rate for a basket of assets.
    
    For each asset, computes mean funding over the 7 calendar days prior to decision_date.
    Then computes basket feature as mean of per-coin 7d funding means.
    
    Args:
        funding: Funding dataframe (asset_id, date, funding_rate)
        asset_ids: List of asset_ids in the basket
        decision_date: Decision time t_k
        lookback_days: Number of calendar days to look back (default 7)
    
    Returns:
        Tuple of (basket_feature, coverage_pct, n_valid)
        - basket_feature: Mean of per-coin 7d funding means (None if coverage < 60%)
        - coverage_pct: Percentage of assets with valid funding
        - n_valid: Number of assets with valid funding
    """
    if len(funding) == 0:
        logger.warning("No funding data available")
        return None, 0.0, 0
    
    if len(asset_ids) == 0:
        logger.warning("No assets in basket")
        return None, 0.0, 0
    
    # Date range: 7 calendar days prior to decision_date (exclusive of decision_date)
    # e.g., if decision_date is 2024-01-08 (Monday), lookback is 2024-01-01 to 2024-01-07
    start_date = decision_date - timedelta(days=lookback_days)
    end_date = decision_date - timedelta(days=1)  # Exclusive of decision_date
    
    # Filter funding data for date range
    funding_window = funding.filter(
        (pl.col("date") >= pl.date(start_date.year, start_date.month, start_date.day)) &
        (pl.col("date") <= pl.date(end_date.year, end_date.month, end_date.day))
    )
    
    if len(funding_window) == 0:
        logger.warning(f"No funding data in window {start_date} to {end_date}")
        return None, 0.0, 0
    
    # For each asset, compute 7-day mean funding
    asset_means = []
    for asset_id in asset_ids:
        asset_funding = funding_window.filter(pl.col("asset_id") == asset_id)
        
        if len(asset_funding) > 0:
            # Compute mean funding rate for this asset over the window
            # If multiple instruments per asset, aggregate by date first, then mean
            asset_daily = (
                asset_funding
                .group_by("date")
                .agg(pl.col("funding_rate").mean().alias("daily_funding"))
            )
            
            if len(asset_daily) > 0:
                mean_funding = asset_daily["daily_funding"].mean()
                if mean_funding is not None and not (mean_funding != mean_funding):  # Check for NaN
                    asset_means.append(mean_funding)
    
    n_valid = len(asset_means)
    n_total = len(asset_ids)
    coverage_pct = (n_valid / n_total * 100.0) if n_total > 0 else 0.0
    
    # Coverage rule: require >=60% of assets have valid funding
    min_coverage_pct = 60.0
    if coverage_pct < min_coverage_pct:
        logger.info(
            f"Coverage {coverage_pct:.1f}% < {min_coverage_pct}% "
            f"({n_valid}/{n_total} assets) - skipping feature computation"
        )
        return None, coverage_pct, n_valid
    
    # Basket feature: mean of per-coin 7d funding means
    basket_feature = sum(asset_means) / len(asset_means) if asset_means else None
    
    logger.debug(
        f"Computed basket feature: {basket_feature:.6f} "
        f"(coverage: {coverage_pct:.1f}%, {n_valid}/{n_total} assets)"
    )
    
    return basket_feature, coverage_pct, n_valid


def compute_feature_for_week(
    funding: pl.DataFrame,
    asset_ids: List[str],
    decision_date: date,
    lookback_days: int = 7,
    min_coverage_pct: float = 60.0,
) -> Tuple[Optional[float], float, int]:
    """
    Compute feature for a single week with coverage check.
    
    Returns None if coverage < min_coverage_pct.
    
    Args:
        funding: Funding dataframe
        asset_ids: List of asset_ids in basket
        decision_date: Decision time t_k
        lookback_days: Number of calendar days to look back
        min_coverage_pct: Minimum coverage percentage required
    
    Returns:
        Tuple of (basket_feature, coverage_pct, n_valid)
        - basket_feature: Feature value or None if coverage insufficient
        - coverage_pct: Coverage percentage
        - n_valid: Number of assets with valid funding
    """
    basket_feature, coverage_pct, n_valid = compute_7d_mean_funding(
        funding, asset_ids, decision_date, lookback_days
    )
    
    if coverage_pct < min_coverage_pct:
        return None, coverage_pct, n_valid
    
    return basket_feature, coverage_pct, n_valid
