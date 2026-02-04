"""Returns calculation for MSM v0."""

import polars as pl
from typing import List, Tuple, Optional, Dict, Union
from datetime import date
import logging

logger = logging.getLogger(__name__)


def get_close_asof(
    prices: pl.DataFrame,
    asset_id: str,
    asof_date: date,
) -> Optional[Tuple[date, float]]:
    """
    Get latest available close price for an asset on or before asof_date.
    
    Args:
        prices: Price dataframe (asset_id, date, close)
        asset_id: Asset identifier
        asof_date: Point-in-time date
    
    Returns:
        Tuple of (date_used, close_price) or None if no data available
    """
    asset_prices = prices.filter(pl.col("asset_id") == asset_id)
    
    if len(asset_prices) == 0:
        return None
    
    # Filter to dates <= asof_date
    asof_prices = asset_prices.filter(
        pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
    )
    
    if len(asof_prices) == 0:
        return None
    
    # Get latest date and corresponding close
    latest = asof_prices.sort("date", descending=True).head(1)
    
    date_used = latest["date"][0]
    close = latest["close"][0]
    
    return (date_used, close)


def compute_returns(
    prices: pl.DataFrame,
    marketcap: pl.DataFrame,  # Reserved for future use (not currently used)
    asset_ids: List[str],
    majors: List[str],
    majors_weights: List[float],
    decision_date: date,
    next_date: date,
    min_price_coverage_pct: float = 60.0,
) -> Tuple[float, Dict[str, float], float, float, float, int]:
    """
    Compute returns from t_k to t_{k+1} using asof pricing.
    
    Args:
        prices: Price dataframe (asset_id, date, close)
        marketcap: Marketcap dataframe (reserved for future use, not currently used)
        asset_ids: List of ALT asset_ids in basket
        majors: List of major asset_ids (e.g., ["BTC", "ETH"])
        majors_weights: Weights for majors (e.g., [0.7, 0.3] for BTC/ETH)
        decision_date: Decision time t_k
        next_date: Next decision time t_{k+1}
        min_price_coverage_pct: Minimum coverage % of ALTs with valid prices (default 60%)
    
    Returns:
        Tuple of (r_alts, r_majors_dict, r_maj_weighted, y, price_coverage_pct, n_valid_alts)
        - r_alts: Equal-weight return of ALT basket (or NaN if coverage insufficient)
        - r_majors_dict: Dict mapping major asset_id to return (e.g., {"BTC": r_btc, "ETH": r_eth})
        - r_maj_weighted: Weighted average of major returns
        - y: r_alts - r_maj_weighted (target, or NaN if insufficient data)
        - price_coverage_pct: Percentage of ALTs with valid prices
        - n_valid_alts: Number of ALTs with valid price data
    """
    # Compute ALT basket return (equal-weight) with asof pricing
    alt_returns = []
    for asset_id in asset_ids:
        prev_result = get_close_asof(prices, asset_id, decision_date)
        curr_result = get_close_asof(prices, asset_id, next_date)
        
        if prev_result is not None and curr_result is not None:
            prev_close = prev_result[1]
            curr_close = curr_result[1]
            if prev_close > 0:
                ret = (curr_close / prev_close) - 1.0
                alt_returns.append(ret)
    
    # Price coverage check for ALTs
    n_valid_alts = len(alt_returns)
    n_total_alts = len(asset_ids)
    price_coverage_pct = (n_valid_alts / n_total_alts * 100.0) if n_total_alts > 0 else 0.0
    
    if price_coverage_pct < min_price_coverage_pct:
        logger.warning(
            f"ALT price coverage {price_coverage_pct:.1f}% < {min_price_coverage_pct}% "
            f"({n_valid_alts}/{n_total_alts} assets) for {decision_date} to {next_date}"
        )
        r_alts = float('nan')
    elif len(alt_returns) == 0:
        logger.warning(f"No ALT returns computed for {decision_date} to {next_date}")
        r_alts = float('nan')
    else:
        r_alts = sum(alt_returns) / len(alt_returns)  # Equal-weight mean
    
    # Compute major returns (using config majors list, not hardcoded)
    r_majors_dict = {}
    for major_id, weight in zip(majors, majors_weights):
        prev_result = get_close_asof(prices, major_id, decision_date)
        curr_result = get_close_asof(prices, major_id, next_date)
        
        if prev_result is not None and curr_result is not None:
            prev_close = prev_result[1]
            curr_close = curr_result[1]
            if prev_close > 0:
                r_major = (curr_close / prev_close) - 1.0
                r_majors_dict[major_id] = r_major
            else:
                logger.warning(f"Zero or negative price for {major_id} at {decision_date}")
                r_majors_dict[major_id] = float('nan')
        else:
            logger.warning(f"No price data for {major_id} for {decision_date} to {next_date}")
            r_majors_dict[major_id] = float('nan')
    
    # Compute weighted major benchmark return
    r_maj_weighted = 0.0
    all_majors_valid = True
    for major_id, weight in zip(majors, majors_weights):
        if major_id in r_majors_dict:
            r_major = r_majors_dict[major_id]
            if r_major == r_major:  # Check for NaN
                r_maj_weighted += weight * r_major
            else:
                all_majors_valid = False
        else:
            all_majors_valid = False
    
    if not all_majors_valid:
        r_maj_weighted = float('nan')
    
    # Compute target: y = r_alts - r_maj_weighted
    if r_alts == r_alts and r_maj_weighted == r_maj_weighted:  # Check for NaN
        y = r_alts - r_maj_weighted
    else:
        y = float('nan')
    
    return r_alts, r_majors_dict, r_maj_weighted, y, price_coverage_pct, n_valid_alts


def compute_returns_for_week(
    prices: pl.DataFrame,
    marketcap: pl.DataFrame,
    asset_ids: List[str],
    majors: List[str],
    majors_weights: List[float],
    decision_date: date,
    next_date: date,
    min_price_coverage_pct: float = 60.0,
) -> Tuple[Optional[Tuple[float, Dict[str, float], float, float]], Optional[str]]:
    """
    Compute returns for a single week using asof pricing.
    
    Returns (None, rejection_reason) if data is insufficient.
    
    Args:
        prices: Price dataframe
        marketcap: Marketcap dataframe
        asset_ids: List of ALT asset_ids
        majors: List of major asset_ids
        majors_weights: Weights for majors
        decision_date: Decision time t_k
        next_date: Next decision time t_{k+1}
        min_price_coverage_pct: Minimum coverage % of ALTs with valid prices
    
    Returns:
        Tuple of (returns_tuple, rejection_reason)
        - returns_tuple: (r_alts, r_majors_dict, r_maj_weighted, y) or None
        - rejection_reason: "skipped_price_coverage" or "skipped_returns_computation" or None
    """
    try:
        returns = compute_returns(
            prices, marketcap, asset_ids, majors, majors_weights,
            decision_date, next_date, min_price_coverage_pct
        )
        
        r_alts, r_majors_dict, r_maj_weighted, y, price_coverage_pct, n_valid_alts = returns
        
        # Classify rejection reason based on actual price coverage
        if price_coverage_pct < min_price_coverage_pct:
            return None, "skipped_price_coverage"
        
        # Check if critical returns are NaN (other reasons)
        if r_alts != r_alts or r_maj_weighted != r_maj_weighted or y != y:
            return None, "skipped_returns_computation"
        
        # Return only the 4-tuple (without coverage info) for backward compatibility
        return (r_alts, r_majors_dict, r_maj_weighted, y), None
    except Exception as e:
        logger.warning(f"Error computing returns: {e}")
        return None, "skipped_returns_computation"
