"""Universe selection for MSM v0: top N eligible ALTs with exclusions."""

import polars as pl
from typing import List, Set, Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)


def get_excluded_assets(
    dim_asset: Optional[pl.DataFrame],
    exclude_categories: List[str],
) -> Set[str]:
    """
    Get set of excluded asset_ids based on categories.
    
    Uses dim_asset tags if available, else hardcodes common exclusions.
    
    Args:
        dim_asset: Dimension asset table (optional)
        exclude_categories: List of categories to exclude (e.g., ["BTC", "ETH", "stablecoins"])
    
    Returns:
        Set of asset_ids to exclude
    """
    excluded = set()
    
    # Always exclude BTC and ETH if in categories
    if "BTC" in exclude_categories:
        excluded.add("BTC")
    if "ETH" in exclude_categories:
        excluded.add("ETH")
    
    if dim_asset is not None:
        # Use dim_asset tags if available
        df = dim_asset
        
        # Stablecoins
        if "stablecoins" in exclude_categories or "stablecoin" in exclude_categories:
            stables = df.filter(pl.col("is_stable") == True)
            excluded.update(stables["asset_id"].to_list())
        
        # Check for other tags (if available in dim_asset)
        # Exchange tokens, wrapped tokens, liquid staking tokens, bridge/pegged assets
        # These would need to be in dim_asset as boolean flags or tags
        # For v0, we'll hardcode common ones if tags not available
        
        # Check if there are tag columns
        tag_columns = [col for col in df.columns if "tag" in col.lower() or "category" in col.lower()]
        if tag_columns:
            for tag_col in tag_columns:
                # Could filter by tag values if needed
                pass
    
    # Hardcoded exclusions for v0 (if dim_asset tags not available)
    # Common stablecoins
    hardcoded_stables = {
        "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "USDD", "FRAX", "LUSD", "GUSD",
        "HUSD", "USDN", "USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "USDD", "FRAX",
        "PAXG", "WBTC", "WETH",  # Wrapped tokens (common)
    }
    
    # Common exchange tokens
    hardcoded_exchange = {
        "BNB", "FTT", "HT", "OKB", "KCS", "GT", "MX", "CRO", "LEO", "VGX",
    }
    
    # Common wrapped/liquid staking tokens
    hardcoded_wrapped = {
        "WBTC", "WETH", "stETH", "rETH", "cbETH", "wstETH", "stSOL", "rBTC",
    }
    
    if "stablecoins" in exclude_categories or "stablecoin" in exclude_categories:
        excluded.update(hardcoded_stables)
    
    if "exchange_tokens" in exclude_categories or "exchange" in exclude_categories:
        excluded.update(hardcoded_exchange)
    
    if "wrapped_tokens" in exclude_categories or "wrapped" in exclude_categories:
        excluded.update(hardcoded_wrapped)
    
    if "liquid_staking" in exclude_categories:
        excluded.update({"stETH", "rETH", "cbETH", "wstETH", "stSOL", "rBTC"})
    
    if "bridge" in exclude_categories or "pegged" in exclude_categories:
        # Common bridge/pegged assets
        excluded.update({"WBTC", "WETH", "renBTC", "tBTC"})
    
    logger.info(f"Excluded {len(excluded)} assets: {sorted(list(excluded))[:20]}...")
    
    return excluded


def select_top_n_alts(
    marketcap: pl.DataFrame,
    asof_date: date,
    n: int = 30,
    min_mcap_usd: float = 50_000_000,
    excluded_assets: Optional[Set[str]] = None,
) -> pl.DataFrame:
    """
    Select top N eligible ALTs by market cap at a specific date.
    
    Args:
        marketcap: Marketcap dataframe (asset_id, date, marketcap)
        asof_date: Point-in-time date for selection
        n: Number of assets to select
        min_mcap_usd: Minimum market cap threshold
        excluded_assets: Set of asset_ids to exclude
    
    Returns:
        DataFrame with (asset_id, marketcap, rank) for selected assets
    """
    if excluded_assets is None:
        excluded_assets = set()
    
    # Get marketcap at asof_date (PIT-safe)
    mcap_at_date = marketcap.filter(
        pl.col("date") <= pl.date(asof_date.year, asof_date.month, asof_date.day)
    )
    
    if len(mcap_at_date) == 0:
        logger.warning(f"No marketcap data available at or before {asof_date}")
        return pl.DataFrame()
    
    # Get most recent marketcap for each asset up to asof_date
    latest_mcap = (
        mcap_at_date
        .sort(["asset_id", "date"], descending=[False, True])
        .group_by("asset_id")
        .first()
        .filter(pl.col("marketcap") >= min_mcap_usd)
        .filter(~pl.col("asset_id").is_in(list(excluded_assets)))
        .sort("marketcap", descending=True)
        .head(n)
        .with_columns([
            pl.int_range(1, pl.len() + 1).alias("rank"),
        ])
        .select(["asset_id", "marketcap", "rank"])
    )
    
    logger.info(f"Selected {len(latest_mcap)} ALTs for {asof_date} (top {n} by marketcap)")
    
    return latest_mcap


def get_universe_hash(asset_ids: List[str]) -> str:
    """
    Generate a hash/identifier for the universe basket.
    
    Args:
        asset_ids: List of asset_ids in the basket
    
    Returns:
        String identifier (sorted comma-separated list)
    """
    return ",".join(sorted(asset_ids))
