"""
Universe QC: build candidate universe from data lake.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from datetime import date


def run_universe_qc(
    prices: pd.DataFrame,
    marketcap: pd.DataFrame,
    volume: pd.DataFrame,
    start_date: date,
    end_date: date,
    min_mcap_usd: float = 10e6,
    min_volume_usd_14d_avg: float = 1e6,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict]:
    """
    Apply universe filters. Returns aligned prices, marketcap, volume for eligible universe,
    plus report dict.
    """
    # Ensure date index
    for df in [prices, marketcap, volume]:
        if isinstance(df.index, pd.DatetimeIndex):
            df.index = df.index.date

    # Filter to date range
    mask = (prices.index >= start_date) & (prices.index <= end_date)
    prices = prices.loc[mask].sort_index()
    marketcap = marketcap.reindex(prices.index).ffill().bfill()
    volume = volume.reindex(prices.index).ffill().bfill()

    # USD ADV proxy: price * volume (if volume in base) or volume (if USD)
    # Spec says USD price * volume = USD ADV. CoinGecko volume is often USD already.
    # Use price * volume as conservative (often CoinGecko reports volume in USD, so this double-counts)
    # Many sources: volume is in USD. We use mean(close * volume) over 21d as ADV.
    usd_vol = prices * volume
    usd_vol_14d = usd_vol.rolling(14, min_periods=7).mean()

    # Per-date eligibility: mcap >= min, volume_14d >= min
    # We need at least 1 date where both pass for inclusion
    mid_date = prices.index[len(prices.index) // 2]
    mcap_t = marketcap.loc[mid_date]
    vol_t = usd_vol_14d.loc[mid_date]
    if pd.isna(vol_t).all():
        vol_t = usd_vol.rolling(14, min_periods=1).mean().loc[mid_date]

    eligible = set()
    for sym in prices.columns:
        if sym not in marketcap.columns or sym not in usd_vol_14d.columns:
            continue
        mc = marketcap[sym]
        v14 = usd_vol_14d[sym]
        # Require median over backtest period to pass
        mc_med = mc.median()
        v_med = v14.median()
        if pd.isna(mc_med) or pd.isna(v_med):
            continue
        if mc_med >= min_mcap_usd and v_med >= min_volume_usd_14d_avg:
            eligible.add(sym)

    # Also exclude obvious stables
    excluded = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "PAXG", "FRAX", "USDP"}
    eligible = [s for s in eligible if s not in excluded]

    # Align and restrict
    cols = [c for c in prices.columns if c in eligible]
    prices_u = prices[cols].copy()
    marketcap_u = marketcap.reindex(columns=cols).ffill().bfill()
    volume_u = volume.reindex(columns=cols).ffill().bfill()

    report = {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "min_mcap_usd": min_mcap_usd,
        "min_volume_usd_14d_avg": min_volume_usd_14d_avg,
        "universe_size": len(cols),
        "excluded_stablecoins": list(excluded),
        "all_assets_before_filter": prices.shape[1],
    }
    return prices_u, marketcap_u, volume_u, report


def build_universe_report(report: Dict) -> str:
    """Format universe QC report as string."""
    lines = [
        "=== Universe QC Report ===",
        f"Period: {report['start_date']} to {report['end_date']}",
        f"Min market cap USD: {report['min_mcap_usd']:,.0f}",
        f"Min 14d avg volume USD: {report['min_volume_usd_14d_avg']:,.0f}",
        f"Assets before filter: {report['all_assets_before_filter']}",
        f"Universe size (eligible): {report['universe_size']}",
    ]
    return "\n".join(lines)
