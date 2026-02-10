"""
Shared utilities for LS basket low-vol pipeline.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import date, datetime, timedelta
import json


def load_data_lake(data_lake_dir) -> Dict[str, pd.DataFrame]:
    """Load fact tables from data lake. Returns prices, marketcap, volume (wide format)."""
    import sys
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from src.utils.data_loader import load_data_lake_wide

    dl_path = Path(data_lake_dir)
    if not dl_path.is_absolute():
        dl_path = repo_root / dl_path

    data = load_data_lake_wide(dl_path, prices=True, marketcap=True, volume=True)
    return data


def compute_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """Compute daily returns from prices."""
    return prices.pct_change().dropna(how="all")


def compute_usd_adv(
    prices: pd.DataFrame, volume: pd.DataFrame, window: int = 21
) -> pd.DataFrame:
    """
    Compute USD ADV = mean(last 21 trading days) of (close * volume).
    Per spec: USD price * volume = USD volume. Assume volume is in USD if not in base.
    CoinGecko volume is typically in USD - but spec says price * volume, so we use that.
    """
    if volume is None or prices is None:
        return None
    # Align on common dates and symbols
    common_dates = prices.index.intersection(volume.index)
    common_cols = prices.columns.intersection(volume.columns)
    p = prices.loc[common_dates, common_cols]
    v = volume.loc[common_dates, common_cols]
    # USD volume = price * volume (if volume in base units; if already USD, this overstates - use volume as-is if 0 in price)
    usd_vol = p * v
    # Where price or volume is NaN, result is NaN
    adv = usd_vol.rolling(window, min_periods=1).mean()
    return adv


def ledoit_wolf_cov(returns: pd.DataFrame) -> Tuple[np.ndarray, pd.Index]:
    """
    Compute Ledoit-Wolf shrunk covariance matrix.
    Returns (cov_matrix, asset_index).
    """
    from sklearn.covariance import LedoitWolf

    R = returns.dropna(axis=1, how="all").dropna(axis=0, how="any")
    if R.empty or R.shape[1] < 2:
        return np.eye(1), R.columns
    lw = LedoitWolf()
    lw.fit(R)
    return lw.covariance_, R.columns


def cvar_95(returns: np.ndarray) -> float:
    """CVaR at 95% via historical simulation (avg of worst 5% of returns)."""
    r = np.asarray(returns).flatten()
    r = r[~np.isnan(r)]
    if len(r) == 0:
        return 0.0
    q = np.percentile(r, 5)
    tail = r[r <= q]
    return -float(np.mean(tail)) if len(tail) > 0 else 0.0


def cvar_99(returns: np.ndarray) -> float:
    """CVaR at 99% via historical simulation."""
    r = np.asarray(returns).flatten()
    r = r[~np.isnan(r)]
    if len(r) == 0:
        return 0.0
    q = np.percentile(r, 1)
    tail = r[r <= q]
    return -float(np.mean(tail)) if len(tail) > 0 else 0.0


def pca_first_component(returns: pd.DataFrame) -> np.ndarray:
    """Return first principal component (loadings) of returns."""
    from sklearn.decomposition import PCA

    R = returns.dropna(axis=1, how="all").dropna(axis=0, how="any")
    if R.shape[0] < 2 or R.shape[1] < 2:
        return np.ones(returns.shape[1]) / np.sqrt(returns.shape[1])
    pca = PCA(n_components=1)
    pca.fit(R.fillna(0))
    return pca.components_[0]


def get_rebalance_dates(
    start: date, end: date, freq: str = "monthly", day: int = 1
) -> List[date]:
    """Generate rebalance dates (monthly on given day)."""
    dates = []
    y, m = start.year, start.month
    d = date(y, m, min(day, 28))
    if d < start:
        m += 1
        if m > 12:
            m, y = 1, y + 1
        d = date(y, m, min(day, 28))
    while d <= end:
        dates.append(d)
        m += 1
        if m > 12:
            m, y = 1, y + 1
        try:
            d = date(y, m, min(day, 28))
        except ValueError:
            d = date(y, m, 28)
    return dates


def compute_turnover(old_w: pd.Series, new_w: pd.Series) -> float:
    """Portfolio turnover = sum(|new - old|) / 2."""
    all_sym = set(old_w.index) | set(new_w.index)
    old_a = pd.Series(0.0, index=sorted(all_sym))
    new_a = pd.Series(0.0, index=sorted(all_sym))
    old_a[old_w.index] = old_w
    new_a[new_w.index] = new_w
    return (new_a - old_a).abs().sum() / 2.0


def ensure_dates(df: pd.DataFrame) -> pd.DatetimeIndex:
    """Ensure index is date-like."""
    if isinstance(df.index, pd.DatetimeIndex):
        return df.index
    return pd.to_datetime(df.index).date

