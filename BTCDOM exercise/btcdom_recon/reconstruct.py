"""
BTCDOM index reconstruction from BTC and constituent USD prices.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .config import (
    BTC_SYMBOL,
    CONSTITUENT_SYMBOLS,
    DEFAULT_FFILL_LIMIT,
    MissingDataMode,
    WEIGHTS,
    WEIGHTS_PCT,
    WEIGHTS_QTY,
)
from .data import load_prices

logger = logging.getLogger(__name__)


def reconstruct_btcdom(
    start: str,
    end: str,
    freq: str,
    *,
    ffill_limit: int = DEFAULT_FFILL_LIMIT,
    missing_mode: MissingDataMode = MissingDataMode.RENORMALIZE,
    use_quantity_weights: bool = True,
    max_last_index_price: Optional[float] = 5_000_000.0,
    data_lake_path: Optional[str | Path] = None,
    btc_df: Optional[pd.DataFrame] = None,
    constituent_dfs: Optional[dict[str, pd.DataFrame]] = None,
) -> pd.DataFrame:
    """
    Reconstruct BTCDOM index series.

    For each timestamp t:
      last_index_price_i(t) = BTCUSD(t) / COINUSD(t)
      If use_quantity_weights: BTCDOM(t) = sum_i WEIGHTS_QTY[i] * last_index_price_i(t)  (Binance level)
      Else: BTCDOM(t) = sum_i WEIGHTS_PCT[i] * last_index_price_i(t), renormalizing if constituents missing.

    With missing data: forward-fill up to ffill_limit; then drop row or renormalize (missing_mode, pct only).

    Parameters
    ----------
    start, end, freq : str
        Date range and frequency (passed to load_prices if data not provided).
    ffill_limit : int
        Max periods to forward-fill missing prices (default 2).
    missing_mode : MissingDataMode
        DROP_ROW or RENORMALIZE when data still missing after ffill.
    max_last_index_price : float or None, optional
        If set, exclude any constituent whose last_index_price (BTC/coin) exceeds this cap for that timestamp.
        Use to avoid spikes from bad data (e.g. wrong units). Default 5e6; set None to disable.
    data_lake_path : optional
        Path to data lake (used by load_prices if btc_df/constituent_dfs None).
    btc_df, constituent_dfs : optional
        Pre-loaded DataFrames with columns [timestamp, price_usd]. If provided,
        start/end/freq/data_lake_path are not used for loading.

    Returns
    -------
    pd.DataFrame
        Columns: timestamp, btcdom_recon, n_constituents_used, weights_renormalized_flag.
    """
    if btc_df is None or constituent_dfs is None:
        btc_df = load_prices(BTC_SYMBOL, start, end, freq, data_lake_path=data_lake_path)
        constituent_dfs = {
            sym: load_prices(sym, start, end, freq, data_lake_path=data_lake_path)
            for sym in CONSTITUENT_SYMBOLS
        }

    # Align all series to common timestamp index (union, then ffill)
    btc_ser = btc_df.set_index("timestamp")["price_usd"].sort_index()
    idx = btc_ser.index
    for df_c in constituent_dfs.values():
        if df_c is not None and not df_c.empty:
            other = df_c.set_index("timestamp")["price_usd"].index
            idx = idx.union(other)
    idx = pd.DatetimeIndex(idx.sort_values().unique())

    price_btc = btc_ser.reindex(idx).ffill(limit=ffill_limit)
    prices: dict[str, pd.Series] = {}
    for sym in CONSTITUENT_SYMBOLS:
        df_c = constituent_dfs.get(sym)
        if df_c is None or df_c.empty:
            continue
        ser = df_c.set_index("timestamp")["price_usd"].reindex(idx).ffill(limit=ffill_limit)
        prices[sym] = ser

    out_ts = []
    out_btcdom = []
    out_n = []
    out_renorm = []

    for t in idx:
        btc_usd = price_btc.loc[t] if t in price_btc.index else np.nan
        if pd.isna(btc_usd) or btc_usd <= 0:
            continue

        contributions: list[float] = []  # last_index_price_i = btc_in_coin
        weights_used: list[float] = []
        for sym in CONSTITUENT_SYMBOLS:
            if sym not in prices:
                continue
            coin_usd = prices[sym].loc[t] if t in prices[sym].index else np.nan
            if pd.isna(coin_usd) or coin_usd <= 0:
                continue
            btc_in_coin = btc_usd / float(coin_usd)
            if not np.isfinite(btc_in_coin):
                continue
            if max_last_index_price is not None and btc_in_coin > max_last_index_price:
                continue  # exclude aberrant constituent (e.g. wrong unit / bad data)
            contributions.append(btc_in_coin)
            weights_used.append(WEIGHTS_QTY[sym] if use_quantity_weights else WEIGHTS_PCT[sym])

        if not contributions:
            continue

        n = len(contributions)
        renormalized = n < len(CONSTITUENT_SYMBOLS)
        if use_quantity_weights:
            # Binance: raw sum of (Weight_Quantity_i * Last_Index_Price_i), no renormalization
            btcdom_t = sum(w * c for w, c in zip(weights_used, contributions))
        else:
            if renormalized and missing_mode == MissingDataMode.RENORMALIZE:
                total_w = sum(weights_used)
                if total_w > 0:
                    weights_used = [w / total_w for w in weights_used]
            elif renormalized and missing_mode == MissingDataMode.DROP_ROW:
                total_w = sum(weights_used)
                if total_w <= 0:
                    continue
                weights_used = [w / total_w for w in weights_used]
            btcdom_t = sum(w * c for w, c in zip(weights_used, contributions))
        if not np.isfinite(btcdom_t):
            continue
        out_ts.append(t)
        out_btcdom.append(float(btcdom_t))
        out_n.append(n)
        out_renorm.append(renormalized)

    return pd.DataFrame({
        "timestamp": out_ts,
        "btcdom_recon": out_btcdom,
        "n_constituents_used": out_n,
        "weights_renormalized_flag": out_renorm,
    })


def _reconstruct_btcdom_from_matrix(
    timestamps: pd.DatetimeIndex,
    btc_prices: np.ndarray,
    constituent_prices: dict[str, np.ndarray],
    ffill_limit: int,
    missing_mode: MissingDataMode,
) -> pd.DataFrame:
    """
    Core reconstruction from aligned arrays (for tests).
    constituent_prices: symbol -> array of price_usd aligned to timestamps.
    """
    out_ts = []
    out_btcdom = []
    out_n = []
    out_renorm = []

    for i, t in enumerate(timestamps):
        btc_usd = btc_prices[i]
        if not np.isfinite(btc_usd) or btc_usd <= 0:
            continue
        contributions = []
        weights_used = []
        for sym in CONSTITUENT_SYMBOLS:
            if sym not in constituent_prices:
                continue
            coin_usd = constituent_prices[sym][i]
            if not np.isfinite(coin_usd) or coin_usd <= 0:
                continue
            btc_in_coin = btc_usd / coin_usd
            contributions.append(btc_in_coin)
            weights_used.append(WEIGHTS[sym])

        if not contributions:
            continue
        n = len(contributions)
        renormalized = n < len(CONSTITUENT_SYMBOLS)
        if renormalized and missing_mode == MissingDataMode.RENORMALIZE:
            total_w = sum(weights_used)
            if total_w > 0:
                weights_used = [w / total_w for w in weights_used]
        elif renormalized and missing_mode == MissingDataMode.DROP_ROW:
            total_w = sum(weights_used)
            if total_w <= 0:
                continue
            weights_used = [w / total_w for w in weights_used]

        btcdom_t = sum(w * c for w, c in zip(weights_used, contributions))
        out_ts.append(t)
        out_btcdom.append(float(btcdom_t))
        out_n.append(n)
        out_renorm.append(renormalized)

    return pd.DataFrame({
        "timestamp": out_ts,
        "btcdom_recon": out_btcdom,
        "n_constituents_used": out_n,
        "weights_renormalized_flag": out_renorm,
    })
