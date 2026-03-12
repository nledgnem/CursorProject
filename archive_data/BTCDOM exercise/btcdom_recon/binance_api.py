"""
Fetch Binance Futures index price klines for validation.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import requests

from .config import BINANCE_INDEX_KLINES_URL

logger = logging.getLogger(__name__)


def fetch_binance_index_klines(
    symbol: str = "BTCDOMUSDT",
    interval: str = "1h",
    start_ms: int | None = None,
    end_ms: int | None = None,
    limit: int = 1500,
) -> pd.DataFrame:
    """
    Fetch index price klines from Binance Futures.

    GET https://fapi.binance.com/fapi/v1/indexPriceKlines
    Params: pair, interval, startTime, endTime, limit.

    Returns
    -------
    pd.DataFrame
        Columns: timestamp, open, high, low, close, volume.
        timestamp is datetime (UTC).
    """
    url = BINANCE_INDEX_KLINES_URL
    params: dict[str, Any] = {
        "pair": symbol,
        "interval": interval,
        "limit": limit,
    }
    if start_ms is not None:
        params["startTime"] = start_ms
    if end_ms is not None:
        params["endTime"] = end_ms

    all_rows: list[list] = []
    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        all_rows.extend(data)
        if len(data) < limit:
            break
        # Paginate
        params["startTime"] = data[-1][0] + 1
        if end_ms is not None and params["startTime"] >= end_ms:
            break

    if not all_rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(
        all_rows,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ],
    )
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    logger.info("Fetched %d Binance index klines for %s %s", len(df), symbol, interval)
    return df
