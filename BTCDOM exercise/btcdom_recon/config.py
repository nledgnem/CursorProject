"""
Configuration: BTCDOM weights and defaults for reconstruction and validation.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

# Binance BTCDOM: Weight (%) and Weight (Quantity) from Component Info.
# Last Index Price = BTCUSD / COINUSD. Index level = sum(Weight_Quantity_i * Last_Index_Price_i).
# Source: https://www.binance.com/en/futures/funding-history/perpetual/index
WEIGHTS_PCT: dict[str, float] = {
    "ETH": 0.4282,
    "XRP": 0.1511,
    "BNB": 0.1482,
    "SOL": 0.0864,
    "TRX": 0.0467,
    "DOGE": 0.0294,
    "ADA": 0.0185,
    "BCH": 0.0170,
    "LINK": 0.0113,
    "XLM": 0.0092,
    "HBAR": 0.0076,
    "LTC": 0.0075,
    "AVAX": 0.0071,
    "ZEC": 0.0070,
    "SUI": 0.0064,
    "DOT": 0.0048,
    "UNI": 0.0044,
    "TAO": 0.0033,
    "AAVE": 0.0031,
    "SKY": 0.0028,
}
# Weight (Quantity) — use for index level to match Binance (~2500–5500)
WEIGHTS_QTY: dict[str, float] = {
    "ETH": 64.55287400,
    "XRP": 0.01591236,
    "BNB": 6.80709263,
    "SOL": 0.55352779,
    "TRX": 0.00098125,
    "DOGE": 0.00021508,
    "ADA": 0.00039858,
    "BCH": 0.62672093,
    "LINK": 0.00765006,
    "XLM": 0.00011028,
    "HBAR": 0.00005645,
    "LTC": 0.03106884,
    "AVAX": 0.00484796,
    "ZEC": 0.12753188,
    "SUI": 0.00044956,
    "DOT": 0.00055799,
    "UNI": 0.00129526,
    "TAO": 0.04510682,
    "AAVE": 0.02645900,
    "SKY": 0.00001418,
}
# Backward compatibility: WEIGHTS = weight % (used when use_quantity_weights=False)
WEIGHTS: dict[str, float] = dict(WEIGHTS_PCT)

WEIGHTS_SUM: float = sum(WEIGHTS.values())

BTC_SYMBOL = "BTC"
CONSTITUENT_SYMBOLS: tuple[str, ...] = tuple(WEIGHTS.keys())


class MissingDataMode(str, Enum):
    """How to handle missing constituent data at a timestamp."""

    DROP_ROW = "drop_row"  # Drop that timestamp
    RENORMALIZE = "renormalize"  # Renormalize weights among available constituents


# Defaults
DEFAULT_FFILL_LIMIT = 2
DEFAULT_MISSING_MODE = MissingDataMode.RENORMALIZE
DEFAULT_DATA_LAKE_PATH = "data/curated/data_lake"
BINANCE_INDEX_KLINES_URL = "https://fapi.binance.com/fapi/v1/indexPriceKlines"

# Interval mapping for Binance (interval string -> milliseconds)
BINANCE_INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


def get_weights_sum() -> float:
    """Return sum of WEIGHTS (for tests and validation)."""
    return sum(WEIGHTS.values())
