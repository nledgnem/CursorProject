"""
btcdom_recon: Reconstruct Binance BTCDOM index from local data lake and validate vs Binance.
"""

from .config import WEIGHTS, WEIGHTS_PCT, WEIGHTS_QTY, MissingDataMode, CONSTITUENT_SYMBOLS
from .data import load_prices
from .reconstruct import reconstruct_btcdom
from .binance_api import fetch_binance_index_klines
from .validate import align_series, compute_metrics, save_validation_outputs

__all__ = [
    "WEIGHTS",
    "WEIGHTS_PCT",
    "WEIGHTS_QTY",
    "MissingDataMode",
    "CONSTITUENT_SYMBOLS",
    "load_prices",
    "reconstruct_btcdom",
    "fetch_binance_index_klines",
    "align_series",
    "compute_metrics",
    "save_validation_outputs",
]
