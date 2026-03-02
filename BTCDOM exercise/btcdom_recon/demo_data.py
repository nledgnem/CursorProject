"""
Generate minimal synthetic fact_price.parquet for demo when data lake is not available.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .config import BTC_SYMBOL, CONSTITUENT_SYMBOLS


def generate_demo_fact_price(
    start: str = "2024-01-01",
    end: str = "2024-03-01",
    out_dir: str | Path = "demo_data",
) -> Path:
    """
    Write a minimal fact_price.parquet with synthetic daily BTC + constituent prices.
    Prices are deterministic so BTCDOM reconstruction runs; level is not calibrated to Binance.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "fact_price.parquet"

    dates = pd.date_range(start, end, freq="1D")
    rows = []
    # Synthetic BTC: 95k -> 100k over the range
    btc_prices = 95_000 + np.linspace(0, 5000, len(dates))
    for i, d in enumerate(dates):
        rows.append({"asset_id": BTC_SYMBOL, "date": d.date(), "close": float(btc_prices[i])})

    # Constituents: deterministic per symbol so btc/coin gives plausible spread
    np.random.seed(42)
    for sym in CONSTITUENT_SYMBOLS:
        base = 1000 + hash(sym) % 5000
        for i, d in enumerate(dates):
            noise = 1.0 + 0.02 * np.sin(i / 5)
            rows.append({"asset_id": sym, "date": d.date(), "close": float(base * noise)})

    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)
    return path
