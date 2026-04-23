#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_l1_sample.py
=====================

Phase 5.1: Targeted L1 Data Extraction for Sanity Check

Extract daily price history for a small set of core Layer-1 tickers:
  - ETH
  - SOL
  - BNB

Data source:
  data/curated/prices_daily.parquet

Output:
  sol_eth_bnb_prices.csv
  written in the *repository root* (current working directory when run
  from repo root is assumed to be correct, but we compute root from __file__).

Strict formatting:
  long format with columns: date, ticker, close
Filter:
  date from 2024-01-01 through the most recent available date.
Drop:
  any rows where close is NaN.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    prices_path = REPO_ROOT / "data" / "curated" / "prices_daily.parquet"
    if not prices_path.exists():
        raise FileNotFoundError(f"Missing prices parquet: {prices_path}")

    out_path = REPO_ROOT / "sol_eth_bnb_prices.csv"

    tickers = ["ETH", "SOL", "BNB"]

    # Load the updated wide panel:
    # - expected layout: rows = daily dates (DatetimeIndex named 'date')
    # - columns = tickers, values = close (unit semantics are defined elsewhere)
    prices = pd.read_parquet(prices_path).sort_index()

    missing = [t for t in tickers if t not in prices.columns]
    if missing:
        raise KeyError(
            f"Requested tickers not present in prices_daily.parquet: {missing}. "
            f"Available columns sample: {list(prices.columns)[:10]}"
        )

    # Normalize index to datetime and name it 'date' for stable downstream CSV usage.
    if not isinstance(prices.index, pd.DatetimeIndex):
        # Defensive: the parquet loader should already return a DatetimeIndex for
        # this curated panel, but we avoid implicit geometry assumptions.
        prices.index = pd.to_datetime(prices.index)
    prices.index.name = "date"

    # Filter date range: [2024-01-01, max_date]
    start_date = pd.Timestamp("2024-01-01")
    end_date = prices.index.max()
    keep_mask = (prices.index >= start_date) & (prices.index <= end_date)
    prices = prices.loc[keep_mask, tickers]

    # Convert to long format:
    # - date: ISO-8601 YYYY-MM-DD string
    # - ticker: ETH/SOL/BNB
    # - close: float
    wide = prices.reset_index()  # brings 'date' out of the index
    wide["date"] = pd.to_datetime(wide["date"]).dt.strftime("%Y-%m-%d")
    long_df = wide.melt(id_vars=["date"], var_name="ticker", value_name="close")

    # Drop NaNs in close.
    long_df = long_df.dropna(subset=["close"]).sort_values(["ticker", "date"])

    # Write to CSV.
    long_df.to_csv(out_path, index=False)
    print(f"Wrote: {out_path} (rows={len(long_df)})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

