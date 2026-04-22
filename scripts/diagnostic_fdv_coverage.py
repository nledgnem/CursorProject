#!/usr/bin/env python3
"""Audit FDV / supply coverage in fact_markets_snapshot.

Reports what fraction of the daily markets snapshot has a non-null
fully_diluted_valuation_usd, broken down overall and within the top-300 by
market_cap_rank (the tradable universe for the Apathy Bleed strategy's
Gate 1 supply-ratio work). Also reports max_supply coverage, since FDV is
derived from max_supply upstream and a missing max_supply usually explains
a missing FDV.

Run: python scripts/diagnostic_fdv_coverage.py

Read-only. Does not write anywhere.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Add repo root to path so repo_paths resolves
sys.path.insert(0, str(Path(__file__).parent.parent))

from repo_paths import data_lake_root


SNAPSHOT_FILE = "fact_markets_snapshot.parquet"
TOP_N_RANK = 300


def _fmt_pct(num: int, denom: int) -> str:
    if denom == 0:
        return "n/a"
    return f"{num / denom * 100:.1f}%"


def main() -> int:
    lake = data_lake_root()
    path = lake / SNAPSHOT_FILE

    if not path.exists():
        print(f"[ERROR] {path} not found. Is the markets snapshot step running?")
        return 1

    df = pd.read_parquet(path)
    if df.empty:
        print(f"[ERROR] {path} is empty.")
        return 1

    required = {"date", "fully_diluted_valuation_usd", "max_supply", "market_cap_rank"}
    missing = required - set(df.columns)
    if missing:
        print(f"[ERROR] Missing expected columns in snapshot: {sorted(missing)}")
        print(f"        Available columns: {sorted(df.columns)}")
        return 1

    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()

    total = len(latest)
    fdv_nn = int(latest["fully_diluted_valuation_usd"].notna().sum())
    fdv_null = total - fdv_nn
    max_supply_nn = int(latest["max_supply"].notna().sum())
    circ_nn = int(latest["circulating_supply"].notna().sum()) if "circulating_supply" in latest.columns else None
    total_supply_nn = int(latest["total_supply"].notna().sum()) if "total_supply" in latest.columns else None

    print("=" * 70)
    print("FDV COVERAGE AUDIT  --  fact_markets_snapshot")
    print("=" * 70)
    print(f"Snapshot date (latest): {latest_date}")
    print(f"Total coins in snapshot: {total:,}")
    print()
    print("-- Overall --")
    print(f"  FDV non-null:                {fdv_nn:>5,} / {total:,}  ({_fmt_pct(fdv_nn, total)})")
    print(f"  FDV null:                    {fdv_null:>5,} / {total:,}  ({_fmt_pct(fdv_null, total)})")
    print(f"  max_supply non-null:         {max_supply_nn:>5,} / {total:,}  ({_fmt_pct(max_supply_nn, total)})")
    if circ_nn is not None:
        print(f"  circulating_supply non-null: {circ_nn:>5,} / {total:,}  ({_fmt_pct(circ_nn, total)})")
    if total_supply_nn is not None:
        print(f"  total_supply non-null:       {total_supply_nn:>5,} / {total:,}  ({_fmt_pct(total_supply_nn, total)})")

    # Strategy universe: top 300 by market_cap_rank. rank may be NaN for
    # coins without a market cap; exclude those from the denominator since
    # they can't be in the top-N.
    ranked = latest[latest["market_cap_rank"].notna()].copy()
    top = ranked[ranked["market_cap_rank"] <= TOP_N_RANK]
    top_total = len(top)
    top_fdv_nn = int(top["fully_diluted_valuation_usd"].notna().sum())
    top_fdv_null = top_total - top_fdv_nn
    top_max_supply_nn = int(top["max_supply"].notna().sum())

    print()
    print(f"-- Top {TOP_N_RANK} by market_cap_rank (strategy universe) --")
    print(f"  Coins in top-{TOP_N_RANK}:           {top_total:>5,}")
    print(f"  FDV non-null:                {top_fdv_nn:>5,} / {top_total:,}  ({_fmt_pct(top_fdv_nn, top_total)})")
    print(f"  FDV null:                    {top_fdv_null:>5,} / {top_total:,}  ({_fmt_pct(top_fdv_null, top_total)})")
    print(f"  max_supply non-null:         {top_max_supply_nn:>5,} / {top_total:,}  ({_fmt_pct(top_max_supply_nn, top_total)})")

    # If FDV is null but max_supply is non-null (or vice versa), that's
    # informative about where the gap is. Log a small sample.
    fdv_null_but_max_supply = top[top["fully_diluted_valuation_usd"].isna() & top["max_supply"].notna()]
    if len(fdv_null_but_max_supply) > 0:
        print()
        print(f"-- Top-{TOP_N_RANK} coins with max_supply but no FDV ({len(fdv_null_but_max_supply)} rows) --")
        cols = [c for c in ["symbol", "name", "market_cap_rank", "max_supply", "fully_diluted_valuation_usd"] if c in fdv_null_but_max_supply.columns]
        print(fdv_null_but_max_supply[cols].head(10).to_string(index=False))

    fdv_null_and_no_max = top[top["fully_diluted_valuation_usd"].isna() & top["max_supply"].isna()]
    if len(fdv_null_and_no_max) > 0:
        print()
        print(f"-- Top-{TOP_N_RANK} coins with neither max_supply nor FDV ({len(fdv_null_and_no_max)} rows) --")
        cols = [c for c in ["symbol", "name", "market_cap_rank"] if c in fdv_null_and_no_max.columns]
        print(fdv_null_and_no_max[cols].head(10).to_string(index=False))

    print()
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
