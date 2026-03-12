#!/usr/bin/env python3
"""
Diagnostics for Silver Layer ETL.

Prints targeted slices for visual verification:
- Price winsorizer clamp for BNB (2024-01-05..2024-01-09)
- Price gap fill for XRP (2023-12-30..2024-01-03)
- Funding capper sanity (5 sample rows where is_capped == True)
"""

import io
import sys
from pathlib import Path

import pandas as pd


if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_LAKE = REPO_ROOT / "data" / "curated" / "data_lake"


def _load_price() -> pd.DataFrame:
    path = DATA_LAKE / "silver_fact_price.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. Run build_silver_layer.py first.")
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def _load_funding() -> pd.DataFrame:
    path = DATA_LAKE / "silver_fact_funding.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. Run build_silver_layer.py first.")
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def main() -> None:
    price = _load_price()
    funding = _load_funding()

    # Test 1: Winsorizer clamp
    print("=" * 80)
    print("TEST 1 — PRICE WINSORIZER (BNB) 2024-01-05..2024-01-09")
    print("=" * 80)
    bnb = price[
        (price["asset_id"] == "BNB")
        & (price["date"] >= pd.Timestamp("2024-01-05"))
        & (price["date"] <= pd.Timestamp("2024-01-09"))
    ][["date", "close", "is_winsorized"]].sort_values("date")
    if bnb.empty:
        print("[WARN] No rows found for BNB in the requested date range.")
    else:
        bnb["date"] = bnb["date"].dt.date
        print(bnb.to_string(index=False))
    print()

    # Test 2: Gap filler
    print("=" * 80)
    print("TEST 2 — PRICE GAP FILLER (XRP) 2023-12-30..2024-01-03")
    print("=" * 80)
    xrp = price[
        (price["asset_id"] == "XRP")
        & (price["date"] >= pd.Timestamp("2023-12-30"))
        & (price["date"] <= pd.Timestamp("2024-01-03"))
    ][["date", "close", "is_ffilled"]].sort_values("date")
    if xrp.empty:
        print("[WARN] No rows found for XRP in the requested date range.")
        has_xrp = (price["asset_id"] == "XRP").any()
        if not has_xrp:
            print("       Reason hint: `asset_id == 'XRP'` does not exist in `silver_fact_price`.")
        else:
            xrp_all = price.loc[price["asset_id"] == "XRP", ["date"]].copy()
            print(
                "       Reason hint: XRP exists but has different date coverage. "
                f"Min={xrp_all['date'].min().date()} Max={xrp_all['date'].max().date()}"
            )
    else:
        xrp["date"] = xrp["date"].dt.date
        print(xrp.to_string(index=False))
    print()

    # Test 3: Funding capper
    print("=" * 80)
    print("TEST 3 — FUNDING CAPPER (is_capped == True) SAMPLE 5 ROWS")
    print("=" * 80)
    capped = funding[funding.get("is_capped", False) == True].copy()
    if capped.empty:
        print("[WARN] No capped funding rows found (is_capped == True).")
        return

    capped = capped.sort_values(["date", "asset_id", "instrument_id", "exchange"])
    sample = capped.head(5).copy()
    sample["date"] = sample["date"].dt.date
    cols = [c for c in ["asset_id", "instrument_id", "exchange", "date", "funding_rate", "is_capped"] if c in sample.columns]
    print(sample[cols].to_string(index=False))


if __name__ == "__main__":
    main()

