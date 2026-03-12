from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import List, Set

import pandas as pd

from data_loader import DataLoader


ROOT = Path("c:/Users/Admin/Documents/Cursor")
DATA_LAKE_BASE = ROOT / "data" / "curated" / "data_lake"


def most_recent_thursday(dates: List[date]) -> date:
    """
    Given a list of dates, return the most recent Thursday (weekday == 3).
    """
    if not dates:
        raise ValueError("No dates provided")
    latest = max(dates)
    # Walk backwards to Thursday
    cur = latest
    while cur.weekday() != 3:
        cur -= timedelta(days=1)
    return cur


def main() -> None:
    dl = DataLoader(base_path=DATA_LAKE_BASE)

    # Determine most recent date in fact_marketcap and then its Thursday
    mc_dates = dl.fact_marketcap["date"].unique().tolist()
    mc_dates = [pd.to_datetime(d).date() for d in mc_dates]
    t_ref = most_recent_thursday(mc_dates)
    print(f"Most recent Thursday with marketcap data: {t_ref}")

    # Our engine's eligible universe on that date
    uni = dl.get_eligible_universe_on_date(t_ref)
    if uni.empty:
        print("Eligible universe is empty on that date.")
        return

    # Rank by marketcap and take top 20
    uni_sorted = uni.sort_values("marketcap", ascending=False)
    top20 = uni_sorted.head(20)
    our_ids: List[str] = top20["asset_id"].tolist()

    print("\nOur Engine's Top 20 asset_ids on that date:")
    for aid in our_ids:
        meta = dl.get_asset_metadata(aid)
        sym = meta.symbol if meta is not None else aid
        print(f"  {aid:10s} (symbol={sym})")

    # Official Binance constituent symbols
    binance_syms = [
        "ETH",
        "BNB",
        "XRP",
        "SOL",
        "TRX",
        "DOGE",
        "ADA",
        "BCH",
        "LINK",
        "XLM",
        "HBAR",
        "LTC",
        "AVAX",
        "ZEC",
        "SUI",
        "DOT",
        "UNI",
        "TAO",
        "AAVE",
        "SKY",
    ]

    # Map Binance symbols to our asset_ids via dim_asset
    dim = dl.dim_asset
    sym_to_id: dict[str, str] = {}
    for _, row in dim.iterrows():
        sym = str(row.get("symbol", "")).upper()
        aid = str(row["asset_id"])
        if sym and sym not in sym_to_id:
            sym_to_id[sym] = aid

    binance_ids: List[str] = []
    for sym in binance_syms:
        aid = sym_to_id.get(sym.upper())
        if aid is None and sym.upper() == "SKY":
            # SKY is often associated with Maker's rebrand; try MKR
            aid = sym_to_id.get("SKY") or sym_to_id.get("MKR")
        if aid is None:
            print(f"Warning: could not map Binance symbol {sym} to an asset_id")
        else:
            binance_ids.append(aid)

    print("\nOfficial Binance constituent asset_ids (mapped from symbols):")
    for aid in binance_ids:
        meta = dl.get_asset_metadata(aid)
        sym = meta.symbol if meta is not None else aid
        print(f"  {aid:10s} (symbol={sym})")

    our_set: Set[str] = set(our_ids)
    binance_set: Set[str] = set(binance_ids)

    false_positives = sorted(our_set - binance_set)
    false_negatives = sorted(binance_set - our_set)

    print("\nFalse Positives (in our Top 20 but NOT in Binance's list):")
    if not false_positives:
        print("  None")
    else:
        for aid in false_positives:
            meta = dl.get_asset_metadata(aid)
            sym = meta.symbol if meta is not None else aid
            print(f"  {aid:10s} (symbol={sym})")

    print("\nFalse Negatives (in Binance's list but NOT in our Top 20):")
    if not false_negatives:
        print("  None")
    else:
        for aid in false_negatives:
            meta = dl.get_asset_metadata(aid)
            sym = meta.symbol if meta is not None else aid
            print(f"  {aid:10s} (symbol={sym})")


if __name__ == "__main__":
    main()

