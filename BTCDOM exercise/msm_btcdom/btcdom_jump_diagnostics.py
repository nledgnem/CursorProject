from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import json
import sqlite3

import pandas as pd

from data_loader import DataLoader


ROOT = Path(__file__).resolve().parents[2]
MSM_DIR = Path(__file__).resolve().parent
DB_PATH = ROOT / "btcdom_state.db"
RECON_CSV = ROOT / "data" / "curated" / "data_lake" / "btcdom_reconstructed.csv"


def find_breakpoint() -> date:
    print("=== 1. Finding breakpoint in reconstructed index ===")
    df = pd.read_csv(RECON_CSV, parse_dates=["date"])
    df["date"] = df["date"].dt.date

    start = date(2025, 1, 15)
    end = date(2025, 2, 28)
    window = df[(df["date"] >= start) & (df["date"] <= end)].copy()
    if window.empty:
        raise RuntimeError("No reconstructed data in the requested window.")

    window = window.sort_values("date")
    window["prev"] = window["reconstructed_index_value"].shift(1)
    window["pct_change"] = (window["reconstructed_index_value"] - window["prev"]) / window["prev"]

    # Use absolute change, ignoring first row (no prev)
    changes = window.dropna(subset=["pct_change"])
    changes["abs_change"] = changes["pct_change"].abs()

    # Candidate: first day where abs change > 15%
    big = changes[changes["abs_change"] > 0.15]
    if not big.empty:
        row = big.iloc[0]
        d_break = row["date"]
        print(f"First date with |daily change| > 15% in window: {d_break} (pct_change={row['pct_change']:.4f})")
    else:
        # Fallback: date with maximum absolute change
        row = changes.sort_values("abs_change", ascending=False).iloc[0]
        d_break = row["date"]
        print(
            "No day exceeded 15% move; using max abs move instead:",
            f"date={d_break}, pct_change={row['pct_change']:.4f}",
        )

    return d_break


def inspect_rebalance_and_universe(d_break: date) -> tuple[date, list[str], list[Decimal]]:
    print("\n=== 2. Inspecting rebalance preceding breakpoint ===")
    if not DB_PATH.exists():
        raise RuntimeError(f"State DB not found at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    snaps = pd.read_sql_query(
        "SELECT rebalance_date, symbols_json, weights_json FROM rebalance_snapshots",
        conn,
    )
    conn.close()

    snaps["rebalance_date"] = pd.to_datetime(snaps["rebalance_date"]).dt.date
    eligible = snaps[snaps["rebalance_date"] <= d_break]
    if eligible.empty:
        raise RuntimeError("No rebalance snapshot on or before breakpoint date.")

    row = eligible.sort_values("rebalance_date").iloc[-1]
    reb_date = row["rebalance_date"]
    symbols = json.loads(row["symbols_json"])
    weights = [Decimal(str(w)) for w in json.loads(row["weights_json"])]

    print(f"Breakpoint date: {d_break}")
    print(f"Preceding rebalance date: {reb_date}")

    dl = DataLoader(base_path=ROOT / "data" / "curated" / "data_lake")

    print("\nTop 20 constituents and weights at that rebalance:")
    for sym, w in zip(symbols, weights):
        meta = dl.get_asset_metadata(sym)
        symbol = meta.symbol if meta is not None else sym
        print(f"  {sym:10s} (symbol={symbol:8s}) weight={w:.6f}")

    # Check for new names that did not appear in any 2024 snapshot
    snaps_2024 = snaps[snaps["rebalance_date"] <= date(2024, 12, 31)]
    prev_symbols: set[str] = set()
    for _, r in snaps_2024.iterrows():
        prev_symbols.update(json.loads(r["symbols_json"]))

    new_assets = [s for s in symbols if s not in prev_symbols]
    if new_assets:
        print("\nNew assets that were NOT in any 2024 rebalance universe:")
        for s in new_assets:
            meta = dl.get_asset_metadata(s)
            symbol = meta.symbol if meta is not None else s
            print(f"  {s:10s} (symbol={symbol})")
    else:
        print("\nNo brand-new assets; all were present in at least one 2024 universe.")

    return reb_date, symbols, weights


def price_denominator_jump_check(d_break: date, symbols: list[str]) -> None:
    print("\n=== 3. Price denominator check around breakpoint ===")
    dl = DataLoader(base_path=ROOT / "data" / "curated" / "data_lake")

    # We'll look at Wednesday before breakpoint, and breakpoint day itself
    # (if breakpoint is Thu/Fri, this captures the potential split day).
    wed = d_break - timedelta(days=1)

    start = wed
    end = d_break
    prices = dl.get_prices(symbols, start, end)
    prices["date"] = pd.to_datetime(prices["date"]).dt.date

    print(f"Inspecting raw prices on {wed} and {d_break} for Top 20 assets.")

    pivot = prices.pivot(index="date", columns="asset_id", values="close")

    for aid in symbols:
        p_wed = pivot.get(aid).get(wed) if aid in pivot.columns and wed in pivot.index else None
        p_break = pivot.get(aid).get(d_break) if aid in pivot.columns and d_break in pivot.index else None
        if p_wed is None or p_break is None:
            print(f"  {aid:10s}: missing price on one of the days (Wed={p_wed}, Break={p_break})")
            continue
        ratio = Decimal(str(p_break)) / Decimal(str(p_wed)) if p_wed != 0 else Decimal("0")
        # Flag if drop > 50% or jump > 200%
        flag = ""
        if ratio < Decimal("0.5"):
            flag = "DROP>50%"
        elif ratio > Decimal("2.0"):
            flag = "JUMP>2x"
        print(
            f"  {aid:10s}: price_wed={p_wed:.6f}, price_break={p_break:.6f}, "
            f"ratio={ratio:.3f} {flag}"
        )


def main() -> None:
    d_break = find_breakpoint()
    reb_date, symbols, weights = inspect_rebalance_and_universe(d_break)
    price_denominator_jump_check(d_break, symbols)


if __name__ == "__main__":
    main()

