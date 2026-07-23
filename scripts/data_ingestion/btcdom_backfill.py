"""
Production BTCDOM backfill: updates data/curated/data_lake/btcdom_reconstructed.csv.

- Uses DataLoader, IndexCalculator, StateStorage from this package.
- Reads from data lake (dim_asset, fact_price, fact_marketcap).
- Writes daily reconstructed BTCDOM index to data/curated/data_lake/btcdom_reconstructed.csv.
- State (SQLite) and paths are resolved from project root for consistent behavior.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import List, Dict

import pandas as pd

from data_loader import DataLoader
from index_calculator import IndexCalculator, compare_to_benchmark
from state_storage import StateStorage, RebalanceSnapshot

from repo_paths import data_lake_root


# Project root: script lives at scripts/data_ingestion/btcdom_backfill.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_DIR = data_lake_root()
OUT_CSV_PATH = DATA_LAKE_DIR / "btcdom_reconstructed.csv"
STATE_DB_PATH = PROJECT_ROOT / "btcdom_state.db"

T0 = date(2024, 7, 4)

# Upper bound of the reconstruction window.
#
# DO NOT REINTRODUCE A HARDCODED ABSOLUTE END DATE HERE.
#
# This was previously `TARGET_END = date(2026, 1, 29)`. It was added on
# 2026-03-05 (commit b17103f) as a deliberate *research* bound, with the comment
# "Hard bound the backtest window to the historically clean period to avoid
# tail-end data drops in March 2026." On 2026-03-12 (commit 81e840e) this script
# was promoted into the live pipeline as Step 3 and that explanatory comment was
# deleted in the same change, while the constant was kept.
#
# Consequence: btcdom_reconstructed.csv was fully rewritten every night -- so its
# mtime advanced daily and it looked fresh -- while its terminal date stayed at
# 2026-01-29 for roughly six months. Every downstream merge on decision_date
# silently produced NaN, and BTCDOM_Trend fabricated "Falling" on 26 consecutive
# weekly rows while BTC dominance actually rose. See docs/runbooks/btcdom_macro_index.md.
#
# The bound is now derived from actual BTC price coverage in fact_price. Research
# windows go through the env overrides below, which are explicit and temporary by
# construction rather than silently permanent.
#
#   BTCDOM_TARGET_END        ISO date; caps the window (research/backtest only)
#   BTCDOM_TAIL_MARGIN_DAYS  drop N trailing days to dodge tail-end data drops
#   BTCDOM_MAX_COVERAGE_LAG_DAYS  fail if BTC price coverage lags today by more
#
_END_OVERRIDE_RAW = os.environ.get("BTCDOM_TARGET_END", "").strip()
TARGET_END_OVERRIDE: date | None = (
    date.fromisoformat(_END_OVERRIDE_RAW) if _END_OVERRIDE_RAW else None
)
TAIL_MARGIN_DAYS = int(os.environ.get("BTCDOM_TAIL_MARGIN_DAYS", "0"))
MAX_COVERAGE_LAG_DAYS = int(os.environ.get("BTCDOM_MAX_COVERAGE_LAG_DAYS", "3"))


def generate_rebalance_dates(start: date, end: date) -> List[date]:
    if start.weekday() != 3:
        raise ValueError(f"start {start} is not a Thursday (weekday={start.weekday()})")
    dates: List[date] = []
    cur = start
    while cur <= end:
        dates.append(cur)
        cur += timedelta(days=7)
    return dates


def compute_last_btc_date_in_window(dl: DataLoader, start: date, end: date) -> date:
    btc_ids = dl.get_btc_asset_ids()
    btc_prices = dl.get_prices(btc_ids, start, end)
    if btc_prices.empty:
        raise ValueError("No BTC price data found in the requested window")
    last = btc_prices["date"].max()
    if last is None:
        raise ValueError("Unable to determine last BTC date from fact_price")
    return last


def build_btc_price_map(dl: DataLoader, start: date, end: date) -> Dict[date, Decimal]:
    btc_ids = dl.get_btc_asset_ids()
    btc_prices = dl.get_prices(btc_ids, start, end)
    btc_prices = btc_prices.sort_values(["date", "asset_id"])
    price_by_date: Dict[date, Decimal] = {}
    for d_val, grp in btc_prices.groupby("date"):
        close_val = grp["close"].iloc[0]
        price_by_date[d_val] = Decimal(str(close_val))
    return price_by_date


def main() -> None:
    print("Initializing DataLoader, IndexCalculator, and StateStorage...")
    dl = DataLoader(base_path=DATA_LAKE_DIR)
    calc = IndexCalculator(
        data_loader=dl,
        base_index_level=Decimal("2448.02529635"),
        delta=Decimal("0.3"),
        max_ffill_days=3,
    )
    storage = StateStorage(db_path=STATE_DB_PATH)

    today_utc = datetime.now(timezone.utc).date()
    window_end = TARGET_END_OVERRIDE or today_utc
    if TARGET_END_OVERRIDE is not None:
        print(
            f"WARNING: BTCDOM_TARGET_END={TARGET_END_OVERRIDE} is set. This caps the "
            f"reconstruction and must not be set in production."
        )

    print(f"Determining last BTC date between {T0} and {window_end}...")
    last_btc_date = compute_last_btc_date_in_window(dl, T0, window_end)
    print(f"Last BTC date in window: {last_btc_date}")

    if last_btc_date < T0:
        raise ValueError(f"BTC coverage ends before T0 ({T0}); cannot backfill")

    # Fail loud if the upstream price feed itself has gone stale. Without this the
    # script would happily emit a short file and report success -- which is exactly
    # how the 2026-02..07 freeze stayed invisible for six months.
    coverage_lag_days = (today_utc - last_btc_date).days
    if TARGET_END_OVERRIDE is None and coverage_lag_days > MAX_COVERAGE_LAG_DAYS:
        raise SystemExit(
            f"BTC price coverage is stale: last fact_price BTC date={last_btc_date}, "
            f"today={today_utc}, lag={coverage_lag_days} days "
            f"(threshold={MAX_COVERAGE_LAG_DAYS}). Step 2 (CoinGecko price ingest) "
            f"likely failed. Refusing to rebuild the macro index on stale prices."
        )

    if TAIL_MARGIN_DAYS > 0:
        last_btc_date = last_btc_date - timedelta(days=TAIL_MARGIN_DAYS)
        print(f"Applied tail margin of {TAIL_MARGIN_DAYS}d -> end date {last_btc_date}")

    print("Generating Thursday rebalance dates...")
    rebalance_dates = generate_rebalance_dates(T0, last_btc_date)
    print(f"Number of rebalance dates: {len(rebalance_dates)}")
    print(f"First rebalance: {rebalance_dates[0]}, last rebalance: {rebalance_dates[-1]}")

    print("Running BTCDOM backfill...")
    df_index = calc.backfill(
        start_date=T0,
        end_date=last_btc_date,
        rebalance_dates=rebalance_dates,
    )
    if df_index.empty:
        raise RuntimeError("Backfill produced an empty index DataFrame")
    print(f"Backfill produced {len(df_index)} daily observations.")

    print("Persisting daily index timeseries to SQLite...")
    storage.write_index_timeseries(df_index)

    OUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_index.to_csv(OUT_CSV_PATH, index=False)
    print(f"Wrote reconstructed index to {OUT_CSV_PATH}")

    print("Persisting weekly rebalance snapshots...")
    btc_price_by_date = build_btc_price_map(dl, T0, last_btc_date)
    for i, reb_date in enumerate(rebalance_dates):
        if i == 0:
            last_index_for_reb = None
        else:
            prev_days = df_index[df_index["date"] < reb_date]
            if prev_days.empty:
                last_index_for_reb = None
            else:
                last_val = prev_days.iloc[-1]["reconstructed_index_value"]
                last_index_for_reb = Decimal(str(last_val))
        params = calc._build_rebalance_params(
            rebalance_date=reb_date,
            btc_price_by_date=btc_price_by_date,
            last_index_value=last_index_for_reb,
        )
        asset_ids = sorted(params.weights.keys())
        weights_list = [str(params.weights[aid]) for aid in asset_ids]
        rebalance_prices_list = [str(params.rebalance_prices[aid]) for aid in asset_ids]
        snapshot = RebalanceSnapshot(
            rebalance_date=reb_date,
            divisor=str(params.divisor),
            symbols=asset_ids,
            weights=weights_list,
            rebalance_prices=rebalance_prices_list,
        )
        storage.upsert_snapshot(snapshot)
    print("Rebalance snapshots persisted.")

    try:
        print("Creating quick matplotlib plot of reconstructed index...")
        compare_to_benchmark(df_index, official_binance_csv=None)
        print("Plot created.")
    except Exception as e:
        print(f"Could not create plot (non-fatal): {e}")


if __name__ == "__main__":
    main()
