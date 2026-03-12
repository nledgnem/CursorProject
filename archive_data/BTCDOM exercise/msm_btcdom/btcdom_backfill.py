"""
BTCDOM historical backfill orchestration.

- Instantiates DataLoader, IndexCalculator, StateStorage
- Automatically generates Thursday rebalance dates from T0 = 2024-06-06
  up to the last available BTC date in January 2026
- Runs the Decimal-precision backfill
- Persists:
    * Weekly rebalance snapshots to SQLite
    * Daily index values to SQLite
    * Daily index values to CSV at data/curated/data_lake/btcdom_reconstructed.csv
- Optionally plots the reconstructed index via compare_to_benchmark()
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Dict

import pandas as pd

from data_loader import DataLoader
from index_calculator import IndexCalculator, compare_to_benchmark
from state_storage import StateStorage, RebalanceSnapshot


# New T0 anchored to first Thursday after Binance CSV start (2024-07-01)
# and base index level matched to Binance close on 2024-07-04.
T0 = date(2024, 7, 4)

# Hard bound the backtest window to the historically clean period
# to avoid tail-end data drops in March 2026.
TARGET_END = date(2026, 1, 29)


def generate_rebalance_dates(start: date, end: date) -> List[date]:
    """
    Generate all Thursdays between start and end inclusive.
    Assumes 'start' is already a Thursday (weekday == 3).
    """
    if start.weekday() != 3:
        raise ValueError(f"start {start} is not a Thursday (weekday={start.weekday()})")
    dates: List[date] = []
    cur = start
    while cur <= end:
        dates.append(cur)
        cur += timedelta(days=7)
    return dates


def compute_last_btc_date_in_window(dl: DataLoader, start: date, end: date) -> date:
    """
    Use the data lake to determine the last date within [start, end] where
    BTC has price data. This ensures we don't backfill beyond coverage.
    """
    btc_ids = dl.get_btc_asset_ids()
    btc_prices = dl.get_prices(btc_ids, start, end)
    if btc_prices.empty:
        raise ValueError("No BTC price data found in the requested window")
    last = btc_prices["date"].max()
    if last is None:
        raise ValueError("Unable to determine last BTC date from fact_price")
    return last


def build_btc_price_map(dl: DataLoader, start: date, end: date) -> Dict[date, Decimal]:
    """
    Convenience helper to construct a date->BTC_close Decimal mapping
    over the given window, using all BTC asset_ids.
    """
    btc_ids = dl.get_btc_asset_ids()
    btc_prices = dl.get_prices(btc_ids, start, end)
    btc_prices = btc_prices.sort_values(["date", "asset_id"])

    price_by_date: Dict[date, Decimal] = {}
    for d_val, grp in btc_prices.groupby("date"):
        close_val = grp["close"].iloc[0]
        price_by_date[d_val] = Decimal(str(close_val))
    return price_by_date


def main() -> None:
    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    print("Initializing DataLoader, IndexCalculator, and StateStorage...")
    dl = DataLoader()
    calc = IndexCalculator(
        data_loader=dl,
        # Base index level anchored to Binance BTCDOM close on 2024-07-04
        base_index_level=Decimal("2448.02529635"),
        delta=Decimal("0.3"),
        max_ffill_days=3,
    )
    storage = StateStorage(db_path="btcdom_state.db")

    # ------------------------------------------------------------------
    # Determine actual backfill end date based on BTC coverage
    # ------------------------------------------------------------------
    print(f"Determining last BTC date between {T0} and {TARGET_END}...")
    last_btc_date = compute_last_btc_date_in_window(dl, T0, TARGET_END)
    print(f"Last BTC date in window: {last_btc_date}")

    if last_btc_date < T0:
        raise ValueError(f"BTC coverage ends before T0 ({T0}); cannot backfill")

    # ------------------------------------------------------------------
    # Generate Thursday rebalance dates
    # ------------------------------------------------------------------
    print("Generating Thursday rebalance dates...")
    rebalance_dates = generate_rebalance_dates(T0, last_btc_date)
    print(f"Number of rebalance dates: {len(rebalance_dates)}")
    print(f"First rebalance: {rebalance_dates[0]}, last rebalance: {rebalance_dates[-1]}")

    # ------------------------------------------------------------------
    # Run backfill
    # ------------------------------------------------------------------
    print("Running BTCDOM backfill...")
    df_index = calc.backfill(
        start_date=T0,
        end_date=last_btc_date,
        rebalance_dates=rebalance_dates,
    )
    if df_index.empty:
        raise RuntimeError("Backfill produced an empty index DataFrame")

    print(f"Backfill produced {len(df_index)} daily observations.")

    # ------------------------------------------------------------------
    # Persist daily index timeseries
    # ------------------------------------------------------------------
    print("Persisting daily index timeseries to SQLite...")
    storage.write_index_timeseries(df_index)

    # Also export to CSV in the data lake for easy inspection
    out_csv_path = Path("data/curated/data_lake/btcdom_reconstructed.csv")
    out_csv_path.parent.mkdir(parents=True, exist_ok=True)
    df_index.to_csv(out_csv_path, index=False)
    print(f"Wrote reconstructed index to {out_csv_path}")

    # ------------------------------------------------------------------
    # Persist weekly rebalance snapshots
    # ------------------------------------------------------------------
    print("Persisting weekly rebalance snapshots...")
    # Rebuild BTC price map (Decimal) for calling _build_rebalance_params
    btc_price_by_date = build_btc_price_map(dl, T0, last_btc_date)

    for i, reb_date in enumerate(rebalance_dates):
        # For continuity divisor, we need I_prev:
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

    # ------------------------------------------------------------------
    # Optional: quick visualization of reconstructed index
    # ------------------------------------------------------------------
    try:
        print("Creating quick matplotlib plot of reconstructed index...")
        compare_to_benchmark(df_index, official_binance_csv=None)
        print("Plot created (display depends on your environment).")
    except Exception as e:
        print(f"Could not create plot (this is non-fatal): {e}")


if __name__ == "__main__":
    main()

