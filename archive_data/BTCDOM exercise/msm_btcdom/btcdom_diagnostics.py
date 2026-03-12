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
BINANCE_CSV = ROOT / "BTCDOM exercise" / "binance.csv"


def load_binance_series() -> pd.DataFrame:
    df = pd.read_csv(BINANCE_CSV)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    return df


def base_value_anchoring():
    print("\n=== 1. Base Value Anchoring (T0 Alignment) ===")
    binance = load_binance_series()
    t0 = date(2024, 6, 6)
    row_t0 = binance[binance["date"] == t0]
    if row_t0.empty:
        print(f"Binance CSV has no row for T0={t0}.")
        first = binance.sort_values("date").iloc[0]
        print(
            "First available Binance row:",
            f"date={first['date']}, close={first['close']}",
        )
        suggested_base = Decimal(str(first["close"]))
        print(
            "Suggested base_index_level (if anchoring to first Binance date):",
            suggested_base,
        )
    else:
        close_t0 = Decimal(str(row_t0.iloc[0]["close"]))
        print(f"Binance BTCDOM close on T0={t0}: {close_t0}")
        print("Use this as base_index_level instead of 1000.")


def november_anomaly_universe():
    print("\n=== 2. November 2024 Anomaly (Universe Leakage Check) ===")
    # Connect to SQLite snapshots
    if not DB_PATH.exists():
        print(f"State DB not found at {DB_PATH}")
        return
    conn = sqlite3.connect(DB_PATH)
    snaps = pd.read_sql_query(
        "SELECT rebalance_date, symbols_json, weights_json FROM rebalance_snapshots",
        conn,
    )
    conn.close()

    snaps["rebalance_date"] = pd.to_datetime(snaps["rebalance_date"]).dt.date
    target = date(2024, 11, 1)
    snaps["abs_diff"] = snaps["rebalance_date"].apply(lambda d: abs(d - target))
    row = snaps.sort_values("abs_diff").iloc[0]
    reb_date = row["rebalance_date"]
    symbols = json.loads(row["symbols_json"])
    weights = [Decimal(str(x)) for x in json.loads(row["weights_json"])]

    print(f"Closest rebalance to 2024-11-01 is {reb_date}")
    print("Top 20 constituents and weights:")

    dl = DataLoader(base_path=ROOT / "data" / "curated" / "data_lake")
    deny = dl._wrapped_staked_denylist()
    btc_ids = dl.get_btc_asset_ids()

    total_weight = sum(weights)
    for sym, w in zip(symbols, weights):
        meta = dl.get_asset_metadata(sym)
        symbol = meta.symbol if meta is not None else sym
        is_stable = meta.is_stable if meta is not None else False
        is_wrapped = meta.is_wrapped_stable if meta is not None else False
        flags = []
        if sym in btc_ids:
            flags.append("BTC")
        if is_stable:
            flags.append("STABLE")
        if is_wrapped or symbol.upper() in deny:
            flags.append("WRAPPED/DERIV")
        if w / total_weight > Decimal("0.10"):
            flags.append(">10%")
        flag_str = ",".join(flags) if flags else "-"
        print(f"  {sym:10s} (symbol={symbol:8s})  weight={w:.6f}  flags={flag_str}")


def divisor_continuity_check():
    print("\n=== 3. Divisor Continuity Check (Rebalance Jumps) ===")
    if not RECON_CSV.exists():
        print(f"Reconstructed CSV not found at {RECON_CSV}")
        return

    df = pd.read_csv(RECON_CSV, parse_dates=["date"])
    df["date"] = df["date"].dt.date

    # Use the same anomaly rebalance date as in (2)
    anomaly_thu = date(2024, 10, 31)
    wed = anomaly_thu - timedelta(days=1)

    row_wed = df[df["date"] == wed]
    row_thu = df[df["date"] == anomaly_thu]

    if row_wed.empty or row_thu.empty:
        print(
            f"Missing reconstructed index rows for Wed={wed} or Thu={anomaly_thu}. "
            f"Wed_empty={row_wed.empty}, Thu_empty={row_thu.empty}"
        )
        return

    idx_wed = Decimal(str(row_wed.iloc[0]["reconstructed_index_value"]))
    idx_thu = Decimal(str(row_thu.iloc[0]["reconstructed_index_value"]))

    print(f"Index on Wednesday {wed}:  {idx_wed}")
    print(f"Index on Thursday  {anomaly_thu}: {idx_thu}")
    print(f"Delta (Thu - Wed): {idx_thu - idx_wed}")


def price_denomination_check():
    print("\n=== 4. Price Denomination Verification (BTC/ETH on 2024-06-06) ===")
    dl = DataLoader(base_path=ROOT / "data" / "curated" / "data_lake")
    t = date(2024, 6, 6)

    btc_ids = dl.get_btc_asset_ids()
    eth_ids = dl.get_eth_asset_ids()

    prices = dl.get_prices(list(btc_ids | eth_ids), t, t)
    prices["date"] = pd.to_datetime(prices["date"]).dt.date
    day = prices[prices["date"] == t]

    if day.empty:
        print(f"No price data for {t}")
        return

    btc_rows = day[day["asset_id"].isin(btc_ids)]
    eth_rows = day[day["asset_id"].isin(eth_ids)]

    if btc_rows.empty or eth_rows.empty:
        print(f"Missing BTC or ETH rows on {t}. BTC_empty={btc_rows.empty}, ETH_empty={eth_rows.empty}")
        return

    btc_close = Decimal(str(btc_rows.iloc[0]["close"]))
    eth_close = Decimal(str(eth_rows.iloc[0]["close"]))
    p_eth = btc_close / eth_close

    print(f"On {t}:")
    print(f"  BTC close (data lake): {btc_close}")
    print(f"  ETH close (data lake): {eth_close}")
    print(f"  P_ETH(t) = BTC / ETH  : {p_eth}")


def main() -> None:
    base_value_anchoring()
    november_anomaly_universe()
    divisor_continuity_check()
    price_denomination_check()


if __name__ == "__main__":
    main()

