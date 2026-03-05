from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd


def main() -> None:
    root = Path("c:/Users/Admin/Documents/Cursor")
    db_path = root / "btcdom_state.db"
    print("Loading rebalance_snapshots from", db_path)

    conn = sqlite3.connect(db_path)
    snaps = pd.read_sql_query(
        "SELECT rebalance_date, divisor, symbols_json, weights_json, rebalance_prices_json "
        "FROM rebalance_snapshots",
        conn,
    )
    conn.close()

    snaps["rebalance_date"] = pd.to_datetime(snaps["rebalance_date"])

    rows = []
    for _, row in snaps.iterrows():
        syms = json.loads(row["symbols_json"])
        ws = json.loads(row["weights_json"])
        ps = json.loads(row["rebalance_prices_json"])
        for s, w, p in zip(syms, ws, ps):
            rows.append(
                {
                    "rebalance_date": row["rebalance_date"].date(),
                    "asset_id": s,
                    "weight": w,
                    "rebalance_price_btc_denom": p,
                    "divisor": row["divisor"],
                }
            )

    comp_df = pd.DataFrame(rows)

    # Attach index level at rebalance date from reconstructed daily CSV
    recon_path = root / "data/curated/data_lake/btcdom_reconstructed.csv"
    print("Loading reconstructed index from", recon_path)
    recon = pd.read_csv(recon_path, parse_dates=["date"])
    recon["date"] = recon["date"].dt.date
    recon = recon[["date", "reconstructed_index_value"]].rename(
        columns={"date": "rebalance_date"}
    )

    comp_df = comp_df.merge(recon, on="rebalance_date", how="left")

    out_path = root / "BTCDOM exercise/msm_btcdom/out/btcdom_weekly_components.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    comp_df.to_csv(out_path, index=False)
    print("Wrote", len(comp_df), "rows to", out_path)


if __name__ == "__main__":
    main()

