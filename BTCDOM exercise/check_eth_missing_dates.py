#!/usr/bin/env python3
from pathlib import Path

import pandas as pd


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    price_path = repo_root / "data" / "curated" / "data_lake" / "fact_price.parquet"
    mcap_path = repo_root / "data" / "curated" / "data_lake" / "fact_marketcap.parquet"

    df_p = pd.read_parquet(price_path)
    df_p["date"] = pd.to_datetime(df_p["date"])
    eth_p = df_p[df_p["asset_id"] == "ETH"].copy()

    df_m = pd.read_parquet(mcap_path)
    df_m["date"] = pd.to_datetime(df_m["date"])
    eth_m = df_m[df_m["asset_id"] == "ETH"].copy()

    end = eth_p["date"].max().normalize()
    start_recent = end - pd.Timedelta(days=3 * 365)

    def find_missing(series: pd.Series, start, end):
        s = series.dt.normalize().drop_duplicates().sort_values()
        idx = pd.date_range(start, end, freq="D")
        return idx.difference(s)

    miss_p_recent = find_missing(eth_p["date"], start_recent, end)
    miss_m_recent = find_missing(eth_m["date"], start_recent, end)

    print("ETH PRICE:")
    print(
        "  total_dates =",
        eth_p["date"].nunique(),
        "range",
        eth_p["date"].min().date(),
        "->",
        eth_p["date"].max().date(),
    )
    print("  recent (last 3y) missing days:", len(miss_p_recent))
    print(
        "  first 10 missing price dates (recent):",
        [d.date() for d in miss_p_recent[:10]],
    )

    print("\nETH MARKETCAP:")
    print(
        "  total_dates =",
        eth_m["date"].nunique(),
        "range",
        eth_m["date"].min().date(),
        "->",
        eth_m["date"].max().date(),
    )
    print("  recent (last 3y) missing days:", len(miss_m_recent))
    print(
        "  first 10 missing mcap dates (recent):",
        [d.date() for d in miss_m_recent[:10]],
    )

    # Also compute full-range gaps
    full_idx_p = pd.date_range(
        eth_p["date"].min().normalize(), eth_p["date"].max().normalize(), freq="D"
    )
    full_missing_p = full_idx_p.difference(
        eth_p["date"].dt.normalize().drop_duplicates()
    )
    full_idx_m = pd.date_range(
        eth_m["date"].min().normalize(), eth_m["date"].max().normalize(), freq="D"
    )
    full_missing_m = full_idx_m.difference(
        eth_m["date"].dt.normalize().drop_duplicates()
    )
    print("\nFull-range gaps:")
    print("  ETH PRICE full-range missing days:", len(full_missing_p))
    print("  ETH MCAP full-range missing days:", len(full_missing_m))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

