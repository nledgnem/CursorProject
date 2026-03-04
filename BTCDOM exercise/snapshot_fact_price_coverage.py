#!/usr/bin/env python3
"""
Snapshot per-asset coverage for fact_price.parquet.

Writes a CSV with columns:
  asset_id, unique_dates

and prints summary stats (rows, assets, min/max date, counts above/below a
configurable threshold, default 720 days).
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import argparse

import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Snapshot per-asset coverage for fact_price.parquet."
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=720,
        help="Day-count threshold for summary stats (default: 720).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    fact_path = repo_root / "data" / "curated" / "data_lake" / "fact_price.parquet"
    if not fact_path.exists():
        print(f"[ERROR] fact_price.parquet not found at {fact_path}")
        return 1

    df = pd.read_parquet(fact_path, columns=["asset_id", "date"])
    df["date"] = pd.to_datetime(df["date"])

    rows = len(df)
    assets = df["asset_id"].nunique()
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()

    counts = df.groupby("asset_id")["date"].nunique()

    threshold = args.threshold
    ge = int((counts >= threshold).sum())
    lt = int((counts < threshold).sum())

    print("fact_price coverage snapshot")
    print(f"Rows: {rows}")
    print(f"Assets: {assets}")
    print(f"Date range: {min_date} -> {max_date}")
    print(f"Assets with >= {threshold} unique dates: {ge}")
    print(f"Assets with < {threshold} unique dates: {lt}")

    out_dir = repo_root / "outputs" / "coverage_snapshots"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"fact_price_coverage_{ts}.csv"

    snap = counts.reset_index()
    snap.columns = ["asset_id", "unique_dates"]
    snap.to_csv(out_path, index=False)
    print(f"\nWrote per-asset coverage snapshot to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

