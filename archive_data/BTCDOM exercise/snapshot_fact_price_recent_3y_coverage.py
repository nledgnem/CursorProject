#!/usr/bin/env python3
"""
Snapshot per-asset coverage over the last 3 years for fact_price.parquet.

Outputs a CSV with:
  asset_id, recent_unique_dates, recent_start, recent_end

where:
- recent_end   = max(date) in fact_price (normalized)
- recent_start = recent_end - 3*365 days
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime

import pandas as pd


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    fact_path = repo_root / "data" / "curated" / "data_lake" / "fact_price.parquet"
    if not fact_path.exists():
        print(f"[ERROR] fact_price.parquet not found at {fact_path}")
        return 1

    df = pd.read_parquet(fact_path, columns=["asset_id", "date"])
    df["date"] = pd.to_datetime(df["date"])

    end = df["date"].max().normalize()
    start = end - pd.Timedelta(days=3 * 365)

    recent = df[(df["date"] >= start) & (df["date"] <= end)].copy()
    recent["date"] = recent["date"].dt.normalize()

    counts = recent.groupby("asset_id")["date"].nunique().reset_index()
    counts.columns = ["asset_id", "recent_unique_dates"]

    counts["recent_start"] = start.date()
    counts["recent_end"] = end.date()

    out_dir = repo_root / "outputs" / "coverage_snapshots"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"fact_price_recent3y_coverage_{ts}.csv"

    counts.to_csv(out_path, index=False)

    print("fact_price recent 3y coverage snapshot")
    print(f"Recent window: {start.date()} -> {end.date()}")
    print(f"Assets: {len(counts)}")
    print(f"CSV written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

