#!/usr/bin/env python3
"""
Targeted backfill for the 2026-01-30 .. 2026-03-03 gap (UTC daily bars).

Uses hardened fetch_price_history (structured JSONL failures under logs/).

Steps (recommended):
  1. Run qc_curate.py on curated panels so the index is contiguous with NaN gaps:
       python scripts/qc_curate.py --raw-dir data/curated --out-dir data/curated --outputs-dir outputs/qc_align
  2. Run this script (requires COINGECKO_API_KEY in environment):
       python scripts/backfill_early_2026.py

Optional:
  python scripts/backfill_early_2026.py --dry-run   # plan only, no HTTP / no writes
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.data_lake.schema import (  # noqa: E402
    FACT_MARKETCAP_SCHEMA,
    FACT_PRICE_SCHEMA,
    FACT_VOLUME_SCHEMA,
)
from src.providers.coingecko import fetch_price_history  # noqa: E402


GAP_START = date(2026, 1, 30)
GAP_END = date(2026, 3, 3)


def _load_incremental_helpers():
    """Load append helper without making scripts a package."""
    path = REPO_ROOT / "scripts" / "incremental_update.py"
    spec = importlib.util.spec_from_file_location("incremental_update", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod.append_to_fact_table


def _wide_from_dicts(
    all_prices: dict,
    all_mcaps: dict,
    all_volumes: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    price_rows = []
    for symbol, price_dict in all_prices.items():
        for d, price in price_dict.items():
            price_rows.append({"date": d, "symbol": symbol, "price": price})
    prices_df = pd.DataFrame(price_rows)
    if not prices_df.empty:
        prices_df = prices_df.pivot(index="date", columns="symbol", values="price")
        prices_df.index = pd.to_datetime(prices_df.index).normalize()
        prices_df.index.name = "date"
        prices_df = prices_df.sort_index()

    mcap_rows = []
    for symbol, mcap_dict in all_mcaps.items():
        for d, mcap in mcap_dict.items():
            mcap_rows.append({"date": d, "symbol": symbol, "marketcap": mcap})
    mcaps_df = pd.DataFrame(mcap_rows)
    if not mcaps_df.empty:
        mcaps_df = mcaps_df.pivot(index="date", columns="symbol", values="marketcap")
        mcaps_df.index = pd.to_datetime(mcaps_df.index).normalize()
        mcaps_df.index.name = "date"
        mcaps_df = mcaps_df.sort_index()

    vol_rows = []
    for symbol, vol_dict in all_volumes.items():
        for d, vol in vol_dict.items():
            vol_rows.append({"date": d, "symbol": symbol, "volume": vol})
    volumes_df = pd.DataFrame(vol_rows)
    if not volumes_df.empty:
        volumes_df = volumes_df.pivot(index="date", columns="symbol", values="volume")
        volumes_df.index = pd.to_datetime(volumes_df.index).normalize()
        volumes_df.index.name = "date"
        volumes_df = volumes_df.sort_index()

    return prices_df, mcaps_df, volumes_df


def main() -> int:
    p = argparse.ArgumentParser(description="Backfill CoinGecko gap 2026-01-30 .. 2026-03-03")
    p.add_argument(
        "--allowlist",
        type=Path,
        default=REPO_ROOT / "data" / "perp_allowlist.csv",
        help="CSV with symbol, coingecko_id, ...",
    )
    p.add_argument(
        "--curated-dir",
        type=Path,
        default=REPO_ROOT / "data" / "curated",
        help="Wide parquet directory",
    )
    p.add_argument(
        "--data-lake-dir",
        type=Path,
        default=REPO_ROOT / "data" / "curated" / "data_lake",
        help="fact_*.parquet directory",
    )
    p.add_argument(
        "--source",
        type=str,
        default="coingecko",
        help="Source label in fact tables",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print plan only; no API calls and no file writes",
    )
    args = p.parse_args()

    append_to_fact_table = _load_incremental_helpers()

    allowlist_df = pd.read_csv(args.allowlist)
    if "symbol" not in allowlist_df.columns or "coingecko_id" not in allowlist_df.columns:
        print("Allowlist must contain symbol and coingecko_id columns.", file=sys.stderr)
        return 1

    print(f"Gap window (UTC calendar days): {GAP_START} .. {GAP_END}")
    print(f"Universe size: {len(allowlist_df)} rows from {args.allowlist}")
    if args.dry_run:
        print("[dry-run] Skipping fetch, merge, and writes.")
        return 0

    all_prices: dict = {}
    all_mcaps: dict = {}
    all_volumes: dict = {}

    total = len(allowlist_df)
    for idx, row in allowlist_df.iterrows():
        symbol = row["symbol"]
        cg_id = row["coingecko_id"]
        n = idx + 1
        print(f"[{n}/{total}] {symbol} ({cg_id}) ... ", flush=True)
        prices, mcaps, vols = fetch_price_history(cg_id, GAP_START, GAP_END)
        if prices:
            all_prices[symbol] = prices
            all_mcaps[symbol] = mcaps
            all_volumes[symbol] = vols
            print(f"ok ({len(prices)} days)")
        else:
            print("no data (see logs/ingestion_failures.jsonl if failed)")

    new_prices, new_mcaps, new_volumes = _wide_from_dicts(all_prices, all_mcaps, all_volumes)
    if new_prices.empty:
        print("No price data fetched; nothing to merge. Exiting.")
        return 1

    curated = args.curated_dir
    prices_path = curated / "prices_daily.parquet"
    mcaps_path = curated / "marketcap_daily.parquet"
    volumes_path = curated / "volume_daily.parquet"

    existing_prices = pd.read_parquet(prices_path) if prices_path.exists() else pd.DataFrame()
    existing_mcaps = pd.read_parquet(mcaps_path) if mcaps_path.exists() else pd.DataFrame()
    existing_volumes = pd.read_parquet(volumes_path) if volumes_path.exists() else pd.DataFrame()

    if not existing_prices.empty and not isinstance(existing_prices.index, pd.DatetimeIndex):
        existing_prices = existing_prices.copy()
        existing_prices.index = pd.to_datetime(existing_prices.index)
    if not existing_mcaps.empty and not isinstance(existing_mcaps.index, pd.DatetimeIndex):
        existing_mcaps = existing_mcaps.copy()
        existing_mcaps.index = pd.to_datetime(existing_mcaps.index)
    if not existing_volumes.empty and not isinstance(existing_volumes.index, pd.DatetimeIndex):
        existing_volumes = existing_volumes.copy()
        existing_volumes.index = pd.to_datetime(existing_volumes.index)

    merged_prices = existing_prices.combine_first(new_prices) if not existing_prices.empty else new_prices
    if new_mcaps.empty:
        merged_mcaps = existing_mcaps if not existing_mcaps.empty else new_mcaps
    else:
        merged_mcaps = existing_mcaps.combine_first(new_mcaps) if not existing_mcaps.empty else new_mcaps
    if new_volumes.empty:
        merged_volumes = existing_volumes if not existing_volumes.empty else new_volumes
    else:
        merged_volumes = existing_volumes.combine_first(new_volumes) if not existing_volumes.empty else new_volumes

    merged_prices = merged_prices.sort_index()
    merged_mcaps = merged_mcaps.sort_index()
    merged_volumes = merged_volumes.sort_index()

    merged_prices.to_parquet(prices_path)
    merged_mcaps.to_parquet(mcaps_path)
    merged_volumes.to_parquet(volumes_path)
    print(f"Wrote wide panels: {prices_path} ({len(merged_prices)} rows)")

    lake = args.data_lake_dir
    lake.mkdir(parents=True, exist_ok=True)

    fp_path = lake / "fact_price.parquet"
    fm_path = lake / "fact_marketcap.parquet"
    fv_path = lake / "fact_volume.parquet"

    existing_fp = pd.read_parquet(fp_path) if fp_path.exists() else pd.DataFrame()
    existing_fm = pd.read_parquet(fm_path) if fm_path.exists() else pd.DataFrame()
    existing_fv = pd.read_parquet(fv_path) if fv_path.exists() else pd.DataFrame()

    if not existing_fp.empty and "date" in existing_fp.columns:
        existing_fp = existing_fp.copy()
        existing_fp["date"] = pd.to_datetime(existing_fp["date"]).dt.date
    if not existing_fm.empty and "date" in existing_fm.columns:
        existing_fm = existing_fm.copy()
        existing_fm["date"] = pd.to_datetime(existing_fm["date"]).dt.date
    if not existing_fv.empty and "date" in existing_fv.columns:
        existing_fv = existing_fv.copy()
        existing_fv["date"] = pd.to_datetime(existing_fv["date"]).dt.date

    updated_fp, _ = append_to_fact_table(
        existing_fp, new_prices, FACT_PRICE_SCHEMA, args.source, "close"
    )
    if new_mcaps.empty:
        updated_fm = existing_fm
    else:
        updated_fm, _ = append_to_fact_table(
            existing_fm, new_mcaps, FACT_MARKETCAP_SCHEMA, args.source, "marketcap"
        )
    if new_volumes.empty:
        updated_fv = existing_fv
    else:
        updated_fv, _ = append_to_fact_table(
            existing_fv, new_volumes, FACT_VOLUME_SCHEMA, args.source, "volume"
        )

    updated_fp.to_parquet(fp_path, index=False)
    updated_fm.to_parquet(fm_path, index=False)
    updated_fv.to_parquet(fv_path, index=False)
    print(f"Updated fact tables under {lake}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
