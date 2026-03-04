#!/usr/bin/env python3
"""
Check if DOGE in fact_price.parquet has correct USD scale, and fix it if not.

DOGE/USD is typically in the range ~0.05–0.50 (or higher). If stored values are
e.g. 0.0001–0.01, they are likely in the wrong unit (e.g. 100x or 1000x too small).

Usage:
  python "BTCDOM exercise/check_and_fix_doge_scale.py" [--dry-run] [--reference-date YYYY-MM-DD] [--reference-price PRICE]
  python "BTCDOM exercise/check_and_fix_doge_scale.py" --reference-date 2025-01-15 --reference-price 0.35

If --reference-date and --reference-price are provided, we scale DOGE so that on that
date the close equals the reference price. Otherwise we use a heuristic: if median(DOGE) < 0.05,
we assume wrong scale and multiply by (0.25 / median) to put median near 0.25 USD.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

DOGE_ASSET_ID = "DOGE"
# Plausible USD range for DOGE (rough bounds)
DOGE_USD_MIN = 0.01
DOGE_USD_MAX_PLAUSIBLE = 2.0
# If median below this, we consider scale likely wrong
DOGE_MEDIAN_THRESHOLD = 0.05


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def check_doge_scale(df: pd.DataFrame) -> dict:
    """Check DOGE close series; return stats and whether scale looks wrong."""
    doge = df[df["asset_id"] == DOGE_ASSET_ID]
    if doge.empty:
        return {"present": False, "n_rows": 0, "scale_ok": None, "message": "DOGE not found in fact_price."}

    close = doge["close"]
    n = len(close)
    low, high = float(close.min()), float(close.max())
    median = float(close.median())

    # Heuristic: if median or max is far below typical USD, scale is wrong
    scale_ok = median >= DOGE_MEDIAN_THRESHOLD and high >= DOGE_USD_MIN
    if scale_ok:
        msg = f"DOGE scale looks like USD (median={median:.4f}, range=[{low:.6f}, {high:.4f}])."
    else:
        msg = (
            f"DOGE scale looks wrong (median={median:.6f}, range=[{low:.6f}, {high:.6f}]). "
            f"Typical USD range is ~{DOGE_USD_MIN}-{DOGE_USD_MAX_PLAUSIBLE}."
        )

    return {
        "present": True,
        "n_rows": n,
        "min": low,
        "max": high,
        "median": median,
        "scale_ok": scale_ok,
        "message": msg,
    }


def fix_doge_scale(
    df: pd.DataFrame,
    *,
    reference_date: str | None = None,
    reference_price: float | None = None,
    target_median: float = 0.25,
) -> pd.DataFrame:
    """
    Return a copy of df with DOGE close rescaled to USD.
    If reference_date and reference_price are set, scale so close on that date = reference_price.
    Else scale so median(DOGE) ≈ target_median (for typical wrong-scale data).
    """
    out = df.copy()
    mask = out["asset_id"] == DOGE_ASSET_ID
    if not mask.any():
        return out

    doge_close = out.loc[mask, "close"].astype(float)
    if reference_date is not None and reference_price is not None:
        ref_dt = pd.to_datetime(reference_date).date()
        if hasattr(out["date"].iloc[0], "date"):
            ref_val = out.loc[mask & (out["date"].astype("datetime64[ns]").dt.date == ref_dt), "close"]
        else:
            ref_val = out.loc[mask & (out["date"] == ref_dt), "close"]
        if ref_val.empty:
            raise ValueError(f"No DOGE row on reference date {reference_date}. Cannot scale.")
        current_on_ref = float(ref_val.iloc[0])
        if current_on_ref <= 0:
            raise ValueError(f"DOGE close on {reference_date} is {current_on_ref}; must be > 0.")
        factor = reference_price / current_on_ref
    else:
        med = float(doge_close.median())
        if med <= 0:
            raise ValueError("DOGE median close is <= 0; cannot infer scale.")
        factor = target_median / med

    out.loc[mask, "close"] = (out.loc[mask, "close"].astype(float) * factor).values
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check DOGE scale in fact_price and optionally fix it."
    )
    parser.add_argument(
        "--data-lake",
        type=Path,
        default=None,
        help="Path to data lake dir (default: data/curated/data_lake)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report scale check; do not write updated parquet.",
    )
    parser.add_argument(
        "--reference-date",
        type=str,
        default=None,
        help="Reference date YYYY-MM-DD for scaling (use with --reference-price).",
    )
    parser.add_argument(
        "--reference-price",
        type=float,
        default=None,
        help="DOGE USD price on reference date (e.g. 0.35).",
    )
    parser.add_argument(
        "--target-median",
        type=float,
        default=0.25,
        help="If not using reference, scale so DOGE median ≈ this (default 0.25).",
    )
    args = parser.parse_args()

    root = _repo_root()
    data_lake = args.data_lake or root / "data" / "curated" / "data_lake"
    data_lake = data_lake if data_lake.is_absolute() else root / data_lake
    path = data_lake / "fact_price.parquet"

    if not path.exists():
        print(f"[ERROR] Not found: {path}")
        return 1

    df = pd.read_parquet(path)
    stats = check_doge_scale(df)

    print("DOGE in fact_price:")
    print(f"  {stats['message']}")
    if stats["present"]:
        print(f"  Rows: {stats['n_rows']}, min={stats['min']:.6f}, max={stats['max']:.6f}, median={stats['median']:.6f}")
        print(f"  Scale OK: {stats['scale_ok']}")

    if not stats["present"]:
        return 0

    if stats["scale_ok"]:
        print("\nNo change needed.")
        return 0

    if args.dry_run:
        print("\n[DRY-RUN] Would fix scale (run without --dry-run to apply).")
        return 0

    use_ref = args.reference_date and args.reference_price is not None
    if use_ref and args.reference_price is not None:
        print(f"\nRescaling DOGE using reference: {args.reference_date} = {args.reference_price} USD")
    else:
        print(f"\nRescaling DOGE so median ~ {args.target_median} USD (heuristic)")

    try:
        out = fix_doge_scale(
            df,
            reference_date=args.reference_date,
            reference_price=args.reference_price,
            target_median=args.target_median,
        )
    except ValueError as e:
        print(f"[ERROR] {e}")
        return 1

    # Verify
    new_stats = check_doge_scale(out)
    print(f"  After: median={new_stats['median']:.4f}, range=[{new_stats['min']:.4f}, {new_stats['max']:.4f}]")
    print(f"  Scale OK: {new_stats['scale_ok']}")

    out.to_parquet(path, index=False)
    print(f"\nUpdated {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
