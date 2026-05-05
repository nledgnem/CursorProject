#!/usr/bin/env python3
"""One-shot re-fetch of writer-race-affected canonical slugs.

Reads outputs/writer_race_canonical_slug_mapping.csv to get the 138 canonical
slugs (139 mapped minus USC, which was dropped from the allowlist entirely).
For each canonical slug, calls /coins/{id}/market_chart/range over the
configured date range, then upserts the results into the three bronze fact
tables: fact_marketcap, fact_price, fact_volume.

Idempotent: re-running overwrites existing rows on (asset_id, date) match.

Designed for ONE-SHOT incident remediation (Phase D of the writer-race fix,
DATA_LAKE_CONTEXT.md §9 entry 0 / §13). Not for production pipeline use --
the production fetcher is src/providers/coingecko.py::download_all_coins.

Defaults match the Phase D incident scope:
    --start-date 2024-05-06  (max reachable on Basic tier from 2026-05-05)
    --end-date   2026-05-05  (verification anchor date)

Usage on Render shell:
    nohup python scripts/refetch_writer_race_affected.py \\
        --start-date 2024-05-06 \\
        --end-date 2026-05-05 \\
        > /tmp/writer_race_refetch.log 2>&1 &

Monitor:
    tail -f /tmp/writer_race_refetch.log
    ps -p <PID>

Exit codes:
    0 -- all 138 slugs processed; <=5% errors
    1 -- >5% slugs errored OR script crashed (no upsert occurred)
    2 -- nothing to do (mapping CSV empty after filters)
"""

from __future__ import annotations

import argparse
import ast
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from repo_paths import data_lake_root  # noqa: E402
from src.providers.coingecko import fetch_price_history  # noqa: E402


def _log(msg: str) -> None:
    print(f"[{datetime.utcnow().isoformat(timespec='seconds')}Z] {msg}", flush=True)


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _load_canonical_mapping(mapping_path: Path) -> pd.DataFrame:
    """Load the canonical slug mapping; filter out USC (dropped from allowlist)."""
    if not mapping_path.exists():
        raise FileNotFoundError(
            f"Canonical mapping not found at {mapping_path}. "
            f"Expected outputs/writer_race_canonical_slug_mapping.csv from Phase A."
        )
    df = pd.read_csv(mapping_path, encoding="utf-8")
    required = {"symbol", "canonical_slug"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Mapping CSV missing required columns: {missing}")

    # Filter out USC (dropped from allowlist entirely in Phase C)
    df = df[df["symbol"].astype(str).str.upper() != "USC"].copy()
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["canonical_slug"] = df["canonical_slug"].astype(str)
    return df.reset_index(drop=True)


def _build_fact_rows(symbol: str, date_to_value: dict, value_col: str) -> pd.DataFrame:
    """Convert {date: float} dict to a fact-table-shaped DataFrame.

    Schema matches existing bronze fact tables:
      fact_marketcap: [asset_id, date, marketcap, source]
      fact_price:     [asset_id, date, close,     source]
      fact_volume:    [asset_id, date, volume,    source]
    """
    if not date_to_value:
        return pd.DataFrame(columns=["asset_id", "date", value_col, "source"])
    rows = [
        {"asset_id": symbol, "date": d, value_col: float(v), "source": "coingecko"}
        for d, v in date_to_value.items()
    ]
    return pd.DataFrame(rows)


def _atomic_upsert_parquet(
    parquet_path: Path,
    new_df: pd.DataFrame,
    value_col: str,
    affected_symbols: set,
    start_date: date,
    end_date: date,
) -> tuple[int, int, int]:
    """Atomically replace rows for affected (asset_id, date) keys.

    Strategy:
      1. Read existing parquet.
      2. Drop rows where asset_id IN affected_symbols AND date IN [start, end].
      3. Concat with new_df.
      4. Sort by (asset_id, date).
      5. Write to .tmp file, then atomic rename.

    Returns (n_dropped, n_inserted, n_total_after).
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Target parquet not found: {parquet_path}")

    existing = pd.read_parquet(parquet_path)
    n_before = len(existing)

    # Schema sanity check
    expected_cols = {"asset_id", "date", value_col, "source"}
    missing_cols = expected_cols - set(existing.columns)
    if missing_cols:
        raise ValueError(
            f"{parquet_path.name} missing expected columns {missing_cols}; "
            f"actual: {list(existing.columns)}"
        )

    # Drop affected rows in the target date range
    mask_affected = (
        existing["asset_id"].astype(str).str.upper().isin(affected_symbols)
        & (existing["date"] >= start_date)
        & (existing["date"] <= end_date)
    )
    n_dropped = int(mask_affected.sum())
    kept = existing[~mask_affected]

    # Concat new rows
    if not new_df.empty:
        # Ensure type compatibility
        new_df = new_df[["asset_id", "date", value_col, "source"]].copy()
        new_df["asset_id"] = new_df["asset_id"].astype(str)
    combined = pd.concat([kept, new_df], ignore_index=True)
    combined = combined.sort_values(["asset_id", "date"]).reset_index(drop=True)

    n_inserted = len(new_df)
    n_total = len(combined)

    # Atomic write: write to .tmp then replace
    tmp_path = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
    combined.to_parquet(tmp_path, index=False)
    tmp_path.replace(parquet_path)

    return n_dropped, n_inserted, n_total


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start-date", type=_parse_date, default=date(2024, 5, 6),
                   help="Re-fetch start (YYYY-MM-DD). Default: 2024-05-06 (max Basic-tier reach from 2026-05-05).")
    p.add_argument("--end-date", type=_parse_date, default=date(2026, 5, 5),
                   help="Re-fetch end (YYYY-MM-DD). Default: 2026-05-05 (Phase D verification anchor).")
    p.add_argument("--mapping-path", type=Path,
                   default=_REPO_ROOT / "outputs" / "writer_race_canonical_slug_mapping.csv",
                   help="Path to canonical slug mapping CSV.")
    p.add_argument("--sleep-seconds", type=float, default=2.0,
                   help="Sleep between API calls (default 2.0s = ~30 calls/min, Basic-tier safe).")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch and report counts but do NOT write to fact tables.")
    args = p.parse_args()

    if args.start_date > args.end_date:
        _log(f"[ERROR] start-date {args.start_date} > end-date {args.end_date}")
        return 1

    # Auth check — fail fast if API key isn't set
    if not os.environ.get("COINGECKO_API_KEY", "").strip():
        _log("[ERROR] COINGECKO_API_KEY env var is not set. This script must run on Render or another env with the key configured.")
        return 1

    _log(f"=== Phase D writer-race re-fetch ===")
    _log(f"Date range: {args.start_date} to {args.end_date} ({(args.end_date - args.start_date).days + 1} days)")
    _log(f"Mapping:    {args.mapping_path}")
    _log(f"Dry-run:    {args.dry_run}")
    _log(f"Sleep:      {args.sleep_seconds}s between calls")

    mapping = _load_canonical_mapping(args.mapping_path)
    _log(f"Loaded {len(mapping)} canonical slugs to re-fetch (USC excluded; dropped from allowlist)")

    if len(mapping) == 0:
        _log("[ERROR] No slugs to process after USC filter.")
        return 2

    # Per-table accumulators (one big DataFrame per fact table, written once at end)
    accum: dict[str, list[pd.DataFrame]] = {
        "marketcap": [],
        "close":     [],
        "volume":    [],
    }

    affected_symbols: set = set()
    n_ok = 0
    n_err = 0
    errors: list[tuple[str, str, str]] = []  # (symbol, slug, error)

    t_start = time.time()
    for idx, row in mapping.iterrows():
        sym = str(row["symbol"]).upper()
        slug = str(row["canonical_slug"])
        affected_symbols.add(sym)
        try:
            prices, mcaps, vols = fetch_price_history(
                slug, args.start_date, args.end_date,
                sleep_seconds=args.sleep_seconds,
            )
        except Exception as e:
            n_err += 1
            errors.append((sym, slug, f"{type(e).__name__}: {e}"))
            _log(f"[{idx+1:>3}/{len(mapping)}] {sym:<10} ({slug}): EXCEPTION {type(e).__name__}: {e}")
            continue

        n_prices = len(prices)
        n_mcaps = len(mcaps)
        n_vols = len(vols)
        if n_prices == 0 and n_mcaps == 0 and n_vols == 0:
            n_err += 1
            errors.append((sym, slug, "empty_response"))
            _log(f"[{idx+1:>3}/{len(mapping)}] {sym:<10} ({slug}): EMPTY response (skipping)")
            continue

        accum["marketcap"].append(_build_fact_rows(sym, mcaps, "marketcap"))
        accum["close"].append(_build_fact_rows(sym, prices, "close"))
        accum["volume"].append(_build_fact_rows(sym, vols, "volume"))
        n_ok += 1
        _log(f"[{idx+1:>3}/{len(mapping)}] {sym:<10} ({slug}): {n_prices} prices, {n_mcaps} mcaps, {n_vols} vols")

    t_fetch = time.time() - t_start
    _log(f"=== Fetch phase done in {t_fetch:.1f}s. ok={n_ok}, err={n_err} ===")

    err_pct = n_err / max(1, len(mapping)) * 100.0
    if err_pct > 5.0:
        _log(f"[ERROR] {n_err} of {len(mapping)} slugs errored ({err_pct:.1f}% > 5% threshold). Halting before any write.")
        for sym, slug, err in errors[:20]:
            _log(f"  err: {sym} ({slug}): {err}")
        return 1

    if args.dry_run:
        _log(f"[DRY-RUN] Would have upserted {n_ok} slug results into 3 fact tables. Skipping writes.")
        return 0

    # Build per-table DataFrames and upsert atomically
    dl = data_lake_root()
    fact_specs = [
        ("fact_marketcap.parquet", "marketcap", accum["marketcap"]),
        ("fact_price.parquet",     "close",     accum["close"]),
        ("fact_volume.parquet",    "volume",    accum["volume"]),
    ]

    upsert_results = []
    for fname, value_col, dfs in fact_specs:
        path = dl / fname
        _log(f"--- Upserting {fname} ({value_col}) ---")
        if not dfs:
            _log(f"  no data accumulated for {value_col}; skipping")
            upsert_results.append((fname, 0, 0, None))
            continue
        new_df = pd.concat(dfs, ignore_index=True)
        try:
            n_dropped, n_inserted, n_total = _atomic_upsert_parquet(
                path, new_df, value_col, affected_symbols,
                args.start_date, args.end_date,
            )
            _log(f"  dropped={n_dropped:,} inserted={n_inserted:,} total_after={n_total:,}")
            upsert_results.append((fname, n_dropped, n_inserted, n_total))
        except Exception as e:
            _log(f"[ERROR] upsert failed for {fname}: {type(e).__name__}: {e}")
            traceback.print_exc()
            return 1

    t_total = time.time() - t_start
    _log(f"=== Phase D re-fetch complete in {t_total:.1f}s ===")
    _log(f"Slugs processed: {n_ok} ok, {n_err} err")
    for fname, n_dropped, n_inserted, n_total in upsert_results:
        _log(f"  {fname}: dropped {n_dropped:,} stale rows, inserted {n_inserted:,} fresh rows, total {n_total:,}")

    if errors:
        _log(f"--- Errors ({len(errors)}) ---")
        for sym, slug, err in errors:
            _log(f"  {sym} ({slug}): {err}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
