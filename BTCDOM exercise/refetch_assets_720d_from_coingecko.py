#!/usr/bin/env python3
"""
Refetch ~720 days of price/marketcap/volume data for specific assets from CoinGecko
using your Pro (Basic) plan, and update:

- data/raw/prices_daily.parquet, marketcap_daily.parquet, volume_daily.parquet
- data/curated/prices_daily.parquet, marketcap_daily.parquet, volume_daily.parquet
- data/curated/data_lake/fact_price.parquet, fact_marketcap.parquet, fact_volume.parquet

Only the specified assets are touched; all other assets remain unchanged.
Existing data for those assets is preserved for dates outside the 720‑day window,
and replaced inside the window with the freshly fetched values.
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple, List

import pandas as pd
import requests


COINGECKO_PRO_BASE = "https://pro-api.coingecko.com/api/v3"


def _to_utc_ts(d: date, offset_days: int = 0) -> int:
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(days=offset_days)
    return int(dt.timestamp())


def fetch_asset_history_pro(
    coingecko_id: str,
    start_date: date,
    end_date: date,
    api_key: str,
    sleep_seconds: float = 0.15,
) -> Tuple[Dict[date, float], Dict[date, float], Dict[date, float]]:
    """
    Fetch daily prices, market caps, and volumes for a single asset from CoinGecko Pro.
    Returns (prices, market_caps, volumes) as dict[date -> float].
    """
    start_ts = _to_utc_ts(start_date, offset_days=-2)
    end_ts = _to_utc_ts(end_date, offset_days=1)

    url = f"{COINGECKO_PRO_BASE}/coins/{coingecko_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_ts,
        "to": end_ts,
    }
    headers = {"x-cg-pro-api-key": api_key}

    time.sleep(sleep_seconds)
    resp = requests.get(url, params=params, headers=headers, timeout=60, proxies={"http": None, "https": None})

    if resp.status_code != 200:
        from textwrap import shorten
        body = shorten(resp.text or "", width=200, placeholder="...")
        _safe_print(
            f"  [ERROR] Pro API for {coingecko_id} returned {resp.status_code}: {body}"
        )
        return {}, {}, {}

    data = resp.json()
    prices: Dict[date, float] = {}
    market_caps: Dict[date, float] = {}
    volumes: Dict[date, float] = {}

    for ts_ms, price in data.get("prices", []):
        d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
        prices[d] = float(price)
    for ts_ms, mcap in data.get("market_caps", []):
        d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
        market_caps[d] = float(mcap)
    for ts_ms, vol in data.get("total_volumes", []):
        d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
        volumes[d] = float(vol)

    return prices, market_caps, volumes


def update_wide_parquet_for_asset(
    path: Path,
    asset_id: str,
    series: pd.Series,
    value_name: str,
    dry_run: bool,
) -> None:
    """
    Update a single asset's column in a wide-format parquet file (dates x symbols).
    Only overwrites rows for dates where the new series has non‑NaN values.
    """
    if not path.exists():
        return
    df = pd.read_parquet(path)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    col = asset_id  # columns in prices_daily/marketcap_daily/volume_daily use asset symbols
    if col not in df.columns:
        # If the symbol column doesn't exist, create it so we can fill just this asset
        df[col] = float("nan")

    series = series.reindex(df.index)
    mask = series.notna()
    df.loc[mask, col] = series[mask].values

    if dry_run:
        print(f"  [DRY-RUN] Would update {path} for {asset_id} ({value_name}) on {mask.sum()} dates")
        return

    df.to_parquet(path)
    print(f"  Updated {path} for {asset_id} ({value_name}) on {mask.sum()} dates")


def update_fact_parquet_for_asset(
    path: Path,
    asset_id: str,
    new_rows: pd.DataFrame,
    value_column: str,
    dry_run: bool,
) -> None:
    """
    Replace fact table rows for `asset_id` within the dates present in new_rows,
    keeping:
      - other assets untouched
      - existing rows for this asset outside the new date set.
    """
    if not path.exists():
        return

    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])

    if new_rows.empty:
        print(f"  [WARN] No new rows for {asset_id} in {path}, skipping")
        return

    new_rows = new_rows.copy()
    new_rows["date"] = pd.to_datetime(new_rows["date"]).dt.normalize()
    new_dates = set(new_rows["date"].dt.date)

    mask_other = df["asset_id"] != asset_id
    mask_keep_existing = (df["asset_id"] == asset_id) & (~df["date"].dt.date.isin(new_dates))
    df_rest = df[mask_other | mask_keep_existing].copy()

    combined = pd.concat([df_rest, new_rows], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.normalize()

    if dry_run:
        print(
            f"  [DRY-RUN] Would update {path} for {asset_id} "
            f"({value_column}) on {len(new_rows)} dates"
        )
        return

    combined.to_parquet(path, index=False)
    print(
        f"  Updated {path} for {asset_id} ({value_column}) on "
        f"{len(new_rows)} dates (rest unchanged)"
    )


def _safe_print(text: str) -> None:
    """Print text safely on Windows consoles that may not support all Unicode."""
    try:
        print(text)
    except UnicodeEncodeError:
        safe = text.encode("ascii", "replace").decode("ascii")
        print(safe)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refetch ~720 days of data for specific assets from CoinGecko Pro and update parquet files.",
    )
    parser.add_argument(
        "--assets",
        nargs="+",
        default=["SOL", "TRX", "ZEC", "SKY"],
        help="Asset IDs to refetch (default: SOL TRX ZEC SKY). Ignored if --auto-short is set.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=720,
        help="Number of days of history to refetch ending today (default: 720).",
    )
    parser.add_argument(
        "--recent-days",
        type=int,
        default=None,
        help=(
            "When using --auto-short, look only at this many most recent days in "
            "fact_price when deciding which assets are short (e.g. 730 for ~2 years). "
            "If not set, auto-short uses total history."
        ),
    )
    parser.add_argument(
        "--recent-min-days",
        type=int,
        default=None,
        help=(
            "When using --auto-short with --recent-days, require at least this many "
            "unique dates in the recent window; assets below this are refetched. "
            "Example: 650 (out of 730) ~90%% coverage."
        ),
    )
    parser.add_argument(
        "--auto-short",
        action="store_true",
        help="Automatically detect assets in fact_price with fewer than DAYS unique dates and refetch only those.",
    )
    parser.add_argument(
        "--max-assets",
        type=int,
        default=None,
        help="Process at most this many assets (for batching).",
    )
    parser.add_argument(
        "--skip-first",
        type=int,
        default=0,
        help="Skip this many assets from the auto/explicit list (for batching).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write files, just report what would change.",
    )
    args = parser.parse_args()

    days = args.days

    api_key = (os.environ.get("COINGECKO_API_KEY") or "").strip()
    if not api_key:
        print("[ERROR] COINGECKO_API_KEY is not set in the environment.")
        return 1

    repo_root = Path(__file__).resolve().parent.parent
    data_root = repo_root / "data"
    curated_dir = data_root / "curated"
    raw_dir = data_root / "raw"
    data_lake_dir = curated_dir / "data_lake"

    # Map asset_id -> coingecko_id from dim_asset
    dim_asset_path = data_lake_dir / "dim_asset.parquet"
    if not dim_asset_path.exists():
        print(f"[ERROR] dim_asset.parquet not found at {dim_asset_path}")
        return 1
    dim_asset = pd.read_parquet(dim_asset_path)
    id_to_cg = {
        str(row["asset_id"]).upper(): str(row.get("coingecko_id") or "").strip()
        for _, row in dim_asset.iterrows()
    }
    # Fix up known coingecko_id mismatches in dim_asset for top coins
    overrides = {
        "SOL": "solana",
        "TRX": "tron",
        "ZEC": "zcash",
        "ETH": "ethereum",
    }
    # Determine which assets to refetch
    if args.auto_short:
        fact_price_path = data_lake_dir / "fact_price.parquet"
        if not fact_price_path.exists():
            print(f"[ERROR] fact_price.parquet not found at {fact_price_path}")
            return 1
        fp = pd.read_parquet(fact_price_path, columns=["asset_id", "date"])
        fp["date"] = pd.to_datetime(fp["date"])

        # Two modes:
        # - recent-window mode (preferred): use --recent-days/--recent-min-days
        # - legacy mode: total unique dates < days
        if args.recent_days is not None and args.recent_min_days is not None:
            end_recent = fp["date"].max().normalize()
            start_recent = end_recent - pd.Timedelta(days=args.recent_days)
            recent = fp[(fp["date"] >= start_recent) & (fp["date"] <= end_recent)]
            counts = recent.groupby("asset_id")["date"].nunique()
            short_assets = counts[counts < args.recent_min_days].index.tolist()
            _safe_print(
                f"Auto-detected {len(short_assets)} assets with < "
                f"{args.recent_min_days} unique dates in the last "
                f"{args.recent_days} days (recent window "
                f"{start_recent.date()} -> {end_recent.date()})."
            )
        else:
            counts = fp.groupby("asset_id")["date"].nunique()
            short_assets = counts[counts < days].index.tolist()
            _safe_print(
                f"Auto-detected {len(short_assets)} assets with < "
                f"{days} unique dates over full history in fact_price."
            )

        assets: List[str] = [a.upper() for a in short_assets]
    else:
        assets = [a.upper() for a in args.assets]

    # Sort and apply batching (skip_first / max_assets)
    assets = sorted(set(assets))
    if args.skip_first:
        assets = assets[args.skip_first :]
    if args.max_assets is not None:
        assets = assets[: args.max_assets]

    # Apply coingecko_id overrides for known cases
    for aid, cg in overrides.items():
        if aid in assets:
            id_to_cg[aid] = cg

    # Validate that every asset we plan to refetch has a coingecko_id
    missing_cg: List[str] = []
    for aid in assets:
        cg = id_to_cg.get(aid, "")
        if not cg:
            missing_cg.append(aid)
    if missing_cg:
        print(f"[WARN] Missing coingecko_id in dim_asset for {len(missing_cg)} assets; they will be skipped.")
        assets = [a for a in assets if a not in set(missing_cg)]
    if not assets:
        print("[INFO] No assets to refetch after filtering; exiting.")
        return 0

    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    print("=" * 70)
    print("Refetch ~720 days for selected assets from CoinGecko Pro")
    print("=" * 70)
    # Avoid Unicode/encoding issues in Windows console by printing counts only
    _safe_print(f"Number of assets to refetch: {len(assets)}")
    _safe_print(f"Date range: {start_date} -> {end_date} (approx {days} days)")
    print()

    # Fetch and apply per asset
    for aid in assets:
        cg_id = id_to_cg[aid]
        _safe_print(f"\n=== {aid} (CoinGecko id: {cg_id}) ===")
        try:
            prices_dict, mcaps_dict, vols_dict = fetch_asset_history_pro(
                coingecko_id=cg_id,
                start_date=start_date,
                end_date=end_date,
                api_key=api_key,
            )
            if not prices_dict:
                print(f"  [WARN] No data fetched for {aid}, skipping updates.")
                continue

            # Build Series for wide files (index = datetime)
            prices_s = pd.Series(prices_dict)
            prices_s.index = pd.to_datetime(prices_s.index)
            mcaps_s = pd.Series(mcaps_dict)
            mcaps_s.index = pd.to_datetime(mcaps_s.index)
            vols_s = pd.Series(vols_dict)
            vols_s.index = pd.to_datetime(vols_s.index)

            # Update raw + curated wide files
            for base_dir, label in [(raw_dir, "raw"), (curated_dir, "curated")]:
                if not base_dir.exists():
                    continue
                print(f"  Updating {label} wide files for {aid}...")
                update_wide_parquet_for_asset(
                    base_dir / "prices_daily.parquet",
                    aid,
                    prices_s,
                    "close",
                    args.dry_run,
                )
                update_wide_parquet_for_asset(
                    base_dir / "marketcap_daily.parquet",
                    aid,
                    mcaps_s,
                    "marketcap",
                    args.dry_run,
                )
                update_wide_parquet_for_asset(
                    base_dir / "volume_daily.parquet",
                    aid,
                    vols_s,
                    "volume",
                    args.dry_run,
                )

            # Build new fact rows
            prices_rows = [
                {"asset_id": aid, "date": d, "close": float(v), "source": "coingecko"}
                for d, v in prices_dict.items()
            ]
            mcap_rows = [
                {"asset_id": aid, "date": d, "marketcap": float(v), "source": "coingecko"}
                for d, v in mcaps_dict.items()
            ]
            vol_rows = [
                {"asset_id": aid, "date": d, "volume": float(v), "source": "coingecko"}
                for d, v in vols_dict.items()
            ]

            prices_df = pd.DataFrame(prices_rows)
            mcap_df = pd.DataFrame(mcap_rows)
            vol_df = pd.DataFrame(vol_rows)

            if not data_lake_dir.exists():
                print(f"  [WARN] Data lake dir not found: {data_lake_dir}, skipping fact tables.")
            else:
                print(f"  Updating data_lake fact tables for {aid}...")
                update_fact_parquet_for_asset(
                    data_lake_dir / "fact_price.parquet",
                    aid,
                    prices_df,
                    "close",
                    args.dry_run,
                )
                update_fact_parquet_for_asset(
                    data_lake_dir / "fact_marketcap.parquet",
                    aid,
                    mcap_df,
                    "marketcap",
                    args.dry_run,
                )
                update_fact_parquet_for_asset(
                    data_lake_dir / "fact_volume.parquet",
                    aid,
                    vol_df,
                    "volume",
                    args.dry_run,
                )
        except Exception as e:
            _safe_print(f"  [ERROR] Unexpected failure while processing {aid}: {e}")
            continue

    if args.dry_run:
        print("\n[DRY-RUN] No files were written. Run without --dry-run to apply changes.")
    else:
        print("\nDone. Selected assets updated with ~720 days of data.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

