#!/usr/bin/env python3
"""
Refetch DOGE price (and market cap, volume) from CoinGecko and update only the DOGE
column/rows in all relevant parquet files (raw, curated, data_lake). Other assets are
left untouched.

Usage:
  python "BTCDOM exercise/refetch_doge_from_coingecko.py"
  python "BTCDOM exercise/refetch_doge_from_coingecko.py" --dry-run
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# Repo root and paths
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

DOGE_SYMBOL = "DOGE"
COINGECKO_ID_DOGE = "dogecoin"
SOURCE_LABEL = "coingecko"

# Free API (no key); use when pro API returns 401 or no data
COINGECKO_FREE_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_PRO_BASE = "https://pro-api.coingecko.com/api/v3"


def _to_utc_ts(d, offset_days: int = 0) -> int:
    from datetime import timedelta
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(days=offset_days)
    return int(dt.timestamp())


def fetch_doge_from_coingecko(
    start_date,
    end_date,
) -> tuple[dict, dict, dict]:
    """
    Fetch DOGE price/mcap/volume from CoinGecko.

    Order of attempts:
    1) Pro API (Basic plan) via pro-api.coingecko.com + x-cg-pro-api-key
    2) Basic/Demo API via api.coingecko.com + x-cg-demo-api-key
       - If a 400 error occurs, log the full body and, if it's a time-range error,
         fall back to chunked requests (<=365 days per chunk) and merge results.
    3) Public/free API (no key, last ~90 days only) as a final fallback.
    """
    import os
    from datetime import timedelta

    api_key = (os.environ.get("COINGECKO_API_KEY") or "").strip()

    def _accumulate(data, prices, mcaps, vols):
        for ts_ms, price in data.get("prices", []):
            d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
            prices[d] = float(price)
        for ts_ms, mcap in data.get("market_caps", []):
            d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
            mcaps[d] = float(mcap)
        for ts_ms, vol in data.get("total_volumes", []):
            d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
            vols[d] = float(vol)

    # 1) Pro API (Basic plan uses Pro base)
    if api_key:
        start_ts = _to_utc_ts(start_date, offset_days=-2)
        end_ts = _to_utc_ts(end_date, offset_days=1)
        url = f"{COINGECKO_PRO_BASE}/coins/{COINGECKO_ID_DOGE}/market_chart/range"
        params = {"vs_currency": "usd", "from": start_ts, "to": end_ts}
        headers = {"x-cg-pro-api-key": api_key}
        try:
            time.sleep(0.15)
            resp = requests.get(url, params=params, headers=headers, timeout=60, proxies={"http": None, "https": None})
            if resp.status_code == 200:
                data = resp.json()
                prices, mcaps, vols = {}, {}, {}
                _accumulate(data, prices, mcaps, vols)
                if prices:
                    print(f"  Pro API: received {len(prices)} days.")
                    return prices, mcaps, vols
            else:
                print(f"  Pro API: {resp.status_code} {resp.text[:300]}")
        except Exception as e:
            print(f"  Pro API error: {e}.")

    # 2) Basic/Demo API (api.coingecko.com) – try full range, then chunked on 400
    if api_key:
        def _fetch_basic_chunk(chunk_start, chunk_end) -> tuple[dict, dict, dict]:
            c_start_ts = _to_utc_ts(chunk_start, offset_days=-2)
            c_end_ts = _to_utc_ts(chunk_end, offset_days=1)
            url = f"{COINGECKO_FREE_BASE}/coins/{COINGECKO_ID_DOGE}/market_chart/range"
            params = {"vs_currency": "usd", "from": c_start_ts, "to": c_end_ts}
            headers = {"x-cg-demo-api-key": api_key}
            time.sleep(0.2)
            resp = requests.get(url, params=params, headers=headers, timeout=60, proxies={"http": None, "https": None})
            return resp

        # First, try full range once so we can log any 400 body clearly.
        resp = _fetch_basic_chunk(start_date, end_date)
        if resp.status_code == 200:
            data = resp.json()
            prices, mcaps, vols = {}, {}, {}
            _accumulate(data, prices, mcaps, vols)
            if prices:
                print(f"  Basic/Demo API: received {len(prices)} days (single call).")
                return prices, mcaps, vols
        elif resp.status_code == 400:
            body = resp.text or ""
            print(f"  Basic/Demo API 400 body (full): {body}")
            # If this looks like a time-range error, fall back to chunked requests.
            if "time range" in body.lower() or "365" in body or "10012" in body:
                print("  Basic/Demo API: splitting into <=365-day chunks...")
                prices, mcaps, vols = {}, {}, {}
                cur_start = start_date
                while cur_start <= end_date:
                    cur_end = min(cur_start + timedelta(days=364), end_date)
                    chunk_resp = _fetch_basic_chunk(cur_start, cur_end)
                    if chunk_resp.status_code != 200:
                        print(
                            f"  Basic/Demo chunk {cur_start} -> {cur_end} failed: "
                            f"{chunk_resp.status_code} {chunk_resp.text[:300]}"
                        )
                        break
                    data = chunk_resp.json()
                    _accumulate(data, prices, mcaps, vols)
                    cur_start = cur_end + timedelta(days=1)
                if prices:
                    print(f"  Basic/Demo API (chunked): received {len(prices)} days.")
                    return prices, mcaps, vols
        else:
            print(f"  Basic/Demo API: {resp.status_code} {resp.text[:300]}")

    # 3) Fallback: project provider helper (uses whatever env key/config it has)
    try:
        from src.providers.coingecko import fetch_price_history
        prices_dict, mcaps_dict, vols_dict = fetch_price_history(
            COINGECKO_ID_DOGE, start_date, end_date
        )
        if prices_dict:
            return prices_dict, mcaps_dict, vols_dict
    except Exception:
        pass

    # 4) Final fallback: free API (no key, last ~90 days)
    from datetime import timedelta
    free_end = end_date
    free_start = free_end - timedelta(days=89)  # 90 days to stay under common limit
    if (end_date - start_date).days > 90:
        print("  Free API: limiting to last 90 days; set COINGECKO_API_KEY for full range.")
    start_ts = _to_utc_ts(free_start, offset_days=-2)
    end_ts = _to_utc_ts(free_end, offset_days=1)
    url = f"{COINGECKO_FREE_BASE}/coins/{COINGECKO_ID_DOGE}/market_chart/range"
    params = {"vs_currency": "usd", "from": start_ts, "to": end_ts}
    print("  Using CoinGecko free API (no key). Rate limits may apply.")
    time.sleep(1.5)  # be nice to free API
    resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
    if resp.status_code != 200:
        raise RuntimeError(f"CoinGecko free API returned {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    prices, mcaps, vols = {}, {}, {}
    _accumulate(data, prices, mcaps, vols)
    return prices, mcaps, vols


def get_date_range_from_data(repo_root: Path) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Infer start/end date from existing price data (curated or data_lake)."""
    curated_prices = repo_root / "data" / "curated" / "prices_daily.parquet"
    fact_price = repo_root / "data" / "curated" / "data_lake" / "fact_price.parquet"
    if curated_prices.exists():
        df = pd.read_parquet(curated_prices)
        idx = df.index
        if hasattr(idx, "min"):
            return pd.Timestamp(idx.min()), pd.Timestamp(idx.max())
    if fact_price.exists():
        df = pd.read_parquet(fact_price)
        df["date"] = pd.to_datetime(df["date"])
        return df["date"].min(), df["date"].max()
    raise FileNotFoundError("No prices_daily.parquet or fact_price.parquet found to infer date range.")


def update_wide_parquet(
    path: Path,
    new_series: pd.Series,
    value_name: str,
    dry_run: bool,
) -> None:
    """Update only the DOGE column in a wide-format parquet (index=date, columns=symbols).
    Only overwrites DOGE for dates present in new_series; other dates are left unchanged.
    """
    if not path.exists():
        return
    df = pd.read_parquet(path)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    # Ensure DOGE column exists (e.g. if it was missing)
    if DOGE_SYMBOL not in df.columns:
        df[DOGE_SYMBOL] = float("nan")
    # Overwrite DOGE only for dates we have in new_series (avoid filling others with NaN)
    new_series = new_series.reindex(df.index)
    mask = new_series.notna()
    df.loc[mask, DOGE_SYMBOL] = new_series[mask].values
    if dry_run:
        print(f"  [DRY-RUN] Would update {path} DOGE column ({value_name})")
        return
    df.to_parquet(path)
    print(f"  Updated {path} (DOGE {value_name} only)")


def update_fact_parquet(
    path: Path,
    new_rows: pd.DataFrame,
    asset_id_col: str,
    dry_run: bool,
) -> None:
    """
    Update only DOGE rows in a fact table: replace DOGE data for refetched dates,
    keep existing DOGE rows for dates not in new_rows. All other assets unchanged.
    """
    if not path.exists():
        return
    df = pd.read_parquet(path)
    # Dates we have new data for (to replace)
    df["date"] = pd.to_datetime(df["date"])
    new_dates = set(pd.to_datetime(new_rows["date"]).dt.date) if len(new_rows) > 0 else set()
    # Keep non-DOGE rows and DOGE rows whose date is NOT in the refetched set
    mask_other = df[asset_id_col] != DOGE_SYMBOL
    mask_doge_keep = (df[asset_id_col] == DOGE_SYMBOL) & (~df["date"].dt.date.isin(new_dates))
    df_rest = df[mask_other | mask_doge_keep].copy()
    combined = pd.concat([df_rest, new_rows], ignore_index=True)
    # Normalize date column so Parquet can serialize (mixed date/datetime causes ArrowTypeError)
    combined["date"] = pd.to_datetime(combined["date"]).dt.normalize()
    if dry_run:
        print(f"  [DRY-RUN] Would update {path} (DOGE rows for {len(new_rows)} dates)")
        return
    combined.to_parquet(path, index=False)
    print(f"  Updated {path} (DOGE rows for {len(new_rows)} dates, rest unchanged)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refetch DOGE from CoinGecko and update only DOGE in raw/curated/data_lake parquet files."
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not write files.")
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date YYYY-MM-DD (default: from existing data).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date YYYY-MM-DD (default: from existing data).",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Refetch DOGE from CoinGecko and update parquet files (DOGE only)")
    print("=" * 60)

    # 1) Infer date range
    if args.start and args.end:
        start_date = pd.to_datetime(args.start).date()
        end_date = pd.to_datetime(args.end).date()
        print(f"Date range: {start_date} to {end_date} (from args)")
    else:
        start_ts, end_ts = get_date_range_from_data(REPO_ROOT)
        start_date = start_ts.date()
        end_date = end_ts.date()
        print(f"Date range: {start_date} to {end_date} (from existing data)")

    # 2) Fetch from CoinGecko (pro API if key set, else free API)
    print(f"\nFetching DOGE ({COINGECKO_ID_DOGE}) from CoinGecko...")
    prices_dict, mcaps_dict, vols_dict = fetch_doge_from_coingecko(start_date, end_date)
    if not prices_dict:
        print("[ERROR] No price data returned from CoinGecko. Check API key and rate limits.")
        return 1
    print(f"  Received {len(prices_dict)} days of price, {len(mcaps_dict)} mcap, {len(vols_dict)} volume")

    # Build series (index = date)
    prices_series = pd.Series(prices_dict)
    prices_series.index = pd.to_datetime(prices_series.index)
    mcaps_series = pd.Series(mcaps_dict)
    mcaps_series.index = pd.to_datetime(mcaps_series.index)
    vols_series = pd.Series(vols_dict)
    vols_series.index = pd.to_datetime(vols_series.index)

    # 3) Update wide-format files (raw and curated)
    raw_dir = REPO_ROOT / "data" / "raw"
    curated_dir = REPO_ROOT / "data" / "curated"

    for dir_path, label in [(raw_dir, "raw"), (curated_dir, "curated")]:
        if not dir_path.exists():
            continue
        print(f"\nUpdating {label} wide parquet files (DOGE column only)...")
        update_wide_parquet(
            dir_path / "prices_daily.parquet",
            prices_series,
            "close",
            args.dry_run,
        )
        update_wide_parquet(
            dir_path / "marketcap_daily.parquet",
            mcaps_series,
            "marketcap",
            args.dry_run,
        )
        update_wide_parquet(
            dir_path / "volume_daily.parquet",
            vols_series,
            "volume",
            args.dry_run,
        )

    # 4) Update data_lake fact tables (DOGE rows only)
    data_lake_dir = curated_dir / "data_lake"
    if not data_lake_dir.exists():
        print(f"\n[SKIP] Data lake not found: {data_lake_dir}")
    else:
        print("\nUpdating data_lake fact tables (DOGE rows only)...")

        # fact_price: asset_id, date, close, source
        fact_price_rows = []
        for d, price in prices_dict.items():
            fact_price_rows.append({
                "asset_id": DOGE_SYMBOL,
                "date": d,
                "close": float(price),
                "source": SOURCE_LABEL,
            })
        fact_price_new = pd.DataFrame(fact_price_rows)
        update_fact_parquet(
            data_lake_dir / "fact_price.parquet",
            fact_price_new,
            "asset_id",
            args.dry_run,
        )

        # fact_marketcap
        fact_mcap_rows = []
        for d, mcap in mcaps_dict.items():
            fact_mcap_rows.append({
                "asset_id": DOGE_SYMBOL,
                "date": d,
                "marketcap": float(mcap),
                "source": SOURCE_LABEL,
            })
        fact_mcap_new = pd.DataFrame(fact_mcap_rows)
        update_fact_parquet(
            data_lake_dir / "fact_marketcap.parquet",
            fact_mcap_new,
            "asset_id",
            args.dry_run,
        )

        # fact_volume
        fact_vol_rows = []
        for d, vol in vols_dict.items():
            fact_vol_rows.append({
                "asset_id": DOGE_SYMBOL,
                "date": d,
                "volume": float(vol),
                "source": SOURCE_LABEL,
            })
        fact_vol_new = pd.DataFrame(fact_vol_rows)
        update_fact_parquet(
            data_lake_dir / "fact_volume.parquet",
            fact_vol_new,
            "asset_id",
            args.dry_run,
        )

    if args.dry_run:
        print("\n[DRY-RUN] No files written. Run without --dry-run to apply.")
    else:
        print("\nDone. DOGE data updated from CoinGecko in all relevant parquet files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
