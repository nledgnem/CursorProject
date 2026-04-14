#!/usr/bin/env python3
"""
Daily panel: top market-cap coins that trade Binance USD-M perps, with spot pre-perp window.

- Universe: CoinGecko top markets (by rank) intersected with currently listed Binance USDT perpetuals.
- Spot/mcap: CoinGecko market_chart/range from min(panel_start, perp_onboard - 180d) through end.
- Funding: Binance /fapi/v1/fundingRate aggregated to mean per UTC day (native decimal).
- Open interest: Binance /futures/data/openInterestHist (1d) when available; else optional DB parquet; else NaN.

Default panel length is **2 years** (CoinGecko Basic rolling history). Override with ``--years`` or raise
``COINGECKO_MAX_HISTORY_LOOKBACK_DAYS`` for Analyst+.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

# Repo root on path; load .env before importing coingecko (it reads COINGECKO_API_KEY at import).
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dotenv import load_dotenv

_env_file = _REPO_ROOT / ".env"
if _env_file.is_file():
    # Prefer .env over empty inherited env vars (otherwise Pro key never applies)
    load_dotenv(dotenv_path=_env_file, override=True)

from src.providers.coingecko import (
    coingecko_v3_get,
    fetch_price_history,
    get_coingecko_api_key,
    to_utc_ts,
)

BINANCE_FAPI = "https://fapi.binance.com"
COINGECKO_PUBLIC_BASE = "https://api.coingecko.com/api/v3"
PRE_PERP_SPOT_BUFFER_DAYS = 180
DEFAULT_LOOKBACK_YEARS = 2
CG_SLEEP = 0.26
BINANCE_SLEEP = 0.08


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _coingecko_oldest_allowed_date(end_d: date) -> date:
    """
    CoinGecko Basic Pro only serves market_chart inside a rolling lookback from `end_d`.
    Analyst+ can raise via env (e.g. 15000). See CoinGecko pricing / error_code 10012.
    """
    raw = (os.environ.get("COINGECKO_MAX_HISTORY_LOOKBACK_DAYS") or "730").strip() or "730"
    try:
        days = max(30, int(raw))
    except ValueError:
        days = 730
    return end_d - timedelta(days=days)


def fetch_binance_exchange_info() -> Optional[Dict[str, Any]]:
    url = f"{BINANCE_FAPI}/fapi/v1/exchangeInfo"
    try:
        r = requests.get(url, timeout=60, proxies={"http": None, "https": None})
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] Binance exchangeInfo: {e}")
        return None


def parse_usdt_perp_universe(exchange_info: Dict[str, Any]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for s in exchange_info.get("symbols", []):
        if s.get("contractType") != "PERPETUAL" or s.get("quoteAsset") != "USDT":
            continue
        sym = s.get("symbol")
        base = (s.get("baseAsset") or "").upper()
        ob = s.get("onboardDate")
        if not sym or not base or ob is None:
            continue
        try:
            onboard = datetime.fromtimestamp(int(ob) / 1000.0, tz=timezone.utc).date()
        except (TypeError, ValueError, OSError):
            continue
        rows.append({"binance_symbol": sym, "base_asset": base, "onboard_date": onboard})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.drop_duplicates(subset=["binance_symbol"]).reset_index(drop=True)


def base_asset_lookup(perps: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Map CoinGecko-style base tickers to a perp row (handles 1000PEPE -> PEPE)."""
    lu: Dict[str, Dict[str, Any]] = {}
    for _, r in perps.iterrows():
        ba = r["base_asset"]
        lu[ba] = r.to_dict()
        if ba.startswith("1000") and len(ba) > 4:
            lu[ba[4:]] = r.to_dict()
        if ba.startswith("1M") and len(ba) > 2:
            lu[ba[2:]] = r.to_dict()
    return lu


def fetch_coingecko_markets_page(page: int, per_page: int = 250) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": min(per_page, 250),
        "page": page,
        "sparkline": "false",
    }
    r = coingecko_v3_get("/coins/markets", params)
    if r.status_code != 200:
        print(f"[ERROR] CoinGecko markets page {page}: {r.status_code} {r.text[:200]}")
        return []
    data = r.json()
    return data if isinstance(data, list) else []


def fetch_spot_mcap_history(
    coingecko_id: str,
    start_date: date,
    end_date: date,
) -> Tuple[Dict[date, float], Dict[date, float]]:
    """
    Daily spot close (last point per UTC day) and market cap from CoinGecko.
    Uses Pro API when COINGECKO_API_KEY is set in env; otherwise public demo endpoint (slower).
    """
    if get_coingecko_api_key():
        prices, mcaps, _vols = fetch_price_history(coingecko_id, start_date, end_date)
        return prices, mcaps

    url = f"{COINGECKO_PUBLIC_BASE}/coins/{coingecko_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": to_utc_ts(start_date, offset_days=-2),
        "to": to_utc_ts(end_date, offset_days=1),
    }
    delay = 6.0
    for attempt in range(5):
        try:
            resp = requests.get(url, params=params, timeout=45, proxies={"http": None, "https": None})
            if resp.status_code == 429:
                time.sleep(delay)
                delay *= 1.8
                continue
            if resp.status_code != 200:
                print(f"[WARN] CoinGecko public range {coingecko_id}: {resp.status_code} {resp.text[:120]}")
                time.sleep(delay)
                return {}, {}
            data = resp.json()
            prices: Dict[date, float] = {}
            market_caps: Dict[date, float] = {}
            for ts_ms, price in data.get("prices", []):
                d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
                prices[d] = float(price)
            for ts_ms, mcap in data.get("market_caps", []):
                d = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
                market_caps[d] = float(mcap)
            time.sleep(delay)
            return prices, market_caps
        except Exception as e:
            print(f"[WARN] CoinGecko public range request error {coingecko_id}: {e}")
            time.sleep(delay)
            delay *= 1.8
    return {}, {}


def build_top_perp_universe(target_n: int, perps: pd.DataFrame) -> pd.DataFrame:
    """
    Walk CoinGecko market-cap ranking; keep first `target_n` coins that have a Binance USDT perp.
    """
    lu = base_asset_lookup(perps)
    picked: List[Dict[str, Any]] = []
    page = 1
    while len(picked) < target_n:
        markets = fetch_coingecko_markets_page(page)
        time.sleep(6.0 if not get_coingecko_api_key() else CG_SLEEP)
        if not markets:
            break
        for coin in markets:
            if len(picked) >= target_n:
                break
            sym = (coin.get("symbol") or "").upper()
            if sym not in lu:
                continue
            row = lu[sym]
            mcap = coin.get("market_cap")
            if mcap is None:
                mcap = 0
            picked.append(
                {
                    "ticker": sym,
                    "coingecko_id": coin["id"],
                    "name": coin.get("name"),
                    "coingecko_market_cap_rank": coin.get("market_cap_rank"),
                    "snapshot_market_cap_usd": float(mcap) if mcap else np.nan,
                    "binance_symbol": row["binance_symbol"],
                    "onboard_date": row["onboard_date"],
                }
            )
        if len(markets) < 250:
            break
        page += 1
    return pd.DataFrame(picked)


def _ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def fetch_funding_daily(
    binance_symbol: str,
    start_d: date,
    end_d: date,
) -> Dict[date, float]:
    """Mean of native 8h funding decimals per UTC calendar day."""
    url = f"{BINANCE_FAPI}/fapi/v1/fundingRate"
    start_ms = _ms(start_d)
    end_ms = _ms(end_d + timedelta(days=1))
    buckets: Dict[date, List[float]] = {}
    cursor = start_ms
    while cursor < end_ms:
        params = {"symbol": binance_symbol, "startTime": cursor, "limit": 1000}
        try:
            r = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
            r.raise_for_status()
            chunk = r.json()
        except Exception:
            break
        if not chunk:
            break
        for item in chunk:
            ft = int(item["fundingTime"])
            rate = float(item["fundingRate"])
            dd = datetime.fromtimestamp(ft / 1000.0, tz=timezone.utc).date()
            buckets.setdefault(dd, []).append(rate)
        last_t = int(chunk[-1]["fundingTime"])
        cursor = last_t + 1
        time.sleep(BINANCE_SLEEP)
        if len(chunk) < 1000:
            break
    return {d: float(np.mean(v)) for d, v in buckets.items()}


def fetch_open_interest_daily(
    binance_symbol: str,
    start_d: date,
    end_d: date,
) -> Dict[date, float]:
    """
    Binance USD-M open interest history (1d). Uses sumOpenInterestValue as USD notional when present.
    """
    url = f"{BINANCE_FAPI}/futures/data/openInterestHist"
    out: Dict[date, float] = {}
    # API returns up to 500 points; paginate by advancing start
    cursor = _ms(start_d)
    end_ms = _ms(end_d + timedelta(days=1))
    while cursor < end_ms:
        params = {
            "symbol": binance_symbol,
            "period": "1d",
            "startTime": cursor,
            "endTime": end_ms,
            "limit": 500,
        }
        try:
            r = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
            r.raise_for_status()
            chunk = r.json()
        except Exception:
            break
        if not isinstance(chunk, list) or not chunk:
            break
        for item in chunk:
            ts = int(item["timestamp"])
            dd = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).date()
            val = item.get("sumOpenInterestValue")
            if val is not None:
                out[dd] = float(val)
            else:
                # fallback: contracts * price not available; skip
                pass
        last_t = int(chunk[-1]["timestamp"])
        cursor = last_t + 24 * 60 * 60 * 1000
        time.sleep(BINANCE_SLEEP)
        if len(chunk) < 500:
            break
    return out


def load_optional_oi_parquet(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"[WARN] Could not read OI parquet {path}: {e}")
        return None
    cols = {c.lower(): c for c in df.columns}
    need = {"date", "open_interest_usd"}
    if not need.issubset(set(cols.keys())):
        print(f"[WARN] OI parquet missing columns date/open_interest_usd: {list(df.columns)}")
        return None
    dcol = cols["date"]
    ocol = cols["open_interest_usd"]
    asset_col = cols.get("asset_id") or cols.get("symbol") or cols.get("ticker")
    if not asset_col:
        print("[WARN] OI parquet needs asset_id, symbol, or ticker")
        return None
    out = df[[asset_col, dcol, ocol]].copy()
    out.columns = ["asset", "date", "open_interest_usd"]
    out["asset"] = out["asset"].astype(str).str.upper()
    out["date"] = pd.to_datetime(out["date"]).dt.date
    return out


def generate_panel_data(
    *,
    top_n: int = 200,
    lookback_years: float = DEFAULT_LOOKBACK_YEARS,
    end_date: Optional[date] = None,
    output_csv: Path = Path("single_coin_panel.csv"),
    skip_funding: bool = False,
    skip_oi_api: bool = False,
    oi_parquet_path: Optional[Path] = None,
) -> pd.DataFrame:
    target_columns = [
        "decision_date_utc",
        "ticker",
        "market_cap_rank",
        "market_cap_usd",
        "close_price_usd",
        "is_perp_active",
        "funding_rate_8h_decimal",
        "open_interest_usd",
    ]

    end_date = end_date or _utc_today()
    panel_start = end_date - timedelta(days=int(round(365.25 * lookback_years)))

    print("Fetching Binance USD-M perpetual universe...")
    ex = fetch_binance_exchange_info()
    if not ex:
        raise RuntimeError("Binance exchangeInfo unavailable")
    perps = parse_usdt_perp_universe(ex)
    if perps.empty:
        raise RuntimeError("No Binance USDT perpetuals parsed")

    print(f"Building top-{top_n} CoinGecko x Binance perp universe (market-cap order)...")
    universe = build_top_perp_universe(top_n, perps)
    if universe.empty:
        raise RuntimeError("Universe empty - check CoinGecko API access / network")

    db_oi: Optional[pd.DataFrame] = None
    default_oi = _REPO_ROOT / "data" / "curated" / "data_lake" / "fact_open_interest.parquet"
    oi_path = oi_parquet_path if oi_parquet_path is not None else default_oi
    db_oi = load_optional_oi_parquet(oi_path)
    if db_oi is not None:
        print(f"Loaded OI reference from {oi_path} ({len(db_oi)} rows)")

    all_frames: List[pd.DataFrame] = []
    for idx, (_, row) in enumerate(universe.iterrows(), start=1):
        ticker = row["ticker"]
        cg_id = row["coingecko_id"]
        bsym = row["binance_symbol"]
        onboard: date = row["onboard_date"]
        print(f"[{idx}/{len(universe)}] {ticker} ({cg_id}) {bsym} onboard={onboard}")

        spot_fetch_start = min(panel_start, onboard - timedelta(days=PRE_PERP_SPOT_BUFFER_DAYS))
        cg_floor = _coingecko_oldest_allowed_date(end_date)
        if spot_fetch_start < cg_floor:
            print(
                f"  [INFO] CoinGecko history clamp: request start {spot_fetch_start} -> {cg_floor} "
                "(set COINGECKO_MAX_HISTORY_LOOKBACK_DAYS for Analyst+ / longer lookback)"
            )
            spot_fetch_start = cg_floor
        prices, mcaps = fetch_spot_mcap_history(cg_id, spot_fetch_start, end_date)
        time.sleep(CG_SLEEP if get_coingecko_api_key() else 0.0)

        days = pd.date_range(panel_start, end_date, freq="D")
        recs: List[Dict[str, Any]] = []
        for ts in days:
            d = ts.date()
            px = prices.get(d)
            mcap = mcaps.get(d)
            recs.append(
                {
                    "decision_date_utc": d,
                    "ticker": ticker,
                    "market_cap_usd": mcap,
                    "close_price_usd": px,
                }
            )
        cdf = pd.DataFrame.from_records(recs)
        cdf["is_perp_active"] = (cdf["decision_date_utc"] >= onboard).astype(int)

        if not skip_funding:
            if cdf["is_perp_active"].max() == 1:
                fund_start = max(onboard, panel_start)
                fdaily = fetch_funding_daily(bsym, fund_start, end_date)
                cdf["funding_rate_8h_decimal"] = cdf["decision_date_utc"].map(
                    lambda x: fdaily.get(x) if x >= onboard else np.nan
                )
            else:
                cdf["funding_rate_8h_decimal"] = np.nan
        else:
            cdf["funding_rate_8h_decimal"] = np.nan

        if cdf["is_perp_active"].max() == 1:
            oi_start = max(onboard, panel_start)
            oi_series: Dict[date, float] = {}
            if not skip_oi_api:
                oi_series = fetch_open_interest_daily(bsym, oi_start, end_date)
            cdf["open_interest_usd"] = cdf["decision_date_utc"].map(
                lambda x: oi_series.get(x) if x >= onboard else np.nan
            )
            if db_oi is not None:
                sub = db_oi[db_oi["asset"] == ticker]
                if not sub.empty:
                    m_oi = dict(zip(sub["date"], sub["open_interest_usd"]))
                    mask = cdf["open_interest_usd"].isna() & (cdf["decision_date_utc"] >= onboard)
                    cdf.loc[mask, "open_interest_usd"] = cdf.loc[mask, "decision_date_utc"].map(
                        lambda d, mm=m_oi: mm.get(d, np.nan)
                    )
        else:
            cdf["open_interest_usd"] = np.nan

        # Native decimal funding check: is_perp_active==0 => NaN funding
        cdf.loc[cdf["is_perp_active"] == 0, "funding_rate_8h_decimal"] = np.nan

        all_frames.append(cdf)

    df = pd.concat(all_frames, ignore_index=True)
    df["decision_date_utc"] = pd.to_datetime(df["decision_date_utc"])

    # Daily cross-sectional rank within universe (1 = largest mcap that day)
    df["market_cap_rank"] = df.groupby("decision_date_utc")["market_cap_usd"].rank(
        ascending=False, method="min"
    )
    df["market_cap_rank"] = df["market_cap_rank"].astype("Int64")

    df = df.sort_values(by=["ticker", "decision_date_utc"]).reset_index(drop=True)
    df["decision_date_utc"] = df["decision_date_utc"].dt.strftime("%Y-%m-%d")

    out = df[target_columns]
    output_csv = Path(output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False)
    print(f"SUCCESS: wrote {len(out)} rows to {output_csv.resolve()}")
    return out


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(line_buffering=True)
            sys.stderr.reconfigure(line_buffering=True)
        except Exception:
            pass

    p = argparse.ArgumentParser(description="Generate single_coin_panel.csv (top mcap and Binance perps)")
    p.add_argument("--top-n", type=int, default=200)
    p.add_argument(
        "--years",
        type=float,
        default=DEFAULT_LOOKBACK_YEARS,
        help="Panel lookback in years (default: 2, matches Basic plan history)",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=_REPO_ROOT / "single_coin_panel.csv",
        help="Output CSV path",
    )
    p.add_argument("--skip-funding", action="store_true", help="Skip Binance funding fetches (faster smoke test)")
    p.add_argument("--skip-oi-api", action="store_true", help="Skip Binance OI history API")
    p.add_argument(
        "--oi-parquet",
        type=Path,
        default=None,
        help="Optional fact_open_interest-style parquet (default: data lake path if present)",
    )
    args = p.parse_args()

    if not get_coingecko_api_key():
        print(
            "[WARN] COINGECKO_API_KEY is empty: using CoinGecko public API (strict rate limits). "
            "Set COINGECKO_API_KEY in .env for Pro throughput (see src/providers/coingecko.py).",
            file=sys.stderr,
        )

    generate_panel_data(
        top_n=args.top_n,
        lookback_years=args.years,
        output_csv=args.output,
        skip_funding=args.skip_funding,
        skip_oi_api=args.skip_oi_api,
        oi_parquet_path=args.oi_parquet,
    )


if __name__ == "__main__":
    main()
