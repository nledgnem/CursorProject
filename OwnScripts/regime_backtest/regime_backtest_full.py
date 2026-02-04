#!/usr/bin/env python3
"""
End-to-end regime backtest:

1. Calls `regime_monitor.py historical N_DAYS` to (re)build regime_history.csv.
2. Loads regime_history.csv and builds a synthetic +BTC / -alts LS book.
3. Backtests:
   - always-on LS
   - bucket-based sizing
   - score-based sizing
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Import symbol universe from existing regime_monitor.py
# ---------------------------------------------------------------------------

sys.path.append(str(Path(__file__).resolve().parent))

from regime_monitor import COINGECKO_IDS, ALT_SYMBOLS  # type: ignore

COINGECKO_BASE = "https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY = "CG-RhUWZY31TcDFBPfj4GWwcsMS"
DEFAULT_HISTORY_FILE = "regime_history.csv"


# =============================== HELPERS ====================================

def to_utc_ts(d: date, offset_days: int = 0) -> int:
    dt_obj = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) + timedelta(days=offset_days)
    return int(dt_obj.timestamp())


def fetch_prices_range_for_symbol(
    symbol: str,
    *,
    start_date: date,
    end_date: date,
    sleep_seconds: float = 8.0,
    max_retries: int = 5,
) -> Dict[date, float]:
    """
    Fetch daily USD prices for one symbol between start_date and end_date (inclusive)
    using CoinGecko's /market_chart/range endpoint, with basic rate-limit handling.

    Returns: dict[date -> close_price].

    If the coin is not found (404) or repeated errors occur, we log and return {}
    so the alt can be skipped from the basket.
    """
    cg_id = COINGECKO_IDS[symbol]
    url = f"{COINGECKO_BASE}/coins/{cg_id}/market_chart/range"

    start_ts = to_utc_ts(start_date, offset_days=-2)
    end_ts = to_utc_ts(end_date, offset_days=1)

    params = {
        "vs_currency": "usd",
        "from": start_ts,
        "to": end_ts,
        "x_cg_pro_api_key": COINGECKO_API_KEY,
    }

    delay = sleep_seconds
    for attempt in range(1, max_retries + 1):
        print(
            f"[CG] Fetching prices for {symbol} ({cg_id}) "
            f"from {start_date} to {end_date}... (attempt {attempt})"
        )
        try:
            resp = requests.get(url, params=params, timeout=20)
        except Exception as e:
            print(f"[ERROR] Request error for {symbol}: {e}. Skipping this symbol.")
            time.sleep(sleep_seconds)
            return {}

        if resp.status_code == 200:
            data = resp.json()
            prices = data.get("prices", [])
            if not prices:
                print(f"[WARN] CoinGecko returned no price points for {symbol}. Skipping.")
                time.sleep(sleep_seconds)
                return {}

            out: Dict[date, float] = {}
            for ts_ms, price in prices:
                try:
                    ts = ts_ms / 1000.0
                    d = datetime.fromtimestamp(ts, tz=timezone.utc).date()
                    out[d] = float(price)
                except Exception:
                    continue

            time.sleep(sleep_seconds)
            return out

        if resp.status_code == 404:
            print(f"[WARN] CoinGecko has no data for {symbol} ({cg_id}) (404). Skipping this symbol.")
            time.sleep(sleep_seconds)
            return {}

        if resp.status_code == 429:
            print(f"[WARN] Rate limited by CoinGecko for {symbol} (429). Backing off for {delay:.1f}s...")
            time.sleep(delay)
            delay *= 2.0
            continue

        print(f"[ERROR] CoinGecko error for {symbol}: {resp.status_code} {resp.text}. Skipping this symbol.")
        time.sleep(sleep_seconds)
        return {}

    print(f"[ERROR] Failed to fetch prices for {symbol} after {max_retries} attempts. Skipping this symbol.")
    return {}


def compute_daily_returns(
    dates: List[date],
    price_by_date: Dict[date, float],
) -> Dict[date, Optional[float]]:
    out: Dict[date, Optional[float]] = {}
    prev_price: Optional[float] = None

    for d in sorted(dates):
        price = price_by_date.get(d)
        if price is None or prev_price is None or prev_price == 0:
            out[d] = None
        else:
            out[d] = price / prev_price - 1.0
        if price is not None:
            prev_price = price
    return out


def compute_max_drawdown(equity: pd.Series) -> float:
    running_max = equity.cummax()
    dd = equity / running_max - 1.0
    return float(dd.min())


def annualized_sharpe(returns: pd.Series, *, periods_per_year: int = 252) -> float:
    r = returns.dropna()
    if r.std() == 0 or len(r) < 5:
        return 0.0
    return float(np.sqrt(periods_per_year) * r.mean() / r.std())


# =========================== CORE BACKTEST LOGIC ============================

def build_ls_returns(
    start_date: date,
    end_date: date,
    *,
    universe_alts: Optional[List[str]] = None,
) -> pd.DataFrame:
    if universe_alts is None:
        universe_alts = [s for s in ALT_SYMBOLS]

    symbols = ["BTC"] + list(universe_alts)
    prices_by_symbol: Dict[str, Dict[date, float]] = {}

    for sym in symbols:
        prices_by_symbol[sym] = fetch_prices_range_for_symbol(
            sym, start_date=start_date, end_date=end_date
        )

    if not prices_by_symbol["BTC"]:
        raise RuntimeError("No BTC price data fetched from CoinGecko; cannot build LS returns.")

    ref_dates = sorted(prices_by_symbol["BTC"].keys())
    ref_dates = [d for d in ref_dates if d >= start_date and d <= (end_date + timedelta(days=1))]

    rets_by_symbol: Dict[str, Dict[date, Optional[float]]] = {}
    for sym in symbols:
        rets_by_symbol[sym] = compute_daily_returns(ref_dates, prices_by_symbol.get(sym, {}))

    rows = []
    for d in ref_dates:
        ret_btc = rets_by_symbol["BTC"].get(d)
        alt_daily: List[float] = []
        for sym in universe_alts:
            sym_rets = rets_by_symbol.get(sym)
            if not sym_rets:
                continue
            r = sym_rets.get(d)
            if r is not None:
                alt_daily.append(r)
        ret_alts = float(np.mean(alt_daily)) if alt_daily else None

        if ret_btc is None or ret_alts is None:
            ret_ls = None
        else:
            ret_ls = ret_btc - ret_alts

        rows.append(
            {
                "date": d,
                "ret_btc": ret_btc,
                "ret_alts": ret_alts,
                "ret_ls": ret_ls,
            }
        )

    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    return df


def run_backtest(
    history_file: Path,
    *,
    output_csv: Optional[Path] = None,
    days: Optional[int] = None,
) -> None:
    reg = pd.read_csv(history_file)

    if "date_iso" not in reg.columns:
        raise ValueError(f"{history_file} must contain a 'date_iso' column.")

    reg["date"] = (
        pd.to_datetime(reg["date_iso"], format="ISO8601", utc=True, errors="coerce")
          .dt.date
    )

    needed_cols = {"regime_score", "bucket"}
    missing = needed_cols.difference(reg.columns)
    if missing:
        raise ValueError(f"{history_file} is missing columns: {missing}")

    reg = reg.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    
    # If days parameter is provided, use it to calculate date range from today
    # Otherwise, use the date range from the CSV file
    if days is not None:
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        print(f"[INFO] Fetching prices for {days} days: {start_date} to {end_date}")
        print(f"[INFO] Regime history file has {len(reg)} rows from {reg['date'].min()} to {reg['date'].max()}")
    else:
        start_date = reg["date"].min()
        end_date = reg["date"].max()
        print(f"[INFO] Regime history from {start_date} to {end_date} ({len(reg)} rows).")

    ls = build_ls_returns(start_date, end_date)

    df = pd.merge(reg, ls, on="date", how="inner")
    df = df.sort_values("date").reset_index(drop=True)

    df["ret_ls_fwd_1d"] = df["ret_ls"].shift(-1)
    df = df.iloc[:-1].copy()

    bucket_weights = {
        "GREEN": 1.5,
        "YELLOWGREEN": 1.0,
        "YELLOW": 0.5,
        "ORANGE": 0.0,
        "RED": 0.0,
    }
    df["w_bucket"] = df["bucket"].map(bucket_weights).fillna(0.0)

    def score_to_weight(score: float) -> float:
        if score <= 50:
            return 0.0
        if score >= 90:
            return 1.5
        return 1.5 * (score - 50.0) / 40.0

    df["w_score"] = df["regime_score"].apply(score_to_weight)

    df["ret_always_on"] = df["ret_ls_fwd_1d"]
    df["ret_bucket_sized"] = df["w_bucket"] * df["ret_ls_fwd_1d"]
    df["ret_score_sized"] = df["w_score"] * df["ret_ls_fwd_1d"]

    for col in ["ret_always_on", "ret_bucket_sized", "ret_score_sized"]:
        eq_col = "equity_" + col.replace("ret_", "")
        df[eq_col] = (1.0 + df[col].fillna(0.0)).cumprod()

    print("\n=== Strategy Summary (daily returns, synthetic LS book) ===")
    for name in ["ret_always_on", "ret_bucket_sized", "ret_score_sized"]:
        sharpe = annualized_sharpe(df[name])
        total_ret = float((1.0 + df[name].fillna(0.0)).prod() - 1.0)
        max_dd = compute_max_drawdown(df["equity_" + name.replace("ret_", "")])
        print(
            f"{name:20s} | "
            f"Ann. Sharpe: {sharpe:5.2f} | "
            f"Total return: {total_ret*100:6.2f}% | "
            f"Max DD: {max_dd*100:6.2f}%"
        )

    print("\n=== Forward 1d LS returns by regime bucket ===")
    bucket_stats = (
        df.groupby("bucket")["ret_ls_fwd_1d"]
        .agg(["mean", "std", "count"])
        .sort_index()
    )
    print(bucket_stats.to_string(float_format=lambda x: f"{x: .4f}"))

    if output_csv is not None:
        df.to_csv(output_csv, index=False)
        print(f"\n[INFO] Full backtest data saved to {output_csv}")


# =========================== REGIME REFRESH STEP ============================

def refresh_regime_history(days: int, history_file: Path) -> None:
    """
    Run regime_monitor.py in historical mode for N days to regenerate history_file.
    """
    print(f"[INFO] Regenerating regime history for last {days} days...")
    cmd = [sys.executable, "regime_monitor.py", "historical", str(days)]
    subprocess.run(cmd, check=True)
    if not history_file.exists():
        raise FileNotFoundError(
            f"Expected {history_file} to be created by regime_monitor.py, but it was not found."
        )
    print(f"[INFO] Regime history refreshed in {history_file}")


# ================================ CLI =======================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Refresh regime history (1–2y) and backtest BTC-long / Alt-short regime monitor."
    )
    p.add_argument(
        "--days",
        type=int,
        default=720,
        help="Number of days of history to rebuild via regime_monitor.py (default: 720 ≈ 2 years - 10 days).",
    )
    p.add_argument(
        "--history-file",
        type=Path,
        default=Path(DEFAULT_HISTORY_FILE),
        help="Path to regime_history.csv produced by regime_monitor.py",
    )
    p.add_argument(
        "--output-csv",
        type=Path,
        default=Path("regime_backtest_output.csv"),
        help="Optional path to write full backtest dataframe.",
    )
    p.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip calling regime_monitor.py and just backtest existing history_file.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not args.skip_refresh:
        refresh_regime_history(args.days, args.history_file)
    else:
        print("[INFO] Skipping regime history refresh; using existing file.")

    run_backtest(args.history_file, output_csv=args.output_csv, days=args.days)


if __name__ == "__main__":
    main()
