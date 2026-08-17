"""
Data acquisition and caching for the BTC trend-agreement study.

TIMESTAMP CONVENTION (applies to every series produced here)
------------------------------------------------------------
All series are DAILY UTC CLOSES.

  * Coinbase Exchange `/products/{id}/candles?granularity=86400` returns
    buckets that open at 00:00:00 UTC. The `close` field is the last trade
    price inside that UTC calendar day. We label the bucket with its UTC
    calendar date, so P(t) is the 23:59:59 UTC price of date t.
  * Binance `/api/v3/klines?interval=1d` daily klines also open at 00:00 UTC
    and close at 23:59:59.999 UTC of the same date. Same labelling.
  * Deribit `/public/get_volatility_index_data?resolution=1D` returns OHLC of
    the DVOL index per UTC day; we take the CLOSE and label it with the UTC
    date of the bucket open.

So for every asset and for DVOL, the value stamped on date t is known at
23:59:59 UTC on date t and NOT before. Any use of a t-stamped value to
influence a return that begins at or before 23:59:59 UTC on t would be
look-ahead; the strategy code enforces a strictly positive lag.

MISSING-DATA TREATMENT
----------------------
Exchange candle feeds occasionally drop a day (venue outage, no trades in an
illiquid early-listing session). We reindex each series onto a complete daily
calendar between its first and last observation and forward-fill gaps, then
record the number and dates of filled days in the provenance manifest. Forward
fill uses only past information, so it introduces no look-ahead. Leading NaNs
before the first real observation are dropped, never back-filled.

Reindexing to a complete calendar is what makes `shift(30)` mean "30 calendar
days ago" rather than "30 observations ago".
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

from config import ASSETS, CACHE_DIR, HISTORY_START

COINBASE_URL = "https://api.exchange.coinbase.com/products/{product}/candles"
BINANCE_URL = "https://api.binance.com/api/v3/klines"
DERIBIT_URL = "https://www.deribit.com/api/v2/public/get_volatility_index_data"

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "btc-trend-agreement-research/1.0"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------
# Raw venue fetchers
# --------------------------------------------------------------------------
def _fetch_coinbase_daily(product: str, start: str) -> pd.DataFrame:
    """Daily UTC candles from Coinbase Exchange, paginated (300-row API cap)."""
    rows: list[list] = []
    cur = pd.Timestamp(start, tz="UTC")
    end_all = pd.Timestamp(_utc_now().date(), tz="UTC") + pd.Timedelta(days=1)
    step = pd.Timedelta(days=280)

    while cur < end_all:
        chunk_end = min(cur + step, end_all)
        for attempt in range(5):
            try:
                r = _SESSION.get(
                    COINBASE_URL.format(product=product),
                    params={
                        "granularity": 86400,
                        "start": cur.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "end": chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    },
                    timeout=30,
                )
                if r.status_code == 429:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                r.raise_for_status()
                rows.extend(r.json())
                break
            except requests.RequestException:
                if attempt == 4:
                    raise
                time.sleep(1.5 * (attempt + 1))
        cur = chunk_end
        time.sleep(0.35)  # stay well inside the 10 req/s public limit

    if not rows:
        return pd.DataFrame(columns=["date", "close"])

    df = pd.DataFrame(rows, columns=["ts", "low", "high", "open", "close", "volume"])
    df["date"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_localize(None).dt.normalize()
    df = df[["date", "close"]].drop_duplicates(subset="date").sort_values("date")
    return df.reset_index(drop=True)


def _fetch_binance_daily(symbol: str, start: str) -> pd.DataFrame:
    """Daily UTC klines from Binance spot, paginated (1000-row API cap)."""
    rows: list[list] = []
    cur_ms = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
    end_ms = int(pd.Timestamp(_utc_now()).timestamp() * 1000)

    while cur_ms < end_ms:
        for attempt in range(5):
            try:
                r = _SESSION.get(
                    BINANCE_URL,
                    params={
                        "symbol": symbol,
                        "interval": "1d",
                        "startTime": cur_ms,
                        "limit": 1000,
                    },
                    timeout=30,
                )
                if r.status_code in (429, 418):
                    time.sleep(2.0 * (attempt + 1))
                    continue
                r.raise_for_status()
                batch = r.json()
                break
            except requests.RequestException:
                if attempt == 4:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if not batch:
            break
        rows.extend(batch)
        last_open = batch[-1][0]
        if last_open <= cur_ms and len(batch) < 2:
            break
        cur_ms = last_open + 86_400_000
        time.sleep(0.25)

    if not rows:
        return pd.DataFrame(columns=["date", "close"])

    df = pd.DataFrame(rows).iloc[:, :5]
    df.columns = ["ts", "open", "high", "low", "close"]
    df["date"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    df["close"] = df["close"].astype(float)
    df = df[["date", "close"]].drop_duplicates(subset="date").sort_values("date")
    # Binance daily klines include the still-open current-day bar. Drop any bar
    # whose UTC day has not closed yet -- it is not a settled close.
    today_utc = pd.Timestamp(_utc_now().date())
    df = df[df["date"] < today_utc]
    return df.reset_index(drop=True)


def _fetch_deribit_dvol(currency: str = "BTC") -> pd.DataFrame:
    """Daily DVOL index closes from Deribit's public volatility-index endpoint.

    DVOL history begins 2021-03-24 for BTC. We do not fabricate anything
    earlier: the DVOL overlay is evaluated only on the genuine sample.
    """
    rows: list[list] = []
    cur = pd.Timestamp("2021-01-01", tz="UTC")
    end_all = pd.Timestamp(_utc_now())
    step = pd.Timedelta(days=500)

    while cur < end_all:
        chunk_end = min(cur + step, end_all)
        for attempt in range(5):
            try:
                r = _SESSION.get(
                    DERIBIT_URL,
                    params={
                        "currency": currency,
                        "start_timestamp": int(cur.timestamp() * 1000),
                        "end_timestamp": int(chunk_end.timestamp() * 1000),
                        "resolution": "1D",
                    },
                    timeout=30,
                )
                r.raise_for_status()
                payload = r.json()
                rows.extend(payload.get("result", {}).get("data", []))
                break
            except requests.RequestException:
                if attempt == 4:
                    raise
                time.sleep(1.5 * (attempt + 1))
        cur = chunk_end
        time.sleep(0.35)

    if not rows:
        return pd.DataFrame(columns=["date", "dvol"])

    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close"])
    df["date"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    df = df.rename(columns={"close": "dvol"})[["date", "dvol"]]
    df = df.drop_duplicates(subset="date").sort_values("date")
    today_utc = pd.Timestamp(_utc_now().date())
    return df[df["date"] < today_utc].reset_index(drop=True)


_FETCHERS = {"coinbase": _fetch_coinbase_daily, "binance": _fetch_binance_daily}


# --------------------------------------------------------------------------
# Cache layer
# --------------------------------------------------------------------------
def _cached(name: str, fetch_fn, refresh: bool = False) -> pd.DataFrame:
    path = CACHE_DIR / f"{name}.csv"
    if path.exists() and not refresh:
        df = pd.read_csv(path, parse_dates=["date"])
        return df
    df = fetch_fn()
    df.to_csv(path, index=False)
    return df


def _regularise(df: pd.DataFrame, value_col: str) -> tuple[pd.Series, dict]:
    """Reindex to a complete daily calendar and forward-fill, reporting gaps."""
    s = df.set_index("date")[value_col].astype(float).sort_index()
    s = s[~s.index.duplicated(keep="last")]
    full = pd.date_range(s.index.min(), s.index.max(), freq="D")
    missing = full.difference(s.index)
    s = s.reindex(full).ffill()
    prov = {
        "start": str(s.index.min().date()),
        "end": str(s.index.max().date()),
        "n_days": int(len(s)),
        "n_filled_days": int(len(missing)),
        "filled_dates": [str(d.date()) for d in missing[:50]],
    }
    return s, prov


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------
def load_prices(refresh: bool = False) -> tuple[pd.DataFrame, dict]:
    """Return a wide DataFrame of daily UTC closes plus a provenance manifest."""
    series: dict[str, pd.Series] = {}
    provenance: dict[str, dict] = {}

    for asset, spec in ASSETS.items():
        venue, ticker = spec["primary"]
        raw = _cached(
            f"{venue}_{ticker.replace('-', '')}_daily",
            lambda v=venue, t=ticker: _FETCHERS[v](t, HISTORY_START),
            refresh=refresh,
        )
        s, prov = _regularise(raw, "close")
        series[asset] = s
        prov.update({"venue": venue, "instrument": ticker, "role": "primary",
                     "frequency": "daily", "timezone": "UTC close"})
        provenance[asset] = prov

        cvenue, cticker = spec["check"]
        craw = _cached(
            f"{cvenue}_{cticker.replace('-', '')}_daily",
            lambda v=cvenue, t=cticker: _FETCHERS[v](t, HISTORY_START),
            refresh=refresh,
        )
        if len(craw):
            cs, _ = _regularise(craw, "close")
            series[f"{asset}_check"] = cs
            overlap = s.index.intersection(cs.index)
            if len(overlap) > 30:
                a = s.loc[overlap].pct_change().dropna()
                b = cs.loc[overlap].pct_change().dropna()
                idx = a.index.intersection(b.index)
                provenance[asset]["crosscheck"] = {
                    "venue": cvenue,
                    "instrument": cticker,
                    "overlap_days": int(len(overlap)),
                    "daily_return_corr": float(a.loc[idx].corr(b.loc[idx])),
                    "median_abs_level_diff_pct": float(
                        ((s.loc[overlap] / cs.loc[overlap] - 1).abs().median()) * 100
                    ),
                }

    px = pd.DataFrame(series).sort_index()
    return px, provenance


def load_dvol(refresh: bool = False) -> tuple[pd.Series, dict]:
    raw = _cached("deribit_btc_dvol_daily", _fetch_deribit_dvol, refresh=refresh)
    s, prov = _regularise(raw, "dvol")
    prov.update({"venue": "deribit", "instrument": "BTC DVOL index",
                 "endpoint": "public/get_volatility_index_data",
                 "frequency": "daily", "timezone": "UTC close"})
    return s, prov


def load_lake_btc_crosscheck() -> Optional[pd.Series]:
    """BTC closes from the project's own curated lake, for an independent check.

    The lake's CoinGecko-sourced history only reaches back ~2 years on the
    current API tier (documented in data_dictionary.yaml) and the local
    checkout is stale, so it cannot be the primary source -- but the overlap
    is still a useful independent sanity check on the exchange series.
    """
    path = RESEARCH_LAKE_PATH
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["asset_id", "date", "close"])
    btc = df[df["asset_id"].str.upper() == "BTC"].copy()
    if btc.empty:
        return None
    btc["date"] = pd.to_datetime(btc["date"])
    return btc.set_index("date")["close"].sort_index()


RESEARCH_LAKE_PATH = (
    CACHE_DIR.parent.parent.parent / "data" / "curated" / "data_lake" / "fact_price.parquet"
)


def write_provenance(price_prov: dict, dvol_prov: dict, lake_note: dict) -> None:
    manifest = {
        "generated_utc": _utc_now().isoformat(timespec="seconds"),
        "timestamp_convention": "daily UTC closes; value stamped on date t is "
                                "known at 23:59:59 UTC on t",
        "missing_data_treatment": "reindex to complete daily calendar, "
                                  "forward-fill (past-information only)",
        "prices": price_prov,
        "dvol": dvol_prov,
        "lake_crosscheck": lake_note,
    }
    (CACHE_DIR / "provenance.json").write_text(json.dumps(manifest, indent=2))
