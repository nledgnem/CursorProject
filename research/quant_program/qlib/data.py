"""Cached fetchers for exchange and vendor data.

Conventions
-----------
* Every dataset is cached as parquet under research/quant_program/cache/<source>/<dataset>/<key>.parquet.
* Timestamps are tz-naive UTC. Bars are stamped by OPEN time (column 'ts').
  A daily bar stamped 2026-08-19 closes at 2026-08-19 23:59:59 UTC.
* Funding prints are stamped by settlement time ('ts'); rates are DECIMAL per interval.
* Incremental: re-running fetches only data after the last cached timestamp.

Sources (free unless noted):
  Binance USD-M fapi/v1 (klines, fundingRate, premiumIndexKlines, exchangeInfo),
  data.binance.vision (daily 'metrics' archive: OI, long/short, taker ratio),
  Hyperliquid info API (fundingHistory), Bybit v5 (funding), CoinGlass v4 (key in .env).
"""

from __future__ import annotations

import io
import os
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests

from qlib.paths import CACHE, REPO

_S = requests.Session()
_S.headers.update({"User-Agent": "quant-program-research/1.0"})
_LIMIT = threading.Semaphore(6)
FAPI = "https://fapi.binance.com"
SPOT = "https://api.binance.com"


def _ms(ts) -> int:
    return int(pd.Timestamp(ts).tz_localize("UTC").timestamp() * 1000) if pd.Timestamp(ts).tzinfo is None \
        else int(pd.Timestamp(ts).timestamp() * 1000)


def _get(url, params=None, method="GET", body=None, headers=None, retries=6):
    for i in range(retries):
        with _LIMIT:
            try:
                r = (_S.post(url, json=body, timeout=40, headers=headers) if method == "POST"
                     else _S.get(url, params=params, timeout=40, headers=headers))
            except requests.RequestException:
                time.sleep(1.5 * (i + 1))
                continue
        if r.status_code == 200:
            return r.json()
        if r.status_code in (418, 429, 500, 502, 503, 504):
            time.sleep(3.0 * (i + 1))
            continue
        if r.status_code in (400, 404):
            return None
        time.sleep(1.5 * (i + 1))
    return None


def _cache_path(source: str, dataset: str, key: str):
    p = CACHE / source / dataset
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{key}.parquet"


def _incremental(source, dataset, key, fetch_after, refresh=False, daily_bars: bool = False) -> pd.DataFrame:
    """fetch_after(last_ts or None) -> new rows DataFrame with 'ts'.

    daily_bars=True drops any bar stamped today (UTC): today's daily bar is still open.
    """
    path = _cache_path(source, dataset, key)
    old = pd.read_parquet(path) if path.exists() and not refresh else pd.DataFrame()
    last = old["ts"].max() if len(old) else None
    new = fetch_after(last)
    if new is not None and daily_bars and len(new):
        new = new[new["ts"] < pd.Timestamp.utcnow().tz_localize(None).normalize()]
    if new is None or new.empty:
        return old
    df = pd.concat([old, new], ignore_index=True).drop_duplicates("ts", keep="last").sort_values("ts")
    df.to_parquet(path, index=False)
    return df


# --------------------------------------------------------------------------
# Binance
# --------------------------------------------------------------------------
def binance_exchange_info() -> pd.DataFrame:
    j = _get(f"{FAPI}/fapi/v1/exchangeInfo")
    rows = [{"symbol": s["symbol"], "pair": s["pair"], "contract_type": s["contractType"], "status": s["status"],
             "base": s["baseAsset"], "quote": s["quoteAsset"],
             "underlying_type": s.get("underlyingType"), "sub_type": ",".join(s.get("underlyingSubType") or []),
             "onboard": pd.to_datetime(s["onboardDate"], unit="ms"),
             "delivery": pd.to_datetime(s["deliveryDate"], unit="ms")} for s in j["symbols"]]
    return pd.DataFrame(rows)


def binance_non_crypto_symbols(refresh: bool = False) -> set[str]:
    """Perps whose underlying is not a crypto coin (EQUITY, KR/HK/CN_EQUITY, COMMODITY, INDEX, PREMARKET).

    Binance TradFi perps (contractType TRADIFI_PERPETUAL) trade equities/commodities with weekend closures
    and earnings risk; they must not enter crypto cross-sectional research. Snapshot cached with its date.
    Delisted symbols are absent from exchangeInfo; all TradFi perps are recent listings, so treating unknown
    symbols as crypto is safe.
    """
    p = CACHE / "binance_underlying_types.csv"
    if p.exists() and not refresh:
        x = pd.read_csv(p)
        col = "underlying_type" if "underlying_type" in x else "underlyingType"
    else:
        x = binance_exchange_info()
        x["snapshot_date"] = pd.Timestamp.utcnow().tz_localize(None).normalize()
        x.to_csv(p, index=False)
        col = "underlying_type"
    return set(x.loc[x[col].fillna("COIN") != "COIN", "symbol"])


def binance_perp_universe(include_delisted: bool = True) -> list[str]:
    """Current USDT perps + every symbol in the survivorship-free alt panel (delisted names kept)."""
    info = binance_exchange_info()
    cur = set(info.loc[(info.contract_type == "PERPETUAL") & (info.quote == "USDT"), "symbol"])
    if include_delisted:
        ap = REPO / "research" / "btc_short_squeeze" / "data" / "alt_panel.parquet"
        if ap.exists():
            cur |= set(pd.read_parquet(ap, columns=["symbol"])["symbol"].unique())
    return sorted(cur)


def clean_klines(df: pd.DataFrame, min_frozen_run: int = 3) -> tuple[pd.DataFrame, pd.Timestamp | None]:
    """Truncate frozen post-delisting candles.

    Binance keeps serving klines for delisted perps with zero volume and high == low for
    months or years (found on 145 of 843 symbols, e.g. SCUSDT 1,550 frozen days). Left in,
    they fake 0% returns and dilute breadth. Returns (clean_df, inferred_delist_ts or None).
    Only a TRAILING run of >= min_frozen_run frozen bars is removed; isolated zero-volume
    bars inside live history are kept (they are flagged by checks.frozen_tail instead).
    """
    if df.empty:
        return df, None
    d = df.sort_values("ts").reset_index(drop=True)
    frozen = ((d["quote_volume"] <= 0) & (d["high"] == d["low"])).to_numpy()
    run = 0
    for z in frozen[::-1]:
        if not z:
            break
        run += 1
    if run >= min_frozen_run:
        cut = len(d) - run
        return d.iloc[:cut].copy(), (d["ts"].iloc[cut] if cut < len(d) else None)
    return d, None


KLINE_COLS = ["ot", "open", "high", "low", "close", "volume", "ct", "quote_volume", "trades",
              "taker_buy_base", "taker_buy_quote", "ignore"]
INTERVAL_MS = {"1h": 3_600_000, "4h": 14_400_000, "8h": 28_800_000, "1d": 86_400_000}


def binance_klines(symbol: str, interval: str = "1d", market: str = "um", start="2019-09-01",
                   refresh: bool = False) -> pd.DataFrame:
    base, path = (FAPI, "/fapi/v1/klines") if market == "um" else (SPOT, "/api/v3/klines")
    step = INTERVAL_MS[interval]

    def after(last):
        cur = _ms(start) if last is None else _ms(last) + step
        now = int(time.time() * 1000)
        out = []
        while cur < now:
            j = _get(base + path, {"symbol": symbol, "interval": interval, "startTime": cur, "limit": 1500 if market == "um" else 1000})
            if not j:
                break
            out.extend(j)
            if len(j) < 2:
                break
            cur = j[-1][0] + step
        if not out:
            return None
        df = pd.DataFrame(out, columns=KLINE_COLS)
        df["ts"] = pd.to_datetime(df["ot"], unit="ms")
        # drop the still-open bar
        df = df[df["ct"] < now]
        for c in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base", "taker_buy_quote"]:
            df[c] = df[c].astype(float)
        return df[["ts", "open", "high", "low", "close", "volume", "quote_volume", "trades",
                   "taker_buy_quote"]]

    return _incremental("binance", f"klines_{market}_{interval}", symbol, after, refresh)


def binance_funding(symbol: str, start="2019-09-01", refresh: bool = False) -> pd.DataFrame:
    """Individual funding prints. rate = decimal per funding interval (8h/4h/1h varies by symbol)."""
    def after(last):
        cur = _ms(start) if last is None else _ms(last) + 1
        out = []
        while True:
            j = _get(f"{FAPI}/fapi/v1/fundingRate", {"symbol": symbol, "startTime": cur, "limit": 1000})
            if not j:
                break
            out.extend(j)
            if len(j) < 1000:
                break
            cur = j[-1]["fundingTime"] + 1
        if not out:
            return None
        df = pd.DataFrame(out)
        df["ts"] = pd.to_datetime(df["fundingTime"], unit="ms").dt.floor("min")
        df["rate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
        df["mark"] = pd.to_numeric(df.get("markPrice"), errors="coerce")
        return df[["ts", "rate", "mark"]]

    return _incremental("binance", "funding", symbol, after, refresh)


def binance_premium_index(symbol: str, interval: str = "1d", start="2020-01-01", refresh: bool = False) -> pd.DataFrame:
    """Perp premium vs index (basis proxy). close = (perp - index)/index at bar end, decimal."""
    step = INTERVAL_MS[interval]

    def after(last):
        cur = _ms(start) if last is None else _ms(last) + step
        now = int(time.time() * 1000)
        out = []
        while cur < now:
            j = _get(f"{FAPI}/fapi/v1/premiumIndexKlines", {"symbol": symbol, "interval": interval,
                                                           "startTime": cur, "limit": 1500})
            if not j:
                break
            out.extend(j)
            if len(j) < 2:
                break
            cur = j[-1][0] + step
        if not out:
            return None
        df = pd.DataFrame([r[:7] for r in out], columns=["ot", "open", "high", "low", "close", "v", "ct"])
        df["ts"] = pd.to_datetime(df["ot"], unit="ms")
        df = df[df["ct"] < now]
        for c in ["open", "high", "low", "close"]:
            df[c] = df[c].astype(float)
        return df[["ts", "open", "high", "low", "close"]].rename(
            columns={"open": "prem_open", "high": "prem_high", "low": "prem_low", "close": "prem_close"})

    return _incremental("binance", f"premium_{interval}", symbol, after, refresh)


def binance_metrics_day(symbol: str, day) -> pd.DataFrame | None:
    """One day of data.binance.vision USD-M 'metrics' (5-min OI, long/short, taker ratio)."""
    d = pd.Timestamp(day).strftime("%Y-%m-%d")
    url = f"https://data.binance.vision/data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-{d}.zip"
    for i in range(4):
        try:
            with _LIMIT:
                r = _S.get(url, timeout=40)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                return pd.read_csv(z.open(z.namelist()[0]))
        except (requests.RequestException, zipfile.BadZipFile):
            pass
        time.sleep(1.5 * (i + 1))
    return None


def binance_metrics_daily(symbol: str, start="2020-09-01", end=None, refresh: bool = False) -> pd.DataFrame:
    """Daily aggregation of 5-min metrics: end-of-day OI, day-mean long/short ratios and taker ratio."""
    end = pd.Timestamp(end or pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=2))

    def agg(day):
        m = binance_metrics_day(symbol, day)
        if m is None or m.empty:
            return None
        m["create_time"] = pd.to_datetime(m["create_time"])
        m = m.sort_values("create_time")
        return {"ts": pd.Timestamp(day), "oi_contracts": m["sum_open_interest"].iloc[-1],
                "oi_usd": m["sum_open_interest_value"].iloc[-1],
                "toptrader_ls_acct": m["count_toptrader_long_short_ratio"].mean(),
                "toptrader_ls_pos": m["sum_toptrader_long_short_ratio"].mean(),
                "global_ls_acct": m["count_long_short_ratio"].mean(),
                "taker_ls_vol": m["sum_taker_long_short_vol_ratio"].mean()}

    def after(last):
        s = pd.Timestamp(start) if last is None else pd.Timestamp(last) + pd.Timedelta(days=1)
        days = pd.date_range(s, end, freq="D")
        if not len(days):
            return None
        with ThreadPoolExecutor(max_workers=6) as ex:
            rows = [r for r in ex.map(agg, days) if r]
        return pd.DataFrame(rows) if rows else None

    return _incremental("binance_vision", "metrics_daily", symbol, after, refresh)


# --------------------------------------------------------------------------
# Coinbase / Deribit
# --------------------------------------------------------------------------
def coinbase_daily(product: str = "BTC-USD", start="2015-07-01", refresh: bool = False) -> pd.DataFrame:
    """Coinbase Exchange daily candles (UTC buckets). Today's open bar is dropped."""
    def after(last):
        cur = pd.Timestamp(start) if last is None else pd.Timestamp(last) + pd.Timedelta(days=1)
        end_all = pd.Timestamp.utcnow().tz_localize(None).normalize()
        rows = []
        while cur < end_all:
            nxt = min(cur + pd.Timedelta(days=280), end_all)
            j = _get(f"https://api.exchange.coinbase.com/products/{product}/candles",
                     {"granularity": 86400, "start": cur.strftime("%Y-%m-%dT%H:%M:%SZ"),
                      "end": nxt.strftime("%Y-%m-%dT%H:%M:%SZ")})
            rows += j or []
            cur = nxt
            time.sleep(0.35)
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["t", "low", "high", "open", "close", "volume"])
        df["ts"] = pd.to_datetime(df["t"], unit="s").dt.normalize()
        return df[["ts", "open", "high", "low", "close", "volume"]].drop_duplicates("ts")

    return _incremental("coinbase", "daily", product, after, refresh, daily_bars=True)


def deribit_dvol(currency: str = "BTC", start="2021-03-01", refresh: bool = False) -> pd.DataFrame:
    """Deribit DVOL daily OHLC (annualised implied vol, vol points). Today's bar dropped."""
    def after(last):
        cur = pd.Timestamp(start) if last is None else pd.Timestamp(last) + pd.Timedelta(days=1)
        end_all = pd.Timestamp.utcnow().tz_localize(None)
        rows = []
        while cur < end_all:
            nxt = min(cur + pd.Timedelta(days=500), end_all)
            j = _get("https://www.deribit.com/api/v2/public/get_volatility_index_data",
                     {"currency": currency, "start_timestamp": _ms(cur), "end_timestamp": _ms(nxt),
                      "resolution": "1D"})
            rows += ((j or {}).get("result") or {}).get("data", [])
            cur = nxt
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close"])
        df["ts"] = pd.to_datetime(df["t"], unit="ms").dt.normalize()
        return df.rename(columns={"close": "dvol"})[["ts", "dvol"]].drop_duplicates("ts")

    return _incremental("deribit", "dvol_1d", currency, after, refresh, daily_bars=True)


# --------------------------------------------------------------------------
# Hyperliquid / Bybit
# --------------------------------------------------------------------------
def hyperliquid_funding(coin: str, start="2023-05-01", refresh: bool = False) -> pd.DataFrame:
    """Hourly funding prints. rate = decimal per hour; premium = decimal."""
    def after(last):
        cur = _ms(start) if last is None else _ms(last) + 1
        now = int(time.time() * 1000)
        out = []
        while cur < now:
            j = _get("https://api.hyperliquid.xyz/info", method="POST",
                     body={"type": "fundingHistory", "coin": coin, "startTime": cur})
            if not j:
                break
            out.extend(j)
            if len(j) < 2:
                break
            cur = j[-1]["time"] + 1
            time.sleep(0.2)
        if not out:
            return None
        df = pd.DataFrame(out)
        df["ts"] = pd.to_datetime(df["time"], unit="ms").dt.floor("min")
        df["rate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
        df["premium"] = pd.to_numeric(df["premium"], errors="coerce")
        return df[["ts", "rate", "premium"]]

    return _incremental("hyperliquid", "funding", coin, after, refresh)


def bybit_funding(symbol: str, start="2020-03-01", refresh: bool = False) -> pd.DataFrame:
    """Bybit linear funding prints (decimal per interval). API pages backwards from endTime."""
    def after(last):
        stop = _ms(start) if last is None else _ms(last) + 1
        end = int(time.time() * 1000)
        out = []
        while end > stop:
            j = _get("https://api.bybit.com/v5/market/funding/history",
                     {"category": "linear", "symbol": symbol, "endTime": end, "limit": 200})
            lst = (j or {}).get("result", {}).get("list", [])
            if not lst:
                break
            out.extend(lst)
            oldest = min(int(x["fundingRateTimestamp"]) for x in lst)
            if oldest >= end:
                break
            end = oldest - 1
        if not out:
            return None
        df = pd.DataFrame(out)
        df["ts"] = pd.to_datetime(df["fundingRateTimestamp"].astype("int64"), unit="ms").dt.floor("min")
        df["rate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
        df = df[df["ts"] > (pd.to_datetime(stop, unit="ms"))]
        return df[["ts", "rate"]]

    return _incremental("bybit", "funding", symbol, after, refresh)


# --------------------------------------------------------------------------
# CoinGlass (key from .env; vendor, plan-tier limits apply)
# --------------------------------------------------------------------------
def _cg_key():
    try:
        from dotenv import load_dotenv
        load_dotenv(REPO / ".env")
    except ImportError:
        pass
    return os.getenv("COINGLASS_API_KEY")


def coinglass(endpoint: str, params: dict):
    key = _cg_key()
    if not key:
        raise RuntimeError("COINGLASS_API_KEY not configured")
    for i in range(8):
        r = _S.get(f"https://open-api-v4.coinglass.com/api/{endpoint}", params=params,
                   headers={"CG-API-KEY": key}, timeout=60)
        if r.status_code == 200:
            j = r.json()
            if str(j.get("code")) == "0":
                return j.get("data") or []
            msg = str(j.get("msg", "")).lower()
            if "limit" in msg or "too many" in msg:
                time.sleep(20 * (i + 1))       # plan-tier rate limit: back off hard
                continue
            raise RuntimeError(f"CoinGlass {endpoint}: {j.get('msg')}")
        if r.status_code in (429, 500, 502, 503):
            time.sleep(20 * (i + 1))
            continue
        raise RuntimeError(f"CoinGlass {endpoint} HTTP {r.status_code}")
    raise RuntimeError(f"CoinGlass {endpoint}: retries exhausted")


CONSISTENT_VENUES = "Binance,OKX,Bybit,HTX"   # comparable 2021-2026 (see research/btc_short_squeeze/fetch_data.py)


def coinglass_liquidations(symbol: str, venues: str = CONSISTENT_VENUES, start="2021-01-01",
                           refresh: bool = False) -> pd.DataFrame:
    def after(last):
        s = _ms(start) if last is None else _ms(last) + 86_400_000
        d = coinglass("futures/liquidation/aggregated-history",
                      {"exchange_list": venues, "symbol": symbol, "interval": "1d",
                       "start_time": s, "end_time": int(time.time() * 1000)})
        if not d:
            return None
        df = pd.DataFrame(d)
        df["ts"] = pd.to_datetime(df["time"], unit="ms").dt.normalize()
        return df.rename(columns={"aggregated_long_liquidation_usd": "long_liq_usd",
                                  "aggregated_short_liquidation_usd": "short_liq_usd"})[
            ["ts", "long_liq_usd", "short_liq_usd"]]

    return _incremental("coinglass", "liquidations_1d", symbol, after, refresh, daily_bars=True)


def coinglass_oi(symbol: str, venues: str = CONSISTENT_VENUES, start="2021-01-01", refresh: bool = False) -> pd.DataFrame:
    def after(last):
        s = _ms(start) if last is None else _ms(last) + 86_400_000
        d = coinglass("futures/open-interest/aggregated-history",
                      {"exchange_list": venues, "symbol": symbol, "interval": "1d",
                       "start_time": s, "end_time": int(time.time() * 1000)})
        if not d:
            return None
        df = pd.DataFrame(d)
        df["ts"] = pd.to_datetime(df["time"], unit="ms").dt.normalize()
        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.rename(columns={"close": "oi_usd", "open": "oi_open"})[["ts", "oi_open", "oi_usd"]]

    return _incremental("coinglass", "oi_1d", symbol, after, refresh, daily_bars=True)


# --------------------------------------------------------------------------
def fetch_many(fn, keys, workers: int = 6, **kw) -> dict:
    """Run a fetcher across keys in a thread pool; failures are recorded, not raised."""
    out, errors = {}, {}

    def one(k):
        try:
            return k, fn(k, **kw), None
        except Exception as e:  # noqa: BLE001 - research fetch loop
            return k, None, repr(e)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for k, df, err in ex.map(one, keys):
            if err:
                errors[k] = err
            else:
                out[k] = df
    if errors:
        import re
        name = re.sub(r"[^A-Za-z0-9_]+", "_", getattr(fn, "__name__", "fn")).strip("_") or "fn"   # '<lambda>' is invalid on Windows
        pd.Series(errors).to_csv(CACHE / f"fetch_errors_{name}_{pd.Timestamp.utcnow():%Y%m%dT%H%M%S}.csv")
    return out
