#!/usr/bin/env python3
"""
Long/Short Regime Monitor for BTC-long / Alt-short strategy.

Modes:
- Live snapshot (default): pull current data incl. CoinGlass funding & OI,
  compute regime, append one row.
- Historical/backfill ("historical"): pull N days of CoinGecko prices,
  and Coinglass funding/OI history, reconstruct daily regimes and append many
  rows to regime_history.csv.
"""

import os
import sys
import csv
import time
import statistics
import datetime as dt
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import requests

# ============================================================
# CONFIG
# ============================================================

COINGLASS_API_KEY = os.environ.get("COINGLASS_API_KEY", "")  # Set in env; never commit keys
COINGLASS_BASE = "https://open-api-v4.coinglass.com"
COINGECKO_BASE = "https://pro-api.coingecko.com/api/v3"
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")  # Set in env; never commit keys

# Coinglass rate limits
COINGLASS_SLEEP_SECONDS = 3.2  # per request (throttling)

# CoinGecko historical rate limit (per coin)
COINGECKO_SLEEP_SECONDS = 2.7

HISTORICAL_LOOKBACK_DAYS = 720  # days of regimes to backfill (2 years - 10 days buffer)

# CoinGecko IDs for the full universe
COINGECKO_IDS: Dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "XRP": "ripple",
    "BNB": "binancecoin",
    "SOL": "solana",
    "TRX": "tron",
    "ADA": "cardano",
    "DOGE": "dogecoin",
    "BCH": "bitcoin-cash",
    "HYPE": "hyperliquid",
    "LINK": "chainlink",
    "ZEC": "zcash",
    "XLM": "stellar",
    "XMR": "monero",
    "LTC": "litecoin",
    "HBAR": "hedera",
    "AVAX": "avalanche-2",
    "SUI": "sui",
    "SHIB": "shiba-inu",
    "WLFI": "world-liberty-financial",
    "UNI": "uniswap",
    "TON": "toncoin",
    "DOT": "polkadot",
    "MNT": "mantle",
    "CC": "canton-network",
    "M": "memecore",
    "TAO": "bittensor",
    "AAVE": "aave",
    "NEAR": "near-protocol",
    "ASTER": "aster",
    "ICP": "internet-computer",
    "ETC": "ethereum-classic",
    "ENA": "ethena",
    "PI": "pi-network",
    "APT": "aptos",
    "PUMP": "pump",
    "ONDO": "ondo",
    "WLD": "worldcoin",
    "POL": "pol",
    "KAS": "kaspa",
    "QNT": "quant-network",
    "ALGO": "algorand",
    "TRUMP": "official-trump",
    "ARB": "arbitrum",
    "FIL": "filecoin",
    "SKY": "sky",
    "IP": "story",
    "NEXO": "nexo",
    "SEI": "sei",
    "CAKE": "pancakeswap",
    "MORPHO": "morpho",
    "BONK": "bonk",
    "JUP": "jupiter",
    "DASH": "dash",
    "FET": "fetch-ai",
    "STRK": "strike",
    "AERO": "aerotoken",
    "OP": "optimism",
    "VIRTUAL": "virtual-protocol",
    "CRV": "curve-dao-token",
    "SPX": "spx6900",
    "STX": "stacks",
    "INJ": "injective-protocol",
    "GRT": "the-graph",
    "XTZ": "tezos",
    "TIA": "celestia",
    "MYX": "myx-finance",
    "MON": "monad",
    "IOTA": "iota",
    "KAIA": "kaia",
    "FLOKI": "floki",
    "ETHFI": "ether-fi",
    "TEL": "telcoin",
    "PYTH": "pyth-network",
    "CFX": "conflux-token",
    "S": "sonic",
    "XPL": "plasma",
    "ENS": "ethereum-name-service",
    "2Z": "doublezero",
    "BTT": "bittorrent-2",
    "PENDLE": "pendle",
    "SUN": "sun-token",
    "HNT": "helium",
    "DCR": "decred",
    "JST": "just",
    "FLOW": "flow",
    "JASMY": "jasmycoin",
    "THETA": "theta-token",
    "WIF": "dogwifhat",
    "GALA": "gala",
    "GNO": "gnosis",
    "OHM": "olympus",
    "SYRUP": "syrup",
    "ZBCN": "zebec-network",
    "MANA": "decentraland",
    "BAT": "basic-attention-token",
    "FARTCOIN": "fartcoin",
    "NEO": "neo",
    "RAY": "raydium",
    "ZK": "zksync",
    "CHZ": "chiliz",
    "ULTIMA": "ultima",
    "COMP": "compound-governance-token",
    "1INCH": "1inch",
    "AR": "arweave",
    "FLUID": "fluid",
    "ZRO": "zero",
    "EIGEN": "eigenlayer",
    "BORG": "swissborg",
    "VSN": "vision-3",
    "IMX": "immutable-x",
    "APE": "apecoin",
    "0G": "0g",
    "TRAC": "origintrail",
    "ATH": "aethir",
    "XEC": "ecash",
    "WAL": "wal",
    "ZORA": "zora",
    "REAL": "realio-network",
    "LION": "loaded-lions",
    "CHEEMS": "cheems-token",
    "W": "wormhole",
    "EGLD": "elrond-erd-2",
    "RUNE": "thorchain",
    "GLM": "golem",
    "SOON": "soon-2",
    "DEXE": "dexe",
    "ZEN": "horizen",
    "RSR": "reserve-right-token",
    "JTO": "jito",
    "DYDX": "dydx",
    "ZANO": "zano",
    "WEMIX": "wemix-token",
    "SNX": "synthetix-network-token",
    "KMNO": "kamino",
    "LPT": "livepeer",
    "LGCT": "legacy-token",
    "AXS": "axie-infinity",
    "AMP": "amp-token",
    "KITE": "kite-ai",
    "KAITO": "kaito",
    "SAND": "the-sandbox",
    "MOVE": "move-to-earn",
    "LAYER": "solayer",
    "ALT": "altlayer",
    "GMT": "stepn",
    "ZETA": "zeta-chain",
    "MANTA": "manta-network",
    "DYM": "dymension",
}

ALT_SYMBOLS: List[str] = [
    sym for sym in COINGECKO_IDS.keys() if sym not in ("BTC", "ETH")
]

# For historical mode we use a smaller subset to avoid CoinGecko 429 spam
HISTORICAL_SYMBOLS: List[str] = ["BTC", "ETH"] + ALT_SYMBOLS[:60]

FUNDING_SYMBOLS: List[str] = ["BTC", "ETH"] + ALT_SYMBOLS
OI_SYMBOL = "BTC"

HISTORY_FILE = "regime_history.csv"
Z_MIN_POINTS = 20

# Exchanges to use for funding history aggregation (not needed for v4 OI-weighted endpoint, kept for reference)
FUNDING_EXCHANGES = ["BINANCE", "OKX", "BYBIT"]

# Funding heating config
H_SHORT = 10      # short window length (days/rows)
H_LONG = 20       # long window length
H_LOW = 0.0       # "no concern" threshold for heating
H_HIGH = 0.0005   # "clearly heating" (~0.05% spread change)
W_FUNDING = 0.25  # weight of funding_risk in total risk_penalty

# How many past rows to use from history for live-mode heating
HEATING_HISTORY_ROWS = 60


# ============================================================
# Helpers
# ============================================================

def safe_get(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
) -> Optional[Dict[str, Any]]:
    """
    Generic safe GET with one retry on 429. If JSON is a list, wrap as {"data": list}.
    Intended for CoinGecko and other non-Coinglass calls (CoinGlass has its own wrapper).
    """
    # Add CoinGecko Pro API key as query parameter if this is a CoinGecko request
    if params is None:
        params = {}
    if "coingecko.com" in url or "pro-api.coingecko.com" in url:
        params["x_cg_pro_api_key"] = COINGECKO_API_KEY
    
    max_429_retries = 1
    for attempt in range(max_429_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 429:
                msg = f"429 Too Many Requests for {url}"
                try:
                    j = resp.json()
                    if isinstance(j, dict) and "error" in j:
                        msg = j["error"]
                except Exception:
                    pass
                print(f"[WARN] {msg}")
                if attempt < max_429_retries:
                    print("[INFO] Sleeping 15s then retrying CoinGecko...")
                    time.sleep(15.0)
                    continue
                return None
            if resp.status_code == 401:
                # Log the actual request URL with params for debugging
                full_url = f"{url}?" + "&".join([f"{k}={v}" for k, v in params.items()])
                print(f"[ERROR] Unauthorized (401) for CoinGecko request. URL: {full_url[:200]}...")
                print(f"[ERROR] Response: {resp.text[:500]}")
                return None
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return {"data": data}
            if not isinstance(data, dict):
                print(f"[WARN] Expected dict/list JSON from {url}, got {type(data)}")
                return None
            return data
        except Exception as e:
            print(f"[WARN] safe_get error for {url}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    print(f"[ERROR] Response status: {e.response.status_code}, body: {e.response.text[:500]}")
                except:
                    pass
            return None
    return None


def coinglass_get(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
) -> Optional[Dict[str, Any]]:
    """
    CoinGlass GET with throttling and simple 429 backoff.
    """
    headers = {"CG-API-KEY": COINGLASS_API_KEY}
    max_429_retries = 1
    for attempt in range(max_429_retries + 1):
        time.sleep(COINGLASS_SLEEP_SECONDS)
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 429:
                try:
                    data = resp.json()
                    msg = data.get("msg", "Too Many Requests")
                except Exception:
                    msg = "Too Many Requests"
                print(f"[WARN] Coinglass 429 for {url} {params}: {msg}")
                if attempt < max_429_retries:
                    print("[INFO] Sleeping 20s then retrying CoinGlass...")
                    time.sleep(20.0)
                    continue
                return None
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return {"data": data}
            if not isinstance(data, dict):
                print(f"[WARN] Expected dict/list JSON from Coinglass {url}, got {type(data)}")
                return None
            return data
        except Exception as e:
            print(f"[WARN] coinglass_get error for {url} {params}: {e}")
            return None
    return None


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# ============================================================
# Live CoinGecko snapshot
# ============================================================

def fetch_coingecko_prices_and_returns() -> Dict[str, Dict[str, float]]:
    print("Fetching CoinGecko prices & returns...")
    ids_str = ",".join(set(COINGECKO_IDS.values()))
    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ids_str,
        "price_change_percentage": "1h,24h,7d",
        "per_page": len(COINGECKO_IDS),
        "page": 1,
    }
    raw = safe_get(url, params=params)
    if raw is None:
        print("[WARN] CoinGecko request failed completely.")
        return {}

    data_list = raw.get("data", [])
    id_to_sym = {cg_id: sym for sym, cg_id in COINGECKO_IDS.items()}
    out: Dict[str, Dict[str, float]] = {}
    for coin in data_list:
        try:
            cg_id = coin.get("id")
            sym = id_to_sym.get(cg_id)
            if not sym:
                continue
            price = coin.get("current_price")
            if price is None:
                print(f"[WARN] CG missing price for coin {cg_id}")
                continue
            pct_24h = coin.get("price_change_percentage_24h_in_currency")
            pct_7d = coin.get("price_change_percentage_7d_in_currency")
            pct_24h = 0.0 if pct_24h is None else float(pct_24h)
            pct_7d = 0.0 if pct_7d is None else float(pct_7d)
            out[sym] = {
                "price_usd": float(price),
                "return_1d": pct_24h,
                "return_7d": pct_7d,
            }
        except Exception as e:
            print(f"[WARN] CG parse error for coin {coin.get('id')}: {e}")
    if "BTC" not in out:
        print("[WARN] BTC missing from CoinGecko output.")
    return out


# ============================================================
# Coinglass live snapshots
# ============================================================

def fetch_coinglass_funding_snapshot(symbol: str) -> Optional[Dict[str, Any]]:
    url = f"{COINGLASS_BASE}/api/futures/funding-rate/exchange-list"
    params = {"symbol": symbol.upper()}
    data = coinglass_get(url, params=params)
    if data is None:
        print(f"[WARN] Funding error for {symbol}: request failed.")
        return None
    if data.get("code") != "0":
        print(f"[WARN] Funding error for {symbol}: {data.get('msg')}")
        return None
    entries: List[Dict[str, Any]] = data.get("data", [])
    if not entries:
        print(f"[WARN] Funding error for {symbol}: empty data list.")
        return None

    entry = None
    for e in entries:
        if str(e.get("symbol", "")).upper() == symbol.upper():
            entry = e
            break
    if entry is None:
        entry = entries[0]

    stable_list = entry.get("stablecoin_margin_list", []) or []
    if not stable_list:
        print(f"[WARN] Funding error for {symbol}: no stablecoin_margin_list.")
        return None

    rates = []
    for ex in stable_list:
        try:
            r = float(ex.get("funding_rate"))
            rates.append(r)
        except Exception:
            continue
    if not rates:
        print(f"[WARN] Funding error for {symbol}: no parsable funding rates.")
        return None

    avg_rate = sum(rates) / len(rates)
    return {
        "avg_funding": avg_rate,
        "max_funding": max(rates),
        "min_funding": min(rates),
        "exchanges": len(rates),
    }


def fetch_coinglass_btc_oi_snapshot(symbol: str = "BTC") -> Dict[str, float]:
    """
    Live BTC OI snapshot:
    - oi_usd_all from /open-interest/exchange-list (ALL aggregate)
    - oi_change_3d_pct from /open-interest/aggregated-history using 8h candles
      over the last ~4 days (approx 72h change).
    """
    print("Fetching BTC open interest (live snapshot)...")

    # 1) Current OI from exchange-list
    url = f"{COINGLASS_BASE}/api/futures/open-interest/exchange-list"
    params = {"symbol": symbol.upper()}
    data = coinglass_get(url, params=params)
    if data is None:
        raise RuntimeError("BTC OI request failed")
    if data.get("code") != "0":
        raise RuntimeError(f"BTC OI error: {data.get('msg')}")
    entries: List[Dict[str, Any]] = data.get("data", [])
    if not entries:
        raise RuntimeError("BTC OI error: empty data list")
    agg_row = None
    for row in entries:
        if str(row.get("symbol", "")).upper() == symbol.upper() and \
           str(row.get("exchange", "")).lower() == "all":
            agg_row = row
            break
    if agg_row is None:
        agg_row = entries[0]
    oi_usd = float(agg_row.get("open_interest_usd"))

    # 2) Approx 3d change from aggregated-history (8h candles)
    now = dt.datetime.now(dt.timezone.utc)
    end_ms = int(now.timestamp() * 1000)
    # 4 days of history to safely cover 72h (9 candles)
    start_ms = end_ms - int(4 * 24 * 60 * 60 * 1000)

    url_hist = f"{COINGLASS_BASE}/api/futures/open-interest/aggregated-history"
    params_hist = {
        "symbol": symbol.upper(),
        "interval": "8h",
        "startTime": start_ms,
        "endTime": end_ms,
    }
    data_hist = coinglass_get(url_hist, params=params_hist)

    oi_change_3d = 0.0
    if data_hist is not None and data_hist.get("code") == "0":
        rows = data_hist.get("data", [])
        if isinstance(rows, list) and rows:
            try:
                rows = sorted(rows, key=lambda r: int(r.get("time", 0)))
            except Exception:
                pass
            n = len(rows)
            if n >= 10:  # need at least 9 candles gap for ~72h
                try:
                    last_close = float(rows[-1].get("close"))
                    old_idx = n - 1 - 9
                    if old_idx >= 0:
                        old_close = float(rows[old_idx].get("close"))
                        if old_close > 0:
                            oi_change_3d = (last_close / old_close - 1.0) * 100.0
                except Exception:
                    oi_change_3d = 0.0

    return {"oi_usd_all": oi_usd, "oi_change_3d_pct": oi_change_3d}


# ============================================================
# Coinglass historical helpers
# ============================================================

def fetch_coingecko_history_prices(
    days: int,
    extra_days_for_lookback: int = 7,
) -> Tuple[List[dt.date], Dict[str, Dict[dt.date, float]]]:
    """
    Fetch daily CoinGecko prices for HISTORICAL_SYMBOLS.

    If BTC data is missing on the first pass (e.g. due to a transient 429),
    we retry BTC once. If it still fails, we return empty results and let the
    caller abort historical mode gracefully instead of throwing.
    """
    print(f"Historical mode: fetching CoinGecko daily prices for ~{days + extra_days_for_lookback} days...")

    total_days = days + extra_days_for_lookback
    symbol_price_by_date: Dict[str, Dict[dt.date, float]] = {}

    def fetch_one_symbol(sym: str, total_days: int) -> Optional[Dict[dt.date, float]]:
        cg_id = COINGECKO_IDS[sym]
        time.sleep(COINGECKO_SLEEP_SECONDS)
        url = f"{COINGECKO_BASE}/coins/{cg_id}/market_chart"
        params = {"vs_currency": "usd", "days": total_days, "interval": "daily"}
        raw = safe_get(url, params=params)
        if raw is None:
            print(f"[WARN] CG history failed for {sym} ({cg_id})")
            return None
        prices = raw.get("prices", [])
        if not isinstance(prices, list):
            print(f"[WARN] CG history missing prices list for {sym}")
            return None

        dmap: Dict[dt.date, float] = {}
        for ts_ms, price in prices:
            try:
                ts = int(ts_ms) / 1000.0
                d = dt.datetime.fromtimestamp(ts, dt.timezone.utc).date()
                dmap[d] = float(price)
            except Exception:
                continue
        return dmap

    # First pass: loop over all historical symbols
    for sym in HISTORICAL_SYMBOLS:
        dmap = fetch_one_symbol(sym, total_days)
        if dmap is not None:
            symbol_price_by_date[sym] = dmap

    # Ensure BTC exists; if not, retry BTC once explicitly
    if "BTC" not in symbol_price_by_date:
        print("[WARN] BTC missing from CoinGecko historical data on first pass. Retrying BTC once...")
        dmap_btc_retry = fetch_one_symbol("BTC", total_days)
        if dmap_btc_retry is not None:
            symbol_price_by_date["BTC"] = dmap_btc_retry

    if "BTC" not in symbol_price_by_date:
        print("[ERROR] BTC still missing from CoinGecko historical data after retry. "
              "Historical backfill will be aborted.")
        return [], {}

    btc_dates = sorted(symbol_price_by_date["BTC"].keys())
    if len(btc_dates) > total_days:
        btc_dates = btc_dates[-total_days:]
    return btc_dates, symbol_price_by_date


def fetch_coinglass_funding_history_symbol(
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
    interval: str = "8h",
) -> Dict[dt.date, float]:
    """
    Fetch historical OI-weighted funding OHLC candles for a symbol using the
    CoinGlass v4 endpoint:

        /api/futures/funding-rate/oi-weight-history

    We aggregate by day using the `close` value of each candle.
    """
    url = f"{COINGLASS_BASE}/api/futures/funding-rate/oi-weight-history"

    params = {
        "symbol": symbol.upper(),
        "interval": interval,          # e.g. "8h" (allowed: >= 4h)
        "startTime": start_ms,
        "endTime": end_ms,
    }

    print(f"[DEBUG] OI-weighted funding history request for {symbol}: {params}")
    data = coinglass_get(url, params=params)
    if data is None:
        print(f"[WARN] OI-weighted funding history error for {symbol}: request failed")
        return {}

    if data.get("code") != "0":
        print(f"[WARN] OI-weighted funding history error for {symbol}: {data.get('msg')}")
        return {}

    rows = data.get("data", [])
    if not isinstance(rows, list) or not rows:
        print(f"[WARN] OI-weighted funding history empty for {symbol}")
        return {}

    per_day_values: Dict[dt.date, List[float]] = {}

    for row in rows:
        try:
            t_ms = int(row.get("time"))
            close_val = float(row.get("close"))
        except Exception:
            continue

        d = dt.datetime.fromtimestamp(t_ms / 1000.0, dt.timezone.utc).date()
        per_day_values.setdefault(d, []).append(close_val)

    daily_avg: Dict[dt.date, float] = {}
    for d, vals in per_day_values.items():
        if vals:
            daily_avg[d] = float(statistics.fmean(vals))

    return daily_avg


def fetch_coinglass_btc_oi_history(
    *,
    symbol: str = "BTC",
    start_ms: int,
    end_ms: int,
    interval: str = "8h",
) -> Dict[dt.date, float]:
    """
    Fetch historical aggregated BTC OI OHLC and return daily close OI (usd).
    """
    url = f"{COINGLASS_BASE}/api/futures/open-interest/aggregated-history"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
    }
    data = coinglass_get(url, params=params)
    if data is None:
        print("[WARN] BTC OI history error: request failed")
        return {}
    if data.get("code") != "0":
        print(f"[WARN] BTC OI history error: {data.get('msg')}")
        return {}

    rows = data.get("data", [])
    if not isinstance(rows, list) or not rows:
        print("[WARN] BTC OI history empty")
        return {}

    try:
        rows = sorted(rows, key=lambda r: int(r.get("time", 0)))
    except Exception:
        pass

    per_day_closes: Dict[dt.date, float] = {}
    for row in rows:
        try:
            t_ms = int(row.get("time"))
            close_val = float(row.get("close"))
        except Exception:
            continue
        d = dt.datetime.fromtimestamp(t_ms / 1000.0, dt.timezone.utc).date()
        per_day_closes[d] = close_val

    return per_day_closes


# ============================================================
# Regime scoring
# ============================================================

def compute_heating_and_funding_risk_from_series(
    f_alt_series: List[float],
    f_btc_series: List[float],
    h_short: int = H_SHORT,
    h_long: int = H_LONG,
    h_low: float = H_LOW,
    h_high: float = H_HIGH,
    default_risk: float = 0.5,
) -> Tuple[float, float]:
    """
    Given time-ordered series of f_alt and f_btc including today's value
    at the end, compute:

      - heating = s_short - s_long, where s(t) = f_alt(t) - f_btc(t)
        and s_short / s_long are short/long window means of s.
      - funding_risk in [0, 1] via piecewise-linear mapping:

            heating <= h_low  → funding_risk = 0
            heating >= h_high → funding_risk = 1
            else              → linear between 0 and 1

    If there is not enough history for h_long points, returns
    (funding_risk=default_risk, heating=0.0).
    """
    n = min(len(f_alt_series), len(f_btc_series))
    if n < h_long:
        return default_risk, 0.0

    # Align on last n points
    f_alt_series = f_alt_series[-n:]
    f_btc_series = f_btc_series[-n:]

    # Spread series s(t) = f_alt - f_btc
    s_series = [fa - fb for fa, fb in zip(f_alt_series, f_btc_series)]

    s_short_vals = s_series[-h_short:]
    s_long_vals = s_series[-h_long:]

    s_short = statistics.fmean(s_short_vals)
    s_long = statistics.fmean(s_long_vals)
    heating = s_short - s_long  # short minus long

    # Piecewise linear mapping → funding_risk
    if heating <= h_low:
        risk = 0.0
    elif heating >= h_high:
        risk = 1.0
    else:
        risk = (heating - h_low) / (h_high - h_low)

    # Final clamp to [0, 1]
    risk = clamp(risk, 0.0, 1.0)
    return risk, heating


def compute_regime(
    prices: Dict[str, Dict[str, float]],
    btc_oi: Dict[str, float],
    funding_risk: float,
    f_alt: float,
    f_btc: float,
    heating: float,
) -> Dict[str, Any]:
    btc_1d = prices.get("BTC", {}).get("return_1d", 0.0)
    btc_7d = prices.get("BTC", {}).get("return_7d", 0.0)

    # Alt basket returns
    alt7 = [prices.get(s, {}).get("return_7d", 0.0) for s in ALT_SYMBOLS if s in prices]
    alt1 = [prices.get(s, {}).get("return_1d", 0.0) for s in ALT_SYMBOLS if s in prices]
    alt7 = [x for x in alt7 if x is not None]
    alt1 = [x for x in alt1 if x is not None]
    alt7_avg = statistics.fmean(alt7) if alt7 else 0.0

    # Trend: BTC vs alt basket, vol-adjusted
    spread7_pct = btc_7d - alt7_avg
    vol_proxy = abs(btc_7d) + 1e-3
    trend_raw = spread7_pct / vol_proxy
    trend_clamped = clamp(trend_raw / 3.0, -1.0, 1.0)

    # Approximate "3d" returns from 1d & 7d
    def approx_3d(ret1, ret7):
        return 0.5 * (ret7 * 3.0 / 7.0 + ret1 / 3.0)

    btc_3d = approx_3d(btc_1d, btc_7d)

    # Breadth branch: % of alts outperforming BTC on approx 3d horizon
    alt3 = []
    for s in ALT_SYMBOLS:
        if s not in prices:
            continue
        r1 = prices[s].get("return_1d", 0.0)
        r7 = prices[s].get("return_7d", 0.0)
        alt3.append(approx_3d(r1, r7))
    if alt3:
        num_outperf = len([x for x in alt3 if x > btc_3d])
        breadth_3d = num_outperf / len(alt3)
    else:
        breadth_3d = 0.0
    breadth_risk = breadth_3d

    # OI branch (using 3d change and btc_3d as quality gate)
    oi_change = btc_oi.get("oi_change_3d_pct", 0.0)
    if oi_change > 0:
        base_oi_risk = clamp(oi_change / 50.0, 0.0, 1.0)
        oi_quality = 1.0 if btc_3d > 0 else 0.5
    else:
        base_oi_risk = 0.0
        oi_quality = 0.0
    oi_risk = base_oi_risk * oi_quality

    # Decomposition in combined-space (-1..+1)
    trend_component = trend_clamped
    funding_penalty = W_FUNDING * funding_risk
    oi_penalty = 0.15 * oi_risk
    breadth_penalty = 0.10 * breadth_risk
    total_penalty = funding_penalty + oi_penalty + breadth_penalty

    combined_raw = trend_component - total_penalty
    combined_clamped = clamp(combined_raw, -1.0, 1.0)
    regime_score_raw = (combined_clamped + 1.0) / 2.0 * 100.0

    # High-vol gate for very strong BTC moves
    high_vol = abs(btc_7d) > 15.0
    regime_score = regime_score_raw
    if high_vol and regime_score > 60.0:
        regime_score = 60.0

    if regime_score >= 70:
        bucket = "GREEN"
    elif regime_score >= 55:
        bucket = "YELLOWGREEN"
    elif regime_score >= 45:
        bucket = "YELLOW"
    elif regime_score >= 30:
        bucket = "ORANGE"
    else:
        bucket = "RED"

    funding_spread = f_alt - f_btc

    return {
        "regime_score": regime_score,          # final (after gate)
        "regime_score_raw": regime_score_raw,  # before high-vol gate
        "bucket": bucket,

        "btc_7d": btc_7d,
        "btc_1d": btc_1d,
        "btc_3d": btc_3d,
        "alt_7d_avg": alt7_avg,
        "spread7": spread7_pct,
        "breadth_3d": breadth_3d,

        # Funding-related outputs
        "avg_alt_funding": f_alt,         # keep name for continuity
        "btc_funding": f_btc,
        "funding_spread": funding_spread,
        "funding_heating": heating,
        "funding_risk": funding_risk,

        # OI / other
        "oi_change_3d": oi_change,
        "oi_risk": oi_risk,
        "high_vol_gate": high_vol,

        # Decomposition pieces
        "trend_component": trend_component,
        "funding_penalty": funding_penalty,
        "oi_penalty": oi_penalty,
        "breadth_penalty": breadth_penalty,
        "total_risk_penalty": total_penalty,
        "combined_raw": combined_raw,
        "combined_clamped": combined_clamped,
    }


# ============================================================
# History & z-scores
# ============================================================

def load_history(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except PermissionError:
        print(f"[WARN] Cannot read history file {path} (PermissionError). "
              f"Is it open in Excel or another program? Proceeding with empty history.")
        return []


def find_missing_dates(
    history_file: str = HISTORY_FILE,
    end_date: Optional[dt.date] = None,
) -> Tuple[Optional[dt.date], List[dt.date]]:
    """
    Find ALL missing dates in the history file, including:
    - Gaps between consecutive dates in the middle of the data
    - Missing dates from the last entry to end_date (default: today)
    
    Returns:
        (last_date_in_history, list_of_all_missing_dates)
        If no history exists, returns (None, []) and caller should do full backfill.
    """
    path = Path(history_file)
    history_rows = load_history(path)
    
    if not history_rows:
        print(f"[INFO] No existing history found in {history_file}. All dates are missing.")
        return None, []
    
    # Parse all dates from history
    existing_dates = set()
    last_date = None
    first_date = None
    
    for row in history_rows:
        date_str = row.get("date_iso", "")
        if not date_str:
            continue
        try:
            # Handle ISO format with timezone
            if "T" in date_str:
                date_dt = dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                d = date_dt.date()
            else:
                d = dt.datetime.fromisoformat(date_str).date()
            existing_dates.add(d)
            if first_date is None or d < first_date:
                first_date = d
            if last_date is None or d > last_date:
                last_date = d
        except Exception as e:
            continue
    
    if not existing_dates:
        print(f"[WARN] History file exists but no valid dates found. Treating as empty.")
        return None, []
    
    # Determine end date (default: today UTC)
    if end_date is None:
        end_date = dt.datetime.now(dt.timezone.utc).date()
    
    # Find all missing dates:
    # 1. Gaps between consecutive dates in the existing data
    # 2. Missing dates from last_date to end_date
    
    missing_dates = []
    
    # Sort existing dates to find gaps
    sorted_dates = sorted(existing_dates)
    
    # Check for gaps between consecutive dates
    for i in range(len(sorted_dates) - 1):
        current_date = sorted_dates[i]
        next_date = sorted_dates[i + 1]
        # If there's a gap (more than 1 day difference)
        gap = (next_date - current_date).days
        if gap > 1:
            # Add all dates in between
            for day_offset in range(1, gap):
                missing_date = current_date + dt.timedelta(days=day_offset)
                missing_dates.append(missing_date)
    
    # Add missing dates from last_date to end_date
    if last_date < end_date:
        current = last_date + dt.timedelta(days=1)
        while current <= end_date:
            if current not in existing_dates:
                missing_dates.append(current)
            current += dt.timedelta(days=1)
    
    # Remove duplicates and sort
    missing_dates = sorted(list(set(missing_dates)))
    
    if missing_dates:
        print(
            f"[INFO] Found {len(missing_dates)} missing days total:\n"
            f"  - First existing date: {first_date}\n"
            f"  - Last existing date: {last_date}\n"
            f"  - Missing date range: {missing_dates[0]} to {missing_dates[-1]}"
        )
    else:
        if last_date >= end_date:
            print(f"[INFO] History is up to date. Last entry: {last_date}, Today: {end_date}")
        else:
            print(f"[INFO] No gaps found in existing data. Last entry: {last_date}, Today: {end_date}")
    
    return last_date, missing_dates


def compute_z_for_feature(
    feature_name: str,
    today_value: float,
    history_rows: List[Dict[str, str]],
    min_points: int = Z_MIN_POINTS,
) -> float:
    values = []
    for row in history_rows:
        try:
            v = float(row.get(feature_name, ""))
            values.append(v)
        except (TypeError, ValueError):
            continue
    if len(values) < min_points:
        return 0.0
    mean = statistics.fmean(values)
    std = statistics.pstdev(values)
    if std == 0:
        return 0.0
    return (today_value - mean) / std


def _open_history_for_append(path: Path, fieldnames: List[str]):
    """
    Open the main history CSV for appending.

    Behaviour:
    - If file does not exist: create regime_history.csv with the new header.
    - If header matches: append.
    - If header differs: upgrade in-place to the new schema by:
        * reading all existing rows
        * re-writing file with new header
        * mapping missing fields to empty strings
      Then append.
    - On PermissionError (file locked), fall back to a new timestamped file.
    """

    def _create_file(path_to_use: Path) -> Tuple[Any, csv.DictWriter, Path]:
        f_new = path_to_use.open("a", newline="")
        writer_new = csv.DictWriter(f_new, fieldnames=fieldnames)
        writer_new.writeheader()
        return f_new, writer_new, path_to_use

    def _create_timestamp_file(base_path: Path) -> Tuple[Any, csv.DictWriter, Path]:
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
        new_path = base_path.with_name(f"{base_path.stem}_{ts}.csv")
        print(
            f"[INFO] Creating NEW history file due to permission/schema issues → {new_path}"
        )
        return _create_file(new_path)

    # 1) If file doesn't exist at all, create regime_history.csv with new schema
    if not path.exists():
        print(f"[INFO] History file {path} does not exist. Creating with schema {fieldnames}.")
        return _create_file(path)

    # 2) File exists – inspect header
    try:
        with path.open("r", newline="") as f_read:
            reader = csv.reader(f_read)
            try:
                existing_header = next(reader)
            except StopIteration:
                existing_header = []
    except PermissionError:
        print(
            f"[WARN] Cannot read header from {path} (PermissionError). "
            f"Falling back to timestamped history file."
        )
        return _create_timestamp_file(path)

    # 2a) Header already matches → simple append
    if existing_header == fieldnames:
        try:
            f_append = path.open("a", newline="")
            writer = csv.DictWriter(f_append, fieldnames=fieldnames)
            return f_append, writer, path
        except PermissionError:
            print(
                f"[WARN] Cannot append to {path} (PermissionError). "
                f"Falling back to timestamped history file."
            )
            return _create_timestamp_file(path)

    # 2b) Header differs → upgrade schema in place
    print(
        "[INFO] Detected schema change for history file.\n"
        f"Existing header: {existing_header}\n"
        f"New header     : {fieldnames}\n"
        "Upgrading existing history file in-place to the new schema."
    )

    try:
        # Read all existing rows with the old header
        with path.open("r", newline="") as f_read_all:
            reader = csv.DictReader(f_read_all)
            old_rows = list(reader)

        # Transform rows to new schema (missing fields → "")
        upgraded_rows = []
        for row in old_rows:
            new_row = {}
            for field in fieldnames:
                # Preserve existing fields when present; else blank
                new_row[field] = row.get(field, "")
            upgraded_rows.append(new_row)

        # Rewrite file with new header + upgraded rows
        with path.open("w", newline="") as f_write:
            writer = csv.DictWriter(f_write, fieldnames=fieldnames)
            writer.writeheader()
            for r in upgraded_rows:
                writer.writerow(r)

        # Reopen for append
        f_append = path.open("a", newline="")
        writer_append = csv.DictWriter(f_append, fieldnames=fieldnames)
        return f_append, writer_append, path

    except PermissionError:
        print(
            f"[WARN] Cannot rewrite {path} to upgrade schema (PermissionError). "
            f"Falling back to timestamped history file."
        )
        return _create_timestamp_file(path)
    except Exception as e:
        print(
            f"[ERROR] Unexpected error while upgrading history schema for {path}: {e}\n"
            f"Falling back to timestamped history file."
        )
        return _create_timestamp_file(path)


def log_and_compute_zscores(
    date_iso: str,
    features: Dict[str, float],
    bucket: str,
    history_file: str = HISTORY_FILE,
) -> Dict[str, float]:
    """
    Compute z-scores vs history and log the current day's features.
    """
    path = Path(history_file)
    history_rows = load_history(path)

    z_scores: Dict[str, float] = {}
    z_targets = [
        "spread7",
        "avg_alt_funding",
        "btc_funding",
        "funding_spread",
        "funding_heating",
        "funding_risk",
        "oi_change_3d",
        "breadth_3d",
        "regime_score",
    ]

    for name in z_targets:
        val = features.get(name)
        if val is None:
            z_scores[f"z_{name}"] = 0.0
            continue
        z_scores[f"z_{name}"] = compute_z_for_feature(name, float(val), history_rows)

    fieldnames = ["date_iso"] + list(features.keys()) + ["bucket"]
    row = {"date_iso": date_iso, "bucket": bucket}
    for k, v in features.items():
        row[k] = f"{float(v):.6f}"

    try:
        f, writer, used_path = _open_history_for_append(path, fieldnames)
        print(f"[INFO] Logging row for {date_iso} into {used_path}")
        with f:
            writer.writerow(row)
    except Exception as e:
        print(
            f"[ERROR] Unexpected error writing history file {history_file}: {e}\n"
            f"Skipping write for {date_iso}, but continuing."
        )

    return z_scores


# ============================================================
# Historical helpers (CoinGecko only)
# ============================================================

def compute_daily_returns_from_prices(
    dates: List[dt.date],
    price_by_date: Dict[dt.date, float],
) -> Dict[dt.date, Dict[str, Optional[float]]]:
    out: Dict[dt.date, Dict[str, Optional[float]]] = {}
    for idx, d in enumerate(dates):
        price = price_by_date.get(d)
        if price is None:
            out[d] = {"return_1d": None, "return_7d": None}
            continue

        if idx == 0:
            r1 = None
        else:
            prev_d = dates[idx - 1]
            prev_price = price_by_date.get(prev_d)
            if prev_price is None or prev_price == 0:
                r1 = None
            else:
                r1 = (price / prev_price - 1.0) * 100.0

        if idx < 7:
            r7 = None
        else:
            prev_d_7 = dates[idx - 7]
            prev_price_7 = price_by_date.get(prev_d_7)
            if prev_price_7 is None or prev_price_7 == 0:
                r7 = None
            else:
                r7 = (price / prev_price_7 - 1.0) * 100.0

        out[d] = {"return_1d": r1, "return_7d": r7}

    return out


# ============================================================
# Live mode
# ============================================================

def run_live_snapshot() -> None:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"=== Long/Short Regime Monitor (LIVE) @ {now} ===")

    prices = fetch_coingecko_prices_and_returns()

    # Live funding snapshot: BTC + alt basket
    print("Fetching Coinglass funding snapshot...")
    funding_snapshots: Dict[str, Optional[Dict[str, Any]]] = {}
    for sym in FUNDING_SYMBOLS:
        funding_snapshots[sym] = fetch_coinglass_funding_snapshot(sym)

    btc_snap = funding_snapshots.get("BTC")
    if btc_snap and "avg_funding" in btc_snap:
        f_btc_today = float(btc_snap["avg_funding"])
    else:
        f_btc_today = 0.0

    # Alt basket: average of all available funding across ALT_SYMBOLS (including negative/zero)
    alt_funding_vals: List[float] = []
    for sym in ALT_SYMBOLS:
        snap = funding_snapshots.get(sym)
        if not snap:
            continue
        try:
            f = float(snap["avg_funding"])
        except Exception:
            continue
        alt_funding_vals.append(f)

    if alt_funding_vals:
        f_alt_today = float(statistics.fmean(alt_funding_vals))
        # breadth here = share of alts with valid funding snapshots
        funding_breadth_today = len(alt_funding_vals) / len(ALT_SYMBOLS)
    else:
        f_alt_today = 0.0
        funding_breadth_today = 0.0

    # Reconstruct short funding history from regime_history.csv
    history_rows = load_history(Path(HISTORY_FILE))
    f_alt_hist: List[float] = []
    f_btc_hist: List[float] = []
    if history_rows:
        for row in history_rows[-HEATING_HISTORY_ROWS:]:
            try:
                fa_str = row.get("avg_alt_funding") or row.get("f_alt")
                fb_str = row.get("btc_funding")
                if not fa_str or not fb_str:
                    continue
                f_alt_hist.append(float(fa_str))
                f_btc_hist.append(float(fb_str))
            except Exception:
                continue

    f_alt_series = f_alt_hist + [f_alt_today]
    f_btc_series = f_btc_hist + [f_btc_today]
    funding_risk, heating = compute_heating_and_funding_risk_from_series(
        f_alt_series,
        f_btc_series,
        default_risk=0.5,  # neutral if not enough history
    )

    # BTC OI (live snapshot with 3d change)
    try:
        btc_oi = fetch_coinglass_btc_oi_snapshot(OI_SYMBOL)
    except Exception as e:
        print(f"FATAL ERROR: BTC OI error: {e}")
        return

    # Regime computation with new funding & 3d OI/breadth branches
    regime = compute_regime(
        prices=prices,
        btc_oi=btc_oi,
        funding_risk=funding_risk,
        f_alt=f_alt_today,
        f_btc=f_btc_today,
        heating=heating,
    )

    # Human-readable snapshot
    print("\n=== Snapshot ===")
    if "BTC" in prices:
        print(
            f"BTC price: ${prices['BTC']['price_usd']:.0f}, "
            f"7d return: {regime['btc_7d']:.2f}%, "
            f"1d return: {regime['btc_1d']:.2f}%"
        )
    print(f"Alt basket avg 7d return (full basket): {regime['alt_7d_avg']:.2f}%")
    print(
        f"Alt breadth 3d (share outperforming BTC): "
        f"{regime['breadth_3d']*100:.1f}%"
    )
    print(
        f"BTC OI (all exchanges): ${btc_oi['oi_usd_all']:.0f}, "
        f"72h OI change: {regime['oi_change_3d']:.2f}%"
    )
    print(
        f"Avg alt funding (stablecoin, across exchanges): "
        f"{regime['avg_alt_funding']*100:.4f}% "
        f"(coverage={funding_breadth_today:.2f})"
    )
    print(
        f"Funding spread (alts - BTC): "
        f"{regime['funding_spread']*100:.4f}%"
    )
    print(
        f"Funding heating vs BTC (10d–20d spread): "
        f"{regime['funding_heating']*100:.4f}% → "
        f"funding_risk={regime['funding_risk']:.2f}"
    )
    print(f"High-vol gate active: {regime['high_vol_gate']}")

    print("\n=== Regime (raw) ===")
    print(f"Regime score (final): {regime['regime_score']:.1f} / 100  →  {regime['bucket']}")
    print(f"Regime score (before vol gate): {regime['regime_score_raw']:.1f} / 100")
    print(
        f"Trend driver (vol-ish adjusted): "
        f"BTC 7d ({regime['btc_7d']:.2f}%) - Alt avg 7d ({regime['alt_7d_avg']:.2f}%) "
        f"= spread {regime['spread7']:.2f}%"
    )

    # Decomposition of combined signal
    print("\nDecomposition (combined space, -1..+1):")
    print(f"  Trend component       : {regime['trend_component']:.3f}")
    print(f"  Funding penalty       : -{regime['funding_penalty']:.3f} (funding_risk={regime['funding_risk']:.2f})")
    print(f"  OI penalty            : -{regime['oi_penalty']:.3f} (oi_risk={regime['oi_risk']:.2f})")
    print(f"  Breadth penalty       : -{regime['breadth_penalty']:.3f} (breadth_3d={regime['breadth_3d']:.2f})")
    print(f"  Total risk penalty    : -{regime['total_risk_penalty']:.3f}")
    print(f"  Combined raw          : {regime['combined_raw']:.3f}")
    print(f"  Combined clamped      : {regime['combined_clamped']:.3f}")

    # Log to history (with existing feature set + funding_spread)
    features_for_history = {
        "btc_1d": regime["btc_1d"],
        "btc_7d": regime["btc_7d"],
        "alt_7d_avg": regime["alt_7d_avg"],
        "spread7": regime["spread7"],
        "avg_alt_funding": regime["avg_alt_funding"],
        "btc_funding": regime["btc_funding"],
        "funding_spread": regime["funding_spread"],
        "funding_heating": regime["funding_heating"],
        "funding_risk": regime["funding_risk"],
        "oi_change_3d": regime["oi_change_3d"],
        "breadth_3d": regime["breadth_3d"],
        "regime_score": regime["regime_score"],
    }

    z_scores = log_and_compute_zscores(
        date_iso=now,
        features=features_for_history,
        bucket=regime["bucket"],
        history_file=HISTORY_FILE,
    )

    print("\n=== Z-scores vs history (feature -> z) ===")
    for name, z in z_scores.items():
        print(f"{name}: {z:+.2f}")
    print("==========================================\n")


# ============================================================
# Historical/backfill mode (with funding + OI)
# ============================================================

def run_historical_backfill(
    lookback_days: int = HISTORICAL_LOOKBACK_DAYS,
) -> None:
    print(f"=== Long/Short Regime Monitor (HISTORICAL) last {lookback_days} days ===")

    btc_dates, symbol_price_by_date = fetch_coingecko_history_prices(
        days=lookback_days,
        extra_days_for_lookback=7,
    )

    # If BTC history is missing even after retry, abort gracefully
    if not btc_dates:
        print("[ERROR] Historical mode aborted: no BTC price history from CoinGecko.")
        return

    dates = btc_dates
    if len(dates) > lookback_days + 7:
        dates = dates[-(lookback_days + 7):]

    symbol_returns_by_date: Dict[str, Dict[dt.date, Dict[str, Optional[float]]]] = {}
    for sym in HISTORICAL_SYMBOLS:
        price_map = symbol_price_by_date.get(sym, {})
        symbol_returns_by_date[sym] = compute_daily_returns_from_prices(dates, price_map)

    if len(dates) <= 7:
        print("[WARN] Not enough days to compute 7d returns; aborting historical mode.")
        return

    usable_dates = dates[7:]
    if len(usable_dates) > lookback_days:
        usable_dates = usable_dates[-lookback_days:]

    # Coinglass time range for history (cover entire date span)
    start_date = dates[0]
    end_date = dates[-1]
    start_dt = dt.datetime(start_date.year, start_date.month, start_date.day, tzinfo=dt.timezone.utc)
    end_dt = dt.datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=dt.timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    print("Historical mode: fetching CoinGlass funding history...")
    funding_history_by_symbol: Dict[str, Dict[dt.date, float]] = {}
    for sym in HISTORICAL_SYMBOLS:
        fh = fetch_coinglass_funding_history_symbol(sym, start_ms=start_ms, end_ms=end_ms)
        if fh:
            funding_history_by_symbol[sym] = fh

    print("Historical mode: fetching CoinGlass BTC OI history...")
    btc_oi_close_by_date = fetch_coinglass_btc_oi_history(
        symbol=OI_SYMBOL,
        start_ms=start_ms,
        end_ms=end_ms,
    )

    # Compute approx 3d (72h) OI change %
    btc_oi_change_3d_by_date: Dict[dt.date, float] = {}
    sorted_oi_dates = sorted(btc_oi_close_by_date.keys())
    for idx, d in enumerate(sorted_oi_dates):
        cur_val = btc_oi_close_by_date.get(d, 0.0)
        if idx < 3:
            btc_oi_change_3d_by_date[d] = 0.0
        else:
            prev_d = sorted_oi_dates[idx - 3]
            prev_val = btc_oi_close_by_date.get(prev_d, 0.0)
            if prev_val > 0:
                btc_oi_change_3d_by_date[d] = (cur_val / prev_val - 1.0) * 100.0
            else:
                btc_oi_change_3d_by_date[d] = 0.0

    # Precompute daily f_alt and f_btc over the full date range
    f_alt_by_date: Dict[dt.date, float] = {}
    f_btc_by_date: Dict[dt.date, float] = {}
    for d in dates:
        # BTC daily funding
        f_btc_by_date[d] = funding_history_by_symbol.get("BTC", {}).get(d, 0.0)

        # Alt basket: average of all available funding across ALT_SYMBOLS (including negative/zero)
        alt_vals: List[float] = []
        for sym in ALT_SYMBOLS:
            daily_f = funding_history_by_symbol.get(sym, {}).get(d)
            if daily_f is None:
                continue
            alt_vals.append(daily_f)
        f_alt_by_date[d] = float(statistics.fmean(alt_vals)) if alt_vals else 0.0

    # Series used to compute heating incrementally
    f_alt_series: List[float] = []
    f_btc_series: List[float] = []

    print(f"Historical mode: reconstructing regimes for {len(usable_dates)} days...")
    for d in usable_dates:
        prices_today: Dict[str, Dict[str, float]] = {}
        for sym in HISTORICAL_SYMBOLS:
            rmap = symbol_returns_by_date.get(sym, {})
            ret_data = rmap.get(d)
            if not ret_data:
                continue
            r1 = ret_data.get("return_1d")
            r7 = ret_data.get("return_7d")
            if r1 is None or r7 is None:
                continue
            price_map = symbol_price_by_date.get(sym, {})
            price_val = price_map.get(d)
            if price_val is None:
                continue
            prices_today[sym] = {
                "price_usd": price_val,
                "return_1d": r1,
                "return_7d": r7,
            }

        if "BTC" not in prices_today:
            print(f"[WARN] Skipping {d} (BTC returns missing)")
            continue

        # funding for this day
        f_alt_today = f_alt_by_date.get(d, 0.0)
        f_btc_today = f_btc_by_date.get(d, 0.0)
        f_alt_series.append(f_alt_today)
        f_btc_series.append(f_btc_today)
        funding_risk, heating = compute_heating_and_funding_risk_from_series(
            f_alt_series,
            f_btc_series,
            default_risk=0.5,  # neutral early in the history
        )

        btc_oi_snapshot = {
            "oi_usd_all": btc_oi_close_by_date.get(d, 0.0),
            "oi_change_3d_pct": btc_oi_change_3d_by_date.get(d, 0.0),
        }

        regime = compute_regime(
            prices=prices_today,
            btc_oi=btc_oi_snapshot,
            funding_risk=funding_risk,
            f_alt=f_alt_today,
            f_btc=f_btc_today,
            heating=heating,
        )

        features_for_history = {
            "btc_1d": regime["btc_1d"],
            "btc_7d": regime["btc_7d"],
            "alt_7d_avg": regime["alt_7d_avg"],
            "spread7": regime["spread7"],
            "avg_alt_funding": regime["avg_alt_funding"],
            "btc_funding": regime["btc_funding"],
            "funding_spread": regime["funding_spread"],
            "funding_heating": regime["funding_heating"],
            "funding_risk": regime["funding_risk"],
            "oi_change_3d": regime["oi_change_3d"],
            "breadth_3d": regime["breadth_3d"],
            "regime_score": regime["regime_score"],
        }

        date_iso = dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).isoformat()
        z_scores = log_and_compute_zscores(
            date_iso=date_iso,
            features=features_for_history,
            bucket=regime["bucket"],
            history_file=HISTORY_FILE,
        )

        print(
            f"{date_iso} -> Regime {regime['regime_score']:.1f} ({regime['bucket']}), "
            f"spread7={regime['spread7']:.2f}%, "
            f"avg_alt_funding={regime['avg_alt_funding']*100:.4f}%, "
            f"btc_funding={regime['btc_funding']*100:.4f}%, "
            f"funding_spread={regime['funding_spread']*100:.4f}%, "
            f"heating={regime['funding_heating']*100:.4f}%, "
            f"funding_risk={regime['funding_risk']:.2f}, "
            f"z_regime={z_scores.get('z_regime_score', 0.0):+.2f}"
        )

    print("=== Historical backfill complete ===")


# ============================================================
# Backfill missing dates only
# ============================================================

def run_backfill_missing(
    history_file: str = HISTORY_FILE,
    end_date: Optional[dt.date] = None,
) -> None:
    """
    Backfill ALL missing dates in the history file, including gaps in the middle of the data.
    This is more efficient than full historical backfill as it only fetches missing data.
    """
    print("=== Long/Short Regime Monitor (BACKFILL MISSING) ===")
    
    last_date, missing_dates = find_missing_dates(history_file, end_date)
    
    if not missing_dates:
        print("[INFO] No missing dates to backfill.")
        return
    
    if last_date is None:
        print(
            "[INFO] No existing history found. "
            "Use 'historical' mode for initial backfill."
        )
        return
    
    # We need at least 7 days before the first missing date to compute 7d returns
    # Find the earliest missing date (could be in the middle of existing data)
    earliest_missing = min(missing_dates)
    latest_missing = max(missing_dates)
    
    # Fetch from 7 days before earliest missing to latest missing
    fetch_start_date = earliest_missing - dt.timedelta(days=7)
    fetch_end_date = latest_missing
    
    # Calculate days to fetch from CoinGecko (from today backwards)
    # CoinGecko fetches last N days, so we need days from today to fetch_start_date
    today = dt.datetime.now(dt.timezone.utc).date()
    days_to_fetch = (today - fetch_start_date).days + 7  # +7 buffer
    
    print(f"Fetching data from {fetch_start_date} to {fetch_end_date} ({days_to_fetch} days from today)...")
    
    # Fetch CoinGecko prices for the required range (last N days from today)
    btc_dates, symbol_price_by_date = fetch_coingecko_history_prices(
        days=days_to_fetch,
        extra_days_for_lookback=0,  # We already accounted for it
    )
    
    if not btc_dates:
        print("[ERROR] Backfill aborted: no BTC price history from CoinGecko.")
        return
    
    # Filter to only dates we need (fetch_start_date to fetch_end_date)
    needed_dates = [d for d in btc_dates if fetch_start_date <= d <= fetch_end_date]
    
    if not needed_dates:
        print("[ERROR] No dates in the required range from CoinGecko.")
        return
    
    # Compute returns for all needed dates
    symbol_returns_by_date: Dict[str, Dict[dt.date, Dict[str, Optional[float]]]] = {}
    for sym in HISTORICAL_SYMBOLS:
        price_map = symbol_price_by_date.get(sym, {})
        symbol_returns_by_date[sym] = compute_daily_returns_from_prices(needed_dates, price_map)
    
    # Coinglass time range
    start_dt = dt.datetime(
        needed_dates[0].year, needed_dates[0].month, needed_dates[0].day,
        tzinfo=dt.timezone.utc
    )
    end_dt = dt.datetime(
        needed_dates[-1].year, needed_dates[-1].month, needed_dates[-1].day,
        23, 59, 59, tzinfo=dt.timezone.utc
    )
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    
    print("Fetching CoinGlass funding history for missing period...")
    funding_history_by_symbol: Dict[str, Dict[dt.date, float]] = {}
    for sym in HISTORICAL_SYMBOLS:
        fh = fetch_coinglass_funding_history_symbol(sym, start_ms=start_ms, end_ms=end_ms)
        if fh:
            funding_history_by_symbol[sym] = fh
    
    print("Fetching CoinGlass BTC OI history for missing period...")
    btc_oi_close_by_date = fetch_coinglass_btc_oi_history(
        symbol=OI_SYMBOL,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    
    # Compute 3d OI change
    btc_oi_change_3d_by_date: Dict[dt.date, float] = {}
    sorted_oi_dates = sorted(btc_oi_close_by_date.keys())
    for idx, d in enumerate(sorted_oi_dates):
        cur_val = btc_oi_close_by_date.get(d, 0.0)
        if idx < 3:
            btc_oi_change_3d_by_date[d] = 0.0
        else:
            prev_d = sorted_oi_dates[idx - 3]
            prev_val = btc_oi_close_by_date.get(prev_d, 0.0)
            if prev_val > 0:
                btc_oi_change_3d_by_date[d] = (cur_val / prev_val - 1.0) * 100.0
            else:
                btc_oi_change_3d_by_date[d] = 0.0
    
    # Precompute daily f_alt and f_btc
    f_alt_by_date: Dict[dt.date, float] = {}
    f_btc_by_date: Dict[dt.date, float] = {}
    for d in needed_dates:
        f_btc_by_date[d] = funding_history_by_symbol.get("BTC", {}).get(d, 0.0)
        alt_vals: List[float] = []
        for sym in ALT_SYMBOLS:
            daily_f = funding_history_by_symbol.get(sym, {}).get(d)
            if daily_f is not None:
                alt_vals.append(daily_f)
        f_alt_by_date[d] = float(statistics.fmean(alt_vals)) if alt_vals else 0.0
    
    # Load existing history to build funding series for heating calculation
    existing_history = load_history(Path(history_file))
    
    # Build a date->funding map from existing history for quick lookup
    existing_funding_by_date: Dict[dt.date, Tuple[float, float]] = {}
    if existing_history:
        for row in existing_history:
            date_str = row.get("date_iso", "")
            if not date_str:
                continue
            try:
                if "T" in date_str:
                    date_dt = dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    d = date_dt.date()
                else:
                    d = dt.datetime.fromisoformat(date_str).date()
                fa_str = row.get("avg_alt_funding") or row.get("f_alt")
                fb_str = row.get("btc_funding")
                if fa_str and fb_str:
                    existing_funding_by_date[d] = (float(fa_str), float(fb_str))
            except Exception:
                continue
    
    # Process missing dates in chronological order
    missing_dates_sorted = sorted(missing_dates)
    print(f"Backfilling {len(missing_dates_sorted)} missing dates (in chronological order)...")
    
    for d in missing_dates_sorted:
        # Build prices dict for this date
        prices_today: Dict[str, Dict[str, float]] = {}
        for sym in HISTORICAL_SYMBOLS:
            rmap = symbol_returns_by_date.get(sym, {})
            ret_data = rmap.get(d)
            if not ret_data:
                continue
            r1 = ret_data.get("return_1d")
            r7 = ret_data.get("return_7d")
            if r1 is None or r7 is None:
                continue
            price_map = symbol_price_by_date.get(sym, {})
            price_val = price_map.get(d)
            if price_val is None:
                continue
            prices_today[sym] = {
                "price_usd": price_val,
                "return_1d": r1,
                "return_7d": r7,
            }
        
        if "BTC" not in prices_today:
            print(f"[WARN] Skipping {d} (BTC returns missing)")
            continue
        
        # Funding for this day
        f_alt_today = f_alt_by_date.get(d, 0.0)
        f_btc_today = f_btc_by_date.get(d, 0.0)
        
        # Build funding series up to this date for heating calculation
        # Include: existing dates before this date + already processed missing dates + current date
        f_alt_series: List[float] = []
        f_btc_series: List[float] = []
        
        # Get all dates up to and including current missing date
        all_dates_up_to_now = sorted([date for date in existing_funding_by_date.keys() if date < d])
        all_dates_up_to_now.append(d)
        
        # Add existing funding data
        for date in all_dates_up_to_now[:-1]:  # All except the last (current missing date)
            if date in existing_funding_by_date:
                f_alt, f_btc = existing_funding_by_date[date]
                f_alt_series.append(f_alt)
                f_btc_series.append(f_btc)
        
        # Add current missing date's funding
        f_alt_series.append(f_alt_today)
        f_btc_series.append(f_btc_today)
        
        # If we don't have enough history, pad with zeros or use default
        if len(f_alt_series) < H_LONG:
            # Try to get more history if available
            all_existing_dates = sorted(existing_funding_by_date.keys())
            if all_existing_dates:
                # Get last H_LONG dates before current missing date
                dates_before = [date for date in all_existing_dates if date < d]
                dates_to_use = dates_before[-H_LONG:] if len(dates_before) >= H_LONG else dates_before
                f_alt_series = []
                f_btc_series = []
                for date in dates_to_use:
                    f_alt, f_btc = existing_funding_by_date[date]
                    f_alt_series.append(f_alt)
                    f_btc_series.append(f_btc)
                f_alt_series.append(f_alt_today)
                f_btc_series.append(f_btc_today)
        
        funding_risk, heating = compute_heating_and_funding_risk_from_series(
            f_alt_series,
            f_btc_series,
            default_risk=0.5,
        )
        
        # Store this date's funding for future missing dates in this run
        existing_funding_by_date[d] = (f_alt_today, f_btc_today)
        
        btc_oi_snapshot = {
            "oi_usd_all": btc_oi_close_by_date.get(d, 0.0),
            "oi_change_3d_pct": btc_oi_change_3d_by_date.get(d, 0.0),
        }
        
        regime = compute_regime(
            prices=prices_today,
            btc_oi=btc_oi_snapshot,
            funding_risk=funding_risk,
            f_alt=f_alt_today,
            f_btc=f_btc_today,
            heating=heating,
        )
        
        features_for_history = {
            "btc_1d": regime["btc_1d"],
            "btc_7d": regime["btc_7d"],
            "alt_7d_avg": regime["alt_7d_avg"],
            "spread7": regime["spread7"],
            "avg_alt_funding": regime["avg_alt_funding"],
            "btc_funding": regime["btc_funding"],
            "funding_spread": regime["funding_spread"],
            "funding_heating": regime["funding_heating"],
            "funding_risk": regime["funding_risk"],
            "oi_change_3d": regime["oi_change_3d"],
            "breadth_3d": regime["breadth_3d"],
            "regime_score": regime["regime_score"],
        }
        
        date_iso = dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).isoformat()
        z_scores = log_and_compute_zscores(
            date_iso=date_iso,
            features=features_for_history,
            bucket=regime["bucket"],
            history_file=history_file,
        )
        
        print(
            f"{date_iso} -> Regime {regime['regime_score']:.1f} ({regime['bucket']}), "
            f"spread7={regime['spread7']:.2f}%, "
            f"funding_risk={regime['funding_risk']:.2f}"
        )
    
    print(f"=== Backfill complete: added {len(missing_dates)} days ===")


# ============================================================
# Extract last N days from existing history
# ============================================================

def extract_last_n_days(
    n_days: int = 365,
    input_file: str = HISTORY_FILE,
    output_file: Optional[str] = None,
) -> None:
    """
    Extract the last N days from regime_history.csv and write to a new file.
    If output_file is None, creates regime_history_365d.csv (or similar).
    """
    path = Path(input_file)
    if not path.exists():
        print(f"[ERROR] History file {input_file} does not exist.")
        return

    history_rows = load_history(path)
    if not history_rows:
        print(f"[ERROR] No data found in {input_file}.")
        return

    # Sort by date (in case it's not already sorted)
    try:
        history_rows.sort(key=lambda r: r.get("date_iso", ""))
    except Exception:
        pass

    # Get last N days
    last_n = history_rows[-n_days:] if len(history_rows) >= n_days else history_rows

    if len(last_n) < n_days:
        print(
            f"[WARN] Only {len(last_n)} days available in history, "
            f"requested {n_days} days."
        )

    # Determine output filename
    if output_file is None:
        output_file = f"regime_history_{n_days}d.csv"
    output_path = Path(output_file)

    # Get fieldnames from first row (or use all keys from last row)
    if last_n:
        fieldnames = list(last_n[0].keys())
    else:
        print("[ERROR] No rows to extract.")
        return

    # Write to output file
    try:
        with output_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in last_n:
                writer.writerow(row)
        print(
            f"[SUCCESS] Extracted {len(last_n)} days to {output_file}\n"
            f"  Date range: {last_n[0].get('date_iso', 'N/A')} to {last_n[-1].get('date_iso', 'N/A')}"
        )
    except Exception as e:
        print(f"[ERROR] Failed to write {output_file}: {e}")


# ============================================================
# Main
# ============================================================

def main():
    """
    CLI entry:
      - default: live snapshot
      - historical [days]: backfill using optional days arg (defaults to HISTORICAL_LOOKBACK_DAYS)
      - backfill or update: backfill only missing dates between last entry and today
      - extract [days] [output_file]: extract last N days from existing history (defaults to 365)
      - 365: shortcut for extract 365
    """
    mode = "live"
    lookback = HISTORICAL_LOOKBACK_DAYS
    extract_days = 365
    output_file = None

    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg.startswith("hist"):
            mode = "historical"
            if len(sys.argv) > 2:
                try:
                    lookback = int(sys.argv[2])
                except ValueError:
                    pass  # fall back to default
        elif arg == "backfill" or arg == "update":
            mode = "backfill"
        elif arg == "extract" or arg == "365":
            mode = "extract"
            if arg == "365":
                extract_days = 365
            elif len(sys.argv) > 2:
                try:
                    extract_days = int(sys.argv[2])
                except ValueError:
                    pass
            if len(sys.argv) > 3:
                output_file = sys.argv[3]

    if mode == "historical":
        run_historical_backfill(lookback)
    elif mode == "backfill":
        run_backfill_missing()
    elif mode == "extract":
        extract_last_n_days(n_days=extract_days, output_file=output_file)
    else:
        run_live_snapshot()


if __name__ == "__main__":
    main()
