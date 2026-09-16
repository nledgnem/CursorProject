"""Daily feature panel for Track A (one asset, one trend system).

No-look-ahead contract: every column stamped t uses information available at the
close of UTC day t. Volatility / ATR / z-score denominators use data up to t-1 only.
Derivatives data are exchange-native daily buckets stamped by UTC open date, so the
bar for day t closes with the spot close of day t. Macro series are lagged one day.

Features are stored UNSIGNED here; `signed_features` direction-adjusts them at event
time so that positive always means "stronger confirmation of the attempted transition".
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE.parents[2] / "btc_confirmation_lag"))

from qlib import data as D  # noqa: E402
from qlib.paths import CACHE, REPO  # noqa: E402
from signals import base_features, trend_signal  # noqa: E402  (reused from the earlier study)

ASSETS = {
    "BTC": dict(coinbase="BTC-USD", perp="BTCUSDT", dvol="BTC", cg="BTC"),
    "ETH": dict(coinbase="ETH-USD", perp="ETHUSDT", dvol="ETH", cg="ETH"),
    "SOL": dict(coinbase="SOL-USD", perp="SOLUSDT", dvol=None, cg="SOL"),
}
SYSTEMS = {
    "GERHARD_SMA120": dict(kind="sma", len=120, neutral_flips=True),
    "LL3_k0.30": dict(kind="ll", fast=32, slow=58, atr_len=60, k=0.30, neutral_flips=True),
    "LL2_k0.30": dict(kind="ll", fast=32, slow=58, atr_len=60, k=0.30, neutral_flips=False),
}
TREND_HORIZONS = (10, 20, 30, 60, 90, 120, 200, 365)
RANGE_WINDOWS = (20, 60, 120)


def _z_prior(x: pd.Series, window: int) -> pd.Series:
    return (x - x.rolling(window).mean().shift(1)) / x.rolling(window).std().shift(1)


def load_price(asset: str) -> pd.DataFrame:
    px = D.coinbase_daily(ASSETS[asset]["coinbase"]).set_index("ts").sort_index()
    full = pd.date_range(px.index.min(), px.index.max(), freq="D")
    px = px.reindex(full)
    px["close"] = px["close"].ffill()
    for c in ("open", "high", "low"):
        px[c] = px[c].fillna(px["close"])
    px["volume"] = px["volume"].fillna(0.0)
    return px


def _daily(df: pd.DataFrame, cols, how="last") -> pd.DataFrame:
    x = df.copy()
    x["day"] = x["ts"].dt.floor("D")
    return getattr(x.groupby("day")[cols], how)()


def market_data(asset: str, idx: pd.DatetimeIndex) -> pd.DataFrame:
    spec = ASSETS[asset]
    m = pd.DataFrame(index=idx)
    try:
        perp = D.binance_klines(spec["perp"], "1d", "um").set_index("ts")
        m["perp_qv"] = perp["quote_volume"]
        m["perp_taker_share"] = perp["taker_buy_quote"] / perp["quote_volume"]
    except Exception:
        pass
    try:
        spot = D.binance_klines(spec["perp"], "1d", "spot").set_index("ts")
        m["spot_qv"] = spot["quote_volume"]
    except Exception:
        pass
    try:
        fund = D.binance_funding(spec["perp"])
        daily = _daily(fund, ["rate"], "sum")
        m["funding_daily"] = daily["rate"]                 # decimal per day (sum of prints settling that UTC day)
    except Exception:
        pass
    try:
        prem = D.binance_premium_index(spec["perp"], "1d").set_index("ts")
        m["premium"] = prem["prem_close"]
    except Exception:
        pass
    for name, fn, col in (("liq", D.coinglass_liquidations, None), ("oi", D.coinglass_oi, None)):
        p = CACHE / "coinglass" / ("liquidations_1d" if name == "liq" else "oi_1d") / f"{spec['cg']}.parquet"
        if p.exists():
            x = pd.read_parquet(p).set_index("ts")
            if name == "liq":
                m["liq_long_usd"], m["liq_short_usd"] = x["long_liq_usd"], x["short_liq_usd"]
            else:
                m["oi_usd"] = x["oi_usd"]
    if spec["dvol"]:
        try:
            m["dvol"] = D.deribit_dvol(spec["dvol"]).set_index("ts")["dvol"]
        except Exception:
            pass
    macro = REPO / "research" / "btc_short_squeeze" / "data" / "macro.parquet"
    if macro.exists():
        mac = pd.read_parquet(macro).set_index("date").sort_index()
        spx = mac["spx"].reindex(pd.date_range(mac.index.min(), idx.max(), freq="D")).ffill()
        m["spx_ret20_lag1"] = (spx / spx.shift(20) - 1).shift(1).reindex(idx)
    return m


def breadth(idx: pd.DatetimeIndex, min_names: int = 30) -> pd.DataFrame:
    """Cross-sectional breadth of Binance USDT perps (survivorship-free): share above own 20d SMA,
    share with positive 30d return. Needs backfill stage 1."""
    d = CACHE / "binance" / "klines_um_1d"
    files = list(d.glob("*.parquet")) if d.exists() else []
    if not files:
        return pd.DataFrame(index=idx)
    closes = {}
    non_crypto = D.binance_non_crypto_symbols()      # TradFi perps (equities, commodities) are not crypto breadth
    for f in files:
        if f.stem in non_crypto:
            continue
        raw = pd.read_parquet(f, columns=["ts", "high", "low", "close", "quote_volume"])
        clean, _ = D.clean_klines(raw)                 # drop frozen post-delisting candles
        x = clean.set_index("ts")["close"]
        if len(x) > 40:
            closes[f.stem] = x
    wide = pd.DataFrame(closes).sort_index()
    above = (wide > wide.rolling(20).mean()).where(wide.rolling(20).count() >= 20)
    up30 = (wide / wide.shift(30) - 1 > 0).where(wide.shift(30).notna())
    cnt = above.notna().sum(axis=1)
    out = pd.DataFrame({"breadth_above20": above.mean(axis=1), "breadth_up30": up30.mean(axis=1),
                        "breadth_names": cnt})
    out.loc[cnt < min_names, ["breadth_above20", "breadth_up30"]] = np.nan
    return out.reindex(idx)


def build_panel(asset: str, system: str, with_breadth: bool = True) -> pd.DataFrame:
    px = load_price(asset)
    spec = SYSTEMS[system]
    f = base_features(px)
    side, line, up, dn = trend_signal(px, spec)
    f["side"], f["line"], f["brk_up"], f["brk_dn"] = side, line, up, dn
    f["sma_slope"] = line / line.shift(10) - 1                   # slope of the trend line over 10d
    c, h, l = f["close"], f["high"], f["low"]

    f["ret_2d"] = np.log(c / c.shift(2))
    f["ret2_z20"] = f["ret_2d"] / (f["sig20"] * np.sqrt(2))
    for n in RANGE_WINDOWS:
        f[f"brkout_up_{n}"] = (c - h.rolling(n).max().shift(1)) / f["atr_prior"]
        f[f"brkout_dn_{n}"] = (l.rolling(n).min().shift(1) - c) / f["atr_prior"]
    f["gap_from_high90"] = c / h.rolling(90).max() - 1
    f["gap_from_low90"] = c / l.rolling(90).min() - 1
    for hz in TREND_HORIZONS:
        f[f"trend_{hz}"] = np.sign(c / c.shift(hz) - 1)

    m = market_data(asset, f.index)
    f = f.join(m)
    if "spot_qv" in f:
        f["spot_vol_z"] = _z_prior(np.log(f["spot_qv"].replace(0, np.nan)), 60)
    if "perp_qv" in f:
        f["perp_vol_z"] = _z_prior(np.log(f["perp_qv"].replace(0, np.nan)), 60)
        f["liquidity_regime"] = np.where(f["perp_qv"].rolling(60).median() >
                                         f["perp_qv"].rolling(60).median().expanding(180).median(), "high", "low")
    if "funding_daily" in f:
        f["funding_ann"] = f["funding_daily"] * 365
        f["funding_z90"] = _z_prior(f["funding_daily"], 90)
    if "premium" in f:
        f["premium_z90"] = _z_prior(f["premium"], 90)
    if "oi_usd" in f:
        f["oi_chg_1d"] = np.log(f["oi_usd"] / f["oi_usd"].shift(1))
        f["oi_chg_7d"] = np.log(f["oi_usd"] / f["oi_usd"].shift(7))
    if "liq_long_usd" in f and "oi_usd" in f:
        f["liq_long_oi"] = f["liq_long_usd"] / f["oi_usd"].shift(1)
        f["liq_short_oi"] = f["liq_short_usd"] / f["oi_usd"].shift(1)
    if "dvol" in f:
        f["dvol_chg5"] = f["dvol"] / f["dvol"].shift(5) - 1
        f["rv_minus_iv"] = f["sig20"] * np.sqrt(365) * 100 - f["dvol"].shift(1)
    if with_breadth:
        f = f.join(breadth(f.index))
    f["weekend"] = (f.index.dayofweek >= 5).astype(int)
    if "spx_ret20_lag1" in f:
        f["macro_risk_on"] = np.where(f["spx_ret20_lag1"].isna(), np.nan, (f["spx_ret20_lag1"] > 0).astype(float))
    f["asset"] = asset
    return f


SIGNED = {"ret_z10": "ret_z10_raw", "ret_z20": "ret_z20_raw", "ret_z30": "ret_z30_raw", "ret_z60": "ret_z60_raw",
          "move_atr": "move_atr_raw", "ret2_z20": "ret2_z20", "mom30": "mom30", "mom90": "mom90",
          "sma_slope": "sma_slope", "funding_z90": "funding_z90", "premium_z90": "premium_z90",
          "oi_chg_1d": "oi_chg_1d", "oi_chg_7d": "oi_chg_7d", "spx_ret20": "spx_ret20_lag1"}
CENTERED = {"perp_taker_share": 0.5, "breadth_above20": 0.5, "breadth_up30": 0.5}
UNSIGNED = ["tr_atr", "range_atr", "spot_vol_z", "perp_vol_z", "vol_pct", "dvol", "dvol_chg5", "rv_minus_iv",
            "funding_ann", "premium", "weekend", "macro_risk_on", "gap_from_high90", "gap_from_low90"]
CATEGORICAL = ["vol_regime", "bull_env", "liquidity_regime"]


def signed_frame(f: pd.DataFrame, d: int) -> pd.DataFrame:
    """Direction-adjusted features for every day, for a transition in price direction d (+1 up / -1 down).

    Positive values = stronger confirmation of a move in direction d.
    Break strength comes in two flavours:
      *_toward : distance beyond the threshold of the state being ENTERED (to_long / to_short);
      *_leave  : distance back through the threshold of the state being LEFT (to_neutral).
    For a two-state SMA signal the two coincide.
    """
    out = pd.DataFrame(index=f.index)
    for k, col in SIGNED.items():
        if col in f:
            out[k] = d * f[col]
    for k, centre in CENTERED.items():
        if k in f:
            out[k] = d * (f[k] - centre)
    for c in UNSIGNED:
        if c in f:
            out[c] = f[c]
    toward = f["brk_up"] if d > 0 else f["brk_dn"]
    leave = -f["brk_dn"] if d > 0 else -f["brk_up"]
    for name, brk in (("toward", toward), ("leave", leave)):
        out[f"brk_atr_{name}"] = brk / f["atr_prior"]
        out[f"brk_vol_{name}"] = brk / f["line"] / f["sig20"]
    for n in RANGE_WINDOWS:
        out[f"range_breakout_{n}"] = f[f"brkout_up_{n}"] if d > 0 else f[f"brkout_dn_{n}"]
    tr = f[[f"trend_{h}" for h in TREND_HORIZONS]]
    agree = (np.sign(tr) == d).astype(float).where(tr.notna())
    out["trend_agree_count"] = agree.sum(axis=1, min_count=1)
    out["trend_agree_share"] = agree.mean(axis=1)
    if "liq_long_oi" in f:
        # liquidations of the side squeezed by the move: shorts on up-moves, longs on down-moves
        out["liq_forced_oi"] = f["liq_short_oi"] if d > 0 else f["liq_long_oi"]
    for c in CATEGORICAL:
        if c in f:
            out[c] = f[c]
    return out


def brk_column(feature: str, target: int) -> str:
    """Resolve 'brk_vol' / 'brk_atr' to the leave/toward variant appropriate for the target state."""
    if feature in ("brk_vol", "brk_atr"):
        return f"{feature}_{'leave' if target == 0 else 'toward'}"
    return feature
