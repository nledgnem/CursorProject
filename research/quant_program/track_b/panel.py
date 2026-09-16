"""Survivorship-free daily panel of Binance USDT perpetuals for Track B.

Build rules
-----------
* Universe: every USDT perp that ever traded (current + delisted), from backfill stage 1.
* Frozen post-delisting candles are truncated (qlib.data.clean_klines); the inferred
  delisting date is kept as a column.
* Stablecoin, fiat and index contracts are excluded (they are not directional crypto risk).
* Eligibility is point-in-time: listed >= 30 days and trailing 30-day median quote volume
  (known at t-1) >= MIN_ADV_USD. No future information decides membership.
* Funding: prints settling during UTC day t are summed into funding_day (decimal); the
  funding interval is inferred from print spacing (Binance shortens it under stress).
* All rolling statistics use data up to t-1 unless the feature is the day-t value itself.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from qlib import data as D
from qlib.paths import CACHE, TABLES

OUT = TABLES / "track_b"
OUT.mkdir(parents=True, exist_ok=True)
MIN_ADV_USD = 5e6
MIN_LISTED_DAYS = 30
EXCLUDE_BASES = {"USDC", "BUSD", "TUSD", "FDUSD", "USDP", "DAI", "EUR", "GBP", "AEUR", "BTCDOM", "DEFI",
                 "BLUEBIRD", "FOOTBALL", "USDE", "XUSD", "BFUSD", "USD1", "RLUSD", "PAXG", "XAUT", "XAU", "XAG"}


def base_of(symbol: str) -> str:
    b = symbol[:-4] if symbol.endswith("USDT") else symbol
    for p in ("1000000", "1000"):
        if b.startswith(p) and len(b) > len(p):
            return b[len(p):]
    return b


def load_klines(min_days: int = 60) -> tuple[pd.DataFrame, pd.DataFrame]:
    frames, meta, excluded = [], [], []
    non_crypto = D.binance_non_crypto_symbols()
    for f in sorted((CACHE / "binance" / "klines_um_1d").glob("*.parquet")):
        sym = f.stem
        if base_of(sym) in EXCLUDE_BASES or sym in non_crypto:
            excluded.append({"asset": sym, "reason": "stable/index base" if base_of(sym) in EXCLUDE_BASES
                             else "non-crypto underlying (TradFi perp)"})
            continue
        raw = pd.read_parquet(f)
        clean, delist = D.clean_klines(raw)
        if len(clean) < min_days:
            continue
        clean = clean.assign(asset=sym, base=base_of(sym))
        frames.append(clean[["ts", "asset", "base", "open", "high", "low", "close", "quote_volume", "taker_buy_quote"]])
        meta.append({"asset": sym, "base": base_of(sym), "first_ts": clean["ts"].min(), "last_ts": clean["ts"].max(),
                     "delist_ts": delist, "frozen_bars_removed": len(raw) - len(clean)})
    pd.DataFrame(excluded).to_csv(OUT / "excluded_symbols.csv", index=False)
    return pd.concat(frames, ignore_index=True), pd.DataFrame(meta)


def load_funding(assets) -> pd.DataFrame:
    rows = []
    for s in assets:
        p = CACHE / "binance" / "funding" / f"{s}.parquet"
        if not p.exists():
            continue
        f = pd.read_parquet(p).sort_values("ts")
        f["day"] = f["ts"].dt.floor("D")
        f["gap_h"] = f["ts"].diff().dt.total_seconds() / 3600
        g = f.groupby("day").agg(funding_day=("rate", "sum"), n_prints=("rate", "size"),
                                 max_abs_print=("rate", lambda x: x.abs().max()), interval_h=("gap_h", "median"))
        g["asset"] = s
        rows.append(g.reset_index().rename(columns={"day": "ts"}))
    return pd.concat(rows, ignore_index=True)


def load_premium(assets) -> pd.DataFrame:
    rows = []
    for s in assets:
        p = CACHE / "binance" / "premium_1d" / f"{s}.parquet"
        if p.exists():
            rows.append(pd.read_parquet(p, columns=["ts", "prem_close"]).assign(asset=s))
    return pd.concat(rows, ignore_index=True)


def load_coinglass(bases) -> pd.DataFrame:
    rows = []
    for b in bases:
        liq = CACHE / "coinglass" / "liquidations_1d" / f"{b}.parquet"
        oi = CACHE / "coinglass" / "oi_1d" / f"{b}.parquet"
        if liq.exists() and oi.exists():
            x = pd.read_parquet(liq).merge(pd.read_parquet(oi), on="ts", how="outer").assign(base=b)
            rows.append(x)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["ts", "base"])


def _grp_roll(s: pd.Series, by: pd.Series, window: int, fn: str, shift: int = 1, min_periods=None) -> pd.Series:
    mp = min_periods or max(5, window // 3)
    return s.groupby(by).transform(lambda x: getattr(x.rolling(window, min_periods=mp), fn)().shift(shift))


def build_panel(save: bool = True) -> pd.DataFrame:
    k, meta = load_klines()
    k = k.sort_values(["asset", "ts"]).reset_index(drop=True)
    a = k["asset"]
    k["ret"] = k.groupby("asset")["close"].pct_change()
    k["lret"] = np.log1p(k["ret"])
    k["sig20"] = _grp_roll(k["lret"], a, 20, "std")
    k["ret_z"] = k["lret"] / k["sig20"]
    for n in (3, 7, 30):
        k[f"mom_{n}d"] = k.groupby("asset")["close"].transform(lambda x: x / x.shift(n) - 1)
    k["mom_3d_z"] = np.log1p(k["mom_3d"]) / (k["sig20"] * np.sqrt(3))
    k["adv30"] = _grp_roll(k["quote_volume"], a, 30, "median")
    k["days_listed"] = k.groupby("asset").cumcount()
    k["eligible"] = (k["days_listed"] >= MIN_LISTED_DAYS) & (k["adv30"] >= MIN_ADV_USD)

    btc = k.loc[k.asset == "BTCUSDT", ["ts", "ret"]].rename(columns={"ret": "btc_ret"})
    k = k.merge(btc, on="ts", how="left")
    cov = (k["ret"] * k["btc_ret"]).groupby(a).transform(lambda x: x.rolling(60, min_periods=30).mean().shift(1)) - \
        _grp_roll(k["ret"], a, 60, "mean", min_periods=30) * _grp_roll(k["btc_ret"], a, 60, "mean", min_periods=30)
    var = _grp_roll(k["btc_ret"], a, 60, "var", min_periods=30)
    k["beta60"] = (cov / var).clip(0, 3).fillna(1.0)
    k.loc[k.asset == "BTCUSDT", "beta60"] = 1.0

    fund = load_funding(k["asset"].unique())
    k = k.merge(fund, on=["ts", "asset"], how="left")
    k["funding_7d_ann"] = _grp_roll(k["funding_day"], a, 7, "mean", shift=0, min_periods=5) * 365
    k["funding_ts_z"] = (k["funding_day"] - _grp_roll(k["funding_day"], a, 90, "mean", min_periods=30)) / \
        _grp_roll(k["funding_day"], a, 90, "std", min_periods=30)
    elig = k["eligible"]
    k["funding_xs_rank"] = k["funding_7d_ann"].where(elig).groupby(k["ts"]).rank(pct=True)
    k["interval_change"] = k.groupby("asset")["interval_h"].transform(lambda x: (x < x.shift(1).rolling(14, min_periods=7).median() * 0.75))

    prem = load_premium(k["asset"].unique())
    k = k.merge(prem, on=["ts", "asset"], how="left")
    k["prem_z90"] = (k["prem_close"] - _grp_roll(k["prem_close"], a, 90, "mean", min_periods=30)) / \
        _grp_roll(k["prem_close"], a, 90, "std", min_periods=30)

    cg = load_coinglass(k["base"].unique())
    if len(cg):
        k = k.merge(cg, on=["ts", "base"], how="left")
        k["oi_prev"] = k.groupby("asset")["oi_usd"].shift(1)
        k["oi_chg_1d"] = np.log(k["oi_usd"] / k["oi_prev"])
        k["oi_chg_z"] = (k["oi_chg_1d"] - _grp_roll(k["oi_chg_1d"], a, 90, "mean", min_periods=30)) / \
            _grp_roll(k["oi_chg_1d"], a, 90, "std", min_periods=30)
        k["oi_7d_chg"] = np.log(k["oi_usd"] / k.groupby("asset")["oi_usd"].shift(7))
        k["oi_7d_z"] = (k["oi_7d_chg"] - _grp_roll(k["oi_7d_chg"], a, 180, "mean", min_periods=60)) / \
            _grp_roll(k["oi_7d_chg"], a, 180, "std", min_periods=60)
        for side in ("long", "short"):
            k[f"{side}_liq_oi"] = k[f"{side}_liq_usd"] / k["oi_prev"]
            k[f"{side}_liq_z"] = (k[f"{side}_liq_oi"] - _grp_roll(k[f"{side}_liq_oi"], a, 90, "mean", min_periods=30)) / \
                _grp_roll(k[f"{side}_liq_oi"], a, 90, "std", min_periods=30)

    # market regime (BTC)
    b = k.loc[k.asset == "BTCUSDT", ["ts", "close", "sig20"]].sort_values("ts")
    b["btc_mom90"] = b["close"] / b["close"].shift(90) - 1
    b["btc_vol_pct"] = b["sig20"].expanding(180).rank(pct=True).shift(1)
    k = k.merge(b[["ts", "btc_mom90", "btc_vol_pct"]], on="ts", how="left")
    k["btc_regime"] = np.where(k["btc_mom90"] > 0, "bull", "bear")
    k["vol_regime"] = pd.cut(k["btc_vol_pct"], [-0.01, 1 / 3, 2 / 3, 1.01], labels=["low", "mid", "high"])
    k["weekday"] = k["ts"].dt.dayofweek

    if save:
        k.to_parquet(OUT / "panel.parquet", index=False)
        meta.to_csv(OUT / "universe_meta.csv", index=False)
    return k
