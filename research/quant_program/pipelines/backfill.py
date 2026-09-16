"""Staged, resumable backfill of the Track B research dataset.

    python pipelines/backfill.py            # all stages
    python pipelines/backfill.py --stages 1 2

Stage 1  Binance USD-M daily klines, survivorship-free USDT-perp universe  -> liquidity ranking
Stage 2  Binance funding prints + daily premium index (basis), all perps
Stage 3  Binance 1h klines (perp + spot) for BTC / ETH / SOL (session & funding-window studies)
Stage 4  Top-N liquid perps: Hyperliquid funding, Bybit funding, CoinGlass liquidations + OI
Stage 5  Binance archive daily 'metrics' (OI, long/short, taker ratio) for the top-M perps

Every stage is incremental (re-running only fetches what is missing) and logs to cache/backfill_log.txt.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qlib import data as D  # noqa: E402
from qlib.paths import CACHE  # noqa: E402

LOG = CACHE / "backfill_log.txt"
TOP_N_CROSS_VENUE = 60      # stage 4
TOP_M_METRICS = 20          # stage 5 (~2,200 daily archive files per symbol)


def log(msg: str) -> None:
    line = f"{pd.Timestamp.utcnow():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:     # symbols can be non-ASCII (e.g. 牛来USDT)
        f.write(line + "\n")


def base_asset(symbol: str) -> str:
    b = symbol[:-4] if symbol.endswith("USDT") else symbol
    for p in ("1000000", "1000"):
        if b.startswith(p) and len(b) > len(p):
            return b[len(p):]
    return b


def stage1() -> pd.DataFrame:
    syms = D.binance_perp_universe(include_delisted=True)
    log(f"stage1: {len(syms)} USDT perp symbols (current + delisted)")
    t = time.time()
    got = D.fetch_many(lambda s: D.binance_klines(s, "1d", "um", start="2019-09-01"), syms, workers=6)
    rows = []
    for s, df in got.items():
        if df is None or df.empty:
            continue
        last365 = df[df["ts"] >= df["ts"].max() - pd.Timedelta(days=365)]
        rows.append({"symbol": s, "base": base_asset(s), "first": df["ts"].min(), "last": df["ts"].max(),
                     "days": len(df), "median_qv_365d": last365["quote_volume"].median()})
    uni = pd.DataFrame(rows).sort_values("median_qv_365d", ascending=False)
    uni.to_csv(CACHE / "universe_binance_perps.csv", index=False)
    log(f"stage1 done: {len(uni)} symbols with history in {time.time()-t:.0f}s")
    return uni


def load_universe() -> pd.DataFrame:
    p = CACHE / "universe_binance_perps.csv"
    return pd.read_csv(p, parse_dates=["first", "last"]) if p.exists() else stage1()


def stage2(uni: pd.DataFrame) -> None:
    syms = uni["symbol"].tolist()
    t = time.time()
    D.fetch_many(lambda s: D.binance_funding(s), syms, workers=3)
    log(f"stage2 funding done in {time.time()-t:.0f}s")
    t = time.time()
    D.fetch_many(lambda s: D.binance_premium_index(s, "1d"), syms, workers=5)
    log(f"stage2 premium index done in {time.time()-t:.0f}s")


def stage3() -> None:
    for s in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        for m in ("um", "spot"):
            df = D.binance_klines(s, "1h", m, start="2019-09-01" if m == "um" else "2017-08-17")
            log(f"stage3 {s} {m} 1h rows={len(df)} {df.ts.min()} -> {df.ts.max()}")


def active_liquid(uni: pd.DataFrame, n: int) -> pd.DataFrame:
    recent = uni[uni["last"] >= uni["last"].max() - pd.Timedelta(days=7)]
    # TradFi perps (equities/commodities/indices) are a separate universe; never spend crypto slots on them
    recent = recent[~recent["symbol"].isin(D.binance_non_crypto_symbols())]
    return recent.drop_duplicates("base").head(n)


def stage4(uni: pd.DataFrame) -> None:
    top = active_liquid(uni, TOP_N_CROSS_VENUE)
    hl_meta = D._get("https://api.hyperliquid.xyz/info", method="POST", body={"type": "meta"})
    hl = {u["name"] for u in hl_meta["universe"]}
    hl_map = {}
    for b, s in zip(top["base"], top["symbol"]):
        cand = b if b in hl else (f"k{b}" if f"k{b}" in hl else None)
        if cand:
            hl_map[s] = cand
    t = time.time()
    D.fetch_many(lambda s: D.hyperliquid_funding(hl_map[s]), list(hl_map), workers=2)
    log(f"stage4 hyperliquid funding: {len(hl_map)} coins in {time.time()-t:.0f}s")
    t = time.time()
    D.fetch_many(lambda s: D.bybit_funding(s), top["symbol"].tolist(), workers=3)
    log(f"stage4 bybit funding: {len(top)} symbols in {time.time()-t:.0f}s")
    t = time.time()
    bases = top["base"].tolist()
    D.fetch_many(lambda b: D.coinglass_liquidations(b), bases, workers=1)
    D.fetch_many(lambda b: D.coinglass_oi(b), bases, workers=1)
    log(f"stage4 coinglass liquidations+OI: {len(bases)} assets in {time.time()-t:.0f}s")
    pd.Series(hl_map).to_csv(CACHE / "hyperliquid_symbol_map.csv")


def stage5(uni: pd.DataFrame) -> None:
    top = active_liquid(uni, TOP_M_METRICS)
    for s in top["symbol"]:
        t = time.time()
        df = D.binance_metrics_daily(s, start="2020-09-01")
        log(f"stage5 metrics {s}: rows={len(df)} in {time.time()-t:.0f}s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stages", nargs="*", type=int, default=[1, 2, 3, 4, 5])
    a = ap.parse_args()
    log(f"backfill start stages={a.stages}")
    uni = stage1() if 1 in a.stages else load_universe()
    if 2 in a.stages:
        stage2(uni)
    if 3 in a.stages:
        stage3()
    if 4 in a.stages:
        stage4(uni)
    if 5 in a.stages:
        stage5(uni)
    log("backfill finished")


if __name__ == "__main__":
    main()
