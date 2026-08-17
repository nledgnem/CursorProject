"""
Head-to-head: Coinbase TrendScore vs a single 200-day moving average.

WHY THIS TEST
-------------
The horse-race regression in run_all.py showed that no individual lookback in
the 30/90/365 triple is significant on its own and each contributes roughly
+2.9pp. That is the signature of a signal whose content is simply "is BTC
trending up", measured three times. If that is all it is, then the industry's
default single-parameter trend filter -- price above its 200-day moving
average -- should capture the same thing with one parameter instead of three.

The 200d SMA is the right null hypothesis specifically because:
  1. It is externally specified by decades of convention, so it has the same
     epistemic status as Coinbase's 30/90/365 -- neither was chosen by us
     after looking at this data. It is a fair benchmark, not a straw man.
  2. It sits in the same horizon region as the triple's dominant term.
  3. It is what would explain the OOS result: TrendScore's strategy beat
     buy-and-hold out of sample while its conditional-return edge vanished.
     Generic trend de-risking explains that; special sauce does not.

DECISION RULE, FIXED BEFORE RUNNING
-----------------------------------
TrendScore earns its complexity only if it beats SMA200 on BOTH:
  (a) the 20-day conditional forward-return spread, out of sample, and
  (b) risk-adjusted strategy performance, out of sample.
Beating it on one of the two, or only in-sample, counts as "does not earn it".

Everything is pre-specified: SMA200 close crossover, long/flat, same 2-day
execution lag, same 5 bps cost, same engine. No variants are tuned.

    python ma_benchmark.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm

sys.path.insert(0, str(Path(__file__).resolve().parent))

import data_io
import stats_tools as st
import strategies as sg
import trend_study as ts
from config import (BASE_COST_BPS, FIGURE_DIR, OOS_SPLIT_DATE, PRIMARY_HORIZON,
                    STRATEGY_MAPS, TABLE_DIR, hac_lags)

MA_WINDOW = 200
LAGS = hac_lags(PRIMARY_HORIZON)


def pct(x) -> str:
    return "n/a" if not np.isfinite(x) else f"{x*100:+.2f}%"


def log(m: str) -> None:
    print(m, flush=True)


def build_signals(price: pd.Series) -> pd.DataFrame:
    """Both signals as 0/1 exposure, NaN until their own warm-up completes."""
    sma = price.rolling(MA_WINDOW).mean()
    ma_on = (price > sma).astype(float).where(sma.notna())

    score = ts.trend_score(price)
    ts3 = (score == 3).astype(float).where(score.notna())      # Strategy C shape
    ts2 = (score >= 2).astype(float).where(score.notna())      # Strategy B shape

    return pd.DataFrame({"sma200": ma_on, "trendscore_ge2": ts2,
                         "trendscore_eq3": ts3, "score": score}, index=price.index)


def conditional_spread(sig: pd.Series, fwd: pd.Series, s=None, e=None) -> dict:
    d = pd.concat([sig.rename("on"), fwd.rename("fwd")], axis=1).dropna()
    if s:
        d = d[d.index >= s]
    if e:
        d = d[d.index < e]
    if len(d) < 150 or d["on"].nunique() < 2:
        return {}
    on, off = d.loc[d.on == 1, "fwd"], d.loc[d.on == 0, "fwd"]
    res = sm.OLS(d["fwd"].values,
                 sm.add_constant(d["on"].values)).fit(cov_type="HAC",
                                                      cov_kwds={"maxlags": LAGS})
    return {"n": len(d), "n_on": len(on), "n_off": len(off),
            "mean_on": on.mean(), "mean_off": off.mean(),
            "spread": on.mean() - off.mean(),
            "hac_t": float(res.tvalues[1]), "hac_p": float(res.pvalues[1])}


def main():
    px, _ = data_io.load_prices()
    btc = px["BTC"].dropna()
    sig = build_signals(btc)
    fwd20 = btc.shift(-PRIMARY_HORIZON) / btc - 1.0

    # Common sample: both signals defined (TrendScore's 365d warm-up binds).
    common = sig[["sma200", "trendscore_ge2"]].dropna().index
    log(f"Common evaluation sample: {common.min().date()} -> {common.max().date()} "
        f"({len(common)} days)")

    # ---------------- (a) conditional forward-return spread ----------------
    log("\n=== (a) 20-day conditional forward-return spread (on minus off) ===")
    rows = []
    eras = [("full sample", None, None),
            ("train (pre-2022)", None, OOS_SPLIT_DATE),
            ("OOS (2022+)", OOS_SPLIT_DATE, None)]
    for name in ("sma200", "trendscore_ge2", "trendscore_eq3"):
        for era, s, e in eras:
            r = conditional_spread(sig[name].loc[common], fwd20, s, e)
            if r:
                rows.append({"signal": name, "era": era, **r})
    spread_tbl = pd.DataFrame(rows)
    spread_tbl.to_csv(TABLE_DIR / "25_ma200_vs_trendscore_spread.csv", index=False)
    for era, _, _ in eras:
        log(f"  -- {era}")
        for _, r in spread_tbl[spread_tbl.era == era].iterrows():
            log(f"     {r['signal']:<16} on-off = {pct(r['spread'])}  "
                f"(HAC t={r['hac_t']:5.2f}, p={r['hac_p']:.3f}, "
                f"n_on={int(r['n_on'])})")

    # ---------------- (b) strategy performance ----------------
    log("\n=== (b) strategy performance, identical engine (5bps, 2-day lag) ===")
    ret = btc.pct_change().loc[common]
    perf = []
    for era, s, e in eras:
        r_sub = ret.copy()
        if s:
            r_sub = r_sub[r_sub.index >= s]
        if e:
            r_sub = r_sub[r_sub.index < e]
        perf.append({**sg.performance_metrics(sg.buy_and_hold(r_sub), r_sub,
                                              "BTC buy & hold"), "era": era})
        for name in ("sma200", "trendscore_ge2", "trendscore_eq3"):
            bt = sg.run_backtest(r_sub, sig[name], cost_bps=BASE_COST_BPS)
            perf.append({**sg.performance_metrics(bt, r_sub, name), "era": era})
    perf_tbl = pd.DataFrame(perf)
    perf_tbl.to_csv(TABLE_DIR / "26_ma200_vs_trendscore_performance.csv", index=False)
    for era, _, _ in eras:
        log(f"  -- {era}")
        for _, r in perf_tbl[perf_tbl.era == era].iterrows():
            log(f"     {r['strategy']:<16} CAGR {pct(r['CAGR']):>9}  vol {r['ann_vol']:.2f}  "
                f"Sharpe {r['Sharpe']:5.2f}  MaxDD {pct(r['max_drawdown']):>8}  "
                f"Calmar {r['Calmar']:5.2f}  turn {r['turnover_ann']:5.1f}")

    # ---------------- (c) incremental value ----------------
    log("\n=== (c) does TrendScore survive controlling for SMA200? ===")
    inc_rows = []
    for era, s, e in eras:
        d = pd.concat([sig[["sma200", "trendscore_ge2", "trendscore_eq3"]].loc[common],
                       fwd20.rename("fwd")], axis=1).dropna()
        if s:
            d = d[d.index >= s]
        if e:
            d = d[d.index < e]
        if len(d) < 150:
            continue
        for extra in ("trendscore_ge2", "trendscore_eq3"):
            X = sm.add_constant(d[["sma200", extra]].values)
            res = sm.OLS(d["fwd"].values, X).fit(cov_type="HAC",
                                                 cov_kwds={"maxlags": LAGS})
            inc_rows.append({
                "era": era, "n": len(d), "extra_term": extra,
                "sma200_coef": float(res.params[1]), "sma200_t": float(res.tvalues[1]),
                "sma200_p": float(res.pvalues[1]),
                "extra_coef": float(res.params[2]), "extra_t": float(res.tvalues[2]),
                "extra_p": float(res.pvalues[2]),
            })
    inc_tbl = pd.DataFrame(inc_rows)
    inc_tbl.to_csv(TABLE_DIR / "27_ma200_incremental_hac.csv", index=False)
    for _, r in inc_tbl.iterrows():
        log(f"  {r['era']:<17} +{r['extra_term']:<16} "
            f"SMA200 {pct(r['sma200_coef'])} (t={r['sma200_t']:5.2f})   "
            f"extra {pct(r['extra_coef'])} (t={r['extra_t']:5.2f}, p={r['extra_p']:.3f})")

    # ---------------- (d) how different are they at all? ----------------
    log("\n=== (d) signal agreement ===")
    ag = sig[["sma200", "trendscore_ge2", "trendscore_eq3"]].loc[common].dropna()
    agree_rows = [{"pair": "sma200 vs trendscore_ge2",
                   "pct_days_same": float((ag.sma200 == ag.trendscore_ge2).mean()),
                   "corr": float(ag.sma200.corr(ag.trendscore_ge2))},
                  {"pair": "sma200 vs trendscore_eq3",
                   "pct_days_same": float((ag.sma200 == ag.trendscore_eq3).mean()),
                   "corr": float(ag.sma200.corr(ag.trendscore_eq3))}]
    pd.DataFrame(agree_rows).to_csv(TABLE_DIR / "28_signal_agreement.csv", index=False)
    for r in agree_rows:
        log(f"  {r['pair']:<28} same exposure on {r['pct_days_same']*100:.1f}% of days "
            f"(corr {r['corr']:.3f})")

    # ---------------- (e) cross-asset: the actual proposed use case ----------
    log("\n=== (e) cross-asset -- BTC signal predicting ETH / SOL 20D returns ===")
    ca_rows = []
    for asset in ("ETH", "SOL"):
        p = px[asset].dropna()
        f = (p.shift(-PRIMARY_HORIZON) / p - 1.0)
        for name in ("sma200", "trendscore_ge2", "trendscore_eq3"):
            for era, s, e in eras:
                r = conditional_spread(sig[name], f, s, e)
                if r:
                    ca_rows.append({"target": asset, "btc_signal": name,
                                    "era": era, **r})
    ca_tbl = pd.DataFrame(ca_rows)
    ca_tbl.to_csv(TABLE_DIR / "29_ma200_vs_trendscore_cross_asset.csv", index=False)
    for asset in ("ETH", "SOL"):
        for era, _, _ in eras:
            log(f"  -- {asset}, {era}")
            for _, r in ca_tbl[(ca_tbl.target == asset) & (ca_tbl.era == era)].iterrows():
                log(f"     {r['btc_signal']:<16} on-off = {pct(r['spread'])}  "
                    f"(HAC t={r['hac_t']:5.2f}, p={r['hac_p']:.3f})")

    # ---------------- figure ----------------
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    r_full = ret
    curves = {"BTC buy & hold": sg.buy_and_hold(r_full),
              "SMA200": sg.run_backtest(r_full, sig["sma200"], cost_bps=BASE_COST_BPS),
              "TrendScore >= 2": sg.run_backtest(r_full, sig["trendscore_ge2"], cost_bps=BASE_COST_BPS),
              "TrendScore == 3": sg.run_backtest(r_full, sig["trendscore_eq3"], cost_bps=BASE_COST_BPS)}
    cols = {"BTC buy & hold": "#8A8A8A", "SMA200": "#B23A48",
            "TrendScore >= 2": "#5B8C5A", "TrendScore == 3": "#D98324"}
    for k, bt in curves.items():
        axes[0].plot(bt.index, bt["equity"], lw=1.5, color=cols[k], label=k)
    axes[0].set_yscale("log")
    axes[0].set_title("Full sample")
    axes[0].set_ylabel("Growth of 1 (log)")
    axes[0].legend(frameon=False, fontsize=9)
    axes[0].grid(alpha=0.25)

    r_oos = ret[ret.index >= OOS_SPLIT_DATE]
    for k, sname in (("BTC buy & hold", None), ("SMA200", "sma200"),
                     ("TrendScore >= 2", "trendscore_ge2"),
                     ("TrendScore == 3", "trendscore_eq3")):
        bt = sg.buy_and_hold(r_oos) if sname is None else sg.run_backtest(
            r_oos, sig[sname], cost_bps=BASE_COST_BPS)
        axes[1].plot(bt.index, bt["equity"], lw=1.5, color=cols[k], label=k)
    axes[1].set_yscale("log")
    axes[1].set_title(f"Out of sample ({OOS_SPLIT_DATE} onward)")
    axes[1].grid(alpha=0.25)
    fig.suptitle("Does the 3-horizon TrendScore beat a single 200-day moving average?", y=0.98)
    fig.savefig(FIGURE_DIR / "fig11_ma200_head_to_head.png", dpi=150,
                bbox_inches="tight", facecolor="white")
    plt.close(fig)
    log(f"\n-> figure fig11_ma200_head_to_head.png")
    log("-> tables 25..29 written")


if __name__ == "__main__":
    main()
