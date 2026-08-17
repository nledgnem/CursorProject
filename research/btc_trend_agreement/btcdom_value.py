"""
Is BTCDOM_Trend earning its place in the regime monitor?

WHAT BTCDOM_Trend DOES IN THE SYSTEM
------------------------------------
majors_alts_monitor/msm_funding_v0/msm_run.py builds it as

    btcd_index_decision > sma_30   ->  "Rising" / "Falling"

and it feeds the MRF gate

    is_mrf_active = (funding_regime == "Q2: Weak") AND (BTCDOM_Trend == "Rising")

which gates a LONG-MAJORS / SHORT-ALTS book.

THE CONCERN
-----------
"BTC dominance is rising" and "long majors / short alts is working" are close to
the same statement -- dominance rising IS majors outperforming alts. So the gate
may be little more than momentum on the strategy's own P&L rather than
independent information. That is not automatically bad (trend-following your own
equity curve is a real technique) but it should be measured, not assumed.

WHAT THIS SCRIPT TESTS
----------------------
  Q1  Does a dominance proxy reproduce the repo's own reconstructed BTCDOM
      index? (validates the proxy against btcdom_reconstructed.csv)
  Q2  Does "dominance above its 30d SMA" predict the NEXT 20 days of
      BTC-minus-alts relative return? -- the actual value-add question.
  Q3  How much of that is just autocorrelation of the relative return itself?
      Compared against plain trailing relative momentum as a benchmark.
  Q4  Does it survive the 2022+ out-of-sample split?

The proxy runs 2019-2026, far longer than the repo's own BTCDOM series
(2024-07 onward), so it can answer questions the production data cannot.

    python btcdom_value.py
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
from alt_basket import (build_basket, clean_redenominations, load_panel,
                        member_map)
from config import FIGURE_DIR, OOS_SPLIT_DATE, TABLE_DIR, hac_lags

H = 20
LAGS = hac_lags(H)
SMA_WINDOW = 30
REPO_ROOT = Path(__file__).resolve().parents[2]


def log(m: str) -> None:
    print(m, flush=True)


def pct(x) -> str:
    return "n/a" if not np.isfinite(x) else f"{x*100:+.2f}%"


def hac_spread(sig: pd.Series, target: pd.Series, s=None, e=None) -> dict:
    d = pd.concat([sig.rename("on"), target.rename("y")], axis=1).dropna()
    if s:
        d = d[d.index >= s]
    if e:
        d = d[d.index < e]
    if len(d) < 150 or d["on"].nunique() < 2:
        return {}
    on, off = d.loc[d.on == 1, "y"], d.loc[d.on == 0, "y"]
    res = sm.OLS(d["y"].values, sm.add_constant(d["on"].values)).fit(
        cov_type="HAC", cov_kwds={"maxlags": LAGS})
    return {"n": len(d), "n_on": len(on), "n_off": len(off),
            "mean_on": on.mean(), "mean_off": off.mean(),
            "spread": on.mean() - off.mean(),
            "hac_t": float(res.tvalues[1]), "hac_p": float(res.pvalues[1])}


def main():
    log("SECTION A -- build a dominance proxy from the alt panel")
    panel, _ = clean_redenominations(load_panel(False))
    close, members = build_basket(panel, 50)
    memb = member_map(close.index, members)

    px, _ = data_io.load_prices()
    btc = px["BTC"].dropna()

    # Equal-weighted alt index: chain daily cross-sectional mean returns of the
    # current members. Membership is fixed between monthly rebalances, so this
    # is point-in-time and tradable in principle.
    ret = close.pct_change(fill_method=None)
    daily = pd.Series(index=close.index, dtype=float)
    for d in close.index:
        m = memb.loc[d]
        if not isinstance(m, list):
            continue
        r = ret.loc[d, [c for c in m if c in ret.columns]]
        if r.notna().sum() >= 5:
            daily.loc[d] = r.mean()
    alt_index = (1 + daily.fillna(0)).cumprod()
    alt_index = alt_index[daily.notna().cumsum() > 0]

    common = btc.index.intersection(alt_index.index)
    dom = (btc.loc[common] / alt_index.loc[common]).rename("dom_proxy")
    dom = dom / dom.iloc[0] * 1000.0
    log(f"  dominance proxy: {dom.index.min().date()} -> {dom.index.max().date()} "
        f"({len(dom)} days)")

    # ---- Q1: validate against the repo's own reconstructed index ----
    log("\nSECTION B -- Q1: does the proxy track the repo's own BTCDOM index?")
    recon_path = REPO_ROOT / "data" / "curated" / "data_lake" / "btcdom_reconstructed.csv"
    val = {}
    if recon_path.exists():
        rc = pd.read_csv(recon_path, parse_dates=["date"]).set_index("date")
        rc = rc["reconstructed_index_value"].astype(float).sort_index()
        ov = dom.index.intersection(rc.index)
        if len(ov) > 60:
            a = dom.loc[ov].pct_change().dropna()
            b = rc.loc[ov].pct_change().dropna()
            i = a.index.intersection(b.index)
            val = {"overlap_days": len(ov),
                   "overlap_start": str(ov.min().date()),
                   "overlap_end": str(ov.max().date()),
                   "daily_change_corr": float(a.loc[i].corr(b.loc[i])),
                   "level_corr": float(dom.loc[ov].corr(rc.loc[ov])),
                   "repo_index_last_date": str(rc.index.max().date())}
            log(f"  repo btcdom_reconstructed.csv ends {val['repo_index_last_date']}")
            log(f"  overlap {val['overlap_start']} -> {val['overlap_end']} "
                f"({val['overlap_days']}d)")
            log(f"  daily-change corr = {val['daily_change_corr']:+.3f}, "
                f"level corr = {val['level_corr']:+.3f}")
    pd.DataFrame([val]).to_csv(TABLE_DIR / "35_btcdom_proxy_validation.csv", index=False)

    # ---- Q2/Q3/Q4 ----
    log("\nSECTION C -- Q2: does dominance trend predict BTC-minus-alts?")
    sma = dom.rolling(SMA_WINDOW).mean()
    rising = (dom > sma).astype(float).where(sma.notna())

    btc_f = (btc.shift(-H) / btc - 1.0)
    alt_f = (alt_index.shift(-H) / alt_index - 1.0)
    rel_f = (btc_f - alt_f).reindex(dom.index).rename("btc_minus_alts_20d")

    # Benchmark: plain trailing 30d relative momentum, the simplest possible
    # alternative that uses the same information.
    rel_mom = ((btc / btc.shift(SMA_WINDOW)) / (alt_index / alt_index.shift(SMA_WINDOW)) - 1.0)
    mom_pos = (rel_mom > 0).astype(float).where(rel_mom.notna()).reindex(dom.index)

    eras = [("full sample", None, None),
            ("train (pre-2022)", None, OOS_SPLIT_DATE),
            ("OOS (2022+)", OOS_SPLIT_DATE, None)]
    rows = []
    for sname, sser in (("BTCDOM_Trend = Rising (dom > SMA30)", rising),
                        ("trailing 30d relative momentum > 0", mom_pos)):
        for era, s, e in eras:
            r = hac_spread(sser, rel_f, s, e)
            if r:
                rows.append({"signal": sname, "era": era, **r})
    res = pd.DataFrame(rows)
    res.to_csv(TABLE_DIR / "36_btcdom_trend_value_add.csv", index=False)
    for _, r in res.iterrows():
        log(f"  {r['signal']:<38} {r['era']:<17} "
            f"BTC-minus-alts 20D: on {pct(r['mean_on'])} / off {pct(r['mean_off'])} "
            f"-> spread {pct(r['spread'])} (HAC t={r['hac_t']:5.2f}, p={r['hac_p']:.4f})")

    # ---- Q3: how much is just autocorrelation of the target? ----
    log("\nSECTION D -- Q3: is this just momentum in the relative return itself?")
    rel_daily = (btc.pct_change() - daily).dropna()
    ac = {f"lag_{k}": float(rel_daily.autocorr(k)) for k in (1, 5, 20, 60)}
    trail = (rel_daily.rolling(SMA_WINDOW).sum())
    fwd = (rel_daily.shift(-H).rolling(H).sum().shift(-(0)))
    ov = trail.dropna().index.intersection(rel_f.dropna().index)
    overlap_corr = float(trail.loc[ov].corr(rel_f.loc[ov]))
    # correlation between the two candidate signals themselves
    sig_ov = pd.concat([rising, mom_pos], axis=1).dropna()
    sig_agree = float((sig_ov.iloc[:, 0] == sig_ov.iloc[:, 1]).mean())
    diag = {**ac, "corr_trailing30d_vs_forward20d_rel": overlap_corr,
            "pct_days_two_signals_agree": sig_agree}
    pd.DataFrame([diag]).to_csv(TABLE_DIR / "37_btcdom_autocorr_diagnostics.csv", index=False)
    log(f"  autocorr of daily BTC-minus-alts return: " +
        ", ".join(f"lag{k}={v:+.3f}" for k, v in ac.items()))
    log(f"  corr(trailing 30d rel return, forward 20d rel return) = {overlap_corr:+.3f}")
    log(f"  the two signals give the same reading on {sig_agree*100:.1f}% of days")

    # ---- figure ----
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                             gridspec_kw={"height_ratios": [2, 1]})
    axes[0].plot(dom.index, dom.values, color="#222", lw=1.1, label="dominance proxy (BTC / alt basket)")
    axes[0].plot(sma.index, sma.values, color="#B23A48", lw=1.0, label=f"SMA{SMA_WINDOW}")
    if recon_path.exists() and val:
        rc2 = rc / rc.iloc[0] * float(dom.reindex(rc.index).dropna().iloc[0])
        axes[0].plot(rc2.index, rc2.values, color="#1F6F8B", lw=1.2, ls="--",
                     label="repo btcdom_reconstructed (rescaled)")
    axes[0].set_yscale("log")
    axes[0].set_ylabel("BTC vs alt basket (log)")
    axes[0].legend(frameon=False, fontsize=9)
    axes[0].grid(alpha=0.25)
    axes[1].fill_between(rising.index, rising.values, step="post", alpha=0.6, color="#5B8C5A")
    axes[1].set_ylabel("Rising = 1")
    axes[1].grid(alpha=0.25)
    fig.suptitle("BTC dominance proxy, its 30-day SMA, and the resulting Rising/Falling flag", y=0.95)
    fig.savefig(FIGURE_DIR / "fig13_btcdom_value.png", dpi=150,
                bbox_inches="tight", facecolor="white")
    plt.close(fig)
    log("\n-> figure fig13_btcdom_value.png")
    log("-> tables 35, 36, 37 written")


if __name__ == "__main__":
    main()
