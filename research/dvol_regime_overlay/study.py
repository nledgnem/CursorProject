"""
Should DVOL join the regime monitor, how, and how alongside APR / Spread?

PRE-SPECIFIED BEFORE LOOKING AT RESULTS
---------------------------------------
Q1  Should we incorporate DVOL at all?
    1a  Is it orthogonal to Environment_APR and Fragmentation_Spread, or a
        repackaging of them?
    1b  Does it predict forward RISK of the gated book beyond them?
    1c  Does it predict forward RETURN of the gated book beyond them?

Q2  How should it be expressed?
    Level vs rolling percentile; one-way brake vs two-way scaler; threshold.

Q3  How does it combine with APR and Spread?
    Interaction, and a backtest of w_risk alone vs w_risk x DVOL variants.

DECISION RULE, FIXED IN ADVANCE
    DVOL earns a place ONLY if it is (a) not redundant with what the monitor
    already has, AND (b) improves risk-adjusted outcomes of the actual gated
    book, AND (c) does so without a threshold chosen on the full sample.
    Failing any one => monitor-only, no gating.

THE BOOK BEING GATED
    Long majors (0.7 BTC / 0.3 ETH) / short a top-50 point-in-time alt basket.
    Note this is a RELATIVE-VALUE book, while DVOL is a directional BTC implied
    vol index. Whether BTC vol says anything about a market-neutral-ish spread's
    risk is genuinely open -- it is not obvious either way, which is the point of
    testing rather than assuming.

SAMPLE
    2023-04-19 -> 2026-08-16, 1,216 daily observations, bounded by funding
    history (not DVOL). Weekly `y` has only 105 rows. Small.

    python study.py
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
from scipy import stats as sps

HERE = Path(__file__).resolve().parent
OUT = HERE / "results"
OUT.mkdir(exist_ok=True)
(OUT / "tables").mkdir(exist_ok=True)

TRADING_DAYS = 365
HORIZONS = (5, 10, 20)
PRIMARY_H = 10          # ~2 weeks: the monitor's decision cadence is weekly
SIGNAL_LAG = 1          # signal known at close t, applied to t+1 onward


def log(m=""):
    print(m, flush=True)


def pct(x):
    return "n/a" if not np.isfinite(x) else f"{x*100:+.2f}%"


def hac(y, X, lags):
    X = sm.add_constant(np.asarray(X, dtype=float))
    return sm.OLS(np.asarray(y, dtype=float), X).fit(
        cov_type="HAC", cov_kwds={"maxlags": lags})


def main():
    d = pd.read_parquet(HERE / "data" / "daily_panel.parquet")
    wk = pd.read_parquet(HERE / "data" / "weekly_y.parquet")
    log(f"panel {d.index.min().date()} -> {d.index.max().date()}  ({len(d)} days)")

    # ---------------------------------------------------------------- A
    log("\n" + "=" * 72)
    log("A. Does the daily L/S proxy track the real weekly strategy return?")
    log("=" * 72)
    # Sum the daily proxy over EXACTLY the strategy's own window,
    # [decision_date, decision_date+7). Do NOT use resample("W-MON") -- it labels
    # weeks by their END, which offsets the comparison by a week and made the
    # proxy look uncorrelated (+0.09) when it is in fact +0.87.
    wk_y = wk["y"].dropna()
    vals = {}
    for dt in wk_y.index:
        w = d["ls_proxy"][(d.index >= dt) & (d.index < dt + pd.Timedelta(days=7))]
        if len(w) >= 5:
            vals[dt] = w.sum()
    prox_wk = pd.Series(vals)
    j = wk_y.index.intersection(prox_wk.index)
    r = np.corrcoef(wk_y.loc[j], prox_wk.loc[j])[0, 1]
    sign = ((wk_y.loc[j] > 0) == (prox_wk.loc[j] > 0)).mean()
    log(f"  n={len(j)} weeks   corr={r:+.3f}   sign agreement={sign*100:.1f}%")
    proxy_ok = r > 0.5
    log(f"  -> proxy is {'USABLE' if proxy_ok else 'NOT usable'} as a higher-n stand-in")
    if not proxy_ok:
        raise SystemExit("proxy failed its pre-specified validation; stopping.")

    # ---------------------------------------------------------------- Q1a
    log("\n" + "=" * 72)
    log("Q1a. Is DVOL orthogonal to what the monitor already has?")
    log("=" * 72)
    cols = ["Environment_APR_daily_pct", "Fragmentation_Spread", "w_risk"]
    rows = []
    for c in cols:
        for dv in ["dvol", "dvol_pct_365", "dvol_z_365"]:
            x = d[[c, dv]].dropna()
            rho, p = sps.spearmanr(x[c], x[dv])
            rows.append({"monitor_field": c, "dvol_form": dv,
                         "spearman_rho": rho, "p": p, "n": len(x)})
    orth = pd.DataFrame(rows)
    orth.to_csv(OUT / "tables" / "01_orthogonality.csv", index=False)
    for _, r in orth[orth.dvol_form == "dvol_pct_365"].iterrows():
        log(f"  DVOL 365d-percentile vs {r['monitor_field']:<28} "
            f"rho={r['spearman_rho']:+.3f} (p={r['p']:.3g}, n={int(r['n'])})")
    maxrho = orth[orth.dvol_form == "dvol_pct_365"]["spearman_rho"].abs().max()
    log(f"  -> max |rho| = {maxrho:.3f}: DVOL is "
        f"{'largely ORTHOGONAL' if maxrho < 0.5 else 'REDUNDANT'} to the existing fields")

    # ---------------------------------------------------------------- Q1b
    log("\n" + "=" * 72)
    log("Q1b. Does DVOL predict forward RISK of the book beyond APR/Spread?")
    log("=" * 72)
    ls = d["ls_proxy"]
    rows = []
    for h in HORIZONS:
        fwd_vol = ls.rolling(h).std().shift(-h) * np.sqrt(TRADING_DAYS)
        X = d[["Environment_APR_daily_pct", "Fragmentation_Spread"]].copy()
        base = pd.concat([X, fwd_vol.rename("fv")], axis=1).dropna()
        m0 = hac(base["fv"], base[X.columns], h)
        full = pd.concat([X, d["dvol_pct_365"], fwd_vol.rename("fv")], axis=1).dropna()
        m1 = hac(full["fv"], full[list(X.columns) + ["dvol_pct_365"]], h)
        rows.append({
            "horizon": h, "n": int(len(full)),
            "R2_apr_spread": m0.rsquared, "R2_plus_dvol": m1.rsquared,
            "dvol_coef": m1.params[-1], "dvol_t": m1.tvalues[-1], "dvol_p": m1.pvalues[-1],
        })
    risk = pd.DataFrame(rows)
    risk.to_csv(OUT / "tables" / "02_forward_risk.csv", index=False)
    for _, r in risk.iterrows():
        log(f"  {int(r['horizon']):>2}d fwd vol:  R2 {r['R2_apr_spread']:.3f} -> "
            f"{r['R2_plus_dvol']:.3f} when DVOL added   "
            f"(DVOL t={r['dvol_t']:+.2f}, p={r['dvol_p']:.4f})")

    # ---------------------------------------------------------------- Q1c
    log("\n" + "=" * 72)
    log("Q1c. Does DVOL predict forward RETURN of the book beyond APR/Spread?")
    log("=" * 72)
    rows = []
    for h in HORIZONS:
        fwd_ret = ls.rolling(h).sum().shift(-h)
        X = d[["Environment_APR_daily_pct", "Fragmentation_Spread"]]
        full = pd.concat([X, d["dvol_pct_365"], fwd_ret.rename("fr")], axis=1).dropna()
        m1 = hac(full["fr"], full[list(X.columns) + ["dvol_pct_365"]], h)
        rows.append({"horizon": h, "n": int(len(full)),
                     "apr_t": m1.tvalues[1], "spread_t": m1.tvalues[2],
                     "dvol_coef": m1.params[3], "dvol_t": m1.tvalues[3],
                     "dvol_p": m1.pvalues[3], "R2": m1.rsquared})
    ret = pd.DataFrame(rows)
    ret.to_csv(OUT / "tables" / "03_forward_return.csv", index=False)
    for _, r in ret.iterrows():
        log(f"  {int(r['horizon']):>2}d fwd ret:  DVOL coef={pct(r['dvol_coef'])} "
            f"(t={r['dvol_t']:+.2f}, p={r['dvol_p']:.4f})   "
            f"APR t={r['apr_t']:+.2f}  Spread t={r['spread_t']:+.2f}   R2={r['R2']:.3f}")

    # ---------------------------------------------------------------- Q2
    log("\n" + "=" * 72)
    log("Q2. How should DVOL be expressed? (level vs percentile)")
    log("=" * 72)
    log(f"  DVOL level over sample: min {d['dvol'].min():.0f}  median "
        f"{d['dvol'].median():.0f}  p95 {d['dvol'].quantile(.95):.0f}  max {d['dvol'].max():.0f}")
    log(f"  DVOL by year (mean):  " + "  ".join(
        f"{y}={v:.0f}" for y, v in d['dvol'].groupby(d.index.year).mean().items()))
    log("  -> a FIXED level threshold drifts with the vol regime; percentile does not.")

    fwd = ls.rolling(PRIMARY_H).sum().shift(-PRIMARY_H)
    fwdv = ls.rolling(PRIMARY_H).std().shift(-PRIMARY_H) * np.sqrt(TRADING_DAYS)
    rows = []
    for q in (0.60, 0.70, 0.80, 0.90):
        hi = (d["dvol_pct_365"] >= q).astype(float)
        for label, target in (("fwd_return", fwd), ("fwd_vol", fwdv)):
            t = pd.concat([hi.rename("hi"), target.rename("t")], axis=1).dropna()
            on, off = t.loc[t.hi == 1, "t"], t.loc[t.hi == 0, "t"]
            m = hac(t["t"], t[["hi"]], PRIMARY_H)
            rows.append({"quantile": q, "target": label, "n_on": len(on), "n_off": len(off),
                         "mean_on": on.mean(), "mean_off": off.mean(),
                         "diff": on.mean() - off.mean(),
                         "t": m.tvalues[1], "p": m.pvalues[1]})
    q2 = pd.DataFrame(rows)
    q2.to_csv(OUT / "tables" / "04_percentile_thresholds.csv", index=False)
    for label in ("fwd_return", "fwd_vol"):
        log(f"  -- {PRIMARY_H}d {label}, high-DVOL minus low-DVOL:")
        for _, r in q2[q2.target == label].iterrows():
            log(f"     DVOL >= p{int(r['quantile']*100)}:  {pct(r['diff']):>9}  "
                f"(t={r['t']:+.2f}, p={r['p']:.3f}, n_on={int(r['n_on'])})")

    # ---------------------------------------------------------------- Q3
    log("\n" + "=" * 72)
    log("Q3. Does DVOL add CONDITIONAL on the APR regime? (interaction)")
    log("=" * 72)
    apr = d["Environment_APR_daily_pct"]
    regime = pd.cut(apr, [-np.inf, 2.0, 5.0, 15.0, np.inf],
                    labels=["Cold Flush", "Recovery Ramp", "Golden Pocket", "Leverage Exhaustion"])
    rows = []
    for name, g in pd.concat([regime.rename("reg"), d["dvol_pct_365"],
                              fwd.rename("fwd"), fwdv.rename("fwdv")],
                             axis=1).dropna().groupby("reg", observed=True):
        if len(g) < 60:
            continue
        hi = (g["dvol_pct_365"] >= 0.80).astype(float)
        if hi.nunique() < 2:
            continue
        m = hac(g["fwd"], hi.to_frame(), PRIMARY_H)
        mv = hac(g["fwdv"], hi.to_frame(), PRIMARY_H)
        rows.append({"apr_regime": name, "n": len(g), "n_high_dvol": int(hi.sum()),
                     "fwd_ret_diff": m.params[1], "fwd_ret_t": m.tvalues[1],
                     "fwd_vol_diff": mv.params[1], "fwd_vol_t": mv.tvalues[1]})
    inter = pd.DataFrame(rows)
    inter.to_csv(OUT / "tables" / "05_apr_regime_interaction.csv", index=False)
    for _, r in inter.iterrows():
        log(f"  {r['apr_regime']:<22} n={int(r['n']):>4}  high-DVOL effect on "
            f"{PRIMARY_H}d return {pct(r['fwd_ret_diff']):>9} (t={r['fwd_ret_t']:+.2f})  "
            f"on vol {r['fwd_vol_diff']:+.3f} (t={r['fwd_vol_t']:+.2f})")

    # ---------------------------------------------------------------- Backtest
    log("\n" + "=" * 72)
    log("Q3b. Backtest: w_risk alone vs w_risk x DVOL brake")
    log("=" * 72)
    r = ls.copy()
    variants = {"w_risk only (current)": pd.Series(1.0, index=d.index)}
    for q in (0.70, 0.80, 0.90):
        variants[f"x brake @ DVOL p{int(q*100)}"] = (d["dvol_pct_365"] < q).astype(float).fillna(1.0)
    for q in (0.80,):
        soft = np.where(d["dvol_pct_365"] >= q, 0.5, 1.0)
        variants[f"x soft(0.5) @ DVOL p{int(q*100)}"] = pd.Series(soft, index=d.index)

    perf = []
    for name, mult in variants.items():
        w = (d["w_risk"] * mult).shift(SIGNAL_LAG).fillna(0.0)
        pnl = (w * r).dropna()
        eq = (1 + pnl).cumprod()
        yrs = len(pnl) / TRADING_DAYS
        cagr = eq.iloc[-1] ** (1 / yrs) - 1
        vol = pnl.std() * np.sqrt(TRADING_DAYS)
        dd = float((eq / eq.cummax() - 1).min())
        dw = w.diff().abs().fillna(0)
        perf.append({"variant": name, "CAGR": cagr, "ann_vol": vol,
                     "Sharpe": cagr / vol if vol else np.nan,
                     "max_drawdown": dd, "Calmar": cagr / abs(dd) if dd else np.nan,
                     "avg_exposure": w.mean(), "turnover_ann": dw.sum() / yrs,
                     "pct_days_braked": float((mult < 1).mean())})
    bt = pd.DataFrame(perf)
    bt.to_csv(OUT / "tables" / "06_backtest.csv", index=False)
    for _, x in bt.iterrows():
        log(f"  {x['variant']:<26} CAGR {pct(x['CAGR']):>8}  vol {x['ann_vol']:.3f}  "
            f"Sharpe {x['Sharpe']:>6.2f}  MaxDD {pct(x['max_drawdown']):>8}  "
            f"Calmar {x['Calmar']:>5.2f}  avgExp {x['avg_exposure']:.2f}  "
            f"braked {x['pct_days_braked']*100:.0f}%")

    # ------------------------------------------------- placebo + split
    log()
    log("=" * 72)
    log("Q3c. Is the brake TIMING, or just less exposure? (placebo + OOS split)")
    log("=" * 72)
    rng = np.random.default_rng(11)
    real = (d["dvol_pct_365"] < 0.70).astype(float).fillna(1.0)
    braked_frac = float((real < 1).mean())

    def sharpe_of(mult):
        w = (d["w_risk"] * mult).shift(SIGNAL_LAG).fillna(0.0)
        pnl = (w * r).dropna()
        eq = (1 + pnl).cumprod()
        yrs = len(pnl) / TRADING_DAYS
        cagr = eq.iloc[-1] ** (1 / yrs) - 1
        vol = pnl.std() * np.sqrt(TRADING_DAYS)
        return (cagr / vol if vol else np.nan), float((eq / eq.cummax() - 1).min())

    real_sh, real_dd = sharpe_of(real)
    base_sh, base_dd = sharpe_of(pd.Series(1.0, index=d.index))

    # Placebo: same number of braked days, but placed at random in blocks of 10
    # so the comparison preserves the brake's persistence, not just its count.
    sims = []
    n = len(d)
    n_blocks = max(1, int(round(braked_frac * n / 10)))
    for _ in range(2000):
        m = np.ones(n)
        starts = rng.integers(0, n - 10, size=n_blocks)
        for st in starts:
            m[st:st + 10] = 0.0
        sh, dd = sharpe_of(pd.Series(m, index=d.index))
        sims.append((sh, dd))
    sims = pd.DataFrame(sims, columns=["sharpe", "dd"])
    pct_better = float((sims["sharpe"] >= real_sh).mean())
    log(f"  base (no brake)          Sharpe {base_sh:.2f}   MaxDD {pct(base_dd)}")
    log(f"  real DVOL p70 brake      Sharpe {real_sh:.2f}   MaxDD {pct(real_dd)}   "
        f"({braked_frac*100:.0f}% of days braked)")
    log(f"  RANDOM brakes, same dose Sharpe {sims['sharpe'].mean():.2f} "
        f"[p5 {sims['sharpe'].quantile(.05):.2f}, p95 {sims['sharpe'].quantile(.95):.2f}]")
    log(f"  -> {pct_better*100:.1f}% of random brakes match or beat the DVOL brake")
    log(f"  -> DVOL timing is {'ADDING something' if pct_better < 0.05 else 'NOT distinguishable from randomly cutting exposure'}")
    pd.DataFrame([{"base_sharpe": base_sh, "real_sharpe": real_sh,
                   "placebo_mean": sims["sharpe"].mean(),
                   "placebo_p05": sims["sharpe"].quantile(.05),
                   "placebo_p95": sims["sharpe"].quantile(.95),
                   "pct_placebos_better": pct_better,
                   "braked_frac": braked_frac}]).to_csv(
        OUT / "tables" / "07_placebo.csv", index=False)

    split = d.index[len(d) // 2]
    log(f"  -- train/test split at {split.date()}")
    for tag, sl in (("train", d.index < split), ("test ", d.index >= split)):
        sub = d[sl]
        rr = r.loc[sub.index]
        for nm, mult in (("no brake", pd.Series(1.0, index=sub.index)),
                         ("p70 brake", (sub["dvol_pct_365"] < 0.70).astype(float).fillna(1.0))):
            w = (sub["w_risk"] * mult).shift(SIGNAL_LAG).fillna(0.0)
            pnl = (w * rr).dropna()
            eq = (1 + pnl).cumprod()
            yrs = len(pnl) / TRADING_DAYS
            cg = eq.iloc[-1] ** (1 / yrs) - 1
            vl = pnl.std() * np.sqrt(TRADING_DAYS)
            dd = float((eq / eq.cummax() - 1).min())
            log(f"     {tag} {nm:<10} CAGR {pct(cg):>8}  Sharpe {cg/vl:>5.2f}  MaxDD {pct(dd):>8}")

    # ---------------------------------------------------------------- figure
    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True,
                             gridspec_kw={"height_ratios": [1.4, 1, 1]})
    eqs = {}
    for name, mult in variants.items():
        w = (d["w_risk"] * mult).shift(SIGNAL_LAG).fillna(0.0)
        eqs[name] = (1 + (w * r).dropna()).cumprod()
    for name, e in eqs.items():
        axes[0].plot(e.index, e.values, lw=1.4, label=name)
    axes[0].set_ylabel("Growth of 1")
    axes[0].legend(frameon=False, fontsize=8)
    axes[0].grid(alpha=0.25)
    axes[1].plot(d.index, d["Environment_APR_daily_pct"], color="#1F6F8B", lw=1.0)
    axes[1].axhline(2.0, ls="--", c="#888", lw=0.8)
    axes[1].axhline(5.0, ls="--", c="#888", lw=0.8)
    axes[1].set_ylabel("Environment APR %")
    axes[1].grid(alpha=0.25)
    axes[2].plot(d.index, d["dvol_pct_365"] * 100, color="#B23A48", lw=1.0)
    axes[2].axhline(80, ls="--", c="#333", lw=0.9, label="p80")
    axes[2].set_ylabel("DVOL 365d %ile")
    axes[2].legend(frameon=False, fontsize=8)
    axes[2].grid(alpha=0.25)
    fig.suptitle("Gated L/S book under w_risk alone vs w_risk x DVOL brake", y=0.93)
    fig.savefig(OUT / "fig01_dvol_overlay.png", dpi=150, bbox_inches="tight",
                facecolor="white")
    plt.close(fig)
    log("\n-> results/fig01_dvol_overlay.png, tables 01-06")


if __name__ == "__main__":
    main()
