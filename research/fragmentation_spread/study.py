"""
Fragmentation_Spread: is the binary gate wasting a continuous signal?

WHAT THE FIELD IS
-----------------
Cross-sectional IQR of harmonized funding rates across the asset universe on a
given day (macro_environment.calculate_spread). It measures DISPERSION: high =
funding is fragmented across coins, low = one coherent market-wide regime.

HOW IT IS USED TODAY
--------------------
As a binary kill-switch on the whole book:

    gate_on     = (Environment_APR >= 2.0) AND (spread < 7.5e-05)
    risk_weight = calculate_risk_weight(APR) if gate_on else 0.0

That ceiling fires on 37.4% of days -- it is not a rare tail guard.

WHY LOOK AT IT
--------------
In the DVOL study it came out as the strongest predictor in the whole panel:
10-day forward return t = -2.61, 5-day t = -4.34, versus Environment_APR at
+1.95 and DVOL at -1.27. A strong continuous signal spent as an on/off switch.

PRE-SPECIFIED QUESTIONS
-----------------------
Q0  Does the EXISTING binary gate add value at all, vs no spread gate?
Q1  Is the ceiling in the right place? (where does the break actually occur?)
Q2  Does a continuous scaler beat the binary gate?
Q3  Does any improvement survive the bar that killed DVOL --
        placebo at matched exposure, block-bootstrap CI, and a train/test split?

THE BAR, FIXED IN ADVANCE
-------------------------
DVOL failed because its backtest gain disappeared once compared against the
right baseline at matched risk. So every variant here is judged on:
    (a) improvement vs the CURRENT rule (not vs "no gate", which flatters),
    (b) a placebo with the SAME fraction of gated days,
    (c) a block-bootstrap CI on the Sharpe difference that must exclude 0,
    (d) consistency across a train/test split.
Failing (b), (c) or (d) => report as unproven, do not recommend.

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

HERE = Path(__file__).resolve().parent
PANEL = HERE.parent / "dvol_regime_overlay" / "data" / "daily_panel.parquet"
OUT = HERE / "results"
(OUT / "tables").mkdir(parents=True, exist_ok=True)

TD = 365
LAG = 1
CEIL = 0.000075          # production FRAGMENTATION_IDIOSYNCRATIC_TOXIC_CEILING
PRIMARY_H = 10


def log(m=""):
    print(m, flush=True)


def pct(x):
    return "n/a" if not np.isfinite(x) else f"{x*100:+.2f}%"


def metrics(pnl):
    eq = (1 + pnl).cumprod()
    yrs = len(pnl) / TD
    cagr = eq.iloc[-1] ** (1 / yrs) - 1
    vol = pnl.std() * np.sqrt(TD)
    dd = float((eq / eq.cummax() - 1).min())
    return {"CAGR": cagr, "vol": vol, "Sharpe": cagr / vol if vol else np.nan,
            "MaxDD": dd, "Calmar": cagr / abs(dd) if dd else np.nan}


def run(d, spread_term):
    w = (d["w_risk"] * spread_term).shift(LAG).fillna(0.0)
    return (w * d["ls_proxy"]).dropna(), w


def main():
    d = pd.read_parquet(PANEL)
    s = d["Fragmentation_Spread"]
    log(f"panel {d.index.min().date()} -> {d.index.max().date()}  ({len(d)} days)")
    log(f"spread: min {s.min():.3g}  median {s.median():.3g}  "
        f"p95 {s.quantile(.95):.3g}  max {s.max():.3g}")
    log(f"production ceiling {CEIL:.3g} -> gate OFF on {(s >= CEIL).mean()*100:.1f}% of days")

    # ---------------------------------------------------------------- Q0
    log("\n" + "=" * 74)
    log("Q0. Does the EXISTING binary spread gate add value at all?")
    log("=" * 74)
    no_gate = pd.Series(1.0, index=d.index)
    cur_gate = (s < CEIL).astype(float)
    p_no, w_no = run(d, no_gate)
    p_cur, w_cur = run(d, cur_gate)
    m_no, m_cur = metrics(p_no), metrics(p_cur)
    log(f"  no spread gate     CAGR {pct(m_no['CAGR']):>9}  Sharpe {m_no['Sharpe']:>5.2f}  "
        f"MaxDD {pct(m_no['MaxDD']):>9}  avgExp {w_no.mean():.2f}")
    log(f"  current binary     CAGR {pct(m_cur['CAGR']):>9}  Sharpe {m_cur['Sharpe']:>5.2f}  "
        f"MaxDD {pct(m_cur['MaxDD']):>9}  avgExp {w_cur.mean():.2f}")
    log(f"  -> the existing gate {'HELPS' if m_cur['Sharpe'] > m_no['Sharpe'] else 'HURTS'} "
        f"(dSharpe {m_cur['Sharpe'] - m_no['Sharpe']:+.2f})")

    # ---------------------------------------------------------------- Q1
    log("\n" + "=" * 74)
    log("Q1. Where does the break actually occur? (forward return by decile)")
    log("=" * 74)
    fwd = d["ls_proxy"].rolling(PRIMARY_H).sum().shift(-PRIMARY_H)
    x = pd.concat([s.rename("spr"), fwd.rename("f")], axis=1).dropna()
    dec = pd.qcut(x["spr"], 10, labels=False, duplicates="drop")
    rows = []
    for k, g in x.groupby(dec):
        rows.append({"decile": int(k) + 1, "n": len(g),
                     "spread_max": g["spr"].max(),
                     "mean_fwd_10d": g["f"].mean(),
                     "pct_above_ceiling": float((g["spr"] >= CEIL).mean())})
    dd_ = pd.DataFrame(rows)
    dd_.to_csv(OUT / "tables" / "01_spread_deciles.csv", index=False)
    for _, r in dd_.iterrows():
        bar = "#" * max(0, int(r["mean_fwd_10d"] * 200))
        log(f"  D{int(r['decile']):<2} spread<={r['spread_max']:.3g}  n={int(r['n']):>4}  "
            f"fwd10d {pct(r['mean_fwd_10d']):>8}  {bar}")
    log(f"  production ceiling sits at the {(s < CEIL).mean()*100:.0f}th percentile of spread")

    # ---------------------------------------------------------------- Q2
    log("\n" + "=" * 74)
    log("Q2. Continuous scalers vs the binary gate")
    log("=" * 74)
    spct = s.rolling(365, min_periods=90).rank(pct=True)
    variants = {
        "no spread gate": no_gate,
        "A current binary @7.5e-05": cur_gate,
        "B binary @ matched pctile": (spct < float((s < CEIL).mean())).astype(float).fillna(1.0),
        "C linear taper on pctile": (1.0 - spct).clip(0, 1).fillna(1.0),
        "D taper floored at 0.25": (1.0 - spct).clip(0.25, 1).fillna(1.0),
        "E taper, top-quartile off": np.minimum(1.0, (1.0 - spct) / 0.75).clip(0, 1).fillna(1.0),
    }
    rows = []
    for name, term in variants.items():
        pnl, w = run(d, term)
        m = metrics(pnl)
        rows.append({"variant": name, **m, "avg_exposure": w.mean(),
                     "turnover_ann": w.diff().abs().sum() / (len(w) / TD)})
    perf = pd.DataFrame(rows)
    perf.to_csv(OUT / "tables" / "02_variants.csv", index=False)
    for _, r in perf.iterrows():
        log(f"  {r['variant']:<28} CAGR {pct(r['CAGR']):>9}  Sharpe {r['Sharpe']:>5.2f}  "
            f"MaxDD {pct(r['MaxDD']):>9}  Calmar {r['Calmar']:>5.2f}  "
            f"avgExp {r['avg_exposure']:.2f}  turn {r['turnover_ann']:.0f}")

    # ---------------------------------------------------------------- Q3
    log("\n" + "=" * 74)
    log("Q3. The bar that killed DVOL: placebo, bootstrap CI, train/test")
    log("=" * 74)
    base_pnl, base_w = run(d, cur_gate)          # baseline = CURRENT production rule
    base_sh = metrics(base_pnl)["Sharpe"]
    rng = np.random.default_rng(3)

    def block_boot_delta(p_alt, p_base, B=60, reps=3000):
        j = p_alt.index.intersection(p_base.index)
        a, b = p_alt.loc[j].values, p_base.loc[j].values
        n = len(j)
        out = []
        for _ in range(reps):
            nb = int(np.ceil(n / B))
            st = rng.integers(0, n, size=nb)
            idx = (st[:, None] + np.arange(B)[None, :]).ravel() % n
            idx = idx[:n]
            out.append(metrics(pd.Series(a[idx]))["Sharpe"] - metrics(pd.Series(b[idx]))["Sharpe"])
        return np.array([v for v in out if np.isfinite(v)])

    boot_rows = []
    for name in ("C linear taper on pctile", "D taper floored at 0.25", "E taper, top-quartile off"):
        alt_pnl, alt_w = run(d, variants[name])
        obs = metrics(alt_pnl)["Sharpe"] - base_sh
        bs = block_boot_delta(alt_pnl, base_pnl)
        lo, hi = np.percentile(bs, [2.5, 97.5])
        p_le0 = float((bs <= 0).mean())
        boot_rows.append({"variant": name, "delta_sharpe": obs, "ci_lo": lo, "ci_hi": hi,
                          "p_delta_le_0": p_le0, "avg_exp": alt_w.mean(),
                          "avg_exp_base": base_w.mean()})
        log(f"  {name:<28} dSharpe vs current {obs:+.3f}  "
            f"95% CI [{lo:+.3f}, {hi:+.3f}]  P(d<=0)={p_le0:.3f}")
    pd.DataFrame(boot_rows).to_csv(OUT / "tables" / "03_bootstrap.csv", index=False)

    # placebo at matched dose, for the best continuous variant
    best = max(boot_rows, key=lambda r: r["delta_sharpe"])["variant"]
    log(f"\n  -- placebo for {best}, matched average exposure")
    target_exp = variants[best].mean()
    sims = []
    n = len(d)
    for _ in range(2000):
        m = pd.Series(rng.permutation(variants[best].values), index=d.index)
        pnl, _ = run(d, m)
        sims.append(metrics(pnl)["Sharpe"])
    sims = np.array([v for v in sims if np.isfinite(v)])
    real_sh = metrics(run(d, variants[best])[0])["Sharpe"]
    beat = float((sims >= real_sh).mean())
    log(f"     real {real_sh:.2f}   shuffled-same-values mean {sims.mean():.2f} "
        f"[p5 {np.percentile(sims,5):.2f}, p95 {np.percentile(sims,95):.2f}]")
    log(f"     {beat*100:.1f}% of shuffles match or beat it -> timing "
        f"{'MATTERS' if beat < 0.05 else 'NOT established'}")
    pd.DataFrame([{"variant": best, "real_sharpe": real_sh,
                   "placebo_mean": sims.mean(), "pct_placebos_better": beat,
                   "target_exposure": target_exp}]).to_csv(
        OUT / "tables" / "04_placebo.csv", index=False)

    # train / test
    split = d.index[len(d) // 2]
    log(f"\n  -- train/test split at {split.date()}")
    rows = []
    for tag, sl in (("train", d.index < split), ("test ", d.index >= split)):
        sub = d[sl]
        for name in ("A current binary @7.5e-05", best):
            pnl, w = run(sub, variants[name].loc[sub.index])
            m = metrics(pnl)
            rows.append({"half": tag.strip(), "variant": name, **m, "avg_exp": w.mean()})
            log(f"     {tag} {name:<28} CAGR {pct(m['CAGR']):>9}  "
                f"Sharpe {m['Sharpe']:>5.2f}  MaxDD {pct(m['MaxDD']):>9}")
    pd.DataFrame(rows).to_csv(OUT / "tables" / "05_train_test.csv", index=False)

    # ---------------------------------------------------------------- figure
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                             gridspec_kw={"height_ratios": [2, 1]})
    for name in ("no spread gate", "A current binary @7.5e-05", best):
        pnl, _ = run(d, variants[name])
        axes[0].plot(pnl.index, (1 + pnl).cumprod(), lw=1.5, label=name)
    axes[0].set_ylabel("Growth of 1")
    axes[0].legend(frameon=False, fontsize=9)
    axes[0].grid(alpha=0.25)
    axes[1].plot(s.index, s.values, color="#B23A48", lw=0.9)
    axes[1].axhline(CEIL, ls="--", color="#333", lw=1.0, label=f"ceiling {CEIL:.0e}")
    axes[1].set_ylabel("Fragmentation_Spread")
    axes[1].legend(frameon=False, fontsize=8)
    axes[1].grid(alpha=0.25)
    fig.suptitle("Fragmentation_Spread: binary gate vs continuous scaler", y=0.94)
    fig.savefig(OUT / "fig01_spread_variants.png", dpi=150, bbox_inches="tight",
                facecolor="white")
    plt.close(fig)
    log("\n-> results/fig01_spread_variants.png, tables 01-05")


if __name__ == "__main__":
    main()
