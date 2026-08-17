"""
End-to-end runner for the BTC trend-agreement study.

    python run_all.py [--refresh]

Writes every table to results/tables/*.csv and every figure to
results/figures/*.png, and prints a running log so the console output itself
is an audit trail.
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
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
from config import (BASE_COST_BPS, BOOTSTRAP_BLOCK_DAYS, CASH_RATE_SENSITIVITY,
                    COINBASE_CLAIM_20D, CORE_LOOKBACKS, COST_BPS_GRID,
                    DVOL_THRESHOLD_DEFAULT, DVOL_THRESHOLD_GRID, FIGURE_DIR,
                    FORWARD_HORIZONS, GRID_LONG, GRID_MEDIUM, GRID_SHORT,
                    OOS_SPLIT_DATE, PRIMARY_HORIZON, SIGNAL_LAG,
                    SIGNAL_LAG_SENSITIVITY, STRATEGY_MAPS, SUBPERIODS,
                    TABLE_DIR, hac_lags)

warnings.filterwarnings("ignore", category=RuntimeWarning)
pd.set_option("display.width", 200)

PALETTE = {0: "#B23A48", 1: "#D98324", 2: "#5B8C5A", 3: "#1F6F8B"}
LINE = {"bh": "#8A8A8A", "A_linear": "#1F6F8B", "B_threshold": "#5B8C5A",
        "C_strong": "#D98324", "D_long_veto": "#8E6C99"}


def log(msg: str) -> None:
    print(f"[{pd.Timestamp.utcnow():%H:%M:%S}] {msg}", flush=True)


def save_table(df: pd.DataFrame, name: str) -> None:
    path = TABLE_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    log(f"  -> table {path.name}  ({len(df)} rows)")


def save_fig(fig, name: str) -> None:
    path = FIGURE_DIR / f"{name}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    log(f"  -> figure {path.name}")


def pct(x) -> str:
    return "n/a" if not np.isfinite(x) else f"{x*100:+.2f}%"


# ==========================================================================
# 1. DATA
# ==========================================================================
def section_data(refresh: bool):
    log("SECTION 1 -- data acquisition")
    px, prov = data_io.load_prices(refresh=refresh)
    dvol, dprov = data_io.load_dvol(refresh=refresh)

    lake_note = {"status": "not_available"}
    lake = data_io.load_lake_btc_crosscheck()
    if lake is not None:
        # Scan a small lead/lag window rather than assuming the lake's date
        # stamp means the same thing as an exchange UTC close. It does not:
        # see the alignment finding recorded below.
        scan = {}
        for k in (-2, -1, 0, 1, 2):
            s = lake.shift(k)
            ov = px["BTC"].index.intersection(s.dropna().index)
            if len(ov) < 30:
                continue
            a = px["BTC"].loc[ov].pct_change().dropna()
            b = s.loc[ov].pct_change().dropna()
            i = a.index.intersection(b.index)
            scan[k] = {
                "daily_return_corr": float(a.loc[i].corr(b.loc[i])),
                "median_abs_level_diff_pct": float(
                    ((px["BTC"].loc[ov] / s.loc[ov] - 1).abs().median()) * 100),
                "overlap_days": int(len(ov)),
            }
        if scan:
            best = max(scan, key=lambda k: scan[k]["daily_return_corr"])
            lake_note = {
                "status": "compared",
                "source": "project curated lake fact_price (CoinGecko), asset_id=BTC",
                "overlap_start": str(lake.index.min().date()),
                "overlap_end": str(lake.index.max().date()),
                "best_alignment_shift_days": int(best),
                "corr_at_naive_alignment": scan.get(0, {}).get("daily_return_corr"),
                "corr_at_best_alignment": scan[best]["daily_return_corr"],
                "median_abs_level_diff_pct_at_best": scan[best]["median_abs_level_diff_pct"],
                "scan": {str(k): v for k, v in scan.items()},
                "finding": (
                    "fact_price.date is stamped ONE DAY LATER than the UTC close "
                    "it represents (lake[t] == exchange close of t-1), consistent "
                    "with CoinGecko market_chart 00:00 UTC snapshots. Once shifted "
                    "the two agree to ~0.05% median. Naive same-date joins between "
                    "the lake and exchange data are misaligned by one day."
                ),
                "note": "lake used as an INDEPENDENT CHECK only -- its BTC history "
                        "starts 2024-01-07 and the local copy is stale (max "
                        "2026-01-05), so it cannot be the primary source for a "
                        "2015-present study",
            }
    data_io.write_provenance(prov, dprov, lake_note)

    rows = []
    for a in ("BTC", "ETH", "SOL"):
        p = prov[a]
        cc = p.get("crosscheck", {})
        rows.append({
            "asset": a, "primary_venue": p["venue"], "instrument": p["instrument"],
            "start": p["start"], "end": p["end"], "n_days": p["n_days"],
            "n_forward_filled_days": p["n_filled_days"],
            "crosscheck_venue": cc.get("venue", ""), "crosscheck_instrument": cc.get("instrument", ""),
            "crosscheck_overlap_days": cc.get("overlap_days", np.nan),
            "crosscheck_daily_ret_corr": cc.get("daily_return_corr", np.nan),
            "crosscheck_median_abs_level_diff_pct": cc.get("median_abs_level_diff_pct", np.nan),
        })
    rows.append({"asset": "BTC_DVOL", "primary_venue": "deribit",
                 "instrument": "BTC DVOL index", "start": dprov["start"],
                 "end": dprov["end"], "n_days": dprov["n_days"],
                 "n_forward_filled_days": dprov["n_filled_days"],
                 "crosscheck_venue": "", "crosscheck_instrument": "",
                 "crosscheck_overlap_days": np.nan,
                 "crosscheck_daily_ret_corr": np.nan,
                 "crosscheck_median_abs_level_diff_pct": np.nan})
    prov_df = pd.DataFrame(rows)
    save_table(prov_df, "01_data_provenance")
    log(f"  BTC {prov['BTC']['start']} -> {prov['BTC']['end']} "
        f"({prov['BTC']['n_days']} days, {prov['BTC']['n_filled_days']} ffilled)")
    log(f"  DVOL {dprov['start']} -> {dprov['end']} ({dprov['n_days']} days)")
    if lake_note["status"] == "compared":
        log(f"  lake cross-check: corr {lake_note['corr_at_naive_alignment']:.4f} at naive "
            f"same-date join -> {lake_note['corr_at_best_alignment']:.4f} at shift "
            f"{lake_note['best_alignment_shift_days']:+d}d "
            f"(median level diff {lake_note['median_abs_level_diff_pct_at_best']:.3f}%)")
    return px, dvol, prov, dprov, lake_note


# ==========================================================================
# 2-3. CORE REPLICATION + STATISTICS
# ==========================================================================
def section_core(btc: pd.Series):
    log("SECTION 2 -- core replication")
    frame = ts.build_asset_frame(btc)
    score = frame["trend_score"]
    fwd = frame[[f"fwd_{h}" for h in FORWARD_HORIZONS]]

    cond = ts.conditional_table(score, fwd)
    save_table(cond, "02_btc_conditional_forward_returns")

    c20 = cond[cond.horizon_days == PRIMARY_HORIZON].set_index("trend_score")
    comp = pd.DataFrame({
        "trend_score": [0, 1, 2, 3],
        "coinbase_claim_20d": [COINBASE_CLAIM_20D[k] for k in range(4)],
        "our_mean_20d": [c20.loc[k, "mean"] for k in range(4)],
        "our_median_20d": [c20.loc[k, "median"] for k in range(4)],
        "difference": [c20.loc[k, "mean"] - COINBASE_CLAIM_20D[k] for k in range(4)],
        "n_obs": [int(c20.loc[k, "n_obs"]) for k in range(4)],
        "pct_of_sample": [c20.loc[k, "pct_of_sample"] for k in range(4)],
        "win_rate": [c20.loc[k, "win_rate"] for k in range(4)],
    })
    save_table(comp, "03_coinbase_replication_20d")
    for _, r in comp.iterrows():
        log(f"  score {int(r.trend_score)}: coinbase {pct(r.coinbase_claim_20d)} | "
            f"ours {pct(r.our_mean_20d)}  (n={int(r.n_obs)})")

    log("SECTION 3 -- autocorrelation-robust statistics")
    f20 = frame[f"fwd_{PRIMARY_HORIZON}"]
    hac = st.hac_group_regression(score, f20, PRIMARY_HORIZON)
    naive_rows, hac_rows = [], []
    for k in range(4):
        g = pd.concat([score, f20], axis=1).dropna()
        gk = g.loc[g.trend_score == k, f"fwd_{PRIMARY_HORIZON}"]
        h = st.hac_mean(gk, hac_lags(PRIMARY_HORIZON))
        naive_t = gk.mean() / (gk.std(ddof=1) / np.sqrt(len(gk))) if len(gk) > 2 else np.nan
        naive_rows.append({"trend_score": k, "n": len(gk), "mean": gk.mean(),
                           "naive_t": naive_t})
        hac_rows.append({"trend_score": k, "n": h["n"], "mean": h["mean"],
                         "hac_se": h["se"], "hac_t": h["t"], "hac_p": h["p"],
                         "naive_t": naive_t,
                         "t_inflation_naive_over_hac": naive_t / h["t"] if h["t"] else np.nan})
    hac_df = pd.DataFrame(hac_rows)
    save_table(hac_df, "04_hac_per_score_means_20d")

    hac_reg = pd.DataFrame([hac])
    save_table(hac_reg, "05_hac_group_regression_20d")
    log(f"  joint Wald 'all score means equal': F={hac['wald_F']:.2f} p={hac['wald_p']:.4g}")
    log(f"  score3-score0 = {pct(hac['score3_minus_score0'])} "
        f"(HAC t={hac['score3_minus_score0_t']:.2f}, p={hac['score3_minus_score0_p']:.4g})")
    log(f"  linear slope per score point = {pct(hac['slope_per_score'])} "
        f"(HAC t={hac['slope_t']:.2f})")

    nov = st.non_overlapping_test(score, f20, PRIMARY_HORIZON)
    save_table(nov, "06_non_overlapping_20d_all_phases")
    log(f"  non-overlapping (20 phases): median 3-0 diff = "
        f"{pct(nov['diff'].median())}, median p = {nov['p'].median():.3f}, "
        f"{int((nov['p'] < 0.05).sum())}/{len(nov)} phases significant at 5%")

    boot = st.block_bootstrap_stats(score, f20)
    save_table(pd.DataFrame([boot]), "07_block_bootstrap_20d")
    log(f"  bootstrap 3-0 spread 95% CI = [{pct(boot['spread_3_0_lo'])}, "
        f"{pct(boot['spread_3_0_hi'])}], P(spread<=0) = {boot['p_spread_le_0']:.4f}")
    log(f"  bootstrap P(strictly monotonic 0<1<2<3) = {boot['p_strictly_monotonic']:.3f}")
    log(f"  bootstrap Spearman rho = {boot['spearman_boot']:.4f} "
        f"[{boot['spearman_lo']:.4f}, {boot['spearman_hi']:.4f}]")

    # multi-horizon HAC summary
    mh = []
    for h in FORWARD_HORIZONS:
        r = st.hac_group_regression(score, frame[f"fwd_{h}"], h)
        r["horizon_days"] = h
        rho, p = st.spearman_score_vs_fwd(score, frame[f"fwd_{h}"])
        r["spearman_rho"] = rho
        mh.append(r)
    mh_df = pd.DataFrame(mh)
    cols = ["horizon_days"] + [c for c in mh_df.columns if c != "horizon_days"]
    save_table(mh_df[cols], "08_multi_horizon_hac")

    return frame, cond, comp, hac, nov, boot, mh_df


# ==========================================================================
# 4. SUBPERIODS + EX-POST REGIMES
# ==========================================================================
def label_regimes_expost(btc: pd.Series) -> pd.Series:
    """EX-POST descriptive regime labels. USES FUTURE INFORMATION BY DESIGN.

    A centred 181-day window (90 days back, 90 days forward) return of
    > +25% is 'bull', < -25% is 'bear', otherwise 'sideways'. This label is
    used for descriptive robustness only and NEVER enters a backtest.
    """
    fwd90 = btc.shift(-90) / btc - 1.0
    bwd90 = btc / btc.shift(90) - 1.0
    centred = (1 + fwd90) * (1 + bwd90) - 1.0
    lab = pd.Series(np.where(centred > 0.25, "bull",
                    np.where(centred < -0.25, "bear", "sideways")),
                    index=btc.index, name="regime")
    lab[centred.isna()] = np.nan
    return lab


def section_subperiods(frame: pd.DataFrame, btc: pd.Series):
    log("SECTION 4 -- subperiod and regime robustness")
    score = frame["trend_score"]
    f20 = frame[f"fwd_{PRIMARY_HORIZON}"]
    df = pd.concat([score, f20.rename("fwd")], axis=1).dropna()

    rows = []
    for name, (s, e) in SUBPERIODS.items():
        sub = df[(df.index >= s) & (df.index <= e)]
        if len(sub) < 100:
            continue
        means = sub.groupby("trend_score")["fwd"].mean().reindex([0, 1, 2, 3])
        counts = sub.groupby("trend_score")["fwd"].size().reindex([0, 1, 2, 3]).fillna(0)
        rho, p = st.spearman_score_vs_fwd(sub["trend_score"], sub["fwd"])
        present = means.dropna()
        rows.append({
            "period": name, "start": str(sub.index.min().date()),
            "end": str(sub.index.max().date()), "n_obs": len(sub),
            **{f"score{k}_mean": means.get(k, np.nan) for k in range(4)},
            **{f"score{k}_n": int(counts.get(k, 0)) for k in range(4)},
            "spread_3_0": means.get(3, np.nan) - means.get(0, np.nan),
            "strictly_monotone": bool(len(present) > 1 and (present.diff().dropna() > 0).all()),
            "spearman_rho": rho, "spearman_p": p,
        })
    sp = pd.DataFrame(rows)
    save_table(sp, "09_subperiod_20d")
    for _, r in sp.iterrows():
        log(f"  {r['period']}: 3-0 = {pct(r['spread_3_0'])}, rho={r['spearman_rho']:.3f}, "
            f"monotone={r['strictly_monotone']}  (n0={r['score0_n']})")

    # calendar-year detail
    yr_rows = []
    for y, g in df.groupby(df.index.year):
        means = g.groupby("trend_score")["fwd"].mean().reindex([0, 1, 2, 3])
        rho, _ = st.spearman_score_vs_fwd(g["trend_score"], g["fwd"])
        yr_rows.append({"year": int(y), "n_obs": len(g),
                        **{f"score{k}_mean": means.get(k, np.nan) for k in range(4)},
                        "spread_3_0": means.get(3, np.nan) - means.get(0, np.nan),
                        "spearman_rho": rho,
                        "n_distinct_scores": int(g["trend_score"].nunique())})
    save_table(pd.DataFrame(yr_rows), "10_by_calendar_year_20d")

    # ex-post regimes (descriptive only)
    reg = label_regimes_expost(btc)
    rdf = pd.concat([df, reg], axis=1).dropna()
    rrows = []
    for name, g in rdf.groupby("regime"):
        means = g.groupby("trend_score")["fwd"].mean().reindex([0, 1, 2, 3])
        counts = g.groupby("trend_score")["fwd"].size().reindex([0, 1, 2, 3]).fillna(0)
        rho, p = st.spearman_score_vs_fwd(g["trend_score"], g["fwd"])
        rrows.append({"regime_EXPOST_DESCRIPTIVE_ONLY": name, "n_obs": len(g),
                      **{f"score{k}_mean": means.get(k, np.nan) for k in range(4)},
                      **{f"score{k}_n": int(counts.get(k, 0)) for k in range(4)},
                      "spread_3_0": means.get(3, np.nan) - means.get(0, np.nan),
                      "spearman_rho": rho, "spearman_p": p})
    save_table(pd.DataFrame(rrows), "11_expost_regime_20d")
    return sp, reg


# ==========================================================================
# 4b. DOES *AGREEMENT* ADD ANYTHING? (the core hypothesis, falsification test)
# ==========================================================================
def section_agreement_value(btc: pd.Series, frame: pd.DataFrame):
    """Is TrendScore better than any ONE of its three ingredients?

    Coinbase's claim is specifically about AGREEMENT across horizons. If a
    single trailing-return sign carries the same information, the multi-horizon
    construction is decoration. This is the cleanest available falsification of
    the stated hypothesis, so it is run before any strategy work.
    """
    log("SECTION 5a -- incremental value of trend AGREEMENT")
    f20 = frame[f"fwd_{PRIMARY_HORIZON}"]
    lags = hac_lags(PRIMARY_HORIZON)

    rows = []
    for lb in CORE_LOOKBACKS:
        sig = (btc / btc.shift(lb) - 1.0) > 0
        sig = sig.where((btc / btc.shift(lb) - 1.0).notna())
        d = pd.concat([sig.rename("on"), f20.rename("fwd")], axis=1).dropna()
        d = d.loc[d.index.isin(frame["trend_score"].dropna().index)]  # same sample
        on, off = d.loc[d.on == 1, "fwd"], d.loc[d.on == 0, "fwd"]
        X = sm.add_constant(d["on"].astype(float).values)
        res = sm.OLS(d["fwd"].values, X).fit(cov_type="HAC", cov_kwds={"maxlags": lags})
        rows.append({"signal": f"R{lb} > 0 (single horizon)", "n_on": len(on), "n_off": len(off),
                     "mean_on": on.mean(), "mean_off": off.mean(),
                     "spread_on_minus_off": on.mean() - off.mean(),
                     "hac_t": float(res.tvalues[1]), "hac_p": float(res.pvalues[1])})

    sc = frame["trend_score"]
    for label, on_mask in (("TrendScore == 3 (full agreement)", sc == 3),
                           ("TrendScore >= 2 (majority agreement)", sc >= 2)):
        d = pd.concat([on_mask.where(sc.notna()).rename("on"), f20.rename("fwd")], axis=1).dropna()
        on, off = d.loc[d.on == 1, "fwd"], d.loc[d.on == 0, "fwd"]
        X = sm.add_constant(d["on"].astype(float).values)
        res = sm.OLS(d["fwd"].values, X).fit(cov_type="HAC", cov_kwds={"maxlags": lags})
        rows.append({"signal": label, "n_on": len(on), "n_off": len(off),
                     "mean_on": on.mean(), "mean_off": off.mean(),
                     "spread_on_minus_off": on.mean() - off.mean(),
                     "hac_t": float(res.tvalues[1]), "hac_p": float(res.pvalues[1])})
    inc = pd.DataFrame(rows)
    save_table(inc, "12b_agreement_vs_single_horizon")
    for _, r in inc.iterrows():
        log(f"  {r['signal']:<38} on-off = {pct(r['spread_on_minus_off'])} "
            f"(HAC t={r['hac_t']:.2f}, p={r['hac_p']:.3f})")

    # Horse race: all three indicators in one HAC regression. If only one is
    # significant, "agreement" is really that one horizon wearing a costume.
    ind = pd.DataFrame({f"R{lb}_pos": ((btc / btc.shift(lb) - 1.0) > 0).astype(float)
                        .where((btc / btc.shift(lb) - 1.0).notna())
                        for lb in CORE_LOOKBACKS})
    d = pd.concat([ind, f20.rename("fwd")], axis=1).dropna()
    res = sm.OLS(d["fwd"].values, sm.add_constant(d[list(ind.columns)].values)).fit(
        cov_type="HAC", cov_kwds={"maxlags": lags})
    race = pd.DataFrame({
        "term": ["const"] + list(ind.columns),
        "coef": res.params, "hac_t": res.tvalues, "hac_p": res.pvalues,
    })
    race["n_obs"] = len(d)
    save_table(race, "12c_horizon_horse_race_hac")
    for _, r in race.iterrows():
        log(f"  horse race {r['term']:<12} coef {pct(r['coef'])} "
            f"(HAC t={r['hac_t']:.2f}, p={r['hac_p']:.3f})")
    return inc, race


# ==========================================================================
# 5. PARAMETER ROBUSTNESS
# ==========================================================================
def section_parameters(btc: pd.Series):
    log("SECTION 5 -- parameter robustness (36 pre-specified combinations)")
    rows = []
    for s in GRID_SHORT:
        for m in GRID_MEDIUM:
            for l in GRID_LONG:
                d = ts.parameter_diagnostics(btc, s, m, l, PRIMARY_HORIZON)
                if d:
                    d["is_coinbase"] = (s, m, l) == CORE_LOOKBACKS
                    rows.append(d)
    grid = pd.DataFrame(rows)
    save_table(grid, "12_parameter_grid_20d")

    core = grid[grid.is_coinbase].iloc[0]
    log(f"  Coinbase (30/90/365): spread={pct(core.spread_3_0)}, rho={core.spearman_rho:.3f}")
    log(f"  grid spread: min={pct(grid.spread_3_0.min())} "
        f"median={pct(grid.spread_3_0.median())} max={pct(grid.spread_3_0.max())}")
    log(f"  {int(grid.strictly_monotone.sum())}/{len(grid)} combos strictly monotone; "
        f"{int((grid.spread_3_0 > 0).sum())}/{len(grid)} have positive 3-0 spread")
    log(f"  Coinbase spread percentile within grid: "
        f"{(grid.spread_3_0 < core.spread_3_0).mean()*100:.0f}th")
    return grid


# ==========================================================================
# 6. STRATEGIES
# ==========================================================================
def build_strategies(btc: pd.Series, frame: pd.DataFrame):
    ret = btc.pct_change()
    score = frame["trend_score"]
    valid = score.dropna().index
    # Backtest starts once the signal exists plus the execution lag.
    start = valid.min() + pd.Timedelta(days=SIGNAL_LAG)
    ret = ret.loc[ret.index >= start]
    return ret, score


def section_strategies(btc: pd.Series, frame: pd.DataFrame):
    log("SECTION 6 -- pre-specified trend strategies")
    ret, score = build_strategies(btc, frame)

    bh = sg.buy_and_hold(ret)
    perf_rows = [sg.performance_metrics(bh, ret, "BTC buy & hold")]
    curves = {"bh": bh}

    cost_rows = []
    for name, mp in STRATEGY_MAPS.items():
        tw = sg.trend_exposure(score, mp)
        for c in COST_BPS_GRID:
            bt = sg.run_backtest(ret, tw, cost_bps=c)
            m = sg.performance_metrics(bt, ret, f"{name} @ {c:.0f}bps")
            m["cost_bps"] = c
            m["map"] = name
            cost_rows.append(m)
            if c == BASE_COST_BPS:
                perf_rows.append(m)
                curves[name] = bt
    save_table(pd.DataFrame(cost_rows), "13_strategy_cost_sensitivity")
    perf = pd.DataFrame(perf_rows)
    save_table(perf, "14_strategy_performance_base")
    for _, r in perf.iterrows():
        log(f"  {r['strategy']:<22} CAGR {pct(r['CAGR'])}  vol {r['ann_vol']:.2f}  "
            f"Sharpe {r['Sharpe']:.2f}  MaxDD {pct(r['max_drawdown'])}  "
            f"Calmar {r['Calmar']:.2f}")

    # execution-lag and cash-rate sensitivities
    sens = []
    for name, mp in STRATEGY_MAPS.items():
        tw = sg.trend_exposure(score, mp)
        for lag in (SIGNAL_LAG_SENSITIVITY, SIGNAL_LAG, 3):
            bt = sg.run_backtest(ret, tw, cost_bps=BASE_COST_BPS, lag=lag)
            m = sg.performance_metrics(bt, ret, f"{name} lag={lag}")
            m["map"] = name
            m["signal_lag_days"] = lag
            m["cash_rate"] = 0.0
            sens.append(m)
        bt = sg.run_backtest(ret, tw, cost_bps=BASE_COST_BPS,
                             cash_rate_annual=CASH_RATE_SENSITIVITY)
        m = sg.performance_metrics(bt, ret, f"{name} cash={CASH_RATE_SENSITIVITY:.0%}")
        m["map"] = name
        m["signal_lag_days"] = SIGNAL_LAG
        m["cash_rate"] = CASH_RATE_SENSITIVITY
        sens.append(m)
    save_table(pd.DataFrame(sens), "15_strategy_lag_and_cash_sensitivity")
    return ret, score, curves, perf


# ==========================================================================
# 7. OUT-OF-SAMPLE / WALK-FORWARD
# ==========================================================================
def section_oos(btc: pd.Series, frame: pd.DataFrame, ret: pd.Series, score: pd.Series):
    log("SECTION 7 -- out-of-sample split")
    f20 = frame[f"fwd_{PRIMARY_HORIZON}"]
    df = pd.concat([score, f20.rename("fwd")], axis=1).dropna()

    rows = []
    for tag, mask in (("train (pre-2022)", df.index < OOS_SPLIT_DATE),
                      ("OOS (2022+)", df.index >= OOS_SPLIT_DATE)):
        sub = df[mask]
        means = sub.groupby("trend_score")["fwd"].mean().reindex([0, 1, 2, 3])
        hac = st.hac_group_regression(sub["trend_score"], sub["fwd"], PRIMARY_HORIZON)
        rho, p = st.spearman_score_vs_fwd(sub["trend_score"], sub["fwd"])
        boot = st.block_bootstrap_stats(sub["trend_score"], sub["fwd"], reps=1000)
        rows.append({
            "sample": tag, "start": str(sub.index.min().date()),
            "end": str(sub.index.max().date()), "n_obs": len(sub),
            **{f"score{k}_mean": means.get(k, np.nan) for k in range(4)},
            **{f"score{k}_n": int((sub.trend_score == k).sum()) for k in range(4)},
            "spread_3_0": means.get(3, np.nan) - means.get(0, np.nan),
            "spread_3_0_hac_t": hac.get("score3_minus_score0_t", np.nan),
            "spread_3_0_hac_p": hac.get("score3_minus_score0_p", np.nan),
            "spearman_rho": rho, "spearman_p": p,
            "boot_spread_lo": boot.get("spread_3_0_lo", np.nan),
            "boot_spread_hi": boot.get("spread_3_0_hi", np.nan),
        })
    oos_signal = pd.DataFrame(rows)
    save_table(oos_signal, "16_oos_signal_split")
    for _, r in oos_signal.iterrows():
        log(f"  {r['sample']}: 3-0 = {pct(r['spread_3_0'])} "
            f"(HAC t={r['spread_3_0_hac_t']:.2f}), rho={r['spearman_rho']:.3f}")

    # Strategy performance in each half
    srows = []
    for tag, s, e in (("train (pre-2022)", ret.index.min(), pd.Timestamp(OOS_SPLIT_DATE)),
                      ("OOS (2022+)", pd.Timestamp(OOS_SPLIT_DATE), ret.index.max())):
        r_sub = ret[(ret.index >= s) & (ret.index <= e)]
        srows.append({**sg.performance_metrics(sg.buy_and_hold(r_sub), r_sub,
                                               "BTC buy & hold"), "sample": tag})
        for name, mp in STRATEGY_MAPS.items():
            tw = sg.trend_exposure(score, mp)
            bt = sg.run_backtest(r_sub, tw, cost_bps=BASE_COST_BPS)
            srows.append({**sg.performance_metrics(bt, r_sub, name), "sample": tag})
    save_table(pd.DataFrame(srows), "17_oos_strategy_split")

    # Honest walk-forward: choose the parameter triple on training data only,
    # freeze it, evaluate on the OOS half. Reported next to the frozen
    # Coinbase triple so the selection premium (or penalty) is visible.
    train_rows = []
    for s_ in GRID_SHORT:
        for m_ in GRID_MEDIUM:
            for l_ in GRID_LONG:
                d = ts.parameter_diagnostics(btc, s_, m_, l_, PRIMARY_HORIZON,
                                             end=OOS_SPLIT_DATE)
                if d:
                    train_rows.append(d)
    train = pd.DataFrame(train_rows)
    best = train.sort_values("spread_3_0", ascending=False).iloc[0]
    log(f"  train-selected triple = ({int(best.short)}/{int(best.medium)}/"
        f"{int(best.long)}) train spread {pct(best.spread_3_0)}")

    wf_rows = []
    for tag, trip in (("coinbase_30_90_365", CORE_LOOKBACKS),
                      ("train_selected", (int(best.short), int(best.medium), int(best.long)))):
        d_tr = ts.parameter_diagnostics(btc, *trip, PRIMARY_HORIZON, end=OOS_SPLIT_DATE)
        d_oos = ts.parameter_diagnostics(btc, *trip, PRIMARY_HORIZON, start=OOS_SPLIT_DATE)
        wf_rows.append({"triple": tag, "short": trip[0], "medium": trip[1], "long": trip[2],
                        "train_spread_3_0": d_tr.get("spread_3_0", np.nan),
                        "train_spearman": d_tr.get("spearman_rho", np.nan),
                        "oos_spread_3_0": d_oos.get("spread_3_0", np.nan),
                        "oos_spearman": d_oos.get("spearman_rho", np.nan),
                        "oos_strictly_monotone": d_oos.get("strictly_monotone", np.nan)})
    wf = pd.DataFrame(wf_rows)
    save_table(wf, "18_walk_forward_parameter_selection")
    for _, r in wf.iterrows():
        log(f"  {r['triple']}: train {pct(r['train_spread_3_0'])} -> "
            f"OOS {pct(r['oos_spread_3_0'])}")
    return oos_signal, wf


# ==========================================================================
# 8. CROSS-ASSET
# ==========================================================================
def _conditional_block(cond_score: pd.Series, price: pd.Series, horizons, tag: str) -> pd.DataFrame:
    rows = []
    for h in horizons:
        fwd = (price.shift(-h) / price - 1.0).rename("fwd")
        df = pd.concat([cond_score.rename("trend_score"), fwd], axis=1).dropna()
        if len(df) < 200:
            continue
        uncond = df["fwd"].mean()
        hac = st.hac_group_regression(df["trend_score"], df["fwd"], h)
        rho, rp = st.spearman_score_vs_fwd(df["trend_score"], df["fwd"])
        spread = hac.get("score3_minus_score0", np.nan)
        for k in range(4):
            g = df.loc[df.trend_score == k, "fwd"]
            if len(g) == 0:
                continue
            rows.append({
                "conditioning": tag, "horizon_days": h, "trend_score": k,
                "n_obs": len(g), "mean": g.mean(), "median": g.median(),
                "std": g.std(ddof=1), "win_rate": (g > 0).mean(),
                "p05": g.quantile(0.05), "p25": g.quantile(0.25), "p75": g.quantile(0.75),
                "mean_worst_decile": g[g <= g.quantile(0.10)].mean(),
                "unconditional_mean": uncond,
                "mean_excess_vs_uncond": g.mean() - uncond,
                "spread_3_0": spread,
                "hac_t_vs_score0": hac.get(f"score{k}_minus_score0_t", np.nan) if k else np.nan,
                "hac_p_vs_score0": hac.get(f"score{k}_minus_score0_p", np.nan) if k else np.nan,
                "spearman_rho_all_scores": rho, "spearman_p_all_scores": rp,
                "sample_start": str(df.index.min().date()),
                "sample_end": str(df.index.max().date()),
            })
    return pd.DataFrame(rows)


def section_cross_asset(px: pd.DataFrame, frame: pd.DataFrame):
    log("SECTION 8 -- cross-asset")
    btc_score = frame["trend_score"]
    horizons = (5, 20, 60)

    own_blocks, ext_blocks = [], []
    for asset in ("BTC", "ETH", "SOL"):
        p = px[asset].dropna()
        own_score = ts.trend_score(p)
        own_blocks.append(_conditional_block(own_score, p, horizons, f"{asset} own TrendScore -> {asset}"))
        if asset != "BTC":
            ext_blocks.append(_conditional_block(btc_score, p, horizons, f"BTC TrendScore -> {asset}"))
    own = pd.concat(own_blocks, ignore_index=True)
    ext = pd.concat(ext_blocks, ignore_index=True)
    save_table(own, "19_cross_asset_own_trendscore")
    save_table(ext, "20_cross_asset_btc_score_conditioning")

    # Does the cross-asset result survive the same era split that kills the
    # BTC-on-BTC result? This is the load-bearing check for the "altcoin
    # risk-on filter" claim, so it gets its own table.
    era_rows = []
    for asset in ("BTC", "ETH", "SOL"):
        p = px[asset].dropna()
        for era, (s, e) in (("train (pre-2022)", ("1900-01-01", OOS_SPLIT_DATE)),
                            ("OOS (2022+)", (OOS_SPLIT_DATE, "2100-01-01"))):
            fwd = (p.shift(-20) / p - 1.0).rename("fwd")
            d = pd.concat([btc_score.rename("trend_score"), fwd], axis=1).dropna()
            d = d[(d.index >= s) & (d.index < e)]
            if len(d) < 150:
                continue
            m = d.groupby("trend_score")["fwd"].mean().reindex([0, 1, 2, 3])
            hac = st.hac_group_regression(d["trend_score"], d["fwd"], 20)
            rho, rp = st.spearman_score_vs_fwd(d["trend_score"], d["fwd"])
            era_rows.append({
                "target_asset": asset, "conditioned_on": "BTC TrendScore",
                "era": era, "n_obs": len(d),
                **{f"score{k}_mean": m.get(k, np.nan) for k in range(4)},
                **{f"score{k}_n": int((d.trend_score == k).sum()) for k in range(4)},
                "spread_3_0": hac.get("score3_minus_score0", np.nan),
                "spread_3_0_hac_t": hac.get("score3_minus_score0_t", np.nan),
                "spread_3_0_hac_p": hac.get("score3_minus_score0_p", np.nan),
                "score3_minus_rest": float(
                    d.loc[d.trend_score == 3, "fwd"].mean()
                    - d.loc[d.trend_score < 3, "fwd"].mean()),
                "spearman_rho": rho, "spearman_p": rp,
            })
    era = pd.DataFrame(era_rows)
    save_table(era, "20b_cross_asset_era_split")
    for _, r in era.iterrows():
        log(f"  [era] BTC score -> {r['target_asset']:<4} {r['era']:<17} "
            f"3-0 = {pct(r['spread_3_0'])} (t={r['spread_3_0_hac_t']:.2f}), "
            f"3-vs-rest = {pct(r['score3_minus_rest'])}")

    for blk in (own, ext):
        for cond_name, g in blk[blk.horizon_days == 20].groupby("conditioning"):
            spread = g["spread_3_0"].dropna()
            tstat = g.loc[g.trend_score == 3, "hac_t_vs_score0"].dropna()
            s_txt = pct(spread.iloc[0]) if len(spread) else "n/a"
            t_txt = f"{tstat.iloc[0]:.2f}" if len(tstat) else "n/a"
            log(f"  {cond_name}: 20D spread 3-0 = {s_txt} (HAC t={t_txt}, n={int(g.n_obs.sum())})")
    return own, ext


# ==========================================================================
# 9. DVOL OVERLAY
# ==========================================================================
def section_dvol(btc: pd.Series, frame: pd.DataFrame, dvol: pd.Series, ret: pd.Series,
                 score: pd.Series):
    log("SECTION 9 -- DVOL overlay")
    common = ret.index.intersection(dvol.index)
    start = common.min()
    log(f"  DVOL-overlapping sample: {start.date()} -> {common.max().date()} "
        f"({len(common)} days)")
    r_d = ret.loc[ret.index >= start]

    rows = []
    for name, mp in STRATEGY_MAPS.items():
        tw = sg.trend_exposure(score, mp)
        bt = sg.run_backtest(r_d, tw, cost_bps=BASE_COST_BPS)
        rows.append({**sg.performance_metrics(bt, r_d, f"{name} trend-only"),
                     "map": name, "overlay": "trend only", "dvol_threshold": np.nan})
        for thr in DVOL_THRESHOLD_GRID:
            mult = sg.dvol_multiplier(dvol, thr).reindex(tw.index).ffill().fillna(1.0)
            bt2 = sg.run_backtest(r_d, tw * mult, cost_bps=BASE_COST_BPS)
            rows.append({**sg.performance_metrics(bt2, r_d, f"{name} +DVOL{thr:.0f}"),
                         "map": name, "overlay": "trend + DVOL", "dvol_threshold": thr})
    bh_d = sg.buy_and_hold(r_d)
    rows.insert(0, {**sg.performance_metrics(bh_d, r_d, "BTC buy & hold (DVOL sample)"),
                    "map": "buy_hold", "overlay": "none", "dvol_threshold": np.nan})
    dv = pd.DataFrame(rows)
    save_table(dv, "21_dvol_threshold_grid")

    sub = dv[dv["map"] == "B_threshold"]
    for _, r in sub.iterrows():
        log(f"  B_threshold {r['overlay']:<13} thr={r['dvol_threshold']!s:<6} "
            f"CAGR {pct(r['CAGR'])} Sharpe {r['Sharpe']:.2f} MaxDD {pct(r['max_drawdown'])} "
            f"Calmar {r['Calmar']:.2f} avgExp {r['avg_exposure']:.2f}")

    # How often does the brake even bind? If DVOL almost never exceeds the
    # threshold, the overlay is a no-op regardless of how sensible it looks.
    dv_al = dvol.reindex(ret.index).ffill().dropna()
    bind = pd.DataFrame([{
        "threshold": thr,
        "pct_days_DVOL_above": float((dv_al > thr).mean()),
        "mean_multiplier": float(np.minimum(1.0, thr / dv_al).mean()),
        "min_multiplier": float(np.minimum(1.0, thr / dv_al).min()),
    } for thr in DVOL_THRESHOLD_GRID])
    bind["dvol_min"] = float(dv_al.min())
    bind["dvol_median"] = float(dv_al.median())
    bind["dvol_p95"] = float(dv_al.quantile(0.95))
    bind["dvol_max"] = float(dv_al.max())
    save_table(bind, "21b_dvol_binding_frequency")
    log(f"  DVOL distribution: min {dv_al.min():.0f}, median {dv_al.median():.0f}, "
        f"p95 {dv_al.quantile(0.95):.0f}, max {dv_al.max():.0f}")
    log(f"  DVOL > 90 on {(dv_al > 90).mean()*100:.1f}% of days "
        f"-> the default brake is inactive {100-(dv_al>90).mean()*100:.1f}% of the time")

    # Crash-speed diagnostic: does DVOL brake fast crashes but miss slow bears?
    px_d = btc.loc[btc.index >= start]
    dd = px_d / px_d.cummax() - 1.0
    mult_default = sg.dvol_multiplier(dvol, DVOL_THRESHOLD_DEFAULT).reindex(px_d.index).ffill()
    dvol_al = dvol.reindex(px_d.index).ffill()
    ep_rows = []
    episodes = _drawdown_episodes(px_d, min_depth=0.20)
    for ep in episodes:
        s, e, trough, depth = ep
        seg_mult = mult_default.loc[s:trough]
        seg_dvol = dvol_al.loc[s:trough]
        days = max(1, (trough - s).days)
        # Worst 20-day stretch inside the episode = the acute phase.
        seg_px = px_d.loc[s:trough]
        r20 = seg_px / seg_px.shift(20) - 1.0
        if r20.notna().any():
            acute_end = r20.idxmin()
            acute_start = acute_end - pd.Timedelta(days=20)
            worst20 = float(r20.min())
            acute_dvol = float(dvol_al.loc[acute_start:acute_end].mean())
            acute_mult = float(mult_default.loc[acute_start:acute_end].mean())
        else:
            acute_end = acute_start = None
            worst20 = acute_dvol = acute_mult = np.nan
        ep_rows.append({
            "episode_start": str(s.date()), "trough": str(trough.date()),
            "recovery_end": str(e.date()) if e is not None else "ongoing",
            "depth": depth, "days_to_trough": days,
            "pct_per_day": depth / days,
            "speed": "fast (<=90d)" if days <= 90 else "slow (>90d)",
            "mean_DVOL": float(seg_dvol.mean()), "max_DVOL": float(seg_dvol.max()),
            "mean_vol_multiplier": float(seg_mult.mean()),
            "min_vol_multiplier": float(seg_mult.min()),
            "pct_days_braked": float((seg_mult < 0.999).mean()),
            "worst_20d_in_episode": worst20,
            "worst_20d_window": f"{acute_start.date()}..{acute_end.date()}" if acute_end is not None else "",
            "mean_DVOL_in_worst_20d": acute_dvol,
            "mean_multiplier_in_worst_20d": acute_mult,
        })
    ep_df = pd.DataFrame(ep_rows)
    save_table(ep_df, "22_dvol_crash_speed_episodes")
    for _, r in ep_df.iterrows():
        log(f"  {r['episode_start']}->{r['trough']} depth {pct(r['depth'])} "
            f"{r['speed']}: meanDVOL {r['mean_DVOL']:.0f}, "
            f"mean mult {r['mean_vol_multiplier']:.2f}, braked {r['pct_days_braked']*100:.0f}% of days")
    return dv, ep_df, start, mult_default, dd


def _drawdown_episodes(price: pd.Series, min_depth: float = 0.20):
    """Peak-to-trough episodes deeper than min_depth (descriptive, ex-post)."""
    peak = price.cummax()
    dd = price / peak - 1.0
    eps = []
    in_ep = False
    s = t = None
    for i, (d, v) in enumerate(dd.items()):
        if not in_ep and v < 0:
            in_ep = True
            s = d
            t = d
        elif in_ep:
            if dd.loc[t] > v:
                t = d
            if v >= 0:  # recovered
                depth = -dd.loc[t]
                if depth >= min_depth:
                    eps.append((s, d, t, depth))
                in_ep = False
    if in_ep:
        depth = -dd.loc[t]
        if depth >= min_depth:
            eps.append((s, None, t, depth))
    return eps


# ==========================================================================
# 10. DRAWDOWN CONTROL + DECOMPOSITION
# ==========================================================================
def section_decomposition(ret: pd.Series, score: pd.Series, dvol: pd.Series,
                          dvol_start: pd.Timestamp):
    log("SECTION 10 -- overlay decomposition")
    r_d = ret.loc[ret.index >= dvol_start]
    results = {}
    rows = []

    def add(label, bt, sample_ret):
        m = sg.performance_metrics(bt, sample_ret, label)
        rows.append(m)
        results[label] = bt

    # Full sample (no DVOL available before 2021) -- trend and trend+DD only
    full_rows = []
    bh_full = sg.buy_and_hold(ret)
    full_rows.append(sg.performance_metrics(bh_full, ret, "1. BTC buy & hold"))
    for name, mp in STRATEGY_MAPS.items():
        tw = sg.trend_exposure(score, mp)
        bt1 = sg.run_backtest(ret, tw, cost_bps=BASE_COST_BPS)
        bt2 = sg.run_backtest(ret, tw, cost_bps=BASE_COST_BPS, use_drawdown_brake=True)
        full_rows.append({**sg.performance_metrics(bt1, ret, f"2. {name} trend only"), "map": name})
        full_rows.append({**sg.performance_metrics(bt2, ret, f"3. {name} trend + DD brake"), "map": name})
    save_table(pd.DataFrame(full_rows), "23_decomposition_full_sample")

    # DVOL sample -- the full four-way ladder
    add("1. BTC buy & hold", sg.buy_and_hold(r_d), r_d)
    for name, mp in STRATEGY_MAPS.items():
        tw = sg.trend_exposure(score, mp)
        mult = sg.dvol_multiplier(dvol, DVOL_THRESHOLD_DEFAULT).reindex(tw.index).ffill().fillna(1.0)
        add(f"2. {name} trend", sg.run_backtest(r_d, tw, cost_bps=BASE_COST_BPS), r_d)
        add(f"3. {name} trend+DVOL", sg.run_backtest(r_d, tw * mult, cost_bps=BASE_COST_BPS), r_d)
        add(f"4. {name} trend+DVOL+DD",
            sg.run_backtest(r_d, tw * mult, cost_bps=BASE_COST_BPS, use_drawdown_brake=True), r_d)
    dec = pd.DataFrame(rows)
    save_table(dec, "24_decomposition_dvol_sample")
    for _, r in dec[dec.strategy.str.contains("B_threshold|buy & hold")].iterrows():
        log(f"  {r['strategy']:<28} CAGR {pct(r['CAGR'])} Sharpe {r['Sharpe']:.2f} "
            f"MaxDD {pct(r['max_drawdown'])} Calmar {r['Calmar']:.2f} "
            f"avgExp {r['avg_exposure']:.2f} turnover {r['turnover_ann']:.1f}")
    return dec, results, r_d


# ==========================================================================
# FIGURES
# ==========================================================================
def make_figures(btc, frame, comp, cond, boot, sp, grid, curves, ret, score,
                 own, ext, dvol, mult_default, dec, results, r_d, dvol_start, reg):
    log("SECTION 11 -- figures")
    f20 = frame[f"fwd_{PRIMARY_HORIZON}"]

    # --- Fig 1: replication bar chart with block-bootstrap CIs
    fig, ax = plt.subplots(figsize=(8, 5))
    xs = np.arange(4)
    ours = comp["our_mean_20d"].values * 100
    lo = np.array([boot[f"score{k}_mean_lo"] for k in range(4)]) * 100
    hi = np.array([boot[f"score{k}_mean_hi"] for k in range(4)]) * 100
    err = np.vstack([ours - lo, hi - ours])
    ax.bar(xs - 0.19, ours, 0.38, yerr=err, capsize=4,
           color=[PALETTE[k] for k in range(4)])
    ax.bar(xs + 0.19, comp["coinbase_claim_20d"].values * 100, 0.38,
           color="none", edgecolor="#333333", hatch="///")
    handles = [plt.Rectangle((0, 0), 1, 1, facecolor="#777777",
                             label="Ours (Coinbase spot, solid, coloured by score)"),
               plt.Rectangle((0, 0), 1, 1, facecolor="none", edgecolor="#333333",
                             hatch="///", label="Coinbase published")]
    ax.legend(handles=handles, frameon=False, loc="upper left")
    ax.axhline(0, color="#333333", lw=0.8)
    ax.set_xticks(xs)
    ax.set_xticklabels([f"Score {k}\nn={int(comp.n_obs[k])}" for k in range(4)])
    ax.set_ylabel("Mean 20-day forward return (%)")
    ax.set_title("BTC 20-day forward return by TrendScore\n"
                 "error bars = 95% circular block bootstrap CI (60-day blocks)")
    ax.grid(axis="y", alpha=0.25)
    save_fig(fig, "fig01_replication_20d")

    # --- Fig 2: multi-horizon heatmap
    piv = cond.pivot(index="trend_score", columns="horizon_days", values="mean") * 100
    fig, ax = plt.subplots(figsize=(8, 4))
    vmax = np.nanmax(np.abs(piv.values))
    im = ax.imshow(piv.values, cmap="RdYlGn", vmin=-vmax, vmax=vmax, aspect="auto")
    ax.set_xticks(range(len(piv.columns)))
    ax.set_xticklabels([f"{c}D" for c in piv.columns])
    ax.set_yticks(range(4))
    ax.set_yticklabels([f"Score {i}" for i in piv.index])
    for i in range(piv.shape[0]):
        for j in range(piv.shape[1]):
            ax.text(j, i, f"{piv.values[i, j]:+.1f}", ha="center", va="center", fontsize=9)
    ax.set_title("Mean BTC forward return (%) by TrendScore and horizon")
    fig.colorbar(im, ax=ax, label="%")
    save_fig(fig, "fig02_multi_horizon_heatmap")

    # --- Fig 3: subperiods
    fig, ax = plt.subplots(figsize=(9, 5))
    width = 0.2
    for k in range(4):
        ax.bar(np.arange(len(sp)) + (k - 1.5) * width,
               sp[f"score{k}_mean"].values * 100, width,
               color=PALETTE[k], label=f"Score {k}")
    ax.set_xticks(range(len(sp)))
    ax.set_xticklabels([f"{r.period}\n(n={r.n_obs})" for _, r in sp.iterrows()])
    ax.axhline(0, color="#333", lw=0.8)
    ax.set_ylabel("Mean 20-day forward return (%)")
    ax.set_title("TrendScore-conditional BTC 20D returns by era")
    ax.legend(frameon=False, ncol=4)
    ax.grid(axis="y", alpha=0.25)
    save_fig(fig, "fig03_subperiods")

    # --- Fig 4: parameter robustness
    fig, axes = plt.subplots(1, len(GRID_SHORT), figsize=(14, 4.2), sharey=True)
    vmax = np.nanmax(np.abs(grid.spread_3_0.values)) * 100
    for ax, s in zip(axes, GRID_SHORT):
        g = grid[grid.short == s].pivot(index="medium", columns="long", values="spread_3_0") * 100
        im = ax.imshow(g.values, cmap="RdYlGn", vmin=-vmax, vmax=vmax, aspect="auto")
        ax.set_xticks(range(len(g.columns)))
        ax.set_xticklabels(g.columns)
        ax.set_yticks(range(len(g.index)))
        ax.set_yticklabels(g.index)
        ax.set_title(f"short = {s}D")
        ax.set_xlabel("long lookback")
        for i in range(g.shape[0]):
            for j in range(g.shape[1]):
                is_core = (s, g.index[i], g.columns[j]) == CORE_LOOKBACKS
                ax.text(j, i, f"{g.values[i, j]:+.1f}", ha="center", va="center",
                        fontsize=9, fontweight="bold" if is_core else "normal",
                        color="black")
                if is_core:
                    ax.add_patch(plt.Rectangle((j - .5, i - .5), 1, 1, fill=False,
                                               edgecolor="black", lw=2.5))
    axes[0].set_ylabel("medium lookback")
    fig.suptitle("Parameter plateau: Score3 - Score0 mean 20D forward return (%)\n"
                 "Coinbase's 30/90/365 boxed in black", y=1.04)
    fig.colorbar(im, ax=axes, label="%", fraction=0.02)
    save_fig(fig, "fig04_parameter_robustness")

    # --- Fig 5: equity curves
    fig, ax = plt.subplots(figsize=(11, 5.5))
    for k, bt in curves.items():
        lbl = "BTC buy & hold" if k == "bh" else k
        ax.plot(bt.index, bt["equity"], lw=1.5, color=LINE.get(k, None), label=lbl)
    ax.set_yscale("log")
    ax.set_ylabel("Growth of 1 (log scale)")
    ax.set_title(f"BTC buy & hold vs pre-specified TrendScore strategies "
                 f"({BASE_COST_BPS:.0f} bps/trade, {SIGNAL_LAG}-day execution lag)")
    ax.legend(frameon=False)
    ax.grid(alpha=0.25)
    save_fig(fig, "fig05_equity_curves")

    # --- Fig 6: drawdowns (B_threshold = the conceptually simplest pre-specified rule)
    fig, ax = plt.subplots(figsize=(11, 4.5))
    for k in ("bh", "B_threshold"):
        eq = curves[k]["equity"]
        ax.fill_between(eq.index, (eq / eq.cummax() - 1) * 100, 0, alpha=0.45,
                        color=LINE.get(k), label="BTC buy & hold" if k == "bh" else k)
    ax.set_ylabel("Drawdown (%)")
    ax.set_title("Drawdown: BTC buy & hold vs Strategy B (pre-specified threshold rule)")
    ax.legend(frameon=False)
    ax.grid(alpha=0.25)
    save_fig(fig, "fig06_drawdowns")

    # --- Fig 7: BTC price + TrendScore + exposure
    tw_b = sg.trend_exposure(score, STRATEGY_MAPS["B_threshold"]).shift(SIGNAL_LAG)
    fig, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True,
                             gridspec_kw={"height_ratios": [2.2, 1, 1]})
    axes[0].plot(btc.index, btc.values, color="#222", lw=1.0)
    axes[0].set_yscale("log")
    axes[0].set_ylabel("BTC (USD, log)")
    axes[0].grid(alpha=0.25)
    sc = frame["trend_score"]
    axes[1].fill_between(sc.index, sc.values, step="post", alpha=0.7, color="#1F6F8B")
    axes[1].set_ylabel("TrendScore")
    axes[1].set_yticks([0, 1, 2, 3])
    axes[1].grid(alpha=0.25)
    axes[2].fill_between(tw_b.index, tw_b.values * 100, step="post", alpha=0.7, color="#5B8C5A")
    axes[2].set_ylabel("Strategy B\nexposure (%)")
    axes[2].grid(alpha=0.25)
    fig.suptitle("BTC price, TrendScore and executed exposure through time", y=0.93)
    save_fig(fig, "fig07_timeline")

    # --- Fig 8: cross-asset
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6), sharey=True)
    for ax, asset in zip(axes, ("ETH", "SOL")):
        g = ext[(ext.horizon_days == 20) & (ext.conditioning.str.endswith(asset))]
        o = own[(own.horizon_days == 20) & (own.conditioning.str.startswith(asset))]
        ax.bar(g.trend_score - 0.19, g["mean"] * 100, 0.38,
               color=[PALETTE[int(k)] for k in g.trend_score], label="conditioned on BTC score")
        ax.bar(o.trend_score + 0.19, o["mean"] * 100, 0.38, color="none",
               edgecolor="#333", hatch="///", label=f"conditioned on {asset}'s own score")
        ax.axhline(0, color="#333", lw=0.8)
        ax.set_xticks(range(4))
        ax.set_title(f"{asset} 20D forward return\n(sample from {g.sample_start.iloc[0]})")
        ax.set_xlabel("TrendScore")
        ax.grid(axis="y", alpha=0.25)
    axes[0].set_ylabel("Mean 20D forward return (%)")
    axes[0].legend(frameon=False, fontsize=8)
    save_fig(fig, "fig08_cross_asset")

    # --- Fig 9: DVOL panel
    px_d = btc.loc[btc.index >= dvol_start]
    dv = dvol.reindex(px_d.index).ffill()
    fig, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True,
                             gridspec_kw={"height_ratios": [2, 1.2, 1]})
    axes[0].plot(px_d.index, px_d.values, color="#222", lw=1.0)
    axes[0].set_yscale("log")
    axes[0].set_ylabel("BTC (USD, log)")
    axes[0].grid(alpha=0.25)
    axes[1].plot(dv.index, dv.values, color="#B23A48", lw=1.0)
    axes[1].axhline(DVOL_THRESHOLD_DEFAULT, ls="--", color="#333", lw=1.0,
                    label=f"threshold {DVOL_THRESHOLD_DEFAULT:.0f}")
    axes[1].set_ylabel("Deribit BTC DVOL")
    axes[1].legend(frameon=False)
    axes[1].grid(alpha=0.25)
    m = mult_default.reindex(px_d.index)
    axes[2].fill_between(m.index, m.values, 1.0, color="#D98324", alpha=0.7)
    axes[2].set_ylim(0.3, 1.02)
    axes[2].set_ylabel("vol multiplier")
    axes[2].grid(alpha=0.25)
    for ax in axes:
        ax.axvspan(pd.Timestamp("2021-11-10"), pd.Timestamp("2022-05-31"),
                   color="#888", alpha=0.15)
    fig.suptitle("BTC, Deribit DVOL and the one-way volatility brake\n"
                 "(shaded: Nov-2021 -> May-2022 slow bear market)", y=0.94)
    save_fig(fig, "fig09_dvol_panel")

    # --- Fig 10: decomposition ladder
    fig, ax = plt.subplots(figsize=(11, 5.5))
    keys = ["1. BTC buy & hold", "2. B_threshold trend", "3. B_threshold trend+DVOL",
            "4. B_threshold trend+DVOL+DD"]
    cols = ["#8A8A8A", "#1F6F8B", "#5B8C5A", "#D98324"]
    for k, c in zip(keys, cols):
        if k in results:
            ax.plot(results[k].index, results[k]["equity"], lw=1.6, color=c, label=k)
    ax.set_yscale("log")
    ax.set_ylabel("Growth of 1 (log scale)")
    ax.set_title(f"Overlay decomposition on the DVOL sample "
                 f"({dvol_start.date()} onward, Strategy B base)")
    ax.legend(frameon=False)
    ax.grid(alpha=0.25)
    save_fig(fig, "fig10_decomposition")


# ==========================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="re-download all cached data")
    args = ap.parse_args()

    px, dvol, prov, dprov, lake_note = section_data(args.refresh)
    btc = px["BTC"].dropna()

    frame, cond, comp, hac, nov, boot, mh = section_core(btc)
    sp, reg = section_subperiods(frame, btc)
    inc, race = section_agreement_value(btc, frame)
    grid = section_parameters(btc)
    ret, score, curves, perf = section_strategies(btc, frame)
    oos_signal, wf = section_oos(btc, frame, ret, score)
    own, ext = section_cross_asset(px, frame)
    dv, ep_df, dvol_start, mult_default, dd = section_dvol(btc, frame, dvol, ret, score)
    dec, results, r_d = section_decomposition(ret, score, dvol, dvol_start)

    make_figures(btc, frame, comp, cond, boot, sp, grid, curves, ret, score,
                 own, ext, dvol, mult_default, dec, results, r_d, dvol_start, reg)

    summary = {
        "sample": {"btc_start": str(btc.index.min().date()),
                   "btc_end": str(btc.index.max().date()),
                   "dvol_start": str(dvol_start.date())},
        "replication_20d": comp.set_index("trend_score")["our_mean_20d"].to_dict(),
        "coinbase_20d": COINBASE_CLAIM_20D,
        "hac": hac,
        "bootstrap": boot,
        "non_overlapping_median_p": float(nov["p"].median()),
        "grid_positive_spread_frac": float((grid.spread_3_0 > 0).mean()),
        "grid_monotone_frac": float(grid.strictly_monotone.mean()),
    }
    (TABLE_DIR.parent / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    log("DONE")


if __name__ == "__main__":
    main()
