"""Track A: Day-1 event dataset, conditional probability tables, predictive tests, rarity."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

from qlib import events as E
from qlib import stats
from track_a.features import SYSTEMS, brk_column, build_panel, signed_frame
from track_a.machine import day1_events, run_machine

EVAL_START = pd.Timestamp("2016-06-01")
HORIZONS = {"1d": 1, "3d": 3, "5d": 5, "10d": 10, "20d": 20}
OUTCOMES = ["confirmed", "reversed_early", "valid_3d", "valid_5d", "valid_10d", "valid_20d"]
SIGMA_BINS = [-np.inf, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, np.inf]
SIGMA_LABELS = ["<0.5", "0.5-1", "1-1.5", "1.5-2", "2-2.5", "2.5-3", ">3"]
STRENGTH_FEATURES = ["ret_z20", "ret2_z20", "move_atr", "tr_atr", "brk_atr", "brk_vol", "range_breakout_20",
                     "range_breakout_60", "range_breakout_120"]
MARKET_FEATURES = ["trend_agree_count", "spot_vol_z", "perp_vol_z", "funding_z90", "premium_z90", "oi_chg_1d",
                   "liq_forced_oi", "dvol_chg5", "breadth_above20", "perp_taker_share", "vol_pct", "mom30"]


def build(asset: str, system: str, exposure: str = "long_flat", K: int = 3):
    """Returns (panel f, K-close base machine, event observations)."""
    f = build_panel(asset, system)
    spec = SYSTEMS[system]
    base = run_machine(f["side"], K, exposure=exposure, neutral_flips=spec["neutral_flips"])
    ev = day1_events(f["side"], base, K, (3, 5, 10, 20), exposure)
    if ev.empty:
        return f, base, ev
    first_valid = f["side"].first_valid_index() + pd.Timedelta(days=30)
    ev = ev[ev["ts"] >= max(EVAL_START, first_valid)].reset_index(drop=True)
    sf = {1: signed_frame(f, 1), -1: signed_frame(f, -1)}
    feats = []
    for r in ev.itertuples():
        x = sf[r.direction].loc[r.ts].to_dict()
        x["brk_atr"] = x[brk_column("brk_atr", r.target)]
        x["brk_vol"] = x[brk_column("brk_vol", r.target)]
        feats.append(x)
    ev = pd.concat([ev, pd.DataFrame(feats)], axis=1)
    bars = f[["open", "high", "low", "close"]].rename_axis("ts").reset_index().assign(asset=asset)
    cfg = E.EventStudyConfig(horizons=HORIZONS, reps=500)
    obs = E.compute_event_returns(bars, ev.assign(asset=asset), cfg)
    obs["system"], obs["exposure"] = system, exposure
    obs["year"] = obs["ts"].dt.year
    return f, base, obs


# --------------------------------------------------------------------------
def conditional_tables(obs: pd.DataFrame, scope_cols=("system",)) -> pd.DataFrame:
    """P(confirm), P(reversal), P(valid h), forward returns, MAE/MFE by feature bucket."""
    rows = []
    horizons = ["3d", "5d", "10d", "20d"]
    for key, g in obs.groupby(list(scope_cols)):
        key = key if isinstance(key, tuple) else (key,)
        for feat in STRENGTH_FEATURES:
            if feat not in g or g[feat].notna().sum() < 20:
                continue
            t = E.conditional_curve(g, feat, SIGMA_BINS, horizons, OUTCOMES, labels=SIGMA_LABELS)
            t["binning"] = "sigma"
            rows.append(t.assign(**dict(zip(scope_cols, key))))
        for feat in MARKET_FEATURES:
            if feat not in g or g[feat].notna().sum() < 40:
                continue
            if feat == "trend_agree_count":
                bins, labels = [-0.5, 2.5, 4.5, 6.5, 8.5], ["0-2", "3-4", "5-6", "7-8"]
            else:
                q = g[feat].quantile([0, 0.2, 0.4, 0.6, 0.8, 1.0]).to_numpy()
                q = np.unique(q)
                if len(q) < 3:
                    continue
                bins, labels = q, [f"Q{i + 1}" for i in range(len(q) - 1)]
            t = E.conditional_curve(g, feat, bins, horizons, OUTCOMES, labels=labels)
            t["binning"] = "count" if feat == "trend_agree_count" else "quintile(full-sample, descriptive)"
            rows.append(t.assign(**dict(zip(scope_cols, key))))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def auc_table(obs: pd.DataFrame, scope_cols=("system",)) -> pd.DataFrame:
    rows = []
    outcomes = {"confirmed": obs["confirmed"], "valid_10d": obs["valid_10d"], "up_10d": (obs["fwd_10d"] > 0).astype(float)}
    for key, g in obs.groupby(list(scope_cols)):
        key = key if isinstance(key, tuple) else (key,)
        for feat in STRENGTH_FEATURES + MARKET_FEATURES:
            if feat not in g:
                continue
            for oc in outcomes:
                y = outcomes[oc].loc[g.index]
                x = g[feat]
                m = x.notna() & y.notna()
                pos, neg = x[m & (y == 1)], x[m & (y == 0)]
                if len(pos) < 8 or len(neg) < 8:
                    continue
                u, p = mannwhitneyu(pos, neg, alternative="two-sided")
                rows.append({**dict(zip(scope_cols, key)), "feature": feat, "outcome": oc,
                             "n_pos": len(pos), "n_neg": len(neg), "AUC": u / (len(pos) * len(neg)), "p": p})
    out = pd.DataFrame(rows)
    if len(out):
        out["fdr_reject_q10"], out["p_bh"] = stats.bh_fdr(out["p"], q=0.10)
    return out


def logit_oos(obs: pd.DataFrame, split="2022-01-01", embargo_days: int = 25) -> pd.DataFrame:
    """Time-split logistic regressions: interpretable feature sets vs base rate (AUC, Brier)."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import brier_score_loss, roc_auc_score
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    sets = {"ret_z20": ["ret_z20"], "brk_vol": ["brk_vol"], "range_breakout_20": ["range_breakout_20"],
            "strength3": ["ret_z20", "brk_vol", "trend_agree_share"],
            "strength+market": ["ret_z20", "brk_vol", "trend_agree_share", "perp_vol_z", "funding_z90", "vol_pct"]}
    rows = []
    s = pd.Timestamp(split)
    for system, g in obs.groupby("system"):
        for oc in ("confirmed", "valid_10d"):
            for name, cols in sets.items():
                cols = [c for c in cols if c in g]
                d = g.dropna(subset=cols + [oc])
                tr = d[d["ts"] < s - pd.Timedelta(days=embargo_days)]
                te = d[d["ts"] >= s]
                if tr[oc].nunique() < 2 or te[oc].nunique() < 2 or len(tr) < 30 or len(te) < 20:
                    continue
                m = make_pipeline(StandardScaler(), LogisticRegression(C=1.0)).fit(tr[cols], tr[oc])
                p = m.predict_proba(te[cols])[:, 1]
                base = np.full(len(te), tr[oc].mean())
                rows.append({"system": system, "outcome": oc, "features": name, "n_train": len(tr), "n_test": len(te),
                             "base_rate_train": tr[oc].mean(), "base_rate_test": te[oc].mean(),
                             "AUC_test": roc_auc_score(te[oc], p), "Brier_test": brier_score_loss(te[oc], p),
                             "Brier_base_rate": brier_score_loss(te[oc], base)})
    return pd.DataFrame(rows)


def rarity(obs: pd.DataFrame) -> pd.DataFrame:
    """How often do August-2026-like first closes happen, and how did they resolve?"""
    defs = {"ret_z20>=3": obs["ret_z20"] >= 3,
            "ret_z20>=3 & brk_vol>=1": (obs["ret_z20"] >= 3) & (obs["brk_vol"] >= 1),
            "ret_z20>=2 & range_breakout_20>=1": (obs["ret_z20"] >= 2) & (obs["range_breakout_20"] >= 1)}
    rows = []
    for name, m in defs.items():
        for (system, asset), g in obs[m].groupby(["system", "asset"]):
            yrs = g["year"].value_counts().sort_index()
            span_years = (obs["ts"].max() - EVAL_START).days / 365.25
            rows.append({"definition": name, "system": system, "asset": asset, "n": len(g),
                         "per_year": len(g) / span_years, "years": dict(yrs), "confirm_rate": g["confirmed"].mean(),
                         "valid_10d_rate": g["valid_10d"].mean(), "fwd_10d_mean": g["fwd_10d"].mean(),
                         "fwd_20d_median": g["fwd_20d"].median(), "mae_10d_median": g["mae_10d"].median()})
    return pd.DataFrame(rows)
