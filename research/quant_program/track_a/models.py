"""Track A execution models M0-M6, walk-forward fitting, sensitivity grids.

M0  3-close (current)                 M4  signal-strength staging (fit on training events)
M1  1-close                           M5  strong-signal override (threshold picked walk-forward)
M2  2-close                           M6  probability-based exposure (logistic, fit on training)
M3  linear staging 1/3, 2/3, 1

Execution: exposure decided at close t, filled at next open, 10 bp per unit turnover
(majors: 5 fee + 5 slippage). Perp implementation optionally subtracts funding.
Training for M4/M6/M5 uses only events whose outcomes were known before the test
period (25-day embargo), and only returns before the test period.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "btc_confirmation_lag"))

from backtest import episodes, pnl_from_target  # noqa: E402
from qlib import stats  # noqa: E402
from track_a.features import brk_column  # noqa: E402
from track_a.machine import run_machine  # noqa: E402

COST_BPS = 10.0
EMBARGO = pd.Timedelta(days=35)   # event P&L windows run up to 30 days after the first close
THRESH_GRID = [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0]
OVERRIDE_FEATURES = ["ret_z20", "brk_vol", "range_breakout_20", "move_atr"]
M6_FEATURES = ["ret_z20", "brk_vol", "trend_agree_share", "perp_vol_z", "vol_pct"]


def _dir(R, s):
    return 1 if s > R else -1


def _feat_value(sf, feat, t, R, s):
    return sf[_dir(R, s)][brk_column(feat, s)].iat[t]


# --------------------------------------------------------------------------
# Schedules
# --------------------------------------------------------------------------
def sched_override(sf, feat, thr):
    def fn(t, R, s):
        v = _feat_value(sf, feat, t, R, s)
        return (1.0, 1.0) if np.isfinite(v) and v >= thr else (0.0, 0.0)
    return fn


def sched_override_agree(sf, feat, thr, min_agree):
    def fn(t, R, s):
        v = _feat_value(sf, feat, t, R, s)
        a = sf[_dir(R, s)]["trend_agree_count"].iat[t]
        return (1.0, 1.0) if np.isfinite(v) and v >= thr and np.isfinite(a) and a >= min_agree else (0.0, 0.0)
    return fn


def fit_strength_staging(train_ev: pd.DataFrame, feat: str):
    """M4: exposure after close #1 = isotonic P(confirm | feature) if it beats the training break-even
    probability; after close #2 = P(confirm | reached 2 closes). Returns (predict_f1, f2, p_star, meta)."""
    from sklearn.isotonic import IsotonicRegression
    d = train_ev.dropna(subset=[feat, "confirmed"])
    if len(d) < 30 or d["confirmed"].nunique() < 2:
        return None
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1).fit(d[feat], d["confirmed"])
    p2 = d.loc[d["closes_reached"] >= 2, "confirmed"].mean()
    p_star = breakeven_probability(d)
    return iso, p2, p_star


def sched_strength(sf, feat, model):
    iso, p2, p_star = model

    def fn(t, R, s):
        v = _feat_value(sf, feat, t, R, s)
        p1 = float(iso.predict([v])[0]) if np.isfinite(v) else 0.0
        f1 = p1 if p1 > p_star else 0.0
        f2 = max(f1, p2 if p2 > p_star else 0.0)
        return (f1, f2)
    return fn


def fit_probability(train_ev: pd.DataFrame, outcome: str = "confirmed"):
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    cols = [c for c in M6_FEATURES if c in train_ev and train_ev[c].notna().mean() > 0.8]
    d = train_ev.dropna(subset=cols + [outcome])
    if len(d) < 40 or d[outcome].nunique() < 2:
        return None
    m = make_pipeline(StandardScaler(), LogisticRegression(C=1.0)).fit(d[cols], d[outcome])
    p2 = d.loc[d["closes_reached"] >= 2, "confirmed"].mean()
    return m, cols, p2


def sched_probability(sf, model, cutoff):
    m, cols, p2 = model

    def fn(t, R, s):
        row = sf[_dir(R, s)].iloc[t]
        x = [row[brk_column(c, s)] for c in cols]
        if not np.all(np.isfinite(x)):
            return (0.0, 0.0)
        p = float(m.predict_proba(np.array(x, dtype=float).reshape(1, -1))[0, 1])
        f1 = float(np.clip((p - cutoff) / max(1 - cutoff, 1e-6), 0, 1))
        f2 = max(f1, float(np.clip((p2 - cutoff) / max(1 - cutoff, 1e-6), 0, 1)))
        return (f1, f2)
    return fn


# --------------------------------------------------------------------------
# Backtest helpers
# --------------------------------------------------------------------------
def backtest(f: pd.DataFrame, w: pd.Series, perp_funding: bool = False, cost_bps: float = COST_BPS) -> pd.DataFrame:
    bt = pnl_from_target(f, w, cost_bps=cost_bps)
    if perp_funding and "funding_daily" in f:
        bt["ret"] = bt["ret"] - bt["pos"] * f["funding_daily"].fillna(0.0)
    return bt


def run_model(f, side, exposure, neutral_flips, schedule_fn=None, K=3, perp_funding=False, cost_bps=COST_BPS):
    m = run_machine(side, K, schedule_fn=schedule_fn, exposure=exposure, neutral_flips=neutral_flips)
    return m, backtest(f, m["w"], perp_funding, cost_bps)


def strategy_metrics(bt: pd.DataFrame, btc_ret: pd.Series, start=None, end=None) -> dict:
    x = bt.loc[start:end]
    r = x["ret"].dropna()
    if len(r) < 30:
        return {}
    # Long and short legs are separate round trips (a long/short system flips -1 <-> +1 without passing 0)
    ep_long = episodes(x.assign(pos=x["pos"].clip(lower=0)))
    ep_short = episodes(x.assign(pos=(-x["pos"]).clip(lower=0)))   # strategy returns are already signed
    ep = pd.concat([ep_long, ep_short], ignore_index=True)
    whips = int(((ep["days"] <= 30) & (ep["ret"] <= 0)).sum()) if len(ep) else 0
    yrs = len(r) / 365
    out = stats.perf_summary(r)
    b = btc_ret.loc[r.index]
    mr, mb = (1 + r).resample("ME").prod() - 1, (1 + b).resample("ME").prod() - 1
    out.update({"Trades": len(ep), "Whipsaws": whips, "WhipsawsPerYr": whips / yrs,
                "TurnoverPerYr": x["turnover"].sum() / yrs, "Exposure": x["pos"].abs().mean(),
                "UpCapture": mr[mb > 0].mean() / mb[mb > 0].mean(), "DownCapture": mr[mb < 0].mean() / mb[mb < 0].mean(),
                "WorstMonth": float(mr.min())})
    return out


def breakeven_probability(ev: pd.DataFrame) -> float:
    """p* = L / (G + L): G = mean gain from entering early on signals that confirm,
    L = mean loss from entering early on signals that fail. Uses 'incr_k1' when present
    (event-window P&L of 1-close minus 3-close); falls back to fwd_3d."""
    col = "incr_k1" if "incr_k1" in ev else "fwd_3d"
    g = ev.loc[ev["confirmed"] == 1, col].mean()
    l = -ev.loc[ev["confirmed"] == 0, col].mean()
    if not (np.isfinite(g) and np.isfinite(l)) or g + l <= 0:
        return 0.5
    return float(np.clip(l / (g + l), 0.05, 0.95))


# --------------------------------------------------------------------------
# Walk-forward
# --------------------------------------------------------------------------
def walk_forward(f, side, obs, exposure, neutral_flips, test_years, sf, btc_ret, perp_funding=False):
    """Fits M4/M5/M6 on data before each test year; M0-M3 need no fitting. Returns stitched daily returns and picks."""
    fixed = {"M0_3close": dict(K=3), "M1_1close": dict(K=1), "M2_2close": dict(K=2),
             "M3_linear": dict(K=3, schedule_fn=lambda t, R, s: (1 / 3, 2 / 3))}
    fixed_bt = {k: run_model(f, side, exposure, neutral_flips, perp_funding=perp_funding, **v)[1] for k, v in fixed.items()}
    stitched = {k: [] for k in list(fixed) + ["M4_strength", "M5_override", "M6_probability"]}
    picks = []
    for Y in test_years:
        lo, hi = pd.Timestamp(f"{Y}-01-01"), pd.Timestamp(f"{Y}-12-31")
        tr_ev = obs[obs["ts"] < lo - EMBARGO]
        for k, bt in fixed_bt.items():
            stitched[k].append(bt["ret"].loc[lo:hi])

        # M5: choose (feature, threshold) by training Sharpe; restrict to a coarse grid
        best, best_s = None, -np.inf
        for feat in OVERRIDE_FEATURES:
            for thr in THRESH_GRID:
                _, bt = run_model(f, side, exposure, neutral_flips, sched_override(sf, feat, thr), perp_funding=perp_funding)
                s = stats.sharpe(bt["ret"].loc[:lo - pd.Timedelta(days=1)])
                if np.isfinite(s) and s > best_s:
                    best, best_s = (feat, thr), s
        _, bt5 = run_model(f, side, exposure, neutral_flips, sched_override(sf, *best), perp_funding=perp_funding)
        stitched["M5_override"].append(bt5["ret"].loc[lo:hi])

        # M4: strength staging on the feature with the best training AUC among brk_vol / ret_z20
        m4_feat, m4 = None, None
        for feat in ("brk_vol", "ret_z20"):
            mod = fit_strength_staging(tr_ev, feat)
            if mod is not None:
                _, bt = run_model(f, side, exposure, neutral_flips, sched_strength(sf, feat, mod), perp_funding=perp_funding)
                s = stats.sharpe(bt["ret"].loc[:lo - pd.Timedelta(days=1)])
                if m4 is None or s > m4[2]:
                    m4 = (feat, mod, s)
        if m4:
            _, bt4 = run_model(f, side, exposure, neutral_flips, sched_strength(sf, m4[0], m4[1]), perp_funding=perp_funding)
            stitched["M4_strength"].append(bt4["ret"].loc[lo:hi])
        else:
            stitched["M4_strength"].append(fixed_bt["M0_3close"]["ret"].loc[lo:hi])

        # M6: logistic P(confirm) -> exposure; cutoff chosen on training returns
        mod6 = fit_probability(tr_ev)
        m6_cut = None
        if mod6 is not None:
            best6 = -np.inf
            for cut in (0.4, 0.5, 0.6, 0.7, 0.8):
                _, bt = run_model(f, side, exposure, neutral_flips, sched_probability(sf, mod6, cut), perp_funding=perp_funding)
                s = stats.sharpe(bt["ret"].loc[:lo - pd.Timedelta(days=1)])
                if s > best6:
                    best6, m6_cut = s, cut
            _, bt6 = run_model(f, side, exposure, neutral_flips, sched_probability(sf, mod6, m6_cut), perp_funding=perp_funding)
            stitched["M6_probability"].append(bt6["ret"].loc[lo:hi])
        else:
            stitched["M6_probability"].append(fixed_bt["M0_3close"]["ret"].loc[lo:hi])

        picks.append({"test_year": Y, "M5_feature": best[0], "M5_threshold": best[1], "M5_train_sharpe": best_s,
                      "M4_feature": m4[0] if m4 else None, "M4_p_star": m4[1][2] if m4 else None,
                      "M4_p2": m4[1][1] if m4 else None, "M6_cutoff": m6_cut,
                      "M6_features": ",".join(mod6[1]) if mod6 else None, "n_train_events": len(tr_ev)})
    oos = pd.DataFrame({k: pd.concat(v) for k, v in stitched.items() if v})
    return oos, pd.DataFrame(picks), fixed_bt


def sensitivity(f, side, exposure, neutral_flips, sf, btc_ret, windows: dict, perp_funding=False) -> pd.DataFrame:
    """Override threshold grids for each strength feature, by validation window."""
    rows = []
    for feat in OVERRIDE_FEATURES:
        for thr in THRESH_GRID:
            m, bt = run_model(f, side, exposure, neutral_flips, sched_override(sf, feat, thr), perp_funding=perp_funding)
            for wname, (a, b) in windows.items():
                met = strategy_metrics(bt, btc_ret, a, b)
                if met:
                    rows.append({"feature": feat, "threshold": thr, "window": wname, **met})
    return pd.DataFrame(rows)
