"""Unit tests for the shared research library (run: python -m pytest research/quant_program/tests -q)."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qlib import checks, costs, events, scoring, stats, walkforward  # noqa: E402


def _bars(n=200, asset="X", drift=0.0, seed=1):
    rng = np.random.default_rng(seed)
    c = 100 * np.exp(np.cumsum(drift + rng.normal(0, 0.01, n)))
    o = np.r_[c[0], c[:-1]]
    return pd.DataFrame({"ts": pd.date_range("2021-01-01", periods=n, freq="D"), "asset": asset,
                         "open": o, "high": np.maximum(o, c) * 1.002, "low": np.minimum(o, c) * 0.998, "close": c})


# ---------------------------------------------------------------- events
def test_forward_return_next_open_exact():
    b = _bars()
    ev = pd.DataFrame({"ts": [b.ts[50]], "asset": ["X"], "direction": [1]})
    cfg = events.EventStudyConfig(horizons={"3d": 3}, baseline="none", reps=50)
    obs = events.compute_event_returns(b, ev, cfg)
    expected = b.close[53] / b.open[51] - 1
    assert obs["fwd_3d"].iloc[0] == pytest.approx(expected)
    assert obs["mae_3d"].iloc[0] <= obs["fwd_3d"].iloc[0] <= obs["mfe_3d"].iloc[0]


def test_short_direction_sign_flips():
    b = _bars()
    ev = pd.DataFrame({"ts": [b.ts[50]] * 2, "asset": ["X", "X"], "direction": [1, -1]})
    obs = events.compute_event_returns(b, ev, events.EventStudyConfig(horizons={"5d": 5}, baseline="none"))
    assert obs["fwd_5d"].iloc[0] == pytest.approx(-obs["fwd_5d"].iloc[1])


def test_prior_vol_excludes_event_bar():
    b = _bars()
    b.loc[100, "close"] = b.close[99] * 1.5          # huge event-day move
    ev = pd.DataFrame({"ts": [b.ts[100]], "asset": ["X"], "direction": [1]})
    obs = events.compute_event_returns(b, ev, events.EventStudyConfig(horizons={"1d": 1}, baseline="none"))
    assert obs["prior_vol"].iloc[0] < 0.05          # would be >> if the event bar leaked in


def test_decluster_keeps_largest():
    b = _bars()
    ev = pd.DataFrame({"ts": b.ts[[10, 12, 40]], "asset": "X", "direction": 1, "score": [1.0, 3.0, 2.0]})
    cfg = events.EventStudyConfig(horizons={"1d": 1}, min_separation=5, rank_col="score", baseline="none")
    obs = events.compute_event_returns(b, ev, cfg)
    assert sorted(obs["score"]) == [2.0, 3.0]


def test_event_at_end_has_nan_forward():
    b = _bars(60)
    ev = pd.DataFrame({"ts": [b.ts[58]], "asset": ["X"], "direction": [1]})
    obs = events.compute_event_returns(b, ev, events.EventStudyConfig(horizons={"5d": 5}, baseline="none"))
    assert np.isnan(obs["fwd_5d"].iloc[0])


def test_summarize_and_curve_run():
    b = pd.concat([_bars(400, "A", seed=1), _bars(400, "B", seed=2)])
    rng = np.random.default_rng(3)
    ev = b.sample(80, random_state=4)[["ts", "asset"]].assign(direction=1, feat=rng.normal(size=80))
    cfg = events.EventStudyConfig(horizons={"1d": 1, "5d": 5}, reps=50)
    obs = events.compute_event_returns(b, ev, cfg)
    s = events.summarize(obs, cfg)
    assert set(["mean_lo", "mean_hi", "excess_mean"]).issubset(s.columns)
    cur = events.conditional_curve(obs, "feat", [-9, 0, 9], ["5d"])
    assert cur["n"].sum() == len(obs)


# ---------------------------------------------------------------- stats
def test_bh_fdr_basic():
    rej, adj = stats.bh_fdr([0.001, 0.01, 0.04, 0.5], q=0.05)
    assert rej.tolist() == [True, True, False, False]
    assert np.all(np.diff(adj[np.argsort([0.001, 0.01, 0.04, 0.5])]) >= 0)


def test_cluster_bootstrap_contains_mean():
    v = np.random.default_rng(0).normal(0.01, 0.05, 300)
    cl = np.repeat(np.arange(30), 10)
    m, lo, hi = stats.cluster_bootstrap_ci(v, cl, reps=300)
    assert lo < m < hi


def test_wilson_bounds():
    lo, hi = stats.wilson(5, 10)
    assert 0 < lo < 0.5 < hi < 1


# ---------------------------------------------------------------- checks
def test_checks_detect_problems():
    df = pd.DataFrame({"ts": ["2024-10-01", "2024-9-01"], "asset": ["A", "A"], "v": [1, 1]})
    assert not checks.datetime_dtype(df, "ts").passed
    assert not checks.alphabetical_sort_trap(pd.Series(["1", "10", "2"])).passed
    assert not checks.duplicates(pd.concat([df, df]), ["ts", "asset"]).passed


def test_causal_check_catches_lookahead():
    df = _bars(400)
    good = lambda d: d["close"].rolling(10).mean()
    bad = lambda d: d["close"].rolling(10, center=True).mean()   # uses future bars
    assert checks.causal(good, df).passed
    assert not checks.causal(bad, df).passed


def test_execution_lag_check():
    s = pd.Series([0, 1, 1, 0, 1, 1, 1, 0], dtype=float)
    assert not checks.execution_lag(s, s).passed           # same-bar execution flagged
    assert checks.execution_lag(s, s.shift(1).fillna(0)).passed


def test_abnormal_turnover_sparse_and_impossible():
    sparse = pd.Series([0.0] * 300)
    sparse.iloc[::40] = 1.0                                   # occasional trend flips: fine
    assert checks.abnormal_turnover(sparse).passed
    bad = sparse.copy()
    bad.iloc[5] = 3.0                                         # impossible for a [-1, 1] strategy
    r = checks.abnormal_turnover(bad)
    assert not r.passed and r.severity == "error"


def test_fills_within_range():
    assert not checks.fills_within_range(pd.Series([11.0]), pd.Series([9.0]), pd.Series([10.0])).passed


def test_clean_klines_truncates_frozen_tail_only():
    from qlib import data as D
    b = _bars(50).assign(quote_volume=1.0)
    b.loc[20, "quote_volume"] = 0.0                      # isolated zero-volume bar inside history: kept
    b.loc[45:, ["quote_volume"]] = 0.0
    b.loc[45:, "high"] = b.loc[45:, "low"] = b.loc[45:, "close"] = b.close[44]
    clean, delist = D.clean_klines(b)
    assert len(clean) == 45 and delist == b.ts[45]
    assert not checks.frozen_tail(b).passed and checks.frozen_tail(clean.assign(asset="X")).passed


# ---------------------------------------------------------------- walkforward
def test_walk_forward_no_overlap_and_picks():
    idx = pd.date_range("2018-01-01", "2022-12-31", freq="D")
    rng = np.random.default_rng(1)
    r = pd.DataFrame({"good": rng.normal(0.001, 0.01, len(idx)), "bad": rng.normal(-0.001, 0.01, len(idx))}, index=idx)
    folds = walkforward.calendar_folds(idx, [2020, 2021, 2022])
    for f in folds:
        assert idx[f["train"]].max() < idx[f["test"]].min()
    oos, picks = walkforward.walk_forward_select(r, folds)
    assert (picks["pick"] == "good").all()
    assert oos.index.min() >= pd.Timestamp("2020-01-01")


def test_plateau_penalises_spike():
    v = pd.Series([0.1, 0.1, 1.0, 0.1, 0.1, 0.5, 0.5, 0.5, 0.5], index=range(9))
    p = walkforward.plateau_score(v, window=1)
    assert p.idxmax() in (5, 6, 7)


# ---------------------------------------------------------------- scoring / costs
def test_scoring_gates():
    c = scoring.Scorecard("x", "f", classification="FORCED FLOW",
                          component_scores={k: 5 for k in scoring.WEIGHTS})
    assert c.aqs() == 100 and c.recommendation() == "PRODUCTION CANDIDATE"
    c.component_scores["mechanism"] = 1
    assert c.aqs() <= 40
    c.component_scores.update(mechanism=5, oos_significance=1)
    assert c.recommendation() == "WATCH"
    c.component_scores.update(oos_significance=5, cost_resilience=0)
    assert c.recommendation() == "REJECT"
    c.component_scores.update(cost_resilience=5)
    c.gates = {"fails BH": "PAPER TRADE"}
    assert c.aqs() == 100 and c.recommendation() == "PAPER TRADE"
    c.gates["matches momentum control"] = "WATCH"
    assert c.recommendation() == "WATCH"


def test_cost_sensitivity_breakeven():
    t = costs.cost_sensitivity([0.01] * 20, grid_bps=(0, 100, 200))
    assert t.attrs["breakeven_bps"] == pytest.approx(100)
    assert t.loc[t.round_trip_bps == 200, "net_mean"].iloc[0] < 0


# --- review fixes 2026-09-15 -------------------------------------------------------------
def test_cluster_p_small_clusters_more_conservative_than_normal():
    from scipy.stats import norm
    rng = np.random.default_rng(1)
    v = rng.normal(0.3, 1, 60)
    c = np.repeat(np.arange(6), 10)            # only 6 clusters
    t = stats.cluster_t(v, c)
    assert stats.cluster_p(v, c) > 2 * (1 - norm.cdf(abs(t)))


def test_wild_cluster_p_null_and_alternative():
    rng = np.random.default_rng(2)
    c = np.repeat(np.arange(40), 5)
    assert stats.wild_cluster_p(rng.normal(0, 1, 200), c) > 0.05
    assert stats.wild_cluster_p(rng.normal(1.0, 1, 200), c) < 0.01


def test_controls_short_signal_uses_gross_not_net():
    """A short signal carrying no information beyond the price move must show ~0 excess over the control,
    whatever the transaction costs are (v1 flipped the control's NET return and turned costs into profit)."""
    from track_b import controls as CT
    months = [f"2024-{m:02d}" for m in range(1, 13)]
    rows = []
    for i in range(240):
        base = dict(ts=pd.Timestamp("2024-01-01") + pd.Timedelta(days=i), asset="X", cluster=months[i % 12],
                    ret_z=1.2, adv30=5e7, beta60=1.0, btc_regime="bull", vol_regime="low", funding_5d=0.0)
        g = 0.01 + 0.001 * np.sin(i)
        rows.append({**base, "signal": CT.CONTROLS[0], "direction": 1, "hedged_5d": g, "net_5d": g - 0.02})
        rows.append({**base, "signal": "S_short", "direction": -1, "hedged_5d": -g, "net_5d": -g - 0.02})
    r = CT.matched_excess(pd.DataFrame(rows), "S_short", reps=200)
    assert abs(r["excess_mean"]) < 1e-9
    assert r["control_coverage"] == 1.0


def test_event_portfolio_missing_price_truncates_not_zero():
    from track_b import studies as S
    idx = pd.date_range("2024-01-01", periods=10, freq="D")
    px = pd.DataFrame({"A": np.linspace(100, 109, 10), "BTCUSDT": np.full(10, 50000.0)}, index=idx)
    ret = px.pct_change()
    ret.loc[idx[4], "A"] = np.nan                   # data gap while held
    wide = {"open": px, "close": px * 1.0, "ret": ret, "funding_day": px * 0.0, "sig20": px * 0 + 0.01}
    obs = pd.DataFrame({"ts": [idx[0]], "asset": ["A"], "direction": [1], "beta60": [0.0], "round_trip_cost": [0.0]})
    out = S.event_portfolio(obs, wide, hold=6, min_slots=1)
    assert out.attrs["positions_truncated_missing_price"] == 1
    assert out.loc[idx[4]:].eq(0).all()             # nothing booked on/after the gap
    assert out.loc[idx[2]] > 0                      # held days before the gap are booked
    px2 = px.copy()
    px2.loc[idx[4], "A"] = np.nan                   # entry bar itself missing (no open/close)
    wide2 = dict(wide, open=px2, close=px2, ret=px2.pct_change(fill_method=None))
    obs2 = obs.assign(ts=[idx[3]])                  # entry at idx[4] -> skipped, not booked as 0%
    out2 = S.event_portfolio(obs2, wide2, hold=3, min_slots=1)
    assert out2.attrs["events_skipped_missing_entry"] == 1 and out2.eq(0).all()


def test_verdict_requires_comparable_control():
    """A failed control test only means 'no incremental alpha' when the control population is like-for-like."""
    from track_b import scorecards as SC
    g = {"OOS_n": 100, "OOS_net_mean": 0.05, "OOS_net_lo": 0.01}
    port = pd.Series({"S": 1.0})
    balanced = pd.DataFrame({"control_coverage": [0.8], "excess_lo": [-0.01], "smd_log_adv": [0.1],
                             "smd_beta": [0.0], "smd_year": [0.2]}, index=["S"])
    assert SC.verdict("x", g, balanced, "S", port, port) == "NO INCREMENTAL STRUCTURAL ALPHA"
    imbalanced = balanced.assign(smd_log_adv=[-1.4])         # e.g. C1: signal fires on far less liquid assets
    assert SC.verdict("x", g, imbalanced, "S", port, port) == "FORWARD TEST ONLY"
    assert SC.verdict("x", dict(g, OOS_n=20), balanced, "S", port, port) == "INSUFFICIENT EVIDENCE"
    assert SC.verdict("x", dict(g, OOS_net_mean=-0.01), balanced, "S", port, port) == "REJECT AS TRADE"
    assert SC.verdict("control", g, balanced, "S", port, port) == "CONTROL"
