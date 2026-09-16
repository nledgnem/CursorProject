"""Robustness audit of the Gerhard SMA120 fast-track (external review, 2026-09-15).

    python -m track_a.robustness            (from research/quant_program)

NO parameter search. Only rules that already existed are evaluated:
  M0      3-close (current)
  M1      1-close (K=1: the regime flips on the first close, no unwind)
  FAST    act on close 1, unwind if the count resets before close 3 (= override with threshold 0)
  M5      act on close 1 only if break/sigma (brk_vol) >= 0.5 (the frozen recommended rule), else 3 closes

Questions answered
  1. Direction attribution: FAST and M5 applied to up-transitions only (earlier ENTRY in long/flat),
     down-transitions only (earlier EXIT), or both.
  2. Window dependence: 2019-2021, 2022+, 2024+, excluding all of 2026, excluding the Aug-2026 episode.
  3. Inference with the economically independent unit: paired bootstrap over transition segments
     (a segment starts at each first qualifying close of the 3-close machine), leave-one-year-out and
     leave-one-cycle-out, next to the original 60-day daily block bootstrap.

Cycle boundaries are fixed calendar blocks chosen before looking at results:
  2019-01..2020-12 pre-bull / COVID, 2021 bull, 2022 bear, 2023-01..2024-03 recovery, 2024-04.. post-ETF/halving.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE.parents[2] / "btc_confirmation_lag"))

from qlib import stats  # noqa: E402
from qlib.paths import TABLES  # noqa: E402
from track_a import events_build as EB  # noqa: E402
from track_a import models as M  # noqa: E402
from track_a.features import signed_frame  # noqa: E402

warnings.filterwarnings("ignore")
OUT = TABLES / "track_a"
START = "2019-01-01"                       # first walk-forward test year
FROZEN_FEATURE, FROZEN_THR = "brk_vol", 0.5
WINDOWS = {"2019+ (walk-forward span)": (START, None), "2019-2021": (START, "2021-12-31"),
           "2022+": ("2022-01-01", None), "2024+": ("2024-01-01", None),
           "2019-2025 (no 2026)": (START, "2025-12-31"),
           "2019-2026-07 (no Aug-2026 episode)": (START, "2026-07-31")}
CYCLES = {"2019-2020": ("2019-01-01", "2020-12-31"), "2021 bull": ("2021-01-01", "2021-12-31"),
          "2022 bear": ("2022-01-01", "2022-12-31"), "2023-2024Q1": ("2023-01-01", "2024-03-31"),
          "2024Q2+": ("2024-04-01", "2100-01-01")}
REPS, SEED = 2000, 20260915


def directional(base_fn, which: str):
    """Restrict a schedule to up-transitions (s > R), down-transitions, or both."""
    def fn(t, R, s):
        up = s > R
        if (which == "up" and not up) or (which == "down" and up):
            return (0.0, 0.0)
        return base_fn(t, R, s)
    return fn


def _sh(x: np.ndarray) -> float:
    sd = x.std(ddof=1)
    return float(x.mean() / sd * np.sqrt(365)) if len(x) > 10 and sd > 0 else np.nan


def _cagr(x: np.ndarray) -> float:
    return float(np.prod(1 + x) ** (365 / len(x)) - 1) if len(x) else np.nan


def variants(sf):
    always = lambda t, R, s: (1.0, 1.0)  # noqa: E731
    m5 = M.sched_override(sf, FROZEN_FEATURE, FROZEN_THR)
    v = {"M0_3close": dict(K=3), "M1_1close": dict(K=1)}
    for which in ("both", "up", "down"):
        v[f"FAST_{which}"] = dict(K=3, schedule_fn=directional(always, which))
        v[f"M5_brk0.5_{which}"] = dict(K=3, schedule_fn=directional(m5, which))
    return v


def segment_ids(m0_n: pd.Series) -> np.ndarray:
    """Transition segments: a new segment starts at each first qualifying close of the 3-close machine."""
    return np.cumsum((m0_n.to_numpy() == 1).astype(int))


def inference(a: pd.Series, b: pd.Series, seg: np.ndarray, rng) -> dict:
    x, y = a.to_numpy(), b.to_numpy()
    d_obs = _sh(x) - _sh(y)
    n = len(x)
    # (i) original: 60-day circular block bootstrap of days
    dd = np.array([(lambda i: _sh(x[i]) - _sh(y[i]))(stats.block_indices(n, 60, rng)) for _ in range(REPS)])
    # (ii) transition-segment bootstrap (paired, whole segments)
    groups = [np.where(seg == k)[0] for k in np.unique(seg)]
    G = len(groups)
    ds = np.empty(REPS)
    for r in range(REPS):
        idx = np.concatenate([groups[j] for j in rng.integers(0, G, G)])
        ds[r] = _sh(x[idx]) - _sh(y[idx])
    inc = np.array([x[g].sum() - y[g].sum() for g in groups])
    nz = inc[np.abs(inc) > 1e-12]
    inc_boot = np.array([inc[rng.integers(0, G, G)].sum() for _ in range(REPS)])
    return {"dSharpe": d_obs,
            "block60_lo": np.nanpercentile(dd, 2.5), "block60_hi": np.nanpercentile(dd, 97.5),
            "segment_lo": np.nanpercentile(ds, 2.5), "segment_hi": np.nanpercentile(ds, 97.5),
            "segment_p_le0": float(np.mean(ds <= 0)),
            "n_segments": G, "n_segments_differing": int(len(nz)),
            "segments_rule_better_share": float((nz > 0).mean()) if len(nz) else np.nan,
            "incremental_return_sum": float(inc.sum()),
            "incremental_sum_lo": np.nanpercentile(inc_boot, 2.5), "incremental_sum_hi": np.nanpercentile(inc_boot, 97.5),
            "largest_segment_share_of_gain": float(nz.max() / inc.sum()) if len(nz) and inc.sum() > 0 else np.nan}


def leave_out(a: pd.Series, b: pd.Series) -> list[dict]:
    rows = []
    for y in sorted(set(a.index.year)):
        keep = a.index.year != y
        rows.append({"left_out": str(y), "kind": "year", "dSharpe": _sh(a[keep].to_numpy()) - _sh(b[keep].to_numpy())})
    for name, (lo, hi) in CYCLES.items():
        keep = ~((a.index >= lo) & (a.index <= hi))
        rows.append({"left_out": name, "kind": "cycle", "dSharpe": _sh(a[keep].to_numpy()) - _sh(b[keep].to_numpy())})
    return rows


def main(assets=("BTC", "ETH", "SOL"), system="GERHARD_SMA120"):
    win_rows, inf_rows, lo_rows = [], [], []
    for asset in assets:
        f, base, obs = EB.build(asset, system, "long_flat")
        side, nf = f["side"], False
        sf = {1: signed_frame(f, 1), -1: signed_frame(f, -1)}
        bts, m0_mach = {}, None
        for name, kw in variants(sf).items():
            mach, bt = M.run_model(f, side, "long_flat", nf, **kw)
            bts[name] = bt["ret"]
            if name == "M0_3close":
                m0_mach = mach
        m0 = bts["M0_3close"]
        for wname, (lo, hi) in WINDOWS.items():
            base_r = m0.loc[lo:hi].dropna()
            b = base_r.to_numpy()
            for name, r in bts.items():
                x = r.loc[base_r.index].to_numpy()
                win_rows.append({"asset": asset, "window": wname, "model": name, "Sharpe": _sh(x), "CAGR": _cagr(x),
                                 "MaxDD": stats.max_drawdown(pd.Series(x)), "dSharpe_vs_M0": _sh(x) - _sh(b),
                                 "dCAGR_vs_M0": _cagr(x) - _cagr(b)})
        a0 = m0.loc[START:].dropna()
        seg = segment_ids(m0_mach["n"].loc[a0.index])
        rng = np.random.default_rng(SEED)
        for name, r in bts.items():
            if name == "M0_3close":
                continue
            a = r.loc[a0.index]
            inf_rows.append({"asset": asset, "model": name, "window": "2019+", **inference(a, a0, seg, rng)})
            for row in leave_out(a, a0):
                lo_rows.append({"asset": asset, "model": name, **row})
        print(f"done {asset}")

    W, I, L = pd.DataFrame(win_rows), pd.DataFrame(inf_rows), pd.DataFrame(lo_rows)
    # direction attribution: interaction = both - up - down (additivity check)
    att = W[W.model.str.contains("_both|_up|_down")].copy()
    att["rule"] = att.model.str.rsplit("_", n=1).str[0]
    att["dir"] = att.model.str.rsplit("_", n=1).str[1]
    piv = att.pivot_table(index=["asset", "window", "rule"], columns="dir", values=["dSharpe_vs_M0", "dCAGR_vs_M0"])
    piv.columns = [f"{m}_{d}" for m, d in piv.columns]
    piv["dCAGR_interaction"] = piv["dCAGR_vs_M0_both"] - piv["dCAGR_vs_M0_up"] - piv["dCAGR_vs_M0_down"]
    piv = piv.reset_index()
    Lsum = L.groupby(["asset", "model", "kind"])["dSharpe"].agg(["min", "max", lambda s: (s > 0).mean()]) \
        .rename(columns={"<lambda_0>": "share_positive"}).reset_index()

    W.to_csv(OUT / "robustness_windows.csv", index=False)
    piv.to_csv(OUT / "robustness_direction_attribution.csv", index=False)
    I.to_csv(OUT / "robustness_inference.csv", index=False)
    L.to_csv(OUT / "robustness_leave_out.csv", index=False)
    Lsum.to_csv(OUT / "robustness_leave_out_summary.csv", index=False)
    pd.set_option("display.width", 260)
    pd.set_option("display.max_columns", 40)
    print(piv.round(3).to_string())
    print(I.round(3).to_string())
    print(Lsum.round(3).to_string())


if __name__ == "__main__":
    main()
