"""
Autocorrelation-aware inference for overlapping forward returns.

Overlapping h-day forward returns built from a daily series are mechanically
an MA(h-1) process even under the null of zero predictability. Naive t-stats
on ~3,900 daily observations of a 20-day forward return overstate precision by
roughly sqrt(20) ~ 4.5x. Everything here exists to avoid that mistake.

Three independent routes, deliberately kept separate:
  1. HAC / Newey-West OLS on the full overlapping sample (uses all data,
     corrects the standard errors).
  2. Strict non-overlapping subsamples (throws away 95% of the data but the
     observations really are close to independent). We evaluate all h phase
     offsets rather than arbitrarily picking one.
  3. Circular block bootstrap (makes no parametric assumption about the
     dependence structure).

If the three disagree, that disagreement IS the result and gets reported.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats as sps

from config import BOOTSTRAP_BLOCK_DAYS, BOOTSTRAP_REPS, BOOTSTRAP_SEED, hac_lags


# --------------------------------------------------------------------------
# 1. HAC / Newey-West
# --------------------------------------------------------------------------
def hac_mean(y: pd.Series, lags: int) -> dict:
    """Newey-West mean and t-stat for a single overlapping return series."""
    y = pd.Series(y).dropna()
    if len(y) < max(30, lags + 5):
        return {"n": int(len(y)), "mean": np.nan, "se": np.nan, "t": np.nan, "p": np.nan}
    X = np.ones((len(y), 1))
    res = sm.OLS(y.values, X).fit(cov_type="HAC", cov_kwds={"maxlags": lags})
    return {
        "n": int(len(y)),
        "mean": float(res.params[0]),
        "se": float(res.bse[0]),
        "t": float(res.tvalues[0]),
        "p": float(res.pvalues[0]),
    }


def hac_group_regression(score: pd.Series, fwd: pd.Series, horizon: int) -> dict:
    """Dummy regression fwd ~ 1 + 1[score=1] + 1[score=2] + 1[score=3].

    The intercept is the Score-0 mean; each coefficient is that score's mean
    EXCESS over Score 0. Also returns the joint Wald test that all three
    dummies are zero (i.e. "means are equal across scores"), and the linear
    slope from regressing forward return on the score treated as a numeric
    0-3 variable (the simplest monotonicity test).
    """
    df = pd.concat([score.rename("score"), fwd.rename("fwd")], axis=1).dropna()
    if df.empty:
        return {}
    lags = hac_lags(horizon)

    dummies = pd.get_dummies(df["score"].astype(int), prefix="s")
    for k in range(4):
        col = f"s_{k}"
        if col not in dummies:
            dummies[col] = 0
    dummies = dummies[[f"s_{k}" for k in range(4)]].astype(float)

    X = sm.add_constant(dummies[["s_1", "s_2", "s_3"]].values)
    res = sm.OLS(df["fwd"].values, X).fit(cov_type="HAC", cov_kwds={"maxlags": lags})

    # Joint test: all score dummies zero.
    R = np.zeros((3, 4))
    R[0, 1] = R[1, 2] = R[2, 3] = 1.0
    wald = res.f_test(R)

    # Linear-in-score slope.
    Xl = sm.add_constant(df["score"].astype(float).values)
    lin = sm.OLS(df["fwd"].values, Xl).fit(cov_type="HAC", cov_kwds={"maxlags": lags})

    out = {
        "n": int(len(df)),
        "hac_lags": lags,
        "score0_mean": float(res.params[0]),
        "wald_F": float(np.ravel(wald.fvalue)[0]),
        "wald_p": float(wald.pvalue),
        "slope_per_score": float(lin.params[1]),
        "slope_t": float(lin.tvalues[1]),
        "slope_p": float(lin.pvalues[1]),
    }
    for i, k in enumerate((1, 2, 3), start=1):
        out[f"score{k}_minus_score0"] = float(res.params[i])
        out[f"score{k}_minus_score0_t"] = float(res.tvalues[i])
        out[f"score{k}_minus_score0_p"] = float(res.pvalues[i])
    return out


# --------------------------------------------------------------------------
# 2. Non-overlapping subsamples
# --------------------------------------------------------------------------
def non_overlapping_test(score: pd.Series, fwd: pd.Series, horizon: int) -> pd.DataFrame:
    """Score-3 vs Score-0 Welch t-test on every phase of a strict h-day grid.

    Taking every h-th observation gives genuinely (near-)independent forward
    returns. Which of the h possible starting offsets you pick is arbitrary,
    so we run all of them and report the distribution of results rather than
    cherry-picking one.
    """
    df = pd.concat([score.rename("score"), fwd.rename("fwd")], axis=1).dropna()
    rows = []
    for offset in range(horizon):
        sub = df.iloc[offset::horizon]
        a = sub.loc[sub["score"] == 3, "fwd"]
        b = sub.loc[sub["score"] == 0, "fwd"]
        if len(a) < 5 or len(b) < 5:
            rows.append({"offset": offset, "n": len(sub), "n3": len(a), "n0": len(b),
                         "diff": np.nan, "t": np.nan, "p": np.nan, "spearman_rho": np.nan,
                         "spearman_p": np.nan})
            continue
        t, p = sps.ttest_ind(a, b, equal_var=False)
        rho, rp = sps.spearmanr(sub["score"], sub["fwd"])
        rows.append({
            "offset": offset, "n": len(sub), "n3": len(a), "n0": len(b),
            "diff": float(a.mean() - b.mean()), "t": float(t), "p": float(p),
            "spearman_rho": float(rho), "spearman_p": float(rp),
        })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# 3. Circular block bootstrap
# --------------------------------------------------------------------------
def _block_indices(n: int, block: int, rng: np.random.Generator) -> np.ndarray:
    n_blocks = int(np.ceil(n / block))
    starts = rng.integers(0, n, size=n_blocks)
    idx = (starts[:, None] + np.arange(block)[None, :]).ravel() % n
    return idx[:n]


def block_bootstrap_stats(
    score: pd.Series,
    fwd: pd.Series,
    reps: int = BOOTSTRAP_REPS,
    block: int = BOOTSTRAP_BLOCK_DAYS,
    seed: int = BOOTSTRAP_SEED,
) -> dict:
    """Block-bootstrap CIs for per-score means, the 3-0 spread and Spearman rho.

    Blocks of consecutive days are resampled jointly, so both the persistence
    of TrendScore and the overlap of forward returns are preserved inside each
    block. This is the least assumption-laden of the three routes.
    """
    df = pd.concat([score.rename("score"), fwd.rename("fwd")], axis=1).dropna()
    n = len(df)
    if n < 200:
        return {}
    sc = df["score"].to_numpy()
    fw = df["fwd"].to_numpy()
    rng = np.random.default_rng(seed)

    means = np.full((reps, 4), np.nan)
    spread = np.full(reps, np.nan)
    rho = np.full(reps, np.nan)
    mono = np.zeros(reps, dtype=bool)

    for r in range(reps):
        idx = _block_indices(n, block, rng)
        s_b, f_b = sc[idx], fw[idx]
        m = np.full(4, np.nan)
        for k in range(4):
            sel = s_b == k
            if sel.sum() >= 5:
                m[k] = f_b[sel].mean()
        means[r] = m
        if np.isfinite(m[0]) and np.isfinite(m[3]):
            spread[r] = m[3] - m[0]
        if len(np.unique(s_b)) > 1:
            rho[r] = sps.spearmanr(s_b, f_b).statistic
        if np.all(np.isfinite(m)):
            mono[r] = bool(np.all(np.diff(m) > 0))

    def ci(a):
        a = a[np.isfinite(a)]
        if a.size == 0:
            return (np.nan, np.nan, np.nan)
        return (float(np.nanpercentile(a, 2.5)), float(np.nanmean(a)),
                float(np.nanpercentile(a, 97.5)))

    out = {"reps": reps, "block_days": block, "n": n}
    for k in range(4):
        lo, mu, hi = ci(means[:, k])
        out[f"score{k}_mean_lo"] = lo
        out[f"score{k}_mean_boot"] = mu
        out[f"score{k}_mean_hi"] = hi
    lo, mu, hi = ci(spread)
    out.update({"spread_3_0_lo": lo, "spread_3_0_boot": mu, "spread_3_0_hi": hi,
                "p_spread_le_0": float(np.mean(spread[np.isfinite(spread)] <= 0))})
    lo, mu, hi = ci(rho)
    out.update({"spearman_lo": lo, "spearman_boot": mu, "spearman_hi": hi,
                "p_rho_le_0": float(np.mean(rho[np.isfinite(rho)] <= 0))})
    out["p_strictly_monotonic"] = float(mono.mean())
    return out


# --------------------------------------------------------------------------
# Descriptive helpers
# --------------------------------------------------------------------------
def spearman_score_vs_fwd(score: pd.Series, fwd: pd.Series) -> tuple[float, float]:
    df = pd.concat([score, fwd], axis=1).dropna()
    if len(df) < 30 or df.iloc[:, 0].nunique() < 2:
        return (np.nan, np.nan)
    r = sps.spearmanr(df.iloc[:, 0], df.iloc[:, 1])
    return float(r.statistic), float(r.pvalue)
