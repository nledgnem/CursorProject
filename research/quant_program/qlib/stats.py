"""Inference utilities that respect overlapping returns and clustered events.

Design principles are carried over from research/btc_trend_agreement/stats_tools.py
and research/btc_short_squeeze/eventstudy.py:
  * overlapping h-bar forward returns are MA(h-1) -> never use naive t-stats;
  * clustered events (same week, same crash) are pseudo-replicates -> resample
    clusters, not events;
  * nulls are resampled from the same calendar so they inherit drift and overlap;
  * every search over rules is reported with its number of trials and an FDR control.
"""

from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd

DAYS = 365


# --------------------------------------------------------------------------
# Bootstrap
# --------------------------------------------------------------------------
def block_indices(n: int, block: int, rng: np.random.Generator) -> np.ndarray:
    """Circular block bootstrap indices (block=1 -> iid)."""
    n_blocks = int(np.ceil(n / max(block, 1)))
    starts = rng.integers(0, n, size=n_blocks)
    return ((starts[:, None] + np.arange(max(block, 1))[None, :]).ravel() % n)[:n]


def block_bootstrap_ci(x, stat: Callable = np.mean, reps: int = 2000, block: int = 1,
                       seed: int = 7, alpha: float = 0.05) -> tuple[float, float, float]:
    """(point, lo, hi) for a statistic of a time-ordered series."""
    a = np.asarray(pd.Series(x).dropna(), dtype=float)
    if a.size < 5:
        return (np.nan, np.nan, np.nan)
    rng = np.random.default_rng(seed)
    draws = np.array([stat(a[block_indices(a.size, block, rng)]) for _ in range(reps)])
    return (float(stat(a)), float(np.nanpercentile(draws, 100 * alpha / 2)),
            float(np.nanpercentile(draws, 100 * (1 - alpha / 2))))


def cluster_bootstrap_ci(values, clusters, stat: Callable = np.mean, reps: int = 2000,
                         seed: int = 7, alpha: float = 0.05) -> tuple[float, float, float]:
    """Resample whole clusters (e.g. calendar months) with replacement."""
    df = pd.DataFrame({"v": np.asarray(values, dtype=float), "c": np.asarray(clusters)}).dropna()
    if len(df) < 5:
        return (np.nan, np.nan, np.nan)
    groups = [g.to_numpy() for _, g in df.groupby("c")["v"]]
    k = len(groups)
    rng = np.random.default_rng(seed)
    if k < 3:
        return block_bootstrap_ci(df["v"], stat, reps, 1, seed, alpha)
    draws = np.empty(reps)
    for r in range(reps):
        pick = rng.integers(0, k, size=k)
        draws[r] = stat(np.concatenate([groups[i] for i in pick]))
    return (float(stat(df["v"].to_numpy())), float(np.nanpercentile(draws, 100 * alpha / 2)),
            float(np.nanpercentile(draws, 100 * (1 - alpha / 2))))


def cluster_t(values, clusters) -> float:
    """t-stat of the mean using cluster-robust (CR0) standard error."""
    df = pd.DataFrame({"v": np.asarray(values, dtype=float), "c": np.asarray(clusters)}).dropna()
    n, k = len(df), df["c"].nunique()
    if n < 5 or k < 3:
        return np.nan
    mu = df["v"].mean()
    s = df.assign(e=df["v"] - mu).groupby("c")["e"].sum()
    var = (s ** 2).sum() / n ** 2 * k / (k - 1)
    return float(mu / np.sqrt(var)) if var > 0 else np.nan


def cluster_p(values, clusters) -> float:
    """Two-sided p-value of cluster_t from a t distribution with (clusters - 1) df.

    The normal approximation is anti-conservative with few clusters (Track B signals have 6-80 months)."""
    from scipy.stats import t as tdist
    df = pd.DataFrame({"v": np.asarray(values, dtype=float), "c": np.asarray(clusters)}).dropna()
    k = df["c"].nunique()
    t = cluster_t(df["v"], df["c"])
    return float(2 * tdist.sf(abs(t), k - 1)) if np.isfinite(t) and k >= 3 else np.nan


def wild_cluster_p(values, clusters, reps: int = 1999, seed: int = 7) -> float:
    """Wild-cluster bootstrap p-value for H0: mean = 0 (Rademacher weights, null imposed, CR t-stat).

    Under H0 the restricted residuals are the observations themselves, so y*_i = w_g * y_i with one
    random sign per cluster; the bootstrap t uses the same CR0 formula as cluster_t."""
    df = pd.DataFrame({"v": np.asarray(values, dtype=float), "c": np.asarray(clusters)}).dropna()
    n, k = len(df), df["c"].nunique()
    if n < 5 or k < 3:
        return np.nan
    t_obs = cluster_t(df["v"], df["c"])
    if not np.isfinite(t_obs):
        return np.nan
    g = df.groupby("c")["v"]
    S, ng = g.sum().to_numpy(), g.size().to_numpy().astype(float)
    W = np.random.default_rng(seed).choice([-1.0, 1.0], size=(reps, k))
    mu = W @ S / n
    resid = W * S[None, :] - mu[:, None] * ng[None, :]
    var = (resid ** 2).sum(axis=1) / n ** 2 * k / (k - 1)
    with np.errstate(divide="ignore", invalid="ignore"):
        t_star = mu / np.sqrt(var)
    return float(np.mean(np.abs(t_star[np.isfinite(t_star)]) >= abs(t_obs)))


def hac_t(y, lags: int) -> tuple[float, float]:
    """Newey-West mean and t-stat for an overlapping series."""
    import statsmodels.api as sm
    y = pd.Series(y).dropna()
    if len(y) < max(30, lags + 5):
        return (np.nan, np.nan)
    res = sm.OLS(y.values, np.ones((len(y), 1))).fit(cov_type="HAC", cov_kwds={"maxlags": lags})
    return (float(res.params[0]), float(res.tvalues[0]))


def calendar_null(pool, n: int, reps: int = 5000, stat: Callable = np.median,
                  seed: int = 7) -> np.ndarray:
    """Distribution of stat() for n random draws from a same-calendar pool."""
    vals = np.asarray(pd.Series(pool).dropna(), dtype=float)
    if n == 0 or vals.size < n:
        return np.array([])
    rng = np.random.default_rng(seed)
    return np.array([stat(vals[rng.integers(0, vals.size, n)]) for _ in range(reps)])


def empirical_p(observed: float, null: np.ndarray, greater: bool = True) -> float:
    if null.size == 0 or not np.isfinite(observed):
        return np.nan
    return float(((null >= observed) if greater else (null <= observed)).mean())


# --------------------------------------------------------------------------
# Proportions and multiple testing
# --------------------------------------------------------------------------
def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (np.nan, np.nan)
    ph = k / n
    den = 1 + z * z / n
    ctr = (ph + z * z / (2 * n)) / den
    half = z * np.sqrt(ph * (1 - ph) / n + z * z / (4 * n * n)) / den
    return (ctr - half, ctr + half)


def bh_fdr(pvals, q: float = 0.10) -> tuple[np.ndarray, np.ndarray]:
    """Benjamini-Hochberg: (reject mask, adjusted p-values), NaNs never rejected."""
    p = np.asarray(pvals, dtype=float)
    ok = np.isfinite(p)
    adj = np.full(p.shape, np.nan)
    rej = np.zeros(p.shape, dtype=bool)
    if ok.sum() == 0:
        return rej, adj
    pv = p[ok]
    order = np.argsort(pv)
    m = pv.size
    ranked = pv[order] * m / np.arange(1, m + 1)
    ranked = np.minimum.accumulate(ranked[::-1])[::-1].clip(max=1)
    a = np.empty(m)
    a[order] = ranked
    adj[ok] = a
    rej[ok] = a <= q
    return rej, adj


# --------------------------------------------------------------------------
# Performance
# --------------------------------------------------------------------------
def sharpe(r, periods: int = DAYS) -> float:
    r = pd.Series(r).dropna()
    return float(r.mean() / r.std() * np.sqrt(periods)) if len(r) > 10 and r.std() > 0 else np.nan


def sortino(r, periods: int = DAYS) -> float:
    r = pd.Series(r).dropna()
    d = r[r < 0].std()
    return float(r.mean() * periods / (d * np.sqrt(periods))) if len(r) > 10 and d > 0 else np.nan


def max_drawdown(r) -> float:
    eq = (1 + pd.Series(r).fillna(0)).cumprod()
    return float((eq / eq.cummax() - 1).min())


def cagr(r, periods: int = DAYS) -> float:
    r = pd.Series(r).dropna()
    if len(r) < 2:
        return np.nan
    return float((1 + r).prod() ** (periods / len(r)) - 1)


def perf_summary(r, periods: int = DAYS) -> dict:
    r = pd.Series(r).dropna()
    mdd = max_drawdown(r)
    g = cagr(r, periods)
    return {"n": len(r), "CAGR": g, "AnnVol": float(r.std() * np.sqrt(periods)), "Sharpe": sharpe(r, periods),
            "Sortino": sortino(r, periods), "MaxDD": mdd, "Calmar": g / abs(mdd) if mdd < 0 else np.nan,
            "WorstDay": float(r.min()) if len(r) else np.nan, "Skew": float(r.skew()) if len(r) > 3 else np.nan}
