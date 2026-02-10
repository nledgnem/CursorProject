"""
15+15 long/short baskets: 5 equal-weight + 5 optimized.
Selection strategies: correlation-pair, min-var-rank, greedy-seq, factor-neutral, diversification.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import date

from .utils import (
    compute_returns,
    ledoit_wolf_cov,
    pca_first_component,
    get_rebalance_dates,
)


N_LONG = 15
N_SHORT = 15


def _select_correlation_pairs(
    returns: pd.DataFrame,
    assets: List[str],
    rd: date,
    lookback: int,
) -> Tuple[List[str], List[str]]:
    """Strategy 1: Cluster by correlation, pick 15 pairs (1 long + 1 short per cluster by 30d return)."""
    from sklearn.cluster import AgglomerativeClustering

    R = returns.loc[:rd].tail(lookback)[assets].dropna(axis=1, how="all").dropna(axis=0, how="any")
    if R.shape[0] < 30 or R.shape[1] < 2 * N_LONG:
        return [], []

    cols = list(R.columns)
    corr = R.corr().fillna(0)
    dist = 1 - np.clip(corr.values, -1, 1)
    n_clust = min(N_LONG, len(cols) // 2)
    clust = AgglomerativeClustering(n_clusters=n_clust, metric="precomputed", linkage="average")
    labels = clust.fit_predict(dist)

    mu = R.tail(30).mean()
    longs, shorts = [], []
    for k in range(n_clust):
        mask = labels == k
        cluster = [cols[i] for i in range(len(cols)) if mask[i]]
        if len(cluster) < 2:
            continue
        mu_k = mu[cluster].sort_values(ascending=False)
        l, s = mu_k.index[0], mu_k.index[-1]
        if l != s and l not in longs and s not in shorts:
            longs.append(l)
            shorts.append(s)
    if len(longs) < N_LONG or len(shorts) < N_SHORT:
        return _select_minvar_rank(returns, assets, rd, lookback)
    return longs[:N_LONG], shorts[:N_SHORT]


def _select_minvar_rank(
    returns: pd.DataFrame,
    assets: List[str],
    rd: date,
    lookback: int,
) -> Tuple[List[str], List[str]]:
    """Strategy 2: Rank by vol, top 15 long (low vol), bottom 15 short (high vol)."""
    R = returns.loc[:rd].tail(lookback)[assets].dropna(axis=1, how="all")
    vol = R.std()
    vol = vol.dropna()
    vol = vol[vol > 0]
    if len(vol) < 2 * N_LONG:
        return [], []
    s = vol.sort_values()
    longs = list(s.head(N_LONG).index)
    shorts = list(s.tail(N_SHORT).index)
    return longs, shorts


def _select_greedy_seq(
    returns: pd.DataFrame,
    assets: List[str],
    rd: date,
    lookback: int,
) -> Tuple[List[str], List[str]]:
    """Strategy 3: Greedily add (long, short) pair that minimizes basket variance."""
    R = returns.loc[:rd].tail(lookback)[assets].dropna(axis=1, how="all").dropna(axis=0, how="any")
    if R.shape[0] < 30 or R.shape[1] < 2 * N_LONG:
        return [], []

    cov, cols = ledoit_wolf_cov(R)
    col_list = list(cols)
    remaining = col_list.copy()
    longs, shorts = [], []

    for _ in range(N_LONG):
        best_var = np.inf
        best_l, best_s = None, None
        cand = remaining[:min(60, len(remaining))]
        for a in cand:
            for b in cand:
                if a == b:
                    continue
                idx_l = longs + [a]
                idx_s = shorts + [b]
                idx = [col_list.index(x) for x in idx_l + idx_s]
                sc = cov[np.ix_(idx, idx)]
                n_l, n_s = len(idx_l), len(idx_s)
                v = (1 / n_l ** 2) * sc[:n_l, :n_l].sum() + (1 / n_s ** 2) * sc[n_l:, n_l:].sum()
                v -= 2 / (n_l * n_s) * sc[:n_l, n_l:].sum()
                if v < best_var:
                    best_var = v
                    best_l, best_s = a, b
        if best_l is None:
            break
        longs.append(best_l)
        shorts.append(best_s)
        remaining = [x for x in remaining if x not in (best_l, best_s)]
    return longs[:N_LONG], shorts[:N_SHORT]


def _select_factor_neutral(
    returns: pd.DataFrame,
    assets: List[str],
    rd: date,
    lookback: int,
) -> Tuple[List[str], List[str]]:
    """Strategy 4: Pick 15+15 that minimize |exposure to first PC| (factor neutral)."""
    R = returns.loc[:rd].tail(lookback)[assets].dropna(axis=1, how="all").dropna(axis=0, how="any")
    if R.shape[0] < 30 or R.shape[1] < 2 * N_LONG:
        return [], []

    try:
        pc1 = pca_first_component(R)
        if pc1 is None or len(pc1) != R.shape[1]:
            return _select_minvar_rank(returns, assets, rd, lookback)
    except Exception:
        return _select_minvar_rank(returns, assets, rd, lookback)

    loadings = pd.Series(pc1, index=R.columns)
    loadings = loadings.sort_values()
    longs = list(loadings.head(N_LONG).index)
    shorts = list(loadings.tail(N_SHORT).index)
    return longs, shorts


def _select_diversification(
    returns: pd.DataFrame,
    assets: List[str],
    rd: date,
    lookback: int,
) -> Tuple[List[str], List[str]]:
    """Strategy 5: Low correlation within longs, low within shorts, high cross (hedge)."""
    R = returns.loc[:rd].tail(lookback)[assets].dropna(axis=1, how="all").dropna(axis=0, how="any")
    if R.shape[0] < 30 or R.shape[1] < 2 * N_LONG:
        return [], []

    cov, cols = ledoit_wolf_cov(R)
    vol = np.sqrt(np.diag(cov))
    vol[vol < 1e-8] = 1e-8
    corr = cov / np.outer(vol, vol)
    cols = list(cols)
    np.random.seed(42)
    idx = np.random.choice(len(cols), min(80, len(cols)), replace=False)
    pool = [cols[i] for i in idx]
    if len(pool) < 2 * N_LONG:
        return _select_minvar_rank(returns, assets, rd, lookback)

    best_score = -np.inf
    best_long, best_short = [], []
    for _ in range(100):
        perm = np.random.permutation(len(pool))
        l_assets = [pool[i] for i in perm[:N_LONG]]
        s_assets = [pool[i] for i in perm[N_LONG : N_LONG + N_SHORT]]
        l_idx = [cols.index(a) for a in l_assets if a in cols]
        s_idx = [cols.index(a) for a in s_assets if a in cols]
        if len(l_idx) < N_LONG or len(s_idx) < N_SHORT:
            continue
        cross = np.mean(corr[np.ix_(l_idx, s_idx)])
        c_ll = corr[np.ix_(l_idx, l_idx)]
        c_ss = corr[np.ix_(s_idx, s_idx)]
        np.fill_diagonal(c_ll, 0)
        np.fill_diagonal(c_ss, 0)
        intra_l = np.mean(np.abs(c_ll)) if c_ll.size > 0 else 0
        intra_s = np.mean(np.abs(c_ss)) if c_ss.size > 0 else 0
        score = cross - 0.3 * (intra_l + intra_s)
        if score > best_score:
            best_score = score
            best_long, best_short = l_assets, s_assets
    return best_long, best_short


def _select_momentum_rank(
    returns: pd.DataFrame,
    assets: List[str],
    rd: date,
    lookback: int,
) -> Tuple[List[str], List[str]]:
    """Strategy: Rank by 30d return; bottom 15 long (mean reversion), top 15 short."""
    R = returns.loc[:rd].tail(lookback)[assets].dropna(axis=1, how="all")
    mu = R.tail(30).mean()
    mu = mu.dropna()
    if len(mu) < 2 * N_LONG:
        return _select_minvar_rank(returns, assets, rd, lookback)
    s = mu.sort_values()
    longs = list(s.head(N_LONG).index)
    shorts = list(s.tail(N_SHORT).index)
    return longs, shorts


def _select_random_diverse(
    returns: pd.DataFrame,
    assets: List[str],
    rd: date,
    lookback: int,
) -> Tuple[List[str], List[str]]:
    """Strategy: Random 15+15, seed by date for reproducibility."""
    R = returns.loc[:rd].tail(lookback)[assets].dropna(axis=1, how="all")
    cols = [c for c in R.columns if R[c].notna().sum() >= 20]
    if len(cols) < 2 * N_LONG:
        return _select_minvar_rank(returns, assets, rd, lookback)
    seed = hash(rd) % (2**32)
    np.random.seed(seed)
    perm = np.random.permutation(len(cols))
    longs = [cols[i] for i in perm[:N_LONG]]
    shorts = [cols[i] for i in perm[N_LONG : N_LONG + N_SHORT]]
    return longs, shorts


STRATEGIES = {
    "correlation_pairs": _select_correlation_pairs,
    "minvar_rank": _select_minvar_rank,
    "greedy_seq": _select_greedy_seq,
    "factor_neutral": _select_factor_neutral,
    "momentum_rank": _select_momentum_rank,
}


def make_equal_weight(longs: List[str], shorts: List[str]) -> Dict[str, float]:
    """Dollar-neutral equal weight: each long = 1/30, each short = -1/30."""
    w = 1.0 / 30
    out = {}
    for a in longs:
        out[a] = w
    for a in shorts:
        out[a] = -w
    return out


def make_optimized_weight(
    longs: List[str],
    shorts: List[str],
    cov: np.ndarray,
    assets: List[str],
) -> Dict[str, float]:
    """Min-variance QP over the 30 assets, dollar-neutral, sum|w|=1."""
    from scipy.optimize import minimize, LinearConstraint, Bounds

    all_a = [a for a in longs + shorts if a in assets]
    if len(all_a) < 2 or len([a for a in longs if a in assets]) < 1 or len([a for a in shorts if a in assets]) < 1:
        return make_equal_weight(longs, shorts)

    idx = [assets.index(a) for a in all_a]
    n = len(all_a)
    cov_sub = cov[np.ix_(idx, idx)] + 1e-6 * np.eye(n)
    eq_con = LinearConstraint(np.ones(n), 0, 0)

    w0 = np.zeros(n)
    w0[: len(longs)] = 0.5 / len(longs)
    w0[len(longs) :] = -0.5 / len(shorts)

    def obj(w):
        return 0.5 * w @ cov_sub @ w

    res = minimize(obj, w0, method="SLSQP", constraints=[eq_con], bounds=Bounds(-0.1, 0.1), options={"maxiter": 500})
    if not res.success:
        return make_equal_weight(longs, shorts)
    w = res.x
    if np.abs(w.sum()) > 1e-3:
        return make_equal_weight(longs, shorts)
    gross = np.abs(w).sum()
    if gross < 1e-6:
        return make_equal_weight(longs, shorts)
    w = w / gross
    return {a: float(w[i]) for i, a in enumerate(all_a)}


def run_baskets_15x15(
    prices: pd.DataFrame,
    start_date: date,
    end_date: date,
    lookback: int = 90,
) -> List[Dict]:
    """Produce 10 baskets (5 equal-weight + 5 optimized) across 5 strategies."""
    returns = compute_returns(prices)
    rebal_dates = get_rebalance_dates(start_date, end_date)
    assets = list(prices.columns)

    results = []
    for name, select_fn in STRATEGIES.items():
        snapshots_ew = []
        snapshots_opt = []
        prev_longs_opt = prev_shorts_opt = None

        for rd in rebal_dates:
            try:
                longs, shorts = select_fn(returns, assets, rd, lookback)
            except Exception:
                longs, shorts = _select_minvar_rank(returns, assets, rd, lookback)
            if len(longs) < N_LONG or len(shorts) < N_SHORT:
                longs, shorts = _select_minvar_rank(returns, assets, rd, lookback)
            if len(longs) < N_LONG or len(shorts) < N_SHORT:
                continue
            ew = make_equal_weight(longs, shorts)
            snapshots_ew.append({"rebalance_date": rd, "weights": ew, "longs": longs, "shorts": shorts})
            cov, cols = ledoit_wolf_cov(returns.loc[:rd].tail(lookback)[assets].dropna(axis=1, how="all"))
            opt = make_optimized_weight(longs, shorts, cov, list(cols))
            snapshots_opt.append({"rebalance_date": rd, "weights": opt, "longs": longs, "shorts": shorts})

        if snapshots_ew:
            results.append({
                "basket_type": "equal_weight",
                "strategy": name,
                "snapshots": snapshots_ew,
            })
        if snapshots_opt:
            results.append({
                "basket_type": "optimized",
                "strategy": name,
                "snapshots": snapshots_opt,
            })

    return results
