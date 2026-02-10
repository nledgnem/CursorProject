"""
Method A: Global Min-Variance QP with CVaR and turnover penalties.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import date

from .utils import (
    compute_returns,
    ledoit_wolf_cov,
    pca_first_component,
    compute_usd_adv,
    get_rebalance_dates,
)


def solve_minvar_qp(
    cov: np.ndarray,
    assets: List[str],
    scenario_returns: np.ndarray,
    prev_weights: Optional[Dict[str, float]],
    G: float,
    max_w_abs: float,
    liquidity_caps: Optional[Dict[str, float]],
    pc1: Optional[np.ndarray],
    epsilon_pca: float,
    alpha_cvar: float,
    beta_turnover: float,
    fee_bps: float,
    slippage_bps: float,
) -> Tuple[Dict[str, float], bool]:
    """
    Solve QP: min 0.5 * w' Sigma w + alpha*CVaR_95(w) + beta*turnover_cost
    s.t. sum(w)=0, sum(|w|)<=G, |w_i|<=min(max_w_abs, liquidity_cap_i), |w'v1|<=eps
    CVaR via Rockafellar-Uryasev linearization.
    """
    n = len(assets)
    T = scenario_returns.shape[0] if scenario_returns.size > 0 else 0
    if n == 0:
        return {}, False

    try:
        import cvxpy as cp
    except ImportError:
        return _solve_minvar_qp_scipy(
            cov, assets, prev_weights, G, max_w_abs, liquidity_caps
        )

    w = cp.Variable(n)
    prev = np.array([prev_weights.get(a, 0.0) for a in assets]) if prev_weights else np.zeros(n)
    turnover_var = cp.sum(cp.abs(w - prev)) / 2.0
    cost_per_turnover = (fee_bps + slippage_bps) / 10000.0

    obj = 0.5 * cp.quad_form(w, cov)

    if beta_turnover > 0:
        obj += beta_turnover * cost_per_turnover * turnover_var

    if alpha_cvar > 0 and T > 0:
        t = cp.Variable()
        u = cp.Variable(T)
        R = scenario_returns
        obj += alpha_cvar * (t + cp.sum(u) / (0.05 * T))

    constraints = [
        cp.sum(w) == 0,
        cp.sum(cp.abs(w)) <= G,
    ]
    if alpha_cvar > 0 and T > 0:
        for s in range(T):
            constraints.append(u[s] >= -R[s, :] @ w - t)
            constraints.append(u[s] >= 0)

    for i, a in enumerate(assets):
        cap = max_w_abs
        if liquidity_caps and a in liquidity_caps:
            cap = min(cap, liquidity_caps[a])
        constraints.append(cp.abs(w[i]) <= cap)

    if pc1 is not None and epsilon_pca is not None and epsilon_pca > 0:
        if len(pc1) == n:
            constraints.append(cp.abs(w @ pc1) <= epsilon_pca)

    prob = cp.Problem(cp.Minimize(obj), constraints)
    try:
        prob.solve(solver=cp.ECOS, verbose=False)
        if prob.status in ("optimal", "optimal_inaccurate"):
            sol = {a: float(w.value[i]) for i, a in enumerate(assets)}
            return sol, True
    except Exception:
        pass
    try:
        prob.solve(solver=cp.OSQP, verbose=False)
        if prob.status in ("optimal", "optimal_inaccurate"):
            sol = {a: float(w.value[i]) for i, a in enumerate(assets)}
            return sol, True
    except Exception:
        pass
    return {}, False


def _solve_minvar_qp_scipy(
    cov: np.ndarray,
    assets: List[str],
    prev_weights: Optional[Dict[str, float]],
    G: float,
    max_w_abs: float,
    liquidity_caps: Optional[Dict[str, float]],
) -> Tuple[Dict[str, float], bool]:
    """Fallback QP solver using scipy when cvxpy unavailable."""
    from scipy.optimize import minimize, Bounds, LinearConstraint

    n = len(assets)
    lc = liquidity_caps or {}
    caps = np.array([min(max_w_abs, lc.get(a, max_w_abs)) for a in assets])
    cov_reg = cov + 1e-6 * np.eye(n)

    def obj(w):
        return 0.5 * w @ cov_reg @ w

    eq_con = LinearConstraint(np.ones(n), 0, 0)
    bounds = Bounds(-caps, caps)

    # Initial guess: equal-weight long/short split to avoid trivial w=0
    k = n // 2
    w0 = np.zeros(n)
    w0[:k] = G / (2 * k)
    w0[k:] = -G / (2 * (n - k))
    w0 = np.clip(w0, -caps, caps)
    w0 = w0 - w0.sum() / n

    res = minimize(obj, w0, method="SLSQP", bounds=bounds, constraints=[eq_con], options={"maxiter": 500})
    if not res.success:
        return {}, False
    w = res.x
    if np.abs(w.sum()) > 1e-3:
        w = w - w.sum() / n
    if np.abs(w.sum()) > 1e-3:
        return {}, False
    gross = np.abs(w).sum()
    if gross < 1e-6:
        return {}, False
    if gross > G + 0.01:
        w = w * (G / gross)
    return {a: float(w[i]) for i, a in enumerate(assets)}, True


def run_method_a(
    prices: pd.DataFrame,
    marketcap: pd.DataFrame,
    volume: pd.DataFrame,
    start_date: date,
    end_date: date,
    config: Dict,
) -> Tuple[List[Dict], Dict]:
    """
    Run Method A for each rebalance date. Returns list of weight snapshots and metadata.
    """
    cov_lookback = config.get("cov_lookback_days", 90)
    cov_fallback = config.get("cov_fallback_days", 60)
    G = config.get("G", 1.0)
    max_w_abs = config.get("max_w_abs", 0.10)
    max_participation = config.get("max_participation", 0.05)
    alpha_cvar = config.get("alpha_cvar", 0.5)
    beta_turnover = config.get("beta_turnover", 0.1)
    epsilon_pca = config.get("epsilon_pca", 0.02)
    use_pca = config.get("use_pca_constraint", True)
    fee_bps = config.get("fee_bps", 5)
    slippage_bps = config.get("slippage_bps", 5)

    returns = compute_returns(prices)
    usd_adv = compute_usd_adv(prices, volume, window=21)
    rebal_dates = get_rebalance_dates(start_date, end_date)

    snapshots = []
    prev_weights: Optional[Dict[str, float]] = None
    assets_list = list(prices.columns)

    for rd in rebal_dates:
        idx = returns.index.get_indexer([rd], method="ffill")[0]
        if idx < 0:
            continue
        lookback_start = returns.index[max(0, idx - cov_lookback)]
        window_returns = returns.loc[lookback_start : returns.index[idx]]
        if len(window_returns) < 30:
            lookback_start = returns.index[max(0, idx - cov_fallback)]
            window_returns = returns.loc[lookback_start : returns.index[idx]]
        if len(window_returns) < 20:
            continue

        # Covariance
        cov, cols = ledoit_wolf_cov(window_returns)
        assets = [c for c in assets_list if c in cols]
        if len(assets) < 2:
            continue

        col_idx = [list(cols).index(a) for a in assets]
        cov_sub = cov[np.ix_(col_idx, col_idx)]

        scenario_returns = window_returns[assets].fillna(0).values

        # PCA
        pc1 = None
        if use_pca and epsilon_pca > 0:
            pc1 = pca_first_component(window_returns[assets])
            if len(pc1) != len(assets):
                pc1 = None

        # Liquidity caps: |w_i| <= max_participation * USD_ADV_i (portfolio notional=1)
        portfolio_notional = 1.0
        liquidity_caps = {}
        if usd_adv is not None and rd in usd_adv.index:
            adv_row = usd_adv.loc[rd]
            for a in assets:
                if a in adv_row.index and pd.notna(adv_row[a]) and adv_row[a] > 0:
                    cap = (max_participation * adv_row[a]) / portfolio_notional
                    liquidity_caps[a] = min(max_w_abs, cap)

        sol, ok = solve_minvar_qp(
            cov=cov_sub,
            assets=assets,
            scenario_returns=scenario_returns,
            prev_weights=prev_weights,
            G=G,
            max_w_abs=max_w_abs,
            liquidity_caps=liquidity_caps if liquidity_caps else None,
            pc1=pc1,
            epsilon_pca=epsilon_pca,
            alpha_cvar=alpha_cvar,
            beta_turnover=beta_turnover,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
        )
        if not ok or not sol:
            continue

        # Drop near-zero weights
        sol = {a: v for a, v in sol.items() if abs(v) > 1e-6}
        if not sol:
            continue

        mcap_row = marketcap.loc[rd] if rd in marketcap.index else None
        mcap_vals = {a: float(mcap_row[a]) for a in sol if mcap_row is not None and a in mcap_row.index and pd.notna(mcap_row[a])} if mcap_row is not None else {}
        adv_vals = {a: float(usd_adv.loc[rd, a]) for a in sol if usd_adv is not None and rd in usd_adv.index and a in usd_adv.columns and pd.notna(usd_adv.loc[rd, a])} if usd_adv is not None else {}

        snapshots.append({
            "rebalance_date": rd,
            "weights": sol,
            "marketcap": mcap_vals,
            "adv_30d": adv_vals,
        })
        prev_weights = sol

    return snapshots, {"method": "A", "config": config}
