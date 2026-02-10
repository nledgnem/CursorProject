"""
Method B: Cluster-Matched Long/Short Pairs.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import date

from .utils import (
    compute_returns,
    compute_usd_adv,
    get_rebalance_dates,
)


def run_method_b(
    prices: pd.DataFrame,
    marketcap: pd.DataFrame,
    volume: pd.DataFrame,
    start_date: date,
    end_date: date,
    config: Dict,
) -> Tuple[List[Dict], Dict]:
    """
    Cluster universe by 90d return correlations, create matched long/short sub-baskets
    within each cluster. Normalize for dollar neutrality.
    """
    np.random.seed(42)
    K = config.get("K", 10)
    cluster_lookback = config.get("cluster_lookback_days", 90)
    signal_lookback = config.get("signal_lookback_days", 30)
    cluster_budget = config.get("cluster_budget", 0.15)
    max_w_abs = config.get("max_w_abs", 0.10)
    max_participation = config.get("max_participation", 0.05)
    fee_bps = config.get("fee_bps", 5)
    slippage_bps = config.get("slippage_bps", 5)

    returns = compute_returns(prices)
    usd_adv = compute_usd_adv(prices, volume, window=21)
    rebal_dates = get_rebalance_dates(start_date, end_date)

    snapshots = []
    assets_list = list(prices.columns)

    for rd in rebal_dates:
        idx = returns.index.get_indexer([rd], method="ffill")[0]
        if idx < 0:
            continue
        # Correlation lookback
        lookback_start = returns.index[max(0, idx - cluster_lookback)]
        window_ret = returns.loc[lookback_start : returns.index[idx]][assets_list]
        window_ret = window_ret.dropna(axis=1, how="all").dropna(axis=0, how="any")
        if len(window_ret) < 30 or window_ret.shape[1] < 2 * K:
            continue

        assets = list(window_ret.columns)
        corr = window_ret.corr()
        corr = corr.fillna(0)
        dist = 1 - np.clip(corr.values, -1, 1)

        # Agglomerative clustering on distance
        from sklearn.cluster import AgglomerativeClustering
        n_clusters = min(K, len(assets) // 2)
        if n_clusters < 2:
            continue
        clust = AgglomerativeClustering(n_clusters=n_clusters, metric="precomputed", linkage="average")
        labels = clust.fit_predict(dist)

        # Signal: 30d mean return for ranking within cluster
        sig_start = returns.index[max(0, idx - signal_lookback)]
        sig_ret = returns.loc[sig_start : returns.index[idx]][assets]
        mu = sig_ret.mean()
        mu = mu.fillna(0)

        # Per cluster: top m long, bottom m short by mu. Budget cluster_budget per cluster.
        # m such that cluster contributes <= cluster_budget gross per side
        weights = {}
        for k in range(n_clusters):
            mask = labels == k
            cluster_assets = [assets[i] for i in range(len(assets)) if mask[i]]
            if len(cluster_assets) < 2:
                continue
            mu_k = mu[cluster_assets].sort_values(ascending=False)
            m = max(1, min(len(cluster_assets) // 2, int(np.ceil(len(cluster_assets) * 0.3))))
            longs = mu_k.head(m).index.tolist()
            shorts = mu_k.tail(m).index.tolist()

            # Equal weight within cluster legs, scaled to cluster_budget
            w_per_long = (cluster_budget / 2) / len(longs) if longs else 0
            w_per_short = -(cluster_budget / 2) / len(shorts) if shorts else 0
            for a in longs:
                weights[a] = weights.get(a, 0) + w_per_long
            for a in shorts:
                weights[a] = weights.get(a, 0) + w_per_short

        if not weights:
            continue

        # Apply per-asset and liquidity caps
        portfolio_notional = 1.0
        liquidity_caps = {}
        if usd_adv is not None and rd in usd_adv.index:
            adv_row = usd_adv.loc[rd]
            for a in weights:
                if a in adv_row.index and pd.notna(adv_row[a]) and adv_row[a] > 0:
                    cap = (max_participation * adv_row[a]) / portfolio_notional
                    liquidity_caps[a] = min(max_w_abs, cap)
                else:
                    liquidity_caps[a] = max_w_abs

        for a in list(weights.keys()):
            cap = liquidity_caps.get(a, max_w_abs)
            w = weights[a]
            if abs(w) > cap:
                weights[a] = np.sign(w) * cap
        weights = {a: v for a, v in weights.items() if abs(v) > 1e-6}

        # Enforce dollar neutrality: scale long or short leg to balance
        total = sum(weights.values())
        if abs(total) > 1e-6:
            long_sum = sum(v for v in weights.values() if v > 0)
            short_sum = abs(sum(v for v in weights.values() if v < 0))
            if long_sum > 0 and short_sum > 0:
                new_w = {}
                if total > 0:
                    scale = short_sum / long_sum
                    for a, v in weights.items():
                        new_w[a] = v * scale if v > 0 else v
                else:
                    scale = long_sum / short_sum
                    for a, v in weights.items():
                        new_w[a] = v if v > 0 else v * scale
                weights = new_w
        weights = {a: v for a, v in weights.items() if abs(v) > 1e-6}

        # Gross exposure cap
        gross = sum(abs(v) for v in weights.values())
        if gross > 1.0:
            for a in weights:
                weights[a] /= gross

        mcap_row = marketcap.loc[rd] if rd in marketcap.index else None
        mcap_vals = {a: float(mcap_row[a]) for a in weights if mcap_row is not None and a in mcap_row.index and pd.notna(mcap_row[a])} if mcap_row is not None else {}
        adv_vals = {a: float(usd_adv.loc[rd, a]) for a in weights if usd_adv is not None and rd in usd_adv.index and a in usd_adv.columns and pd.notna(usd_adv.loc[rd, a])} if usd_adv is not None else {}

        snapshots.append({
            "rebalance_date": rd,
            "weights": weights,
            "marketcap": mcap_vals,
            "adv_30d": adv_vals,
        })

    return snapshots, {"method": "B", "config": config}
