"""
Ranking and quality scoring for basis-trade candidates.
- Ranking A: Highest apr_simple (funding-only)
- Ranking B: Quality score that penalizes neg_frac, stdev, top10_share (event-driven carry)
- Configurable weights via CLI
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def quality_score(
    df: pd.DataFrame,
    w_neg_frac: float = 1.0,
    w_stdev: float = 1.0,
    w_top10_share: float = 1.0,
) -> pd.Series:
    """
    Compute quality score: penalizes neg_frac, stdev, top10_share.
    Higher is better. Normalize components to [0,1] and combine.
    """
    apr = df["apr_simple"].fillna(-np.inf)
    neg = df["neg_frac"].fillna(1.0)
    stdev = df["stdev"].fillna(np.inf)
    top10 = df["top10_share"].fillna(1.0)

    # Penalty terms: lower is better for neg_frac, stdev, top10_share
    # We want: score = apr_component - penalties
    # Or: score proportional to apr, reduced by penalties
    neg_penalty = neg * w_neg_frac
    stdev_norm = np.clip(stdev / (stdev.quantile(0.9) + 1e-10), 0, 2)
    stdev_penalty = stdev_norm * w_stdev
    top10_penalty = top10 * w_top10_share

    # Quality = apr (scaled) minus penalties; ensure non-negative baseline
    apr_scaled = np.clip(apr, -100, 100) / 100.0
    score = apr_scaled - 0.33 * neg_penalty - 0.33 * stdev_penalty - 0.33 * top10_penalty
    return score


def rank_by_apr(df: pd.DataFrame) -> pd.DataFrame:
    """Rank by apr_simple descending (highest first)."""
    out = df.copy()
    out["rank_apr"] = out["apr_simple"].rank(ascending=False, method="min").astype(int)
    return out.sort_values("apr_simple", ascending=False).reset_index(drop=True)


def rank_by_quality(
    df: pd.DataFrame,
    w_neg_frac: float = 1.0,
    w_stdev: float = 1.0,
    w_top10_share: float = 1.0,
) -> pd.DataFrame:
    """Rank by quality score descending."""
    out = df.copy()
    out["quality_score"] = quality_score(out, w_neg_frac, w_stdev, w_top10_share)
    out["rank_quality"] = out["quality_score"].rank(ascending=False, method="min").astype(int)
    return out.sort_values("quality_score", ascending=False).reset_index(drop=True)
