"""
Compute funding-only carry metrics per symbol per window.
- funding_return = sum(fundingRate) as % return on notional (short-perp earns +rate when rate>0)
- apr_simple, pos_frac, neg_frac, zero_frac, stdev, top10_share, max_drawdown
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_metrics(
    rates: pd.Series,
    window_days: float,
) -> dict:
    """
    Compute all metrics for a single symbol/window.
    rates: series of funding rates (float, e.g. 0.0001 = 0.01%)
    """
    if rates.empty or len(rates) == 0:
        return {
            "funding_return": np.nan,
            "apr_simple": np.nan,
            "pos_frac": np.nan,
            "neg_frac": np.nan,
            "zero_frac": np.nan,
            "stdev": np.nan,
            "top10_share": np.nan,
            "max_drawdown": np.nan,
            "n_prints": 0,
        }

    arr = np.asarray(rates, dtype=float)
    n = len(arr)

    # funding_return = sum(fundingRate) as % (e.g. 0.01 = 1% on notional)
    funding_return = float(np.sum(arr))

    # apr_simple = funding_return / window_days * 365
    apr_simple = funding_return / max(window_days, 1e-6) * 365.0 if window_days > 0 else np.nan

    # pos_frac, neg_frac, zero_frac
    pos = np.sum(arr > 0)
    neg = np.sum(arr < 0)
    zero = np.sum(arr == 0)
    pos_frac = pos / n
    neg_frac = neg / n
    zero_frac = zero / n

    # stdev of fundingRate
    stdev = float(np.std(arr)) if n > 1 else 0.0

    # top10_share: share of total positive funding from top 10 positive prints (concentration)
    positive = arr[arr > 0]
    sum_positive = float(np.sum(positive))
    if len(positive) > 0 and sum_positive > 0:
        top10_pos = np.partition(positive, -min(10, len(positive)))[-min(10, len(positive)) :]
        top10_sum = float(np.sum(top10_pos))
        top10_share = top10_sum / sum_positive
    else:
        top10_share = 0.0

    # max_drawdown of cumulative funding curve (funding-only, short-perp perspective)
    cum = np.cumsum(arr)
    running_max = np.maximum.accumulate(cum)
    drawdowns = running_max - cum
    max_drawdown = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0.0

    return {
        "funding_return": funding_return,
        "apr_simple": apr_simple,
        "pos_frac": pos_frac,
        "neg_frac": neg_frac,
        "zero_frac": zero_frac,
        "stdev": stdev,
        "top10_share": top10_share,
        "max_drawdown": max_drawdown,
        "n_prints": n,
    }


def compute_metrics_for_windows(
    df: pd.DataFrame,
    windows: list[int],
) -> pd.DataFrame:
    """
    Compute metrics for multiple windows from a funding DataFrame.
    df must have columns: fundingTime (ms), fundingRate (float)
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
    df = df.dropna(subset=["fundingRate"])
    if df.empty:
        return pd.DataFrame()

    df["ts"] = pd.to_datetime(df["fundingTime"], unit="ms")
    df = df.sort_values("ts").reset_index(drop=True)
    rates = df["fundingRate"]

    rows: list[dict] = []
    end_ts = df["ts"].max()

    for w in windows:
        start_ts = end_ts - pd.Timedelta(days=w)
        mask = (df["ts"] >= start_ts) & (df["ts"] <= end_ts)
        sub = rates[mask]

        m = compute_metrics(sub, float(w))
        m["window_days"] = w
        rows.append(m)

    return pd.DataFrame(rows)
