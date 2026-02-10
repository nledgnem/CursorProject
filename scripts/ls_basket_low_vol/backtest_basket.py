"""
Backtest basket weights and compute metrics.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import date, timedelta

from .utils import compute_returns, compute_turnover


def run_backtest(
    snapshots: List[Dict],
    prices: pd.DataFrame,
    fee_bps: float = 5,
    slippage_bps: float = 5,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Backtest weight snapshots. Returns daily PnL series and metrics dict.
    """
    if not snapshots:
        return pd.DataFrame(), {}

    returns = compute_returns(prices)
    rebal_dates = {s["rebalance_date"] for s in snapshots}
    weight_by_date = {s["rebalance_date"]: s["weights"] for s in snapshots}
    rebal_sorted = sorted(rebal_dates)

    all_dates = sorted(set(returns.index) | set(prices.index))
    all_dates = [d for d in all_dates if d >= min(rebal_sorted) and d <= max(rebal_sorted)]
    for i, d in enumerate(all_dates):
        if hasattr(d, "date"):
            all_dates[i] = d.date() if callable(getattr(d, "date", None)) else d

    rows = []
    current_weights: Dict[str, float] = {}
    prev_weights: Dict[str, float] = {}
    equity = 1.0

    for i, d in enumerate(all_dates):
        if d in weight_by_date:
            prev_weights = current_weights.copy()
            current_weights = weight_by_date[d].copy()
            if prev_weights:
                turnover = compute_turnover(pd.Series(prev_weights), pd.Series(current_weights))
                cost = (fee_bps + slippage_bps) / 10000.0 * turnover
            else:
                turnover = 1.0
                cost = (fee_bps + slippage_bps) / 10000.0 * turnover
        else:
            cost = 0.0
            turnover = 0.0

        if d not in returns.index or not current_weights:
            rows.append({"date": d, "pnl": np.nan, "pnl_long": np.nan, "pnl_short": np.nan, "gross_exposure": 0.0, "cost": cost, "turnover": turnover})
            continue

        ret_row = returns.loc[d]
        pnl = 0.0
        pnl_long = 0.0
        pnl_short = 0.0
        for sym, w in current_weights.items():
            if sym in ret_row.index and pd.notna(ret_row[sym]):
                r = ret_row[sym]
                pnl += w * r
                if w > 0:
                    pnl_long += w * r
                else:
                    pnl_short += w * r
        pnl -= cost
        gross = sum(abs(w) for w in current_weights.values())

        rows.append({
            "date": d,
            "pnl": pnl,
            "pnl_long": pnl_long,
            "pnl_short": pnl_short,
            "gross_exposure": gross,
            "cost": cost,
            "turnover": turnover,
            "equity": np.nan,
        })
        equity *= 1.0 + pnl

    df = pd.DataFrame(rows)
    df["equity"] = (1.0 + df["pnl"].fillna(0)).cumprod()

    metrics = compute_metrics(df, snapshots, prices, fee_bps, slippage_bps)
    return df, metrics


def compute_metrics(
    pnl_df: pd.DataFrame,
    snapshots: List[Dict],
    prices: pd.DataFrame,
    fee_bps: float,
    slippage_bps: float,
) -> Dict:
    """Compute backtest metrics."""
    pnl = pnl_df["pnl"].dropna()
    if len(pnl) < 2:
        return {"error": "Insufficient data"}

    vol_ann = pnl.std() * np.sqrt(252)
    skew = pnl.skew()
    kurt = pnl.kurtosis()

    cvar95 = -pnl.quantile(0.05)
    cvar99 = -pnl.quantile(0.01)
    tail = pnl[pnl <= pnl.quantile(0.05)]
    cvar95_hist = -tail.mean() if len(tail) > 0 else np.nan
    tail99 = pnl[pnl <= pnl.quantile(0.01)]
    cvar99_hist = -tail99.mean() if len(tail99) > 0 else np.nan

    equity = pnl_df["equity"].dropna()
    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1.0
    max_dd = drawdown.min()

    turnover = pnl_df["turnover"]
    avg_turnover = turnover.mean()
    med_turnover = turnover.median()
    if "date" in pnl_df.columns:
        td = pnl_df.copy()
        td["date"] = pd.to_datetime(td["date"])
        td = td.set_index("date")
        monthly_turnover = td["turnover"].resample("ME").sum()
    else:
        monthly_turnover = pd.Series([turnover.sum()])

    pnl_long = pnl_df["pnl_long"].dropna()
    pnl_short = pnl_df["pnl_short"].dropna()
    common = pnl_long.index.intersection(pnl_short.index)
    if len(common) > 5:
        ls_corr = pnl_long.loc[common].corr(pnl_short.loc[common])
    else:
        ls_corr = np.nan

    gross = pnl_df["gross_exposure"]
    avg_gross = gross.mean()
    max_gross = gross.max()

    max_per_asset = 0.0
    for s in snapshots:
        w = s.get("weights", {})
        if w:
            max_per_asset = max(max_per_asset, max(abs(v) for v in w.values()))

    return {
        "realized_vol_ann": float(vol_ann),
        "skewness": float(skew),
        "kurtosis": float(kurt),
        "cvar95": float(cvar95_hist) if not np.isnan(cvar95_hist) else float(cvar95),
        "cvar99": float(cvar99_hist) if not np.isnan(cvar99_hist) else float(cvar99),
        "max_drawdown": float(max_dd),
        "avg_turnover": float(avg_turnover),
        "median_turnover": float(med_turnover),
        "avg_monthly_turnover": float(monthly_turnover.mean()) if len(monthly_turnover) > 0 else float(avg_turnover),
        "long_short_corr": float(ls_corr) if not np.isnan(ls_corr) else None,
        "avg_gross_exposure": float(avg_gross),
        "max_gross_exposure": float(max_gross),
        "max_per_asset_exposure": float(max_per_asset),
    }


def identify_tail_dates(pnl_df: pd.DataFrame, n: int = 5) -> List[Tuple[date, float]]:
    """Dates and assets dominating tail moves."""
    pnl = pnl_df["pnl"].dropna()
    if len(pnl) == 0:
        return []
    worst = pnl.nsmallest(n)
    return [(d, float(pnl.loc[d])) for d in worst.index]
