"""Compute forward returns for regime evaluation.

Forward returns are computed as: fwd_ret(t, H) = compounded return from t+1 to t+H.
Explicitly excludes same-day returns (no lookahead).
"""

import pandas as pd
import numpy as np
from typing import List


def compute_forward_returns(
    ls_returns: pd.Series,
    horizons: List[int],
) -> pd.DataFrame:
    """
    Compute forward returns for multiple horizons.
    
    For each date t and horizon H:
    fwd_ret(t, H) = compounded return from ls_ret(t+1) ... ls_ret(t+H)
    
    Uses log returns for numerical stability: exp(sum(log(1+r))) - 1
    
    Args:
        ls_returns: Series with date index and LS returns (r_ls_net)
        horizons: List of horizons (e.g., [5, 10, 20])
        
    Returns:
        DataFrame with date index and columns fwd_ret_H for each horizon H.
        Missing values (NaN) where insufficient data for forward window.
    """
    if ls_returns.empty:
        return pd.DataFrame(index=ls_returns.index)
    
    # Convert to log returns (handle zeros/negatives)
    log_returns = np.log1p(ls_returns)  # log(1+r)
    
    result = pd.DataFrame(index=ls_returns.index)
    
    for H in horizons:
        # For each date t, we need returns from t+1 to t+H
        # So we shift by -H and then take the sum of the next H periods
        
        # Forward-looking: sum of log returns from t+1 to t+H
        # We can use rolling sum with min_periods=H
        # But we need to shift by 1 first (exclude same-day)
        
        # Shift by 1 to exclude same-day, then rolling sum of H periods
        shifted = log_returns.shift(-1)  # Shift forward by 1 (exclude same-day)
        
        # Rolling sum of next H periods (but we want sum of t+1 to t+H, not t to t+H-1)
        # So we need to reverse the shift logic
        # Actually: for date t, we want sum from t+1 to t+H
        # We can use: shift(-1) then rolling(H).sum() but that gives t+1 to t+H-1
        # Better: manually compute for each date
        
        fwd_log_returns = []
        for i in range(len(log_returns)):
            # For date at index i, we need returns from i+1 to i+H
            if i + H >= len(log_returns):
                fwd_log_returns.append(np.nan)
            else:
                # Sum log returns from index i+1 to i+H (inclusive)
                window_log_returns = log_returns.iloc[i+1:i+H+1]
                if window_log_returns.isna().any():
                    fwd_log_returns.append(np.nan)
                else:
                    log_sum = window_log_returns.sum()
                    fwd_log_returns.append(log_sum)
        
        # Convert back to simple returns: exp(sum(log(1+r))) - 1
        fwd_returns = np.expm1(fwd_log_returns)  # exp(x) - 1
        
        result[f"fwd_ret_{H}"] = fwd_returns
    
    return result


def align_regime_and_returns(
    regime_df: pd.DataFrame,
    ls_returns_df: pd.DataFrame,
    drop_missing: bool = True,
) -> pd.DataFrame:
    """
    Align regime scores with LS returns using inner join.
    
    Args:
        regime_df: DataFrame with date index and regime_1_5 column
        ls_returns_df: DataFrame with date index and ls_ret column (e.g., r_ls_net)
        drop_missing: If True, drop dates not in both series (inner join)
                     If False, keep all dates and fill with NaN
        
    Returns:
        DataFrame with date index and columns:
        - regime_1_5
        - ls_ret
        - Any forward return columns if present in ls_returns_df
    """
    # Ensure date is index (not column)
    if "date" in regime_df.columns:
        regime_df = regime_df.set_index("date")
    if "date" in ls_returns_df.columns:
        ls_returns_df = ls_returns_df.set_index("date")
    
    # Find ls_ret column (could be r_ls_net, r_ls, ls_ret, etc.)
    ls_col = None
    for col in ["r_ls_net", "r_ls", "ls_ret"]:
        if col in ls_returns_df.columns:
            ls_col = col
            break
    
    if ls_col is None:
        raise ValueError("No LS return column found (expected r_ls_net, r_ls, or ls_ret)")
    
    # Extract relevant columns
    regime_cols = ["regime_1_5"]
    if "score_raw" in regime_df.columns:
        regime_cols.append("score_raw")
    if "monitor_name" in regime_df.columns:
        regime_cols.append("monitor_name")
    
    ls_cols = [ls_col]
    # Also include forward return columns if present
    for col in ls_returns_df.columns:
        if col.startswith("fwd_ret_"):
            ls_cols.append(col)
    
    # Join
    if drop_missing:
        aligned = regime_df[regime_cols].join(
            ls_returns_df[ls_cols],
            how="inner"
        )
    else:
        aligned = regime_df[regime_cols].join(
            ls_returns_df[ls_cols],
            how="outer"
        )
    
    # Rename ls_ret column to standard name
    if ls_col != "ls_ret":
        aligned = aligned.rename(columns={ls_col: "ls_ret"})
    
    # Drop rows where regime or ls_ret is missing
    aligned = aligned.dropna(subset=["regime_1_5", "ls_ret"])
    
    return aligned




