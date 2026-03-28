"""
Macro Environment Sensor: daily cross-sectional IQM (Environment APR),
fragmentation spread, regime Z-score, weekly momentum / gating helpers.

Rates in Silver are pure-decimal 8-hour funding; annualization uses ×1095 (365×3).
Environment_APR in weekly outputs is in percentage points (e.g. 4.0 = 4% APR)
to match risk-weight thresholds.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Tuple

import numpy as np
import pandas as pd


def calculate_spread(group: pd.DataFrame) -> float:
    """Cross-sectional spread for one epoch (one calendar day in Silver grain)."""
    rates = group["harmonized_rate"].dropna().sort_values().values
    n = len(rates)
    if n < 4:
        return np.nan
    q25 = rates[int(n * 0.25)]
    q75 = rates[int(n * 0.75)]
    return float(q75 - q25)


def _truncated_interquartile_mean(rates: np.ndarray) -> float:
    rates = np.sort(rates[~np.isnan(rates)])
    n = len(rates)
    if n == 0:
        return np.nan
    k = n // 4
    if k == 0:
        return float(np.mean(rates))
    middle = rates[k:-k]
    if len(middle) == 0:
        return np.nan
    return float(np.mean(middle))


def _collapse_funding_to_asset_daily(funding: pd.DataFrame) -> pd.DataFrame:
    """One harmonized rate per (date, asset_id)."""
    need = {"date", "asset_id", "funding_rate_raw_pct"}
    missing = need - set(funding.columns)
    if missing:
        raise KeyError(f"funding DataFrame missing columns: {sorted(missing)}")
    fd = funding[["date", "asset_id", "funding_rate_raw_pct"]].copy()
    fd["date"] = pd.to_datetime(fd["date"]).dt.normalize()
    fd["harmonized_rate"] = fd["funding_rate_raw_pct"].astype(float)
    return (
        fd.groupby(["date", "asset_id"], as_index=False)["harmonized_rate"]
        .mean()
        .sort_values(["date", "asset_id"])
        .reset_index(drop=True)
    )


def build_daily_environment_table(funding: pd.DataFrame) -> pd.DataFrame:
    """
    Per calendar day: IQM environment rate, APR in percentage points, fragmentation spread.

    Spread is computed on the cross-section of asset-level harmonized rates (Silver grain;
    underlying sampling is 8-hour native).
    """
    if funding is None or len(funding) == 0:
        return pd.DataFrame(
            columns=[
                "date",
                "env_rate_dec",
                "Environment_APR_daily_pct",
                "Fragmentation_Spread",
            ]
        )

    asset_daily = _collapse_funding_to_asset_daily(funding)
    rows = []
    for d, grp in asset_daily.groupby("date"):
        rates = grp["harmonized_rate"].values
        iqm = _truncated_interquartile_mean(rates)
        spr = calculate_spread(grp)
        apr_dec = iqm * 1095.0 if np.isfinite(iqm) else np.nan
        apr_pct = apr_dec * 100.0 if np.isfinite(apr_dec) else np.nan
        rows.append(
            {
                "date": d,
                "env_rate_dec": iqm,
                "Environment_APR_daily_pct": apr_pct,
                "Fragmentation_Spread": spr,
            }
        )

    out = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    out["Z_Score_90d"] = _zscore_90d_on_series(out["Environment_APR_daily_pct"])
    return out


def _zscore_90d_on_series(s: pd.Series) -> pd.Series:
    rolling_90d = s.rolling(window=90, min_periods=30)
    return (s - rolling_90d.mean()) / rolling_90d.std()


def calculate_risk_weight(apr_pct: float) -> float:
    """Continuous safety dome w_risk from weekly Environment_APR (% points)."""
    if pd.isna(apr_pct):
        return 0.0
    if apr_pct < 2.0:
        return 0.0
    if 2.0 <= apr_pct < 5.0:
        return (apr_pct - 2.0) / 3.0
    if 5.0 <= apr_pct <= 15.0:
        return 1.0
    if 15.0 < apr_pct <= 35.0:
        return 1.0 - ((apr_pct - 15.0) / 20.0)
    return 0.0


def weekly_lookback_means(
    daily: pd.DataFrame,
    decision_date: date,
    lookback_days: int,
) -> Tuple[float, float, float]:
    """
    Mean daily Environment_APR (% pts) and Fragmentation_Spread over
    [decision_date - lookback_days, decision_date - 1].
    Z_Score_90d snapshot at decision_date (as-of prior if missing).
    """
    if daily is None or len(daily) == 0:
        return float("nan"), float("nan"), float("nan")

    start = decision_date - timedelta(days=lookback_days)
    end = decision_date - timedelta(days=1)
    d = daily.copy()
    d["date"] = pd.to_datetime(d["date"]).dt.normalize()

    win = d[(d["date"] >= pd.Timestamp(start)) & (d["date"] <= pd.Timestamp(end))]
    env_m = float(win["Environment_APR_daily_pct"].mean()) if len(win) else float("nan")
    spr_m = float(win["Fragmentation_Spread"].mean()) if len(win) else float("nan")

    snap = d[d["date"] == pd.Timestamp(decision_date)]
    if len(snap) > 0 and np.isfinite(snap["Z_Score_90d"].iloc[0]):
        z = float(snap["Z_Score_90d"].iloc[0])
    else:
        prior = d[d["date"] < pd.Timestamp(decision_date)].sort_values("date")
        z = float(prior["Z_Score_90d"].iloc[-1]) if len(prior) and np.isfinite(prior["Z_Score_90d"].iloc[-1]) else float("nan")

    return env_m, spr_m, z


def apply_weekly_momentum_and_gate(df_weekly: pd.DataFrame) -> pd.DataFrame:
    """Delta_APR, LogModulus, Conditioned_Momentum, w_risk on sorted weekly frame."""
    out = df_weekly.sort_values("decision_date").copy()
    out["Delta_APR"] = out["Environment_APR"].diff()
    out["LogModulus_APR"] = np.sign(out["Environment_APR"]) * np.log1p(
        np.abs(out["Environment_APR"])
    )
    out["Conditioned_Momentum"] = out["Delta_APR"] * out["LogModulus_APR"]
    out["w_risk"] = out["Environment_APR"].apply(calculate_risk_weight)
    return out
