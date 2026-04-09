from __future__ import annotations

"""
Shared macro-regime gate policy used by both the Streamlit dashboard and Telegram alerts.

Notes:
- Environment_APR is stored as percentage points (e.g., 4.0 == 4% APR).
- Fragmentation_Spread is a raw decimal (no percent scaling).
"""

# Gate constants (kept identical to dashboards/app_regime_monitor.py).
ENVIRONMENT_APR_ENTRY_GATE_PCT: float = 2.0
FRAGMENTATION_IDIOSYNCRATIC_TOXIC_CEILING: float = 0.000075

def calculate_risk_weight(apr_pct: float) -> float:
    """
    Continuous safety dome w_risk from weekly Environment_APR (% points).

    Taper logic (unchanged):
    - < 2.0%: 0.0
    - 2.0–5.0%: linear 0→1
    - 5.0–15.0%: 1.0
    - 15.0–35.0%: linear 1→0
    - > 35.0%: 0.0
    """
    import pandas as pd

    if pd.isna(apr_pct):
        return 0.0
    if apr_pct < ENVIRONMENT_APR_ENTRY_GATE_PCT:
        return 0.0
    if ENVIRONMENT_APR_ENTRY_GATE_PCT <= apr_pct < 5.0:
        return (apr_pct - ENVIRONMENT_APR_ENTRY_GATE_PCT) / 3.0
    if 5.0 <= apr_pct <= 15.0:
        return 1.0
    if 15.0 < apr_pct <= 35.0:
        return 1.0 - ((apr_pct - 15.0) / 20.0)
    return 0.0

