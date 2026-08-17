from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
logger = logging.getLogger(__name__)

from src.macro_regime.gate_policy import ENVIRONMENT_APR_ENTRY_GATE_PCT
from src.macro_regime.btcdom_trend import format_regime_label

def _safe_float(v) -> float:
    try:
        if v is None:
            return float("nan")
        return float(v)
    except Exception:
        return float("nan")


def _regime_label(row: dict) -> str:
    """Thin delegate to the canonical renderer.

    This function used to carry its own copy of the funding/trend/gate
    formatting, byte-identical to the copy in
    ``scripts/live/live_data_fetcher.py`` -- which is how one rendering bug
    (``bool(float("nan")) is True``, so an un-evaluable gate displayed as
    GATE:ON) shipped in two places at once. See ADR 003.
    """
    return format_regime_label(row)


def _environment_regime_phrase(apr_pct_points: float) -> str:
    """Human regime bucket from Environment_APR (percentage points)."""
    if apr_pct_points < ENVIRONMENT_APR_ENTRY_GATE_PCT:
        return "Cold Flush"
    if apr_pct_points < 5.0:
        return "Recovery Ramp"
    if apr_pct_points <= 15.0:
        return "Golden Pocket"
    return "Leverage Exhaustion"


def load_latest_macro_snapshot(db_path: Path) -> tuple[str, float] | None:
    """
    Returns (regime_line, environment_apr_pct_points) from macro_features, or None.
    """
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                """
SELECT funding_regime, BTCDOM_Trend, is_mrf_active, Environment_APR
FROM macro_features
ORDER BY decision_date DESC
LIMIT 1;
                """.strip()
            )
            r = cur.fetchone()
        if r is None:
            return None
        row = {
            "funding_regime": r[0],
            "BTCDOM_Trend": r[1],
            "is_mrf_active": r[2],
            "Environment_APR": r[3],
        }
        apr = _safe_float(row.get("Environment_APR"))
        if apr != apr:  # NaN
            apr_display = float("nan")
        else:
            apr_display = apr
        label = _regime_label(row)
        return label, apr_display
    except Exception as e:
        logger.warning("Macro snapshot read failed (non-fatal): %s", e)
        return None


def format_regime_apathy_line(db_path: Path) -> str:
    snap = load_latest_macro_snapshot(db_path)
    if snap is None:
        return "Regime: (unavailable) | APR: n/a"
    _label, apr = snap
    if apr != apr:
        return "Regime: (unavailable) | APR: n/a"
    phrase = _environment_regime_phrase(apr)
    return f"Regime: {phrase} | APR: {apr:.2f}%"
