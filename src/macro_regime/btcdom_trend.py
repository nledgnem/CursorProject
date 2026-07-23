from __future__ import annotations

"""
Canonical BTCDOM trend + MRF gate logic (single source of truth).

Exports
-------
- ``compute_btcdom_trend(index, sma)``  -> nullable "Rising"/"Falling"
- ``compute_mrf_gate(funding_regime, trend)`` -> nullable boolean
- ``apply_gate(values, gate)`` -> gated series that preserves "unknown"
- ``trend_label(value)`` -> human-facing string, never "None"/"nan"

Why this module exists
----------------------
The trend expression used to be duplicated as a bare
``np.where(index > sma, "Rising", "Falling")`` in three places:
``majors_alts_monitor/msm_funding_v0/msm_run.py``,
``scripts/generate_equity_curve_comparison.py`` and
``scripts/generate_underwater_chart.py``.

``np.where`` on a NaN comparison evaluates to False, so a MISSING index
silently produced the string "Falling" -- a confident wrong reading with no
null channel, indistinguishable from a real downtrend.

Between 2026-02-02 and 2026-07-21 that fallback emitted "Falling" on 26
consecutive weekly rows of ``msm_timeseries.csv`` while BTC dominance actually
ROSE (~54% -> ~56.3%). Root cause was upstream: a hardcoded ``TARGET_END``
in ``scripts/data_ingestion/btcdom_backfill.py`` froze the reconstructed index
at 2026-01-29, so every ``decision_date`` after that merged to NaN. The fake
"Falling" then propagated to macro_state.db, the regime logs and the
apathy_bleed 08:00 UTC Telegram snapshot.

Design rule: a derived field must not emit a plausible value when its input is
missing. Everything here returns NULL on missing input and forces the caller to
handle it. Do not re-inline these comparisons.
"""

from typing import Any

import pandas as pd

# Trend labels (string values persisted to msm_timeseries.csv / macro_state.db)
TREND_RISING = "Rising"
TREND_FALLING = "Falling"

# Human-facing rendering when the trend cannot be computed. Deliberately NOT
# "Falling" -- an unknown trend must never be displayed as a direction.
TREND_UNKNOWN_LABEL = "Unknown"

# The funding regime bucket that the Macro Regime Filter gate requires.
MRF_FUNDING_REGIME = "Q2: Weak"


def _as_series(value: Any, like: pd.Series | None = None) -> pd.Series:
    """Coerce array-like/scalar input to a Series, preserving index when possible."""
    if isinstance(value, pd.Series):
        return value
    if like is not None and not hasattr(value, "__len__"):
        return pd.Series([value] * len(like), index=like.index)
    return pd.Series(value)


def compute_btcdom_trend(index_value: Any, sma_value: Any) -> pd.Series:
    """
    BTCDOM trend label from the index level vs its 30-day SMA.

    Returns a nullable ``string``-dtype Series:
      - ``TREND_RISING``  where index > sma
      - ``TREND_FALLING`` where index <= sma
      - ``pd.NA``         where EITHER input is missing

    The third case is the whole point. A null index or a null SMA means the
    trend is unknown, not falling.
    """
    index_s = _as_series(index_value)
    sma_s = _as_series(sma_value, like=index_s)

    index_num = pd.to_numeric(index_s, errors="coerce")
    sma_num = pd.to_numeric(sma_s, errors="coerce")

    trend = pd.Series(pd.NA, index=index_num.index, dtype="string")
    both_present = index_num.notna() & sma_num.notna()
    trend[both_present] = (
        (index_num[both_present] > sma_num[both_present])
        .map({True: TREND_RISING, False: TREND_FALLING})
        .astype("string")
    )
    return trend


def compute_mrf_gate(funding_regime: Any, trend: Any) -> pd.Series:
    """
    Macro Regime Filter gate: funding regime is ``Q2: Weak`` AND trend is Rising.

    Returns a nullable ``boolean``-dtype Series. ``pd.NA`` where either input is
    missing, so "the gate was evaluated and declined" stays distinguishable from
    "the gate could not be evaluated".
    """
    regime_s = _as_series(funding_regime)
    trend_s = _as_series(trend, like=regime_s)

    # Categorical (from pd.cut) compares fine against a plain string.
    regime_ok = pd.Series(
        (regime_s.astype("object") == MRF_FUNDING_REGIME).values,
        index=regime_s.index,
        dtype="boolean",
    )
    trend_ok = pd.Series(
        (trend_s.astype("object") == TREND_RISING).values,
        index=regime_s.index,
        dtype="boolean",
    )

    unknown = regime_s.isna().values | trend_s.isna().values
    gate = regime_ok & trend_ok
    gate[unknown] = pd.NA
    return gate


def apply_gate(values: Any, gate: Any) -> pd.Series:
    """
    Apply an MRF gate to a return series without inventing a number.

      - gate True  -> the value
      - gate False -> 0.0   (flat: the gate was evaluated and declined)
      - gate NA    -> NaN   (unknown: the gate could not be evaluated)

    The old ``np.where(gate, y, 0.0)`` collapsed the last two cases into an
    identical 0.0, which is how six months of un-evaluable weeks read as
    "the strategy was flat".
    """
    values_s = pd.to_numeric(_as_series(values), errors="coerce")
    gate_s = _as_series(gate, like=values_s).astype("boolean")

    out = pd.Series(float("nan"), index=values_s.index, dtype="float64")
    known = gate_s.notna()
    on = known & gate_s.fillna(False).astype(bool)
    off = known & ~gate_s.fillna(False).astype(bool)
    out[on] = values_s[on]
    out[off] = 0.0
    return out


def trend_label(value: Any) -> str:
    """
    Render a trend for humans (dashboard, Telegram, logs).

    Any missing value -- ``None``, ``NaN``, ``pd.NA``, empty string, or the
    literal strings "None"/"nan"/"<NA>" that leak out of ``str()`` on a NULL
    read back from SQLite -- renders as ``TREND_UNKNOWN_LABEL``.
    """
    if value is None:
        return TREND_UNKNOWN_LABEL
    try:
        if pd.isna(value):
            return TREND_UNKNOWN_LABEL
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text.lower() in {"", "none", "nan", "<na>", "null"}:
        return TREND_UNKNOWN_LABEL
    return text
