from __future__ import annotations

"""
Canonical BTCDOM trend + MRF gate logic (single source of truth).

Exports
-------
- ``compute_btcdom_trend(index, sma)``  -> nullable "Rising"/"Falling"
- ``compute_mrf_gate(funding_regime)`` -> nullable boolean
  (BTCDOM_Trend was removed from this gate on 2026-08-17 -- ADR 003)
- ``apply_gate(values, gate)`` -> gated series that preserves "unknown"
- ``is_missing(value)`` -> True for None / NaN / pd.NA / "None" / "nan" / "<NA>"
- ``trend_label(value)`` -> human-facing string, never "None"/"nan"
- ``gate_label(value)`` -> "GATE:ON" / "GATE:OFF" / "GATE:UNKNOWN", never 2-state
- ``format_regime_label(row)`` -> the canonical "funding | btcdom | gate" string

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

import numpy as np
import pandas as pd

# Trend labels (string values persisted to msm_timeseries.csv / macro_state.db)
TREND_RISING = "Rising"
TREND_FALLING = "Falling"

# Human-facing rendering when the trend cannot be computed. Deliberately NOT
# "Falling" -- an unknown trend must never be displayed as a direction.
TREND_UNKNOWN_LABEL = "Unknown"

# Human-facing rendering of the MRF gate. THREE states: an un-evaluable gate is
# neither open nor closed, and must not be displayed as either.
GATE_ON_LABEL = "GATE:ON"
GATE_OFF_LABEL = "GATE:OFF"
GATE_UNKNOWN_LABEL = "GATE:UNKNOWN"

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


def compute_mrf_gate(funding_regime: Any) -> pd.Series:
    """
    Macro Regime Filter gate: funding regime is ``Q2: Weak``.

    Returns a nullable ``boolean``-dtype Series. ``pd.NA`` where the funding
    regime is missing, so "the gate was evaluated and declined" stays
    distinguishable from "the gate could not be evaluated".

    BTCDOM_Trend REMOVED FROM THIS GATE -- 2026-08-17, see ADR 003
    --------------------------------------------------------------
    The gate used to be ``funding_regime == "Q2: Weak" AND trend == "Rising"``.
    The BTCDOM condition was removed because it did not survive testing:

      * Out of sample (2022+) the premise INVERTS. Conditioning on a validated
        dominance measure, "dominance rising" was followed by BTC-minus-alts
        20-day returns of -1.63pp, versus +7.31pp (HAC t=2.05) pre-2022. The
        gate assumed the opposite sign to the one the recent data shows.
      * It was not independent information. "BTC dominance is rising" and
        "long-majors/short-alts is working" are near-restatements of each
        other -- it is momentum on the gated book's own P&L. A plain trailing
        30-day relative-momentum flag gives the same reading on 84.9% of days.
      * The production series (2024-07 onward) was never long enough to detect
        either problem.

    Full evidence: research/btc_trend_agreement/btcdom_value.py and
    results/tables/36_btcdom_trend_value_add.csv.

    The signature deliberately DROPPED the ``trend`` parameter rather than
    accepting and ignoring it. A silently-ignored argument is the same class of
    defect as the silent-null incident this module exists to prevent: every
    caller must be updated consciously, and a stale two-argument call fails
    loudly with a TypeError instead of quietly changing meaning.

    BTCDOM_Trend itself is still computed and displayed -- it is now a
    context field, not a gating input. See compute_btcdom_trend.
    """
    regime_s = _as_series(funding_regime)

    # Categorical (from pd.cut) compares fine against a plain string.
    gate = pd.Series(
        (regime_s.astype("object") == MRF_FUNDING_REGIME).values,
        index=regime_s.index,
        dtype="boolean",
    )
    gate[regime_s.isna().values] = pd.NA
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


def is_missing(value: Any) -> bool:
    """
    True for every representation of "no value" that reaches the display layer.

    The same logical NULL arrives in a different shape depending on where it was
    read from, and those shapes are NOT interchangeable in Python:

        SQLite NULL  -> None          bool(None)  is False
        CSV/pandas   -> float("nan")  bool(nan)   is True   <-- the trap
        nullable col -> pd.NA         bool(pd.NA) raises
        str() of any -> "None" / "nan" / "<NA>"

    Any code that branches on a raw value therefore gets a *different answer for
    the same missing data* depending on the read path. Route every human-facing
    render through this predicate instead.
    """
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return str(value).strip().lower() in {"", "none", "nan", "<na>", "null"}


def trend_label(value: Any) -> str:
    """
    Render a trend for humans (dashboard, Telegram, logs).

    Any missing value -- ``None``, ``NaN``, ``pd.NA``, empty string, or the
    literal strings "None"/"nan"/"<NA>" that leak out of ``str()`` on a NULL
    read back from SQLite -- renders as ``TREND_UNKNOWN_LABEL``.
    """
    if is_missing(value):
        return TREND_UNKNOWN_LABEL
    return str(value).strip()


def gate_label(value: Any) -> str:
    """
    Render the MRF gate for humans. THREE states, never two.

    ``is_mrf_active`` is a NULLABLE boolean: ``pd.NA`` means "could not be
    evaluated", which is not the same as ``False`` ("evaluated and declined").
    Collapsing those two into "GATE:OFF" throws away the distinction the whole
    nullable-gate design exists to preserve -- and collapsing them the other way
    is worse.

    The bug this replaces (live until 2026-08-17)::

        gate_on = bool(int(gate)) if ... else bool(gate)   # bool(nan) is True!

    A gate read from CSV as ``NaN`` rendered as **GATE:ON** -- an un-evaluable
    risk-on gate displayed as open. The same NULL read from SQLite arrived as
    ``None`` and rendered GATE:OFF, so the daily job also emitted phantom
    "REGIME CHANGE DETECTED" alerts on days when nothing had changed: ``prev``
    came from SQLite and the new row from the CSV frame.
    """
    if is_missing(value):
        return GATE_UNKNOWN_LABEL
    if isinstance(value, (bool, np.bool_)):
        return GATE_ON_LABEL if value else GATE_OFF_LABEL
    if isinstance(value, (int, float, np.integer, np.floating)):
        return GATE_ON_LABEL if value != 0 else GATE_OFF_LABEL
    text = str(value).strip().lower()
    if text in {"true", "1", "1.0", "yes", "y", "on"}:
        return GATE_ON_LABEL
    if text in {"false", "0", "0.0", "no", "n", "off"}:
        return GATE_OFF_LABEL
    # Anything else is unrecognised, which is a form of not-known.
    return GATE_UNKNOWN_LABEL


def format_regime_label(row: Any) -> str:
    """
    The canonical ``funding | btcdom | gate`` regime string.

    Single source of truth for the Telegram snapshot, the regime-change alert
    and the dashboard. This logic was previously duplicated verbatim in
    ``src/apathy_bleed/macro_snapshot.py`` and ``scripts/live/live_data_fetcher.py``,
    which is how the same rendering bug shipped in two places at once.

    Every component is rendered through a null-safe labeller, so a given logical
    NULL produces an identical string regardless of whether the row was read
    from SQLite (``None``) or from a pandas frame (``NaN``). That property is
    what makes regime-change comparison trustworthy: an alert now fires only
    when the regime actually changed, not when the read path did.
    """
    get = row.get if hasattr(row, "get") else (lambda k, d=None: getattr(row, k, d))
    funding_raw = get("funding_regime", None)
    funding = "Unknown" if is_missing(funding_raw) else str(funding_raw).strip()
    btcd = trend_label(get("BTCDOM_Trend", None))
    gate = gate_label(get("is_mrf_active", None))
    return f"{funding} | {btcd} | {gate}"
