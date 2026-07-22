"""
Regression tests for the 2026-02..07 BTCDOM silent-null incident.

What happened: BTCDOM_Trend was computed as a bare
``np.where(index > sma, "Rising", "Falling")``. NaN comparisons evaluate False,
so a MISSING index produced the confident string "Falling". The upstream index
had been frozen at 2026-01-29 by a hardcoded TARGET_END, so 26 consecutive
weekly rows read "Falling" while BTC dominance actually rose (~54% -> ~56.3%).
The data-quality gate did not catch it because it dropped null rows before
asserting the remainder was dense.

These tests pin the three properties that make that impossible to repeat:
  1. a null input yields a NULL trend, never a direction
  2. an un-evaluable gate yields NaN, never a flat 0.0
  3. the gate raises on interior/oversized null blocks in btcdom_7d_ret
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from majors_alts_monitor.utils.data_quality_gate import (
    MAX_TRAILING_INCOMPLETE_ROWS,
    run_gold_layer_audit,
)
from src.macro_regime.btcdom_trend import (
    TREND_FALLING,
    TREND_RISING,
    TREND_UNKNOWN_LABEL,
    apply_gate,
    compute_btcdom_trend,
    compute_mrf_gate,
    trend_label,
)


# ----------------------------------------------------------------------------
# 1. Trend must be nullable
# ----------------------------------------------------------------------------

def test_trend_is_null_when_index_missing():
    """The exact 2026-02-02 onward shape: index and sma both NaN."""
    trend = compute_btcdom_trend(
        pd.Series([4109.36, np.nan, np.nan]),
        pd.Series([4169.97, np.nan, np.nan]),
    )
    assert trend.iloc[0] == TREND_FALLING  # real comparison still works
    assert pd.isna(trend.iloc[1])
    assert pd.isna(trend.iloc[2])
    # The regression: these must NOT be the string "Falling".
    assert (trend.iloc[1:] == TREND_FALLING).sum() == 0


def test_trend_is_null_when_only_sma_missing():
    """SMA warm-up rows (first 29 days of the index) must be null, not Falling."""
    trend = compute_btcdom_trend(pd.Series([2552.6, 2660.7]), pd.Series([np.nan, np.nan]))
    assert trend.isna().all()


def test_trend_rising_and_falling_are_still_correct():
    trend = compute_btcdom_trend(pd.Series([100.0, 90.0, 100.0]), pd.Series([90.0, 100.0, 100.0]))
    assert list(trend) == [TREND_RISING, TREND_FALLING, TREND_FALLING]


def test_trend_never_emits_a_direction_for_a_null_row():
    """Property check across a mixed frame: null in => null out, always."""
    idx = pd.Series([100.0, np.nan, 120.0, np.nan, 80.0])
    sma = pd.Series([90.0, 95.0, np.nan, np.nan, 90.0])
    trend = compute_btcdom_trend(idx, sma)
    missing = idx.isna() | sma.isna()
    assert trend[missing].isna().all()
    assert trend[~missing].notna().all()


# ----------------------------------------------------------------------------
# 2. Gate must distinguish "declined" from "could not evaluate"
# ----------------------------------------------------------------------------

def test_gate_is_na_when_trend_unknown():
    gate = compute_mrf_gate(
        pd.Series(["Q2: Weak", "Q2: Weak", "Q2: Weak"]),
        pd.Series([TREND_RISING, TREND_FALLING, pd.NA], dtype="string"),
    )
    assert gate.iloc[0] is True or gate.iloc[0] == True  # noqa: E712
    assert gate.iloc[1] == False  # noqa: E712
    assert pd.isna(gate.iloc[2])


def test_gate_is_na_when_funding_regime_unknown():
    gate = compute_mrf_gate(
        pd.Series([None, "Q3: Neutral"], dtype="object"),
        pd.Series([TREND_RISING, TREND_RISING], dtype="string"),
    )
    assert pd.isna(gate.iloc[0])
    assert gate.iloc[1] == False  # noqa: E712


def test_apply_gate_separates_flat_from_unknown():
    """
    The core of silent-degradation site B: np.where(gate, y, 0.0) collapsed
    "gate declined" and "gate un-evaluable" into an identical 0.0.
    """
    values = pd.Series([0.05, 0.05, 0.05])
    gate = pd.Series([True, False, pd.NA], dtype="boolean")
    out = apply_gate(values, gate)
    assert out.iloc[0] == pytest.approx(0.05)
    assert out.iloc[1] == 0.0        # evaluated and declined -> genuinely flat
    assert np.isnan(out.iloc[2])     # un-evaluable -> unknown, NOT flat


# ----------------------------------------------------------------------------
# 3. Human-facing rendering must never show a fabricated direction
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("value", [None, np.nan, pd.NA, "", "None", "nan", "<NA>", "null"])
def test_trend_label_renders_unknown_for_missing(value):
    """SQLite NULL round-trips through str() as 'None'; that must not ship."""
    assert trend_label(value) == TREND_UNKNOWN_LABEL


def test_trend_label_passes_through_real_values():
    assert trend_label(TREND_RISING) == TREND_RISING
    assert trend_label(TREND_FALLING) == TREND_FALLING


# ----------------------------------------------------------------------------
# 4. Data quality gate must fail loud on the incident shape
# ----------------------------------------------------------------------------

def _gold_frame(n: int = 40) -> pd.DataFrame:
    start = date(2025, 1, 6)  # a Monday
    decision = [start + timedelta(days=7 * i) for i in range(n)]
    return pd.DataFrame(
        {
            "decision_date": decision,
            "next_date": [d + timedelta(days=7) for d in decision],
            "F_tk_apr": np.linspace(0.01, 0.05, n),
            "y": np.linspace(-0.01, 0.01, n),
            "btcdom_7d_ret": np.linspace(-0.02, 0.02, n),
        }
    )


def test_gate_passes_on_a_clean_frame():
    run_gold_layer_audit(_gold_frame())


def test_gate_tolerates_a_bounded_trailing_incomplete_window():
    """The last Monday + the Live T-0 row legitimately have no closed window."""
    df = _gold_frame()
    df.loc[df.index[-MAX_TRAILING_INCOMPLETE_ROWS:], "btcdom_7d_ret"] = np.nan
    df.loc[df.index[-MAX_TRAILING_INCOMPLETE_ROWS:], "y"] = np.nan
    run_gold_layer_audit(df)


def test_gate_raises_on_long_stale_tail():
    """
    The actual incident: 26 trailing null rows. The OLD gate passed this
    because it dropped them before asserting.
    """
    df = _gold_frame()
    df.loc[df.index[-26:], "btcdom_7d_ret"] = np.nan
    df.loc[df.index[-26:], "y"] = np.nan
    with pytest.raises(ValueError, match="stale"):
        run_gold_layer_audit(df)


def test_gate_raises_on_interior_null_hole():
    """A gap in the middle of the series is never a 'recent window' excuse."""
    df = _gold_frame()
    df.loc[df.index[10:14], "btcdom_7d_ret"] = np.nan
    with pytest.raises(ValueError, match="NOT a trailing incomplete window"):
        run_gold_layer_audit(df)


def test_gate_raises_when_ftk_apr_has_any_null():
    """F_tk_apr is independent of BTCDOM and must be dense on every row."""
    df = _gold_frame()
    df.loc[df.index[5], "F_tk_apr"] = np.nan
    with pytest.raises(ValueError, match="F_tk_apr"):
        run_gold_layer_audit(df)


def test_gate_still_raises_when_everything_is_null():
    df = _gold_frame()
    df["btcdom_7d_ret"] = np.nan
    with pytest.raises(ValueError, match="No rows with valid btcdom_7d_ret"):
        run_gold_layer_audit(df)


# ----------------------------------------------------------------------------
# 5. Incomplete return windows must be NaN, not exactly 0.0
# ----------------------------------------------------------------------------

# The Variance Shield aborts a week (forcing Y = 0.0) when fewer than 20 alts
# have valid prices, so the fixture must carry a full-sized basket to exercise
# the incomplete-window path rather than the shield.
_ALTS = [f"ALT{i:02d}" for i in range(25)]


def _price_frame(last_day: date):
    import polars as pl

    rows = []
    day = date(2026, 7, 1)
    price = 100.0
    while day <= last_day:
        for asset in ["BTC", "ETH", *_ALTS]:
            rows.append({"asset_id": asset, "date": day, "close": price})
        day += timedelta(days=1)
        price *= 1.001
    return pl.DataFrame(rows)


def test_incomplete_window_returns_nan_not_zero():
    """
    The 2026-07-21 row: both decision_date and next_date ran past the end of the
    price data, so the unbounded as-of lookup resolved both ends to the SAME
    close and produced exactly 0.0 for every realised return.
    """
    from majors_alts_monitor.msm_funding_v0.msm_returns import compute_returns_for_week
    import polars as pl

    prices = _price_frame(date(2026, 7, 21))
    returns, reason = compute_returns_for_week(
        prices,
        pl.DataFrame(),
        _ALTS,
        ["BTC", "ETH"],
        [0.7, 0.3],
        date(2026, 7, 21),
        date(2026, 7, 28),  # past the end of the price data
    )
    assert reason is None, "the row must survive: its macro columns are still valid"
    r_alts, r_majors, r_maj_weighted, y = returns
    assert np.isnan(r_alts) and np.isnan(r_maj_weighted) and np.isnan(y)
    assert all(np.isnan(v) for v in r_majors.values())
    # The regression: none of these may be exactly 0.0.
    assert not any(v == 0.0 for v in (r_alts, r_maj_weighted, y))


def test_truncated_window_returns_nan_not_a_short_return():
    """
    The 2026-07-20 row: only next_date ran past the end, so the old code emitted
    a ~1-day move wearing a 7-day label -- plausible enough to pass any eyeball check.
    """
    from majors_alts_monitor.msm_funding_v0.msm_returns import compute_returns_for_week
    import polars as pl

    prices = _price_frame(date(2026, 7, 21))
    returns, reason = compute_returns_for_week(
        prices, pl.DataFrame(), _ALTS, ["BTC", "ETH"], [0.7, 0.3],
        date(2026, 7, 20), date(2026, 7, 27),
    )
    _, _, _, y = returns
    assert np.isnan(y)


def test_complete_window_still_computes_a_real_return():
    from majors_alts_monitor.msm_funding_v0.msm_returns import compute_returns_for_week
    import polars as pl

    prices = _price_frame(date(2026, 7, 21))
    returns, reason = compute_returns_for_week(
        prices, pl.DataFrame(), _ALTS, ["BTC", "ETH"], [0.7, 0.3],
        date(2026, 7, 7), date(2026, 7, 14),
    )
    assert reason is None
    r_alts, _, r_maj_weighted, y = returns
    assert not np.isnan(r_alts) and r_alts != 0.0
