"""Tests for the Track A three-state confirmation machine."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE.parents[2] / "btc_confirmation_lag"))

from track_a.machine import day1_events, run_machine  # noqa: E402


def _side(vals):
    return pd.Series(vals, index=pd.date_range("2022-01-01", periods=len(vals)), dtype=float)


def test_three_closes_needed_and_reset():
    s = _side([1, 1, -1, -1, 1, -1, -1, -1, -1])
    m = run_machine(s, K=3)
    assert list(m["R"].iloc[:6]) == [1, 1, 1, 1, 1, 1]      # the 2-close dip and 1-close poke reset
    assert m["R"].iloc[7] == -1 and m["flip"].iloc[7] == 1
    assert m["w"].iloc[7] == 0.0                              # long/flat: short state = flat


def test_neutral_state_in_three_state_mode():
    s = _side([1, 1, 0, 0, 0, 1])
    m = run_machine(s, K=3, exposure="long_short")
    assert m["R"].iloc[4] == 0 and m["w"].iloc[4] == 0.0


def test_two_state_ll_grey_only_resets():
    s = _side([1, 1, -1, 0, -1, -1, -1])
    m = run_machine(s, K=3, neutral_flips=False)
    assert m["R"].iloc[4] == 1                                # grey at t=3 reset the blue count
    assert m["R"].iloc[6] == -1


def test_staging_and_unwind():
    s = _side([1, 1, -1, -1, 1, 1])
    m = run_machine(s, K=3, schedule_fn=lambda t, R, x: (1 / 3, 2 / 3), exposure="long_short")
    assert np.isclose(m["w"].iloc[2], 1 - 2 / 3)              # 1/3 of the way from +1 to -1
    assert np.isclose(m["w"].iloc[3], 1 - 4 / 3)
    assert m["w"].iloc[4] == 1.0                              # reset -> unwind to regime


def test_matches_existing_two_state_engine():
    """For a +1/-1 SMA side the machine must reproduce btc_confirmation_lag.signals.run_rule exactly."""
    from signals import run_rule
    rng = np.random.default_rng(3)
    c = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.03, 1500))),
                  index=pd.date_range("2018-01-01", periods=1500))
    sma = c.rolling(120).mean()
    side = pd.Series(np.where(c > sma, 1.0, -1.0), index=c.index)
    side[sma.isna()] = np.nan
    old = run_rule(pd.DataFrame({"side": side.fillna(0)}), K=3)["w"]
    new = run_machine(side, K=3)["w"]
    mask = old.notna() & new.notna()
    assert (old[mask] == new[mask]).all()


def test_day1_event_labels():
    s = _side([1, 1, 1, -1, -1, 1, -1, -1, -1, -1, -1, -1])
    m = run_machine(s, K=3)
    ev = day1_events(s, m, K=3, valid_horizons=(3,))
    assert list(ev["confirmed"]) == [0, 1]
    assert ev["reversed_early"].iloc[0] == 1
    assert ev["kind"].iloc[1] == "to_short" and ev["direction"].iloc[1] == -1
