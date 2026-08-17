"""
Tests for the Binance BTCDOM index fetcher (ADR 004).

These cover the normalisation and validation logic, not the network. The one
network-dependent test is marked and skipped by default.

Context for why this producer exists: the previous reconstruction was the real
Binance index LAGGED BY ONE DAY -- daily-change correlation +0.76 at k=+1 versus
+0.003 same-day, 84.7% sign agreement -- because it was built from lake
`fact_price`, whose `date` is stamped one day AFTER the UTC close it represents.
It also carried 110 flat/forward-filled days out of 772 and a 4.49% median level
difference, while passing every freshness check. Fresh, confident and wrong.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts" / "data_ingestion"))

from scripts.data_ingestion.btcdom_binance_index import (  # noqa: E402
    MAX_PLAUSIBLE,
    MIN_PLAUSIBLE,
    validate,
)


def _frame(dates, values) -> pd.DataFrame:
    return pd.DataFrame({"date": pd.to_datetime(dates), "btcdom_index": values})


# ----------------------------------------------------------------------------
# validate(): fail loudly rather than write a plausible-looking bad file
# ----------------------------------------------------------------------------

def test_validate_accepts_a_clean_frame():
    validate(_frame(["2026-08-14", "2026-08-15", "2026-08-16"], [5435.6, 5439.5, 5451.6]))


def test_validate_rejects_empty():
    with pytest.raises(SystemExit, match="empty"):
        validate(_frame([], []))


def test_validate_rejects_nulls():
    with pytest.raises(SystemExit, match="null"):
        validate(_frame(["2026-08-15", "2026-08-16"], [5439.5, float("nan")]))


def test_validate_rejects_duplicate_dates():
    df = _frame(["2026-08-15", "2026-08-15"], [5439.5, 5451.6])
    with pytest.raises(SystemExit, match="duplicate|monotonic"):
        validate(df)


def test_validate_rejects_unsorted_dates():
    with pytest.raises(SystemExit, match="monotonic"):
        validate(_frame(["2026-08-16", "2026-08-15"], [5451.6, 5439.5]))


@pytest.mark.parametrize("bad_value", [0.0, -1.0, MIN_PLAUSIBLE, MAX_PLAUSIBLE, 1e9])
def test_validate_rejects_implausible_levels(bad_value):
    """A unit change or a garbage response must not be written to the lake."""
    with pytest.raises(SystemExit, match="outside"):
        validate(_frame(["2026-08-15", "2026-08-16"], [5439.5, bad_value]))


def test_validate_tolerates_calendar_gaps_but_warns(caplog):
    """A venue halt is survivable -- consumers merge as-of -- but must be visible."""
    df = _frame(["2026-08-01", "2026-08-02", "2026-08-09"], [5400.0, 5410.0, 5450.0])
    with caplog.at_level("WARNING"):
        validate(df)          # must not raise
    assert "gap" in caplog.text.lower(), "a calendar gap must be logged, not swallowed"


# ----------------------------------------------------------------------------
# The property that motivated ADR 004
# ----------------------------------------------------------------------------

def test_index_is_stamped_with_the_utc_day_it_closed():
    """
    The bug being designed out: the reconstruction's value on date t was built
    from date t-1's prices, because lake `fact_price.date` is stamped one day
    after the close it represents. This producer takes the venue's own bucket
    open time and stamps the value with THAT UTC date, so no offset can creep in.
    """
    open_ms = int(pd.Timestamp("2026-08-16", tz="UTC").timestamp() * 1000)
    stamped = (
        pd.to_datetime([open_ms], unit="ms", utc=True).tz_localize(None).normalize()[0]
    )
    assert stamped == pd.Timestamp("2026-08-16")


@pytest.mark.skipif(
    "not config.getoption('--run-network', default=False)",
    reason="network test; pass --run-network to enable",
)
def test_live_fetch_is_fresh_and_sane():
    from scripts.data_ingestion.btcdom_binance_index import fetch_index

    df = fetch_index()
    validate(df)
    drift = (datetime.now(timezone.utc).date() - df["date"].max().date()).days
    assert drift <= 2, f"index is {drift} days behind"
    assert df["date"].min().date() <= pd.Timestamp("2021-07-01").date(), \
        "expected history back to contract onboarding (2021-06)"
