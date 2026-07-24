"""
Regression test for the BTCDOM reconstruction NaN-price crash (2026-07-24).

Once the frozen TARGET_END was lifted and the reconstruction extended into the
2026-01-29+ window for the first time, `_build_rebalance_params` read a NaN close
for a top-20 constituent, turned it into a NaN rebalance price, and the clamp
`max(lb, min(ub, p_raw))` raised `decimal.InvalidOperation` (index_calculator.py).

These tests pin the fix:
  1. a basket with a NaN-priced top constituent reconstructs without raising,
  2. the output contains no NaN index values,
  3. an all-valid basket is UNCHANGED (so historical dates stay byte-identical),
  4. a basket with too few valid prices fails loud rather than degenerating.
"""

import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# index_calculator.py imports its siblings by bare module name.
ICALC_DIR = Path(__file__).parent.parent / "scripts" / "data_ingestion"
sys.path.insert(0, str(ICALC_DIR))

from index_calculator import (  # noqa: E402
    IndexCalculator,
    MIN_BASKET_N,
    TARGET_BASKET_N,
    _to_decimal_opt,
)


class _StubLoader:
    """Minimal DataLoader stand-in: a fixed universe with injectable price gaps."""

    def __init__(self, n_assets: int, nan_assets: set[str], start: date, end: date):
        self.start, self.end = start, end
        self.asset_ids = ["BTC"] + [f"ALT{i:02d}" for i in range(n_assets)]
        self.nan_assets = nan_assets
        # marketcap descending so ALT00 is the largest alt, ALT01 next, ...
        self._mc = {aid: float(10_000 - i) for i, aid in enumerate(self.asset_ids)}

    def get_btc_asset_ids(self):
        return {"BTC"}

    def iter_days(self, start: date, end: date):
        days, cur = [], start
        while cur <= end:
            days.append(cur)
            cur += timedelta(days=1)
        return days

    def get_eligible_universe_on_date(self, d_val: date) -> pd.DataFrame:
        alts = [a for a in self.asset_ids if a != "BTC"]
        return pd.DataFrame(
            {"asset_id": alts, "marketcap": [self._mc[a] for a in alts]}
        )

    def get_prices(self, asset_ids, start: date, end: date) -> pd.DataFrame:
        rows = []
        day = start
        while day <= end:
            for aid in asset_ids:
                if aid in self.nan_assets:
                    close = np.nan
                else:
                    # deterministic, positive, asset-specific
                    close = 100.0 + self._mc.get(aid, 1.0) / 100.0
                rows.append({"asset_id": aid, "date": day, "close": close})
            day += timedelta(days=1)
        return pd.DataFrame(rows)


def _calc(loader) -> IndexCalculator:
    return IndexCalculator(
        data_loader=loader,
        base_index_level=Decimal("2448.02529635"),
        delta=Decimal("0.3"),
        max_ffill_days=3,
    )


def _run(loader, start: date, end: date) -> pd.DataFrame:
    rebs = [start + timedelta(days=7 * i) for i in range((end - start).days // 7 + 1)]
    return _calc(loader).backfill(start_date=start, end_date=end, rebalance_dates=rebs)


# ---------------------------------------------------------------------------
# 1 + 2. NaN constituent no longer crashes, and output is NaN-free
# ---------------------------------------------------------------------------

def test_nan_priced_top_constituent_does_not_crash():
    start, end = date(2026, 1, 22), date(2026, 2, 19)  # Thursdays
    # ALT00 is the single largest alt; give it a NaN close -> used to crash.
    loader = _StubLoader(n_assets=40, nan_assets={"ALT00"}, start=start, end=end)
    df = _run(loader, start, end)
    assert not df.empty
    vals = df["reconstructed_index_value"].astype(float)
    assert vals.notna().all(), "reconstruction must not emit NaN index values"
    assert (vals > 0).all()


def test_multiple_nan_constituents_refill_and_survive():
    start, end = date(2026, 1, 22), date(2026, 2, 19)
    loader = _StubLoader(
        n_assets=40, nan_assets={"ALT00", "ALT01", "ALT05"}, start=start, end=end
    )
    df = _run(loader, start, end)
    assert df["reconstructed_index_value"].astype(float).notna().all()


# ---------------------------------------------------------------------------
# 3. All-valid basket is unchanged (historical seam preserved)
# ---------------------------------------------------------------------------

def test_all_valid_basket_matches_baseline():
    start, end = date(2026, 1, 22), date(2026, 2, 19)
    clean = _StubLoader(n_assets=40, nan_assets=set(), start=start, end=end)
    baseline = _run(clean, start, end).reset_index(drop=True)

    # Same universe, but with extra low-cap names carrying NaN prices that should
    # never have been in the top-20 anyway -> must not perturb the result.
    noisy = _StubLoader(
        n_assets=40, nan_assets={"ALT30", "ALT39"}, start=start, end=end
    )
    got = _run(noisy, start, end).reset_index(drop=True)

    pd.testing.assert_frame_equal(baseline, got)


# ---------------------------------------------------------------------------
# 4. Too few valid prices -> fail loud, not a degenerate basket
# ---------------------------------------------------------------------------

def test_degenerate_basket_fails_loud():
    start, end = date(2026, 1, 22), date(2026, 1, 22)
    # Only enough candidates that fewer than MIN_BASKET_N have valid prices.
    n = TARGET_BASKET_N * 2
    bad = {f"ALT{i:02d}" for i in range(n - (MIN_BASKET_N - 1))}
    loader = _StubLoader(n_assets=n, nan_assets=bad, start=start, end=end)
    with pytest.raises(ValueError, match="degenerate basket"):
        _run(loader, start, end)


# ---------------------------------------------------------------------------
# 5. The NaN-safe conversion helper itself
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value",
    [None, np.nan, float("nan"), float("inf"), float("-inf"), "not-a-number"],
)
def test_to_decimal_opt_rejects_non_numbers(value):
    assert _to_decimal_opt(value) is None


@pytest.mark.parametrize("value,expected", [(0, "0"), (123.5, "123.5"), (Decimal("7"), "7")])
def test_to_decimal_opt_accepts_finite(value, expected):
    assert _to_decimal_opt(value) == Decimal(expected)
