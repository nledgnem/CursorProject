"""
Unit tests for metrics module with synthetic funding data.
"""

import numpy as np
import pandas as pd
import pytest

from metrics import compute_metrics, compute_metrics_for_windows


def test_compute_metrics_basic() -> None:
    """Simple synthetic: 4 prints of 0.0001 each over 7 days."""
    rates = pd.Series([0.0001, 0.0001, 0.0001, 0.0001])
    m = compute_metrics(rates, 7.0)
    assert m["funding_return"] == pytest.approx(0.0004)
    assert m["apr_simple"] == pytest.approx(0.0004 / 7 * 365, rel=1e-5)
    assert m["pos_frac"] == 1.0
    assert m["neg_frac"] == 0.0
    assert m["zero_frac"] == 0.0
    assert m["stdev"] == 0.0
    assert m["n_prints"] == 4


def test_compute_metrics_mixed() -> None:
    """Mixed positive/negative: sum = 0.0002 - 0.0001 = 0.0001."""
    rates = pd.Series([0.0001, -0.0001, 0.0001, 0.0001])
    m = compute_metrics(rates, 7.0)
    assert m["funding_return"] == pytest.approx(0.0002)
    assert m["pos_frac"] == 0.75
    assert m["neg_frac"] == 0.25
    assert m["zero_frac"] == 0.0


def test_compute_metrics_stdev() -> None:
    """Non-zero stdev."""
    rates = pd.Series([0.0002, 0.0000, -0.0002])
    m = compute_metrics(rates, 7.0)
    assert m["stdev"] > 0


def test_compute_metrics_empty() -> None:
    """Empty series returns NaNs."""
    m = compute_metrics(pd.Series(dtype=float), 7.0)
    assert np.isnan(m["funding_return"])
    assert np.isnan(m["apr_simple"])
    assert m["n_prints"] == 0


def test_compute_metrics_top10_share() -> None:
    """Top 10 positive share: all positive equal -> top10_share = 1 if n<=10."""
    rates = pd.Series([0.0001] * 5)  # 5 equal positive
    m = compute_metrics(rates, 7.0)
    assert m["top10_share"] == pytest.approx(1.0)


def test_compute_metrics_max_drawdown() -> None:
    """Cumulative: 1, 0, -1 -> max drawdown from peak 1 to trough -1 = 2."""
    rates = pd.Series([1.0, -1.0, -1.0])
    m = compute_metrics(rates, 7.0)
    cum = np.cumsum([1.0, -1.0, -1.0])
    peak = np.maximum.accumulate(cum)
    dd = peak - cum
    assert m["max_drawdown"] == pytest.approx(np.max(dd))


def test_compute_metrics_for_windows() -> None:
    """Synthetic DataFrame with fundingTime and fundingRate."""
    n = 100
    ts = pd.date_range("2024-01-01", periods=n, freq="8h")
    df = pd.DataFrame({
        "fundingTime": (ts.astype("int64") // 1_000_000).tolist(),
        "fundingRate": [0.0001] * n,
    })
    result = compute_metrics_for_windows(df, [7, 14])
    assert len(result) == 2
    assert 7 in result["window_days"].values
    assert 14 in result["window_days"].values
    assert all(result["apr_simple"] > 0)
