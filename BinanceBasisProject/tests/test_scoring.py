"""
Unit tests for scoring module with synthetic data.
"""

import numpy as np
import pandas as pd
import pytest

from scoring import quality_score, rank_by_apr, rank_by_quality


@pytest.fixture
def synthetic_df() -> pd.DataFrame:
    """Synthetic metrics for 3 symbols."""
    return pd.DataFrame({
        "symbol": ["A", "B", "C"],
        "apr_simple": [50.0, 20.0, -10.0],
        "neg_frac": [0.1, 0.3, 0.8],
        "stdev": [0.0001, 0.0002, 0.0005],
        "top10_share": [0.3, 0.5, 0.9],
    })


def test_rank_by_apr(synthetic_df: pd.DataFrame) -> None:
    """Highest APR first."""
    ranked = rank_by_apr(synthetic_df)
    assert ranked["symbol"].iloc[0] == "A"
    assert ranked["symbol"].iloc[1] == "B"
    assert ranked["symbol"].iloc[2] == "C"
    assert ranked["rank_apr"].iloc[0] == 1


def test_rank_by_quality(synthetic_df: pd.DataFrame) -> None:
    """Quality penalizes neg_frac, stdev, top10_share."""
    ranked = rank_by_quality(synthetic_df)
    assert "quality_score" in ranked.columns
    assert "rank_quality" in ranked.columns
    # A has best APR and low penalties -> should rank high
    assert ranked["symbol"].iloc[0] == "A"


def test_quality_score_penalizes_neg_frac() -> None:
    """Higher neg_frac -> lower quality."""
    df = pd.DataFrame({
        "apr_simple": [10.0, 10.0],
        "neg_frac": [0.1, 0.9],
        "stdev": [0.0001, 0.0001],
        "top10_share": [0.3, 0.3],
    })
    s = quality_score(df, w_neg_frac=1.0, w_stdev=0, w_top10_share=0)
    assert s.iloc[0] > s.iloc[1]
