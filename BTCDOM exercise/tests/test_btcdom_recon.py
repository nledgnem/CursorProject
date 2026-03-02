"""
Unit tests: weight summation, reconstruction math on toy data, missing data handling.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from btcdom_recon.config import WEIGHTS, WEIGHTS_SUM, get_weights_sum, MissingDataMode
from btcdom_recon.reconstruct import reconstruct_btcdom


class TestWeights:
    """Weight summation and sanity."""

    def test_weights_sum_close_to_one(self) -> None:
        s = get_weights_sum()
        assert abs(s - 1.0) < 0.01, f"Weights should sum to ~1.0, got {s}"

    def test_weights_all_positive(self) -> None:
        for sym, w in WEIGHTS.items():
            assert w > 0, f"Weight for {sym} should be positive, got {w}"

    def test_weights_twenty_constituents(self) -> None:
        assert len(WEIGHTS) == 20, "Binance BTCDOM has 20 constituents"


class TestReconstructionMath:
    """Basic reconstruction formula on toy data."""

    @pytest.fixture
    def toy_daily_data(self) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """One day: BTC=100, ETH=2 -> BTC/ETH=50; single constituent weight 1.0 -> BTCDOM=50."""
        ts = pd.date_range("2024-01-01", periods=3, freq="1D")
        btc = pd.DataFrame({"timestamp": ts, "price_usd": [100.0, 101.0, 102.0]})
        # One constituent only for simple math: ETH with weight 1.0 would give btc/eth
        eth = pd.DataFrame({"timestamp": ts, "price_usd": [2.0, 2.02, 2.04]})
        # We use full WEIGHTS in reconstruct; for a minimal test we need all constituents
        # So use same ratio for all: btc/coin = 50, 50, 50 -> weighted sum = 50 * sum(weights) = 50
        constituent_dfs = {}
        for sym in WEIGHTS:
            # price such that btc/price = 50 for first day
            constituent_dfs[sym] = pd.DataFrame({
                "timestamp": ts,
                "price_usd": [100.0 / 50.0] * 3,
            })
        return btc, constituent_dfs

    def test_reconstruct_toy_btcdom_level(self, toy_daily_data: tuple) -> None:
        btc_df, constituent_dfs = toy_daily_data
        out = reconstruct_btcdom(
            "2024-01-01",
            "2024-01-03",
            "1D",
            use_quantity_weights=False,
            btc_df=btc_df,
            constituent_dfs=constituent_dfs,
        )
        assert len(out) == 3
        # btc_in_coin = btc_usd/2 for each row; all constituents same ratio; BTCDOM = ratio * sum(weights)
        assert out["n_constituents_used"].iloc[0] == len(WEIGHTS)
        # btc 100,101,102 and coin 2,2,2 -> 50, 50.5, 51
        np.testing.assert_allclose(out["btcdom_recon"].values, [50.0, 50.5, 51.0], rtol=1e-5)

    def test_reconstruct_btc_in_coin_formula(self) -> None:
        """Spot-check: BTCDOM(t) = sum_i weight_i * (BTCUSD(t) / COIN_i_USD(t))."""
        ts = pd.date_range("2024-01-01", periods=1, freq="1D")
        btc_df = pd.DataFrame({"timestamp": ts, "price_usd": [100.0]})
        # ETH=2 -> 50, BNB=20 -> 5; others -> 50 (price 2.0)
        constituent_dfs: dict[str, pd.DataFrame] = {}
        for sym in WEIGHTS:
            if sym == "ETH":
                constituent_dfs[sym] = pd.DataFrame({"timestamp": ts, "price_usd": [2.0]})
            elif sym == "BNB":
                constituent_dfs[sym] = pd.DataFrame({"timestamp": ts, "price_usd": [20.0]})
            else:
                constituent_dfs[sym] = pd.DataFrame({"timestamp": ts, "price_usd": [2.0]})
        out = reconstruct_btcdom(
            "2024-01-01",
            "2024-01-01",
            "1D",
            use_quantity_weights=False,
            btc_df=btc_df,
            constituent_dfs=constituent_dfs,
        )
        assert len(out) == 1
        expected = WEIGHTS["ETH"] * (100 / 2) + WEIGHTS["BNB"] * (100 / 20)
        for sym in WEIGHTS:
            if sym not in ("ETH", "BNB"):
                expected += WEIGHTS[sym] * (100 / 2.0)
        np.testing.assert_allclose(out["btcdom_recon"].iloc[0], expected, rtol=1e-5)


class TestMissingDataModes:
    """Missing data: renormalize vs drop behavior."""

    def test_renormalize_when_constituent_missing(self) -> None:
        ts = pd.date_range("2024-01-01", periods=2, freq="1D")
        btc_df = pd.DataFrame({"timestamp": ts, "price_usd": [100.0, 100.0]})
        # Only ETH (price 2 -> 50) and BNB (price 20 -> 5) present; others missing -> renormalize
        constituent_dfs = {}
        for sym in WEIGHTS:
            if sym == "ETH":
                constituent_dfs[sym] = pd.DataFrame({"timestamp": ts, "price_usd": [2.0, 2.0]})
            elif sym == "BNB":
                constituent_dfs[sym] = pd.DataFrame({"timestamp": ts, "price_usd": [20.0, 20.0]})
            else:
                constituent_dfs[sym] = pd.DataFrame(columns=["timestamp", "price_usd"])
        out = reconstruct_btcdom(
            "2024-01-01",
            "2024-01-02",
            "1D",
            missing_mode=MissingDataMode.RENORMALIZE,
            use_quantity_weights=False,
            btc_df=btc_df,
            constituent_dfs=constituent_dfs,
        )
        assert len(out) == 2
        assert out["n_constituents_used"].iloc[0] == 2
        assert bool(out["weights_renormalized_flag"].iloc[0]) is True
        # BTCDOM = (w_eth_norm * 50 + w_bnb_norm * 5) with w_eth_norm + w_bnb_norm = 1
        w_eth = WEIGHTS["ETH"] / (WEIGHTS["ETH"] + WEIGHTS["BNB"])
        w_bnb = WEIGHTS["BNB"] / (WEIGHTS["ETH"] + WEIGHTS["BNB"])
        expected = w_eth * 50.0 + w_bnb * 5.0
        np.testing.assert_allclose(out["btcdom_recon"].iloc[0], expected, rtol=1e-5)

    def test_drop_row_when_no_btc(self) -> None:
        ts = pd.date_range("2024-01-01", periods=2, freq="1D")
        btc_df = pd.DataFrame({"timestamp": ts, "price_usd": [np.nan, 100.0]})  # first row missing BTC
        constituent_dfs = {
            sym: pd.DataFrame({"timestamp": ts, "price_usd": [1.0, 1.0]})
            for sym in WEIGHTS
        }
        out = reconstruct_btcdom(
            "2024-01-01",
            "2024-01-02",
            "1D",
            use_quantity_weights=False,
            btc_df=btc_df,
            constituent_dfs=constituent_dfs,
        )
        # First timestamp dropped (no BTC), second kept
        assert len(out) == 1
        assert out["timestamp"].iloc[0] == ts[1]
