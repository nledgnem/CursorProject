"""Unit tests for turnover accounting in backtest engine."""

import pytest
import pandas as pd
from datetime import date
from src.backtest.engine import compute_turnover


def test_turnover_identical_baskets():
    """Test that identical baskets produce zero turnover and zero entered/exited."""
    # Create two identical weight dictionaries
    weights_1 = {
        "BTC": 0.5,
        "ETH": 0.3,
        "BNB": 0.2,
    }
    weights_2 = {
        "BTC": 0.5,
        "ETH": 0.3,
        "BNB": 0.2,
    }
    
    # Compute turnover
    turnover = compute_turnover(
        pd.Series(weights_1),
        pd.Series(weights_2)
    )
    
    # Turnover should be zero for identical baskets
    assert abs(turnover) < 1e-10, f"Expected zero turnover, got {turnover}"
    
    # Check entered/exited counts
    symbols_1 = set(weights_1.keys())
    symbols_2 = set(weights_2.keys())
    entered = len(symbols_2 - symbols_1)
    exited = len(symbols_1 - symbols_2)
    
    assert entered == 0, f"Expected 0 entered, got {entered}"
    assert exited == 0, f"Expected 0 exited, got {exited}"


def test_turnover_different_baskets():
    """Test that different baskets produce non-zero turnover and correct entered/exited."""
    # Create two different weight dictionaries
    weights_1 = {
        "BTC": 0.5,
        "ETH": 0.3,
        "BNB": 0.2,
    }
    weights_2 = {
        "BTC": 0.4,
        "ETH": 0.3,
        "SOL": 0.3,  # BNB exited, SOL entered
    }
    
    # Compute turnover
    turnover = compute_turnover(
        pd.Series(weights_1),
        pd.Series(weights_2)
    )
    
    # Turnover should be non-zero for different baskets
    assert turnover > 0, f"Expected non-zero turnover, got {turnover}"
    
    # Check entered/exited counts
    symbols_1 = set(weights_1.keys())
    symbols_2 = set(weights_2.keys())
    entered = len(symbols_2 - symbols_1)
    exited = len(symbols_1 - symbols_2)
    
    assert entered == 1, f"Expected 1 entered (SOL), got {entered}"
    assert exited == 1, f"Expected 1 exited (BNB), got {exited}"
    assert "SOL" in symbols_2 - symbols_1, "SOL should be entered"
    assert "BNB" in symbols_1 - symbols_2, "BNB should be exited"


def test_turnover_weight_changes_same_assets():
    """Test that weight changes in same assets produce non-zero turnover."""
    # Same assets, different weights
    weights_1 = {
        "BTC": 0.5,
        "ETH": 0.5,
    }
    weights_2 = {
        "BTC": 0.6,
        "ETH": 0.4,
    }
    
    # Compute turnover
    turnover = compute_turnover(
        pd.Series(weights_1),
        pd.Series(weights_2)
    )
    
    # Turnover should be non-zero due to weight changes
    assert turnover > 0, f"Expected non-zero turnover due to weight changes, got {turnover}"
    
    # Check entered/exited counts (should be zero since same assets)
    symbols_1 = set(weights_1.keys())
    symbols_2 = set(weights_2.keys())
    entered = len(symbols_2 - symbols_1)
    exited = len(symbols_1 - symbols_2)
    
    assert entered == 0, f"Expected 0 entered (same assets), got {entered}"
    assert exited == 0, f"Expected 0 exited (same assets), got {exited}"


def test_turnover_first_rebalance():
    """Test that first rebalance (empty previous basket) produces full turnover."""
    # Empty previous basket
    weights_1 = {}
    # New basket
    weights_2 = {
        "BTC": 0.5,
        "ETH": 0.3,
        "BNB": 0.2,
    }
    
    # Compute turnover
    turnover = compute_turnover(
        pd.Series(weights_1),
        pd.Series(weights_2)
    )
    
    # Turnover should be 1.0 (100%) for first rebalance
    assert abs(turnover - 1.0) < 1e-10, f"Expected 1.0 turnover for first rebalance, got {turnover}"
    
    # Check entered/exited counts
    symbols_1 = set(weights_1.keys())
    symbols_2 = set(weights_2.keys())
    entered = len(symbols_2 - symbols_1)
    exited = len(symbols_1 - symbols_2)
    
    assert entered == 3, f"Expected 3 entered, got {entered}"
    assert exited == 0, f"Expected 0 exited, got {exited}"

