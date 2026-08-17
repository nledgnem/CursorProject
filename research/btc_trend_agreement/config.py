"""
Central configuration for the BTC trend-agreement research study.

Every tunable lives here so the whole study is reproducible from one place.
Nothing in this file is fitted to data -- all values are either specified by
Coinbase's published description or pre-specified by us before looking at
results.
"""

from __future__ import annotations

from pathlib import Path

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
RESEARCH_DIR = Path(__file__).resolve().parent
CACHE_DIR = RESEARCH_DIR / "cache"
RESULTS_DIR = RESEARCH_DIR / "results"
TABLE_DIR = RESULTS_DIR / "tables"
FIGURE_DIR = RESULTS_DIR / "figures"

for _d in (CACHE_DIR, RESULTS_DIR, TABLE_DIR, FIGURE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------
# Sample window
# --------------------------------------------------------------------------
# Coinbase Exchange BTC-USD spot begins 2015-07-20. We request from 2015-07-01
# and take whatever the venue actually returns.
HISTORY_START = "2015-07-01"

# --------------------------------------------------------------------------
# Core Coinbase-specified signal parameters (PRE-SPECIFIED, not fitted)
# --------------------------------------------------------------------------
CORE_LOOKBACKS = (30, 90, 365)

FORWARD_HORIZONS = (1, 5, 10, 20, 30, 60)
PRIMARY_HORIZON = 20

# Coinbase's published average subsequent 20-day BTC returns by TrendScore.
COINBASE_CLAIM_20D = {0: -0.007, 1: 0.013, 2: 0.038, 3: 0.077}

# --------------------------------------------------------------------------
# Statistics
# --------------------------------------------------------------------------
# Newey-West lag for h-day overlapping returns. h-1 is the minimum needed to
# span the mechanical MA(h-1) overlap; we use h for a small safety margin.
def hac_lags(horizon: int) -> int:
    return int(horizon)


BOOTSTRAP_REPS = 2000
BOOTSTRAP_BLOCK_DAYS = 60  # circular block bootstrap block length
BOOTSTRAP_SEED = 20260817

# --------------------------------------------------------------------------
# Subperiods (calendar, pre-specified in the brief)
# --------------------------------------------------------------------------
SUBPERIODS = {
    "2015-2019": ("2015-01-01", "2019-12-31"),
    "2020-2022": ("2020-01-01", "2022-12-31"),
    "2023-present": ("2023-01-01", "2100-01-01"),
}

# Train / out-of-sample split for the walk-forward exercise.
OOS_SPLIT_DATE = "2022-01-01"

# --------------------------------------------------------------------------
# Parameter-robustness grid (36 combinations -- a plateau probe, not a search)
# --------------------------------------------------------------------------
GRID_SHORT = (20, 30, 40)
GRID_MEDIUM = (60, 90, 120)
GRID_LONG = (250, 300, 365, 400)

# --------------------------------------------------------------------------
# Strategies: PRE-SPECIFIED TrendScore -> target BTC exposure maps
# --------------------------------------------------------------------------
STRATEGY_MAPS = {
    "A_linear": {0: 0.0, 1: 1 / 3, 2: 2 / 3, 3: 1.0},
    "B_threshold": {0: 0.0, 1: 0.0, 2: 1.0, 3: 1.0},
    "C_strong": {0: 0.0, 1: 0.0, 2: 0.0, 3: 1.0},
    # D is included because "long-horizon trend as the veto, short-horizon as
    # the throttle" is a standard trend-following construction, not because it
    # tested well. It is reported alongside A/B/C, never used to pick a winner.
    "D_long_veto": {0: 0.0, 1: 0.0, 2: 0.5, 3: 1.0},
}

# Signal computed on close(t) is executed at close(t+1); the position therefore
# earns the return from close(t+1) to close(t+2). exposure.shift(SIGNAL_LAG).
SIGNAL_LAG = 2
SIGNAL_LAG_SENSITIVITY = 1  # same-close execution -- reported as a sensitivity

COST_BPS_GRID = (0.0, 5.0, 10.0)
BASE_COST_BPS = 5.0
CASH_RATE_ANNUAL = 0.0  # residual capital earns 0% in the base case
CASH_RATE_SENSITIVITY = 0.04  # 4% p.a. on uninvested capital, reported separately

TRADING_DAYS_PER_YEAR = 365  # crypto trades every calendar day

# --------------------------------------------------------------------------
# DVOL overlay (OUR INTERPRETATION of Coinbase's description -- see report)
# --------------------------------------------------------------------------
DVOL_THRESHOLD_DEFAULT = 90.0
DVOL_THRESHOLD_GRID = (60.0, 70.0, 80.0, 90.0, 100.0, 110.0, 120.0)

# --------------------------------------------------------------------------
# Equity-curve drawdown brake (Coinbase-described)
# --------------------------------------------------------------------------
DD_FULL = 0.20   # drawdown <= 20%  -> multiplier 1.00
DD_FLOOR = 0.40  # drawdown >= 40%  -> multiplier 0.25
DD_MIN_MULT = 0.25

# --------------------------------------------------------------------------
# Assets
# --------------------------------------------------------------------------
# venue: "coinbase" -> Coinbase Exchange (api.exchange.coinbase.com)
#        "binance"  -> Binance spot klines
ASSETS = {
    "BTC": {"primary": ("coinbase", "BTC-USD"), "check": ("binance", "BTCUSDT")},
    "ETH": {"primary": ("coinbase", "ETH-USD"), "check": ("binance", "ETHUSDT")},
    # SOL: Coinbase spot begins 2021-06-30; Binance SOLUSDT begins 2020-08-11.
    # We take the longer clean exchange series as primary and cross-check
    # against Coinbase. Documented explicitly in the report.
    "SOL": {"primary": ("binance", "SOLUSDT"), "check": ("coinbase", "SOL-USD")},
}
