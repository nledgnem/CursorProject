# LS Basket Low-Vol: Process & Results Explained

This document explains the full pipeline for building a **dollar-neutral long/short basket** that minimizes realized volatility and controls tail risk, and what the outputs mean.

---

## 1. High-Level Objective

The goal is to find a portfolio of crypto assets that:

- Is **dollar-neutral** (total long exposure = total short exposure, so net market exposure ≈ 0)
- **Minimizes volatility** of daily P&L (primary objective)
- **Controls tail risk** via CVaR penalties and constraints
- Is **practical** (respects liquidity, turnover, and per-asset position limits)

You hold long positions in some assets and short positions in others. When the market moves, gains on one side tend to offset losses on the other, producing smoother returns than a directional bet.

---

## 2. Data Sources

The pipeline uses only daily data from the data lake:

| Table | Use |
|-------|-----|
| `fact_price.parquet` | Daily close prices → returns |
| `fact_volume.parquet` | Volume (with price) → USD ADV for liquidity checks |
| `fact_marketcap.parquet` | Market cap for universe filters |
| `dim_asset.parquet` | Asset IDs and symbols |

**fact_funding** exists for carry but is not used in the current pipeline.

---

## 3. Step-by-Step Process

### Step 1: Universe QC

Before building the basket, we filter the universe to tradable, liquid assets:

- **Min market cap:** $10M (configurable)
- **Min 14-day average USD volume:** $1M
- **Exclusions:** Stablecoins (USDT, USDC, BUSD, DAI, etc.)

Assets must meet these criteria over the backtest period (e.g., 2023-01-01 to 2024-12-31) to be eligible. The result is the **candidate universe** (e.g., 383 assets) used for basket construction.

### Step 2: Rebalance Dates

Rebalancing happens **monthly** on a fixed day (e.g., 1st). On each rebalance date:

1. New weights are computed from recent data
2. The portfolio is rebalanced to those weights
3. Turnover (sum of absolute weight changes) drives trading costs

### Step 3: Method A — Global Min-Variance QP

Method A solves a quadratic program (QP) at each rebalance date:

**Objective:**
- Minimize portfolio variance (from daily returns)
- Add penalty for CVaR (Conditional Value at Risk) at 95% to limit tail losses
- Add penalty for expected turnover to limit trading

**Constraints:**
- **Dollar neutrality:** Sum of weights = 0 (longs = shorts)
- **Gross exposure:** Sum of |weights| ≤ G (default 1.0)
- **Per-asset cap:** |weight_i| ≤ 0.10 (or configurable)
- **Liquidity:** |weight_i| limited by max 5% of asset’s 1-day ADV
- **PCA factor exposure (optional):** Limit exposure to the first principal component

**Implementation details:**
- Covariance: rolling 90-day returns with **Ledoit–Wolf shrinkage** (fallback to 60 days if needed)
- CVaR via Rockafellar–Uryasev linearization in the QP
- If cvxpy is unavailable, a scipy-based fallback QP solver is used

The output is a **weight snapshot** per rebalance date: for each asset, a weight (positive = long, negative = short).

### Step 4: Backtest

For each rebalance date, we have weights. The backtest:

1. Applies those weights on each trading day until the next rebalance
2. Computes daily P&L = Σ (weight_i × return_i) − costs
3. Costs = (fee_bps + slippage_bps) / 10000 × turnover at rebalance

From the daily P&L series we compute metrics (volatility, drawdown, CVaR, etc.).

### Step 5: Metrics & Ranking

Candidates are ranked by **realized annualized volatility** (lower is better), subject to constraints:

- Average monthly turnover ≤ 20%
- Long/short leg correlation ≥ 0.80 (ideally high so legs hedge each other)

If no candidate meets all constraints, the lowest-volatility basket is still reported, with a note.

---

## 4. Output Files & What They Represent

### `runs/rank1_A_weights.csv`

Latest rebalance weights:

| Column | Meaning |
|--------|---------|
| **symbol** | Asset ticker (e.g., BTC, ETH) |
| **weight** | Portfolio weight; positive = long, negative = short |
| **side** | "long" or "short" |
| **marketcap** | Market cap at rebalance (USD) |
| **adv_30d** | 30-day average daily volume (USD) |

Weights are as a fraction of notional (e.g., 0.05 = 5% of notional long or short). Sum of longs = sum of shorts (dollar-neutral).

### `runs/rank1_A_daily_pnl.csv`

Daily time series:

| Column | Meaning |
|--------|---------|
| **date** | Trading date |
| **pnl** | Net daily P&L (including costs) |
| **pnl_long** | P&L from long positions only |
| **pnl_short** | P&L from short positions only |
| **gross_exposure** | Sum of |weights| |
| **cost** | Trading cost applied that day (mainly at rebalance) |
| **turnover** | Turnover at rebalance (0 on non-rebalance days) |
| **equity** | Cumulative equity curve (1 + cumulative returns) |

### `runs/rank1_A_summary.csv`

One-row summary of backtest metrics:

| Metric | Meaning |
|--------|---------|
| **realized_vol_ann** | Annualized volatility of daily P&L (std × √252) |
| **kurtosis** | Tail heaviness; high values indicate fat tails |
| **cvar95** | Average loss in worst 5% of days (tail risk) |
| **cvar99** | Average loss in worst 1% of days |
| **max_drawdown** | Largest peak-to-trough decline in equity |
| **avg_turnover** | Average turnover per rebalance |
| **avg_long_short_corr** | Correlation between daily long-leg and short-leg returns |
| **gross_exposure** | Average gross exposure over time |

### `reports/summary.md`

Text report with universe stats, top baskets, and a recommendation.

### `reports/rank1_A_diagnostics.png`

Diagnostic plots:

- **Equity curve** — cumulative P&L over time
- **Rolling 90d volatility** — time-varying risk
- **Turnover histogram** — distribution of rebalance turnover
- **Daily gross exposure** — leverage over time

---

## 5. Interpreting Your Results

From the latest run:

| Metric | Value | Interpretation |
|--------|-------|----------------|
| Realized vol | 6.96% | Annualized volatility of the basket’s daily P&L |
| Kurtosis | 31.99 | Fat tails; some very large moves in the P&L distribution |
| CVaR 95% | 1.10% | On worst 5% of days, average loss ~1.1% |
| CVaR 99% | 2.32% | On worst 1% of days, average loss ~2.3% |
| Max drawdown | -16.04% | Largest peak-to-trough decline |
| Avg turnover | 0.40% | Low rebalance turnover |
| Long/short corr | -0.93 | Strong negative correlation between long and short legs |

**Long/short correlation:**  
Target is ≥ 0.80. A value of -0.93 means when the long leg gains, the short leg tends to lose, and vice versa. That can still reduce net volatility if the legs move together in magnitude but in opposite directions. The basket does not meet the formal “high positive correlation” constraint but is the lowest-volatility option among those evaluated.

---

## 6. Tradeoffs & Caveats

- **cvxpy vs scipy:** Without cvxpy, the pipeline uses a simpler scipy QP. The full cvxpy formulation supports CVaR and PCA constraints more fully.
- **Turnover:** Costs are approximated as (fee_bps + slippage_bps) × turnover at each rebalance.
- **Data quality:** Missing prices or gaps can affect returns and metrics. The pipeline does not fill gaps by default.
- **In-sample vs out-of-sample:** The backtest uses the same period for optimization and evaluation; out-of-sample testing would require a hold-out period.

---

## 7. How to Run

```bash
# Quick run (1 Method A config)
python run_ls_basket_low_vol.py --quick

# Full parameter sweep (more Method A and Method B configs)
python run_ls_basket_low_vol.py
```

Configuration is in `scripts/ls_basket_low_vol/config_default.json` and can be overridden with `--config path/to/config.json`.
