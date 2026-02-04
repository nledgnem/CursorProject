# Detailed Explanation: Monitor Evaluation Process for Mads

## Overview
This document explains the complete process of evaluating regime monitors against long-short (LS) basket strategy returns, including what each component means and how the backtest was constructed.

---

## 1. What Are "LS Returns" (Long-Short Returns)?

### Definition
**LS Returns** (`r_ls_net`) represent the daily performance of a **long-short basket strategy** relative to BTC (the base asset).

### How They're Calculated

The backtest engine computes LS returns as follows:

1. **Basket Return** (`r_basket`): 
   - Daily return of a portfolio of altcoins (top 10 by market cap, excluding BTC)
   - Portfolio is rebalanced monthly on the 1st of each month
   - Uses `sqrt_cap_weighted` weighting scheme (square-root of market cap, capped at 10% per asset)
   - Example: If ETH has 20% market cap, it gets ~14.1% weight (sqrt(0.20) normalized)

2. **BTC Return** (`r_btc`):
   - Simple daily return of Bitcoin: `(price_today / price_yesterday) - 1`

3. **Long-Short Return** (`r_ls`):
   - **`r_ls = r_btc - r_basket`**
   - This means: We're **long BTC** and **short the altcoin basket**
   - Positive returns occur when BTC outperforms alts
   - Negative returns occur when alts outperform BTC

4. **Transaction Costs** (`cost`):
   - Applied only on rebalance dates (monthly)
   - Fee: 5 bps (0.05%) per trade
   - Slippage: 5 bps (0.05%) per trade
   - Total cost = `turnover * (fee_bps + slippage_bps)`
   - Turnover = sum of absolute weight changes / 2

5. **Net LS Return** (`r_ls_net`):
   - **`r_ls_net = r_ls - cost`**
   - This is the final daily return series used for evaluation

### Why This Strategy?
- **Market-neutral-ish**: Long BTC, short alts means we're betting on BTC outperformance
- **Diversified short side**: Top 10 alts by market cap (not just one coin)
- **Realistic costs**: Includes fees and slippage on rebalances
- **Point-in-time**: Uses only data available at each rebalance date (no lookahead)

---

## 2. What Is "Regime History"?

### Definition
**Regime History** is a daily time series that classifies market conditions into 5 discrete buckets (1-5), where:
- **Regime 1 = WORST/RED** (bad environment for LS strategies)
- **Regime 2 = ORANGE**
- **Regime 3 = YELLOW** (neutral)
- **Regime 4 = YELLOWGREEN**
- **Regime 5 = BEST/GREEN** (good environment for LS strategies)

### Source
The regime history comes from `OwnScripts/regime_backtest/regime_history.csv`, which contains:
- **Date**: Daily timestamp
- **Regime Score**: Raw score (0-100 scale, typically)
- **Bucket**: String label (RED, ORANGE, YELLOW, YELLOWGREEN, GREEN)
- **Inputs**: Various market signals like:
  - `btc_1d`, `btc_7d`: BTC returns over 1 and 7 days
  - `alt_7d_avg`: Average altcoin returns over 7 days
  - `spread7`: Spread between BTC and alt returns
  - `funding_spread`: Funding rate differences
  - `oi_change_3d`: Open interest changes
  - `breadth_3d`: Market breadth indicators

### How It's Used
The monitor evaluation framework:
1. Loads the regime history CSV
2. Converts bucket strings (RED→1, GREEN→5) to integer `regime_1_5`
3. Aligns regime scores with LS returns by date (inner join)
4. Evaluates whether regime scores predict forward LS returns

---

## 3. How Was the Backtest Done?

### Pipeline Steps

#### Step 1: Data Acquisition & QC
- **Input**: Raw price/marketcap/volume data from CoinGecko API
- **Output**: Curated wide-format parquet files (`prices_daily.parquet`, etc.)
- **Coverage**: 730 days (2024-01-07 to 2026-01-05), 2,718 symbols
- **QC**: Gap detection, repair logging, data quality checks

#### Step 2: Data Lake Conversion
- **Input**: Wide-format parquet files
- **Output**: Normalized fact tables (`fact_price`, `fact_marketcap`, `fact_volume`)
- **Schema**: Star schema with `dim_asset`, `dim_instrument`, fact tables
- **Purpose**: Enables point-in-time queries and data integrity

#### Step 3: Universe & Basket Snapshots
- **Input**: Config file (`golden_2year.yaml`) + data lake
- **Process**:
  1. Generate rebalance dates (monthly, 1st of month)
  2. For each rebalance date:
     - Apply eligibility filters (must have perp, min 30 days listing age, etc.)
     - Select top 10 coins by market cap (excluding BTC)
     - Calculate weights using `sqrt_cap_weighted` scheme
     - Cap weights at 10% per asset
  3. Output: `universe_snapshots.parquet` with columns:
     - `rebalance_date`, `symbol`, `weight`, `rank`, `marketcap`, etc.

**Key Point**: Snapshots are **point-in-time** - only data available on the rebalance date is used (no lookahead).

#### Step 4: Backtest Execution
- **Input**: Snapshots + price data
- **Process** (for each trading day):
  1. Check if it's a rebalance date → update basket weights
  2. Calculate BTC return: `(BTC_price_today / BTC_price_yesterday) - 1`
  3. Calculate basket return: weighted sum of altcoin returns
  4. Calculate LS return: `r_ls = r_btc - r_basket`
  5. If rebalance date: calculate costs and subtract from `r_ls`
  6. Store: `r_ls_net = r_ls - cost`
  7. Build equity curve: cumulative product of `(1 + r_ls_net)`

**Output**: `backtest_results.csv` with columns:
- `date`: Trading day
- `r_btc`: BTC daily return
- `r_basket`: Basket daily return
- `r_ls`: Long-short return (before costs)
- `cost`: Transaction costs (non-zero only on rebalance dates)
- `r_ls_net`: Net LS return (after costs)
- `equity_curve`: Cumulative performance (starts at 1.0)

#### Step 5: Validation
- Checks universe/basket invariants (weights sum to 1.0, no duplicates, etc.)
- Validates perp listing linkage
- Validates backtest results (no negative equity, reasonable returns, etc.)

---

## 4. Monitor Evaluation Process

### Overview
The monitor evaluation framework tests whether regime scores **predict forward LS returns**. The key principle: **NO LOOKAHEAD** - regime score on day `t` can only use information available up to day `t`.

### Step-by-Step Process

#### Step 1: Load Data
- **LS Returns**: From `backtest_results.csv` → extract `r_ls_net` column
- **Regime History**: From `regime_history.csv` → convert buckets to `regime_1_5` (1-5)
- **Date Range**: 2024-03-01 to 2025-12-31 (670 aligned trading days)

#### Step 2: Compute Forward Returns
For each date `t` and horizon `H` (e.g., H=5, 10, 20, 40, 60 trading days):

```
fwd_ret(t, H) = compounded return from t+1 to t+H
              = exp(sum(log(1 + r_ls_net[t+1] ... r_ls_net[t+H]))) - 1
```

**Critical**: Same-day returns (`r_ls_net[t]`) are **explicitly excluded** to prevent lookahead bias.

**Example**: For H=5 (1 week), if today is Monday:
- Forward return = compounded return from Tuesday to next Monday
- Does NOT include Monday's return

#### Step 3: Align Data
- Inner join regime scores and LS returns by date
- Result: 670 common trading days with both regime score and LS returns
- Sample sizes:
  - Regime 1 (WORST): 181-190 days
  - Regime 5 (BEST): 125-133 days

#### Step 4: Compute Statistics

**A. Bucket Statistics** (per regime, per horizon):
- `n`: Sample size
- `mean`: Mean forward return
- `median`: Median forward return
- `std`: Standard deviation
- `sharpe_like`: Mean / Std (approximate Sharpe ratio)
- `min`, `max`: Min/max forward returns

**B. Edge Statistics** (comparing regimes to overall):
- `edge_best`: `mean(fwd_ret | regime=5) - mean(fwd_ret | ALL)`
  - How much better is regime 5 (BEST) vs. average?
- `edge_worst`: `mean(fwd_ret | regime=1) - mean(fwd_ret | ALL)`
  - How much worse is regime 1 (WORST) vs. average?
- `spread_1_5`: `mean(fwd_ret | regime=5) - mean(fwd_ret | regime=1)`
  - Direct comparison: BEST vs WORST

**C. Significance Testing** (Block Bootstrap):
- **Purpose**: Account for autocorrelation and overlapping forward windows
- **Method**: 
  - Resample blocks of 10 consecutive days (configurable)
  - Run 300 bootstrap iterations
  - Compute p-value: fraction of bootstrap samples with absolute edge >= observed absolute edge
  - Compute 95% confidence interval: 2.5th and 97.5th percentiles
- **Interpretation**: 
  - p-value < 0.05: Statistically significant (reject null hypothesis)
  - p-value > 0.5: No evidence of signal (null hypothesis likely true)

---

## 5. Key Results Summary

### Data Coverage
- **LS Returns**: 725 days (2024-01-07 to 2025-12-31)
- **Regime History**: 743 days (2023-12-23 to 2026-01-01)
- **Aligned Dates**: 670 common trading days

### Findings Across Horizons

| Horizon | Spread (5-1) | P-value | Interpretation |
|---------|--------------|---------|----------------|
| H=5 (1 week) | -0.0038 | 0.72 | Regime 1 outperforms; not significant |
| H=10 (2 weeks) | -0.0004 | 0.98 | Essentially zero; not significant |
| H=20 (1 month) | -0.0116 | 0.61 | Regime 1 outperforms; not significant |
| H=40 (2 months) | -0.0242 | 0.52 | Regime 1 outperforms; not significant |
| H=60 (3 months) | -0.0217 | 0.64 | Regime 1 outperforms; not significant |

### Key Observations

1. **Signal is weak/inconsistent**: Regime 5 (BEST) does NOT consistently outperform Regime 1 (WORST)
2. **Direction flips**: At H=10, spread is essentially zero; at other horizons, Regime 1 often outperforms
3. **No statistical significance**: All p-values > 0.5 (bootstrap with block_size=10, n_boot=300)
4. **Sample sizes are adequate**: n1=181-190, n5=125-133 (well above minimum threshold of 30)

### Interpretation

The existing monitor's regime classification (1=WORST/RED, 5=BEST/GREEN) is **not aligned with actual forward LS return performance**. The monitor does not appear to have meaningful predictive power for forward returns at any horizon tested.

---

## 6. Technical Details

### Forward Return Calculation (No Lookahead)
```python
# For date t and horizon H:
# Forward return = compounded return from t+1 to t+H
log_returns = log(1 + r_ls_net)
fwd_log_sum = sum(log_returns[t+1] ... log_returns[t+H])
fwd_ret = exp(fwd_log_sum) - 1
```

### Block Bootstrap (Handling Overlap)
- **Problem**: Forward windows overlap (e.g., H=5 means day 1-5, day 2-6, day 3-7 all overlap)
- **Solution**: Block bootstrap resamples blocks of consecutive days (default: 10 days)
- **Rationale**: Preserves autocorrelation structure while allowing statistical inference

### Data Quality Guardrails
- **Inner join**: Only evaluate dates where both regime score and LS returns exist
- **Missing data**: Forward returns are NaN if any day in the forward window has missing data
- **Small samples**: Warns if n1 < 30 or n5 < 30 (but still outputs results)

---

## 7. Files & Artifacts

### Input Files
- `configs/golden_2year.yaml`: Backtest configuration (date range, rebalancing, eligibility, etc.)
- `configs/monitor_eval.yaml`: Evaluation configuration (horizons, bootstrap settings)
- `outputs/runs/20260106_032842_453e33332daace30/outputs/backtest_results.csv`: LS returns from backtest
- `OwnScripts/regime_backtest/regime_history.csv`: Regime history (daily scores)

### Output Files
- `outputs/monitor_eval/20260106_113019/regime_bucket_stats.csv`: Statistics per regime bucket
- `outputs/monitor_eval/20260106_113019/regime_edges.csv`: Edge statistics with bootstrap significance
- `outputs/monitor_eval/20260106_113019/run_receipt.json`: Full run metadata, config snapshot, input hashes

---

## 8. Next Steps (If Iterating)

1. **Try weekly evaluation points**: For H=5, evaluate only weekly (e.g., every Friday) to reduce overlap
2. **Experiment with monitor parameters**: Different lookbacks, thresholds, smoothing
3. **Consider alternative regime definitions**: Maybe the current scoring method doesn't capture the right signal
4. **Test on different strategies**: Maybe this monitor works better for different LS strategy variants

---

## Questions for Mads

1. **Is this monitor promising enough to iterate on, or should it be deprioritized?**
   - Given weak signal and lack of significance, recommendation is to **deprioritize** unless there's a strong hypothesis for why it should work differently.

2. **Are there other regime monitors to test?**
   - The framework is ready to evaluate any monitor that outputs daily regime scores (1-5).

3. **Should we test on different LS strategy variants?**
   - Different rebalancing frequencies, weighting schemes, or universe selection criteria?


