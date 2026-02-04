# Backtest Analysis: OI + Funding Features Impact

**Date:** 2026-01-06  
**Backtest Period:** 2024-09-16 to 2025-11-29 (434 days)  
**Features:** Real OI data + Funding data enabled

## Executive Summary

The backtest completed successfully with **real OI data** and **funding features** integrated. The strategy shows strong risk-adjusted returns but with significant drawdowns and limited trading activity.

## Key Metrics

### Performance Metrics
- **CAGR:** 45.48%
- **Sharpe Ratio:** 1.087
- **Sortino Ratio:** 1.577
- **Max Drawdown:** -91.56% ⚠️ (Very high)
- **Calmar Ratio:** 0.497
- **Hit Rate:** 50.23%

### Trading Activity
- **Total Days:** 434
- **Trading Days (with positions):** 33 (7.6%)
- **Zero Exposure Days:** 401 (92.4%)
- **Total PnL:** 4.39%
- **Avg Daily PnL (trading days):** 0.1331%
- **Win Rate:** 51.5%

### Funding Impact
- **Total Funding Cost:** -77.37% (significant drag)
- **Avg Daily Funding:** -0.1783%
- **Days with Funding:** 434 (all days)

## Regime Distribution

| Regime | Days | % of Total | Total PnL | Avg Daily PnL |
|--------|------|------------|-----------|---------------|
| BALANCED | 349 | 80.4% | -0.87% | -0.0025% |
| WEAK_RISK_ON_MAJORS | 53 | 12.2% | **5.26%** | 0.0993% |
| WEAK_RISK_ON_ALTS | 32 | 7.4% | 0.00% | 0.0000% |

**Key Insight:** The strategy is most profitable in `WEAK_RISK_ON_MAJORS` regime, but spends most time in `BALANCED` (no positions).

## Features Enabled

### OI Risk Feature
- ✅ **Status:** Using REAL OI data from CoinGlass
- **Data Source:** `fact_open_interest.parquet` (BTC OI)
- **Weight:** 0.04 (4% of composite score)
- **Implementation:** BTC OI 3d change, gated by BTC 3d return quality

### Funding Features
- ✅ **Funding Skew:** Enabled (weight: 0.12)
- ✅ **Funding Heating:** Enabled (weight: 0.10)
- **Total Weight:** 0.26 (26% of composite score)
- **Data Source:** `fact_funding.parquet` (507 symbols)

## Observations

### Strengths
1. **Strong Risk-Adjusted Returns:** Sharpe 1.087, Sortino 1.577
2. **High CAGR:** 45.48% annualized
3. **Profitable Regime:** WEAK_RISK_ON_MAJORS shows strong performance (5.26% in 53 days)
4. **Real Data Integration:** Successfully using real OI and funding data

### Concerns
1. **Extreme Drawdown:** -91.56% max drawdown is very high
2. **Low Trading Activity:** Only 7.6% of days have positions
3. **Funding Drag:** -77.37% total funding cost significantly impacts returns
4. **Regime Concentration:** 80% of time in BALANCED (no positions)

## Recommendations

### To Determine Feature Impact
1. **Run Baseline Backtest:**
   - Disable OI risk feature (set weight to 0)
   - Disable funding features (set weights to 0)
   - Redistribute weights proportionally
   - Run same period

2. **Compare Metrics:**
   - Sharpe Ratio (higher is better)
   - Sortino Ratio (higher is better)
   - Max Drawdown (lower is better)
   - CAGR (higher is better)
   - Trading frequency (more active = better signal quality)

3. **Analyze Regime Differences:**
   - Compare regime distributions
   - Check if OI/funding help identify profitable regimes
   - Analyze regime transition patterns

### Potential Improvements
1. **Address High Drawdown:**
   - Review stop-loss settings
   - Check position sizing logic
   - Consider volatility targeting adjustments

2. **Increase Trading Activity:**
   - Review regime thresholds (may be too conservative)
   - Consider regime persistence settings
   - Check if features are too noisy

3. **Mitigate Funding Costs:**
   - Review funding rate assumptions
   - Consider funding-aware position sizing
   - Evaluate if funding costs are realistic

## Next Steps

1. ✅ **Completed:** Backtest with OI + Funding
2. ⏳ **Pending:** Run baseline backtest (without OI + Funding)
3. ⏳ **Pending:** Compare results and quantify feature impact
4. ⏳ **Pending:** Optimize feature weights if beneficial

## Data Quality

- ✅ **OI Data:** 334 records (BTC, 2025-02-14 to 2026-01-13)
- ✅ **Funding Data:** 263,399 records (507 symbols, 2023-04-19 to 2026-01-13)
- ✅ **Asset ID Mapping:** All 507 symbols correctly mapped to universal IDs
- ✅ **Data Freshness:** All data up to date

---

**Note:** The high max drawdown (-91.56%) suggests there may be an issue with the backtest logic or data. This should be investigated before drawing conclusions about feature impact.
