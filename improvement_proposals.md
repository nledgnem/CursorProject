# Improvement Proposals for Majors/Alts Monitor

## Current Performance
- Sharpe: 1.05
- Max Drawdown: -91.32%
- CAGR: 43.28%
- Volatility: 43.22%

## Key Issues Identified
1. **Extremely high drawdown** (~91%) despite risk management
2. **High volatility** (43% annualized)
3. **Strategy is inherently risky** (shorting volatile ALTs)

## Proposed Improvements

### 1. **Enhanced ALT Selection** (HIGH PRIORITY)
**Problem**: Currently selects top-N liquid alts, but doesn't filter by volatility or correlation.

**Solution**:
- Add volatility filter: exclude ALTs with >100% annualized volatility
- Add correlation filter: exclude ALTs with low correlation to BTC/ETH (might be idiosyncratic risk)
- Add momentum filter: exclude ALTs with extreme recent momentum (avoid catching falling knives)
- Weight by inverse volatility: give more weight to less volatile ALTs

**Expected Impact**: Reduce portfolio volatility by 10-20%, improve Sharpe by 0.1-0.2

### 2. **Take-Profit Levels** (HIGH PRIORITY)
**Problem**: No profit-taking mechanism, positions held until regime change or stop-loss.

**Solution**:
- Add take-profit levels: exit positions when cumulative return > threshold (e.g., +10%)
- Time-based exits: exit positions after N days regardless of PnL (e.g., 30 days)
- Partial profit-taking: reduce position size by 50% when profit target hit

**Expected Impact**: Lock in profits, reduce drawdown by 5-10%, improve Sharpe by 0.1-0.15

### 3. **Position-Level Stop-Losses** (MEDIUM PRIORITY)
**Problem**: Only portfolio-level stop-loss exists. Individual ALT positions can have large losses.

**Solution**:
- Per-position stop-loss: exit individual ALT if its return exceeds threshold (e.g., -15%)
- Dynamic stop-loss: adjust stop-loss based on ALT volatility
- Trailing stop per position: move stop-loss up as position becomes profitable

**Expected Impact**: Reduce worst-case losses, improve drawdown by 5-10%

### 4. **Reduced Rebalancing Frequency** (MEDIUM PRIORITY)
**Problem**: Rebalancing daily creates high turnover and costs.

**Solution**:
- Rebalance only when regime changes significantly
- Rebalance weekly instead of daily
- Rebalance only when position drift exceeds threshold (e.g., 10%)

**Expected Impact**: Reduce costs by 30-50%, improve net returns by 2-5%

### 5. **Better Beta Estimation** (MEDIUM PRIORITY)
**Problem**: Beta estimation uses simple ridge regression, may not capture regime-dependent betas.

**Solution**:
- Use regime-dependent betas: estimate betas separately for each regime
- Use longer lookback for more stable estimates
- Use actual tracker betas from data lake if available
- Add confidence intervals: reduce position size if beta estimate is uncertain

**Expected Impact**: Improve neutrality, reduce drawdown by 2-5%

### 6. **Volatility-Adjusted Position Sizing** (LOW PRIORITY)
**Problem**: Position sizes are fixed regardless of current volatility environment.

**Solution**:
- Scale positions by inverse of current volatility (already partially implemented)
- Use VIX-like indicator for crypto (if available)
- Reduce positions during high volatility periods (e.g., >60% annualized)

**Expected Impact**: Reduce volatility by 5-10%, improve Sharpe by 0.05-0.1

### 7. **Correlation-Based Risk Limits** (LOW PRIORITY)
**Problem**: ALT basket might have high correlation, creating concentration risk.

**Solution**:
- Limit correlation between ALT positions (e.g., max 0.7 correlation)
- Diversify across sectors (DeFi, L1, L2, etc.)
- Add correlation penalty to selection criteria

**Expected Impact**: Reduce tail risk, improve drawdown by 3-5%

### 8. **Improved Regime Model** (LOW PRIORITY)
**Problem**: Regime model might not be predictive enough.

**Solution**:
- Add more features: correlation, momentum, volume trends
- Use machine learning (XGBoost, Random Forest) for regime prediction
- Ensemble multiple regime models
- Add regime persistence: require N consecutive days in regime before trading

**Expected Impact**: Improve entry/exit timing, improve Sharpe by 0.1-0.2

## Recommended Implementation Order

1. **Take-Profit Levels** - Easy to implement, high impact
2. **Enhanced ALT Selection** - Medium complexity, high impact
3. **Position-Level Stop-Losses** - Medium complexity, medium impact
4. **Reduced Rebalancing Frequency** - Easy to implement, medium impact
5. **Better Beta Estimation** - Medium complexity, medium impact

## Expected Combined Impact

If all top 5 improvements are implemented:
- Sharpe: 1.05 → 1.25-1.35 (+20-30%)
- Max Drawdown: -91% → -75-80% (+11-16% improvement)
- CAGR: 43% → 45-50% (+2-7%)
- Volatility: 43% → 35-38% (-5-8%)

## Notes

- The high drawdown is partially inherent to the strategy (shorting volatile ALTs)
- Some improvements may conflict (e.g., take-profit vs. holding for regime)
- Need to test each improvement individually to measure impact
- Consider A/B testing different parameter values
