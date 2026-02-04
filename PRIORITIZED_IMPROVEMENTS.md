# Prioritized Improvement Suggestions

## Current Performance
- **Sharpe**: 1.00
- **CAGR**: 40.47%
- **Max Drawdown**: -91.66%
- **Sortino**: 1.47

## 🎯 Top 5 Priority Improvements (High Impact, Low Effort)

### 1. Dynamic Position Sizing (Kelly/Volatility Parity) ⭐⭐⭐
**Impact**: +5-10% Sharpe, -5-10% drawdown  
**Effort**: Medium (2-3 hours)

**What to do:**
- Implement volatility parity: scale positions to target 20% annualized portfolio volatility
- Or use Kelly Criterion: size = (expected_return / variance) * fraction
- Currently positions are fixed size; this adapts to opportunity quality

**Code location**: `majors_alts_monitor/backtest.py` - `_get_volatility_scaling_factor` (already exists but disabled)

### 2. Correlation-Based Position Limits ⭐⭐⭐
**Impact**: -3-7% drawdown reduction  
**Effort**: Medium (2-3 hours)

**What to do:**
- Compute rolling correlation matrix of ALT basket (60-day window)
- Group assets with correlation > 0.7
- Cap total exposure to each correlation group (e.g., max 30% per group)
- Prevents concentration risk from correlated ALTs

**Code location**: `majors_alts_monitor/beta_neutral.py` - `build_alt_basket`

### 3. Continuous Position Scaling ⭐⭐
**Impact**: +2-4% Sharpe  
**Effort**: Low (30 minutes)

**What to do:**
- Replace binary scaling (100% vs 60%) with continuous scaling
- Scale = min(1.0, |score| / threshold_high)
- More granular position sizing based on regime confidence

**Code location**: `majors_alts_monitor/backtest.py` - `_get_regime_scaling_factor`

### 4. Regime Persistence Scoring ⭐⭐
**Impact**: +2-5% hit rate, reduced costs  
**Effort**: Low (1-2 hours)

**What to do:**
- Track days since last regime change
- Require stronger signal (score change > 0.5) if regime is < 3 days old
- Prevents churn from frequent regime switches

**Code location**: `majors_alts_monitor/regime.py` - `_classify_regimes`

### 5. Dynamic Rebalancing ⭐
**Impact**: +1-3% net returns (reduced costs)  
**Effort**: Low (1-2 hours)

**What to do:**
- Only rebalance when:
  - Regime changes
  - Score moves > 0.3
  - Position drift > 10%
- Reduces unnecessary transaction costs

**Code location**: `majors_alts_monitor/backtest.py` - `_run_window`

## 🔧 Medium Priority (Higher Effort, Good Impact)

### 6. Better Transaction Cost Modeling
- Volume-based slippage: `slippage = base + (trade_size / ADV) * multiplier`
- Market impact for large positions
- **Impact**: More realistic returns
- **Effort**: Medium (2-3 hours)

### 7. Portfolio-Level Risk Limits
- Maximum portfolio volatility (20% annualized)
- Maximum correlation to BTC/ETH
- Maximum single-day loss
- **Impact**: -5-10% drawdown
- **Effort**: Medium (2-3 hours)

### 8. Regime-Aware Trailing Stop
- Tighter stops in BALANCED/RISK_ON_ALTS (-10%)
- Wider stops in RISK_ON_MAJORS (-20%)
- **Impact**: +3-5% win rate
- **Effort**: Low (1 hour)

## 📊 Advanced (Long Term)

### 9. Dynamic Feature Weights
- Rolling feature importance
- Regime-specific weights
- **Impact**: +3-7% Sharpe
- **Effort**: High (4-6 hours)

### 10. Multi-Timeframe Features
- Weekly/monthly features
- **Impact**: +2-4% Sharpe
- **Effort**: Medium (3-4 hours)

## 🚀 Recommended Implementation Order

**This Week:**
1. ✅ High-Vol Gate (DONE)
2. ✅ Volatility-Adjusted Stop-Loss (DONE)
3. **Continuous Position Scaling** (30 min) ← Start here
4. **Regime Persistence** (1-2 hours)

**Next Week:**
5. **Dynamic Position Sizing** (2-3 hours)
6. **Correlation-Based Limits** (2-3 hours)
7. **Dynamic Rebalancing** (1-2 hours)

## 💡 Expected Combined Impact

If top 5 are implemented:
- **Sharpe**: 1.00 → **1.25-1.35** (+25-35%)
- **CAGR**: 40.47% → **48-55%** (+19-36%)
- **Max Drawdown**: -91.66% → **-85-88%** (+3-6% improvement)
- **Sortino**: 1.47 → **1.70-1.90** (+16-29%)

## 🎯 Biggest Opportunities

1. **Dynamic Position Sizing** - Biggest impact (+5-10% Sharpe)
2. **Correlation-Based Limits** - Best drawdown reduction (-3-7%)
3. **Continuous Scaling** - Easiest win (30 min, +2-4% Sharpe)
