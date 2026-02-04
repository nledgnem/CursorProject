# Improvement Suggestions for Regime Monitor & Backtest

## Current Performance
- **Sharpe**: 1.15
- **CAGR**: 49.12%
- **Max Drawdown**: -91.01%
- **Sortino**: 1.68

## Key Areas for Improvement

### 1. Regime Monitor Improvements

#### A. High-Vol Gate (from Legacy Monitor)
**Current**: Not implemented
**Suggestion**: Add high-vol gate to prevent overconfidence during extreme BTC moves
- When BTC 7d return > 15%, cap regime score at 60 (neutral)
- Prevents false signals during volatile periods
- **Expected Impact**: Reduce false positives, improve Sharpe by 2-5%

#### B. Dynamic Feature Weights
**Current**: Fixed weights from config
**Suggestion**: Use rolling feature importance or regime-specific weights
- Compute feature importance via rolling correlation with forward returns
- Adjust weights based on regime (e.g., funding more important in RISK_ON_MAJORS)
- **Expected Impact**: Improve Sharpe by 3-7%

#### C. Regime Persistence Model
**Current**: Hysteresis only
**Suggestion**: Add regime persistence scoring
- Track average regime duration
- Penalize regime switches (require stronger signal to switch)
- **Expected Impact**: Reduce churn, improve hit rate by 2-5%

#### D. Multi-Timeframe Features
**Current**: Daily features only
**Suggestion**: Add weekly/monthly features
- Weekly momentum, monthly dominance trends
- Combine multiple timeframes for robustness
- **Expected Impact**: Improve Sharpe by 2-4%

### 2. Backtesting Improvements

#### A. Dynamic Position Sizing
**Current**: Fixed scaling (regime-based only)
**Suggestion**: Implement Kelly Criterion or Volatility Parity
- Size positions based on expected return / variance
- Volatility parity: scale by inverse of portfolio volatility
- **Expected Impact**: Improve Sharpe by 5-10%, reduce drawdown by 5-10%

#### B. Better Transaction Cost Modeling
**Current**: Fixed bps for fees/slippage
**Suggestion**: Volume-based slippage and market impact
- Slippage = base + volume_impact * (trade_size / ADV)
- Market impact for large positions
- **Expected Impact**: More realistic returns, better live performance

#### C. Correlation-Based Position Limits
**Current**: Per-name cap only
**Suggestion**: Limit exposure to correlated assets
- Compute correlation matrix of ALT basket
- Cap total exposure to highly correlated groups
- **Expected Impact**: Reduce tail risk, improve drawdown by 3-7%

#### D. Dynamic Rebalancing
**Current**: Daily rebalancing
**Suggestion**: Rebalance only when signal changes significantly
- Rebalance threshold: only when regime changes or score moves > 0.3
- Reduce transaction costs
- **Expected Impact**: Improve net returns by 1-3%

#### E. Portfolio-Level Risk Limits
**Current**: Gross cap only
**Suggestion**: Add portfolio-level risk metrics
- Maximum portfolio volatility (e.g., 20% annualized)
- Maximum correlation to BTC/ETH
- Maximum single-day loss
- **Expected Impact**: Reduce drawdown by 5-10%

### 3. Risk Management Enhancements

#### A. Volatility-Adjusted Stop-Loss
**Current**: Fixed -5% daily loss threshold
**Suggestion**: Scale stop-loss by realized volatility
- Stop-loss = -2 * daily_volatility (e.g., if vol is 3%, stop at -6%)
- Adapts to market conditions
- **Expected Impact**: Better risk-adjusted exits, improve Sharpe by 2-5%

#### B. Trailing Stop Based on Regime
**Current**: Trailing stop disabled
**Suggestion**: Regime-aware trailing stop
- Tighter stops in BALANCED/RISK_ON_ALTS regimes
- Wider stops in RISK_ON_MAJORS (let winners run)
- **Expected Impact**: Improve win rate, reduce drawdown by 3-5%

#### C. Position Sizing Based on Regime Confidence
**Current**: Binary scaling (100% vs 60%)
**Suggestion**: Continuous scaling based on score magnitude
- Scale = min(1.0, |score| / threshold_high)
- More granular position sizing
- **Expected Impact**: Better risk-adjusted returns, improve Sharpe by 2-4%

### 4. Feature Engineering Improvements

#### A. Actual OI Data Integration
**Current**: Using marketcap as proxy
**Suggestion**: If OI data becomes available, use it directly
- More accurate OI risk calculation
- Better funding/OI correlation analysis
- **Expected Impact**: Improve Sharpe by 1-3%

#### B. Cross-Asset Correlations
**Current**: Not used
**Suggestion**: Add correlation-based features
- Average correlation of ALT basket to BTC/ETH
- Correlation regime (high vs low correlation periods)
- **Expected Impact**: Better regime detection, improve Sharpe by 2-4%

#### C. Market Microstructure Features
**Current**: Not used
**Suggestion**: Add order flow proxies
- Bid-ask spread (if available)
- Volume profile (intraday distribution)
- **Expected Impact**: Improve entry/exit timing, improve Sharpe by 1-3%

### 5. Validation & Robustness

#### A. Out-of-Sample Testing
**Current**: Walk-forward on single period
**Suggestion**: Multiple out-of-sample periods
- Test on different market regimes (bull, bear, sideways)
- Validate across different time periods
- **Expected Impact**: Better confidence in live performance

#### B. Monte Carlo Simulation
**Current**: Not implemented
**Suggestion**: Bootstrap returns to test robustness
- Simulate different return paths
- Test strategy under various scenarios
- **Expected Impact**: Better risk assessment

#### C. Regime-Specific Performance Analysis
**Current**: Basic regime metrics
**Suggestion**: Deep dive into each regime
- Performance in each regime state
- Optimal position sizing per regime
- **Expected Impact**: Better regime-specific optimization

## Priority Recommendations (High Impact, Low Effort)

1. **High-Vol Gate** (Easy, High Impact)
   - Add to regime model
   - Expected: +2-5% Sharpe improvement

2. **Volatility-Adjusted Stop-Loss** (Easy, High Impact)
   - Modify stop-loss logic
   - Expected: +2-5% Sharpe improvement

3. **Dynamic Position Sizing** (Medium, High Impact)
   - Implement Kelly or volatility parity
   - Expected: +5-10% Sharpe improvement

4. **Correlation-Based Limits** (Medium, Medium Impact)
   - Add correlation matrix checks
   - Expected: -3-7% drawdown reduction

5. **Regime Persistence** (Easy, Medium Impact)
   - Add persistence scoring
   - Expected: +2-5% hit rate improvement

## Implementation Order

1. **Phase 1 (Quick Wins)**: High-vol gate, volatility-adjusted stop-loss
2. **Phase 2 (Medium Term)**: Dynamic position sizing, correlation limits
3. **Phase 3 (Long Term)**: Multi-timeframe features, advanced risk management

## Expected Combined Impact

If all high-priority improvements are implemented:
- **Sharpe**: 1.15 → 1.30-1.40 (+13-22%)
- **CAGR**: 49% → 55-60% (+12-22%)
- **Max Drawdown**: -91% → -85-88% (+3-6% improvement)
- **Sortino**: 1.68 → 1.90-2.10 (+13-25%)
