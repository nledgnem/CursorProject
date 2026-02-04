# How the Regime Monitor and Backtest Work

## Strategy Overview

**You are LONG majors (BTC/ETH) and SHORT alts (ALT basket)** when the regime is favorable.

## The Regime Monitor

### What It Does
The regime monitor identifies market conditions using a composite score based on 8 features:

1. **ALT Breadth**: How many alts are moving up vs down
2. **BTC Dominance**: BTC market cap relative to total crypto market
3. **Funding Skew**: Difference between ALT funding rates and major funding rates
4. **Funding Heating**: Short-term vs long-term funding spread acceleration
5. **OI Risk**: Open interest changes (using real OI data from CoinGlass)
6. **Liquidity**: Trading volume and flow proxies
7. **Volatility Spread**: ALT volatility vs BTC volatility
8. **Momentum**: Cross-sectional momentum (ALT vs major performance)

### Regime Classification

The composite score (weighted sum of z-scored features) is classified into regimes:

**5-Regime Mode** (current):
- `STRONG_RISK_ON_MAJORS` (score > 1.5): Very favorable for long majors/short alts
- `WEAK_RISK_ON_MAJORS` (score > 0.5): Moderately favorable
- `BALANCED` (-0.5 < score < 0.5): Neutral, no positions
- `WEAK_RISK_ON_ALTS` (score < -0.5): Unfavorable
- `STRONG_RISK_ON_ALTS` (score < -1.5): Very unfavorable

**3-Regime Mode** (alternative):
- `RISK_ON_MAJORS` (score > 0.5): Favorable
- `BALANCED` (-0.5 < score < 0.5): Neutral
- `RISK_ON_ALTS` (score < -0.5): Unfavorable

## The Backtest Strategy

### When Do We Trade?

**We ONLY trade when regime is RISK_ON_MAJORS** (or STRONG/WEAK_RISK_ON_MAJORS in 5-regime mode):

```python
should_trade = (
    regime == "STRONG_RISK_ON_MAJORS" or 
    regime == "WEAK_RISK_ON_MAJORS" or 
    regime == "RISK_ON_MAJORS"
)
```

If regime is BALANCED or RISK_ON_ALTS, we exit all positions.

### What Positions Do We Take?

**LONG: BTC and ETH** (the "majors")
**SHORT: Basket of top 20 liquid ALTs** (selected dynamically)

### Portfolio Construction

The strategy uses **dual-beta neutral** construction:

1. **Build ALT Basket**: 
   - Select top 20 liquid ALTs (by volume, market cap)
   - Apply filters: volatility, correlation to BTC/ETH, momentum
   - Weight by inverse volatility (less volatile = higher weight)
   - **ALT weights are NEGATIVE** (short positions)

2. **Size Majors (BTC/ETH)**:
   - Estimate each ALT's beta to BTC and ETH (using 60-day rolling ridge regression)
   - Calculate total ALT beta exposure
   - Size BTC/ETH to **offset the ALT beta exposure** (beta neutrality)
   - **Major weights are POSITIVE** (long positions)

3. **Result**:
   - ~50% short ALT exposure
   - ~50% long BTC/ETH exposure
   - Beta-neutral to BTC and ETH (isolates ALT-specific returns)

### PnL Calculation

```python
# ALT returns (SHORT positions)
for alt_id, weight in alt_weights.items():
    ret = (price_curr / price_prev) - 1.0
    pnl += -weight * ret  # Negative weight × return = profit when ALT goes down

# Major returns (LONG positions)  
for major_id, weight in major_weights.items():
    ret = (price_curr / price_prev) - 1.0
    pnl += weight * ret  # Positive weight × return = profit when major goes up
```

**You profit when:**
- ALTs go DOWN (you're short)
- Majors go UP (you're long)
- ALTs underperform majors (relative performance)

**You lose when:**
- ALTs go UP (you're short)
- Majors go DOWN (you're long)
- ALTs outperform majors (relative performance)

## Example

**Day 1**: Regime = STRONG_RISK_ON_MAJORS
- Enter positions:
  - Long: 30% BTC, 20% ETH (50% total)
  - Short: -10% ALT1, -8% ALT2, ... (50% total ALT basket)

**Day 2**: 
- BTC: +2%, ETH: +1.5%
- ALT basket: -1% average
- PnL = (0.30 × 0.02) + (0.20 × 0.015) + (-0.50 × -0.01) = +0.006 + 0.003 + 0.005 = +1.4%

**Day 3**: Regime changes to BALANCED
- Exit all positions (close longs and shorts)
- No positions held

**Day 4**: Regime = RISK_ON_ALTS
- Stay out (no positions)

## Risk Management

1. **Stop-Loss**: Exit if cumulative loss exceeds -5% (volatility-adjusted, capped at -7.5%)
2. **Take-Profit**: Exit if cumulative return exceeds +10% or held for 30 days
3. **Volatility Targeting**: Scale positions to target 20% annualized volatility
4. **Regime Position Scaling**: Scale positions based on regime strength (STRONG = 100%, WEAK = 60%)

## Why This Strategy?

The hypothesis is that when the regime is "RISK_ON_MAJORS":
- Market favors BTC/ETH over alts
- ALTs underperform majors
- Shorting alts while longing majors captures this relative performance

The beta-neutral construction ensures you're not just betting on BTC/ETH direction, but on the **relative performance** between majors and alts.

## Current Performance Issues

Despite the strategy logic, the backtest shows:
- **Max Drawdown: -92%** (very high)
- **CAGR: -19.68%** (negative)
- **Sharpe: -0.34** (negative)

This suggests either:
1. The regime detection isn't working well
2. The strategy is fundamentally flawed for this period
3. Risk management needs improvement (which we're working on)
