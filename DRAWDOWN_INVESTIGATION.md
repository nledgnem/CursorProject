# Drawdown Investigation Report

## Problem
Max drawdown of -96% is unrealistically high for a long-short strategy.

## Root Causes Identified

### 1. Equity Curve Calculation Bug
- **Issue**: Equity curve starts at 1.0288 instead of 1.0
- **Cause**: First return (0.0288) is being applied incorrectly
- **Fix**: Start equity at 1.0, then compound: `equity = 1.0 * cumprod(1 + returns)`

### 2. Excessive Gross Exposure
- **Issue**: Position sizing creates >100% gross exposure
- **Current behavior**:
  - ALT weights sum to ~1.0 (100% short exposure)
  - Major weights can be large (e.g., 0.5 BTC + 0.5 ETH = 100% long)
  - Total gross exposure = 200%
- **Impact**: A -10% move in alts with 200% gross = -20% return (amplified)
- **Fix**: Cap total gross exposure and normalize weights properly

### 3. Flawed Major Sizing Logic
- **Issue**: `_size_majors_for_neutrality()` doesn't constrain gross exposure
- **Current formula**: `btc_weight = -alt_btc_exp / 2.0`
- **Problem**: If alt_btc_exp is large, this creates very large major positions
- **Fix**: Cap individual major weights and ensure total gross is reasonable

### 4. Extreme Daily Returns
- **Worst day**: -18.98% return on 2024-11-07
- **Cause**: Combination of:
  - High gross exposure (200%+)
  - Large ALT moves against position
  - Compounding effect over time
- **Impact**: Equity drops from 1.05 to 0.04 over ~2 months

## Recommended Fixes

1. **Fix equity curve calculation** (DONE)
   - Start at 1.0, compound correctly

2. **Constrain gross exposure** (PARTIAL)
   - Cap total gross at 150% or less
   - Normalize weights to ensure reasonable exposure

3. **Improve major sizing** (DONE)
   - Cap individual major weights
   - Better beta-neutrality calculation

4. **Add position sizing validation**
   - Log gross exposure on each day
   - Warn if gross > 150%
   - Cap positions if needed

## Next Steps

1. Re-run backtest with fixes
2. Verify equity curve starts at 1.0
3. Check that gross exposure is reasonable (<150%)
4. Verify drawdown is more reasonable (<30-40% max)
