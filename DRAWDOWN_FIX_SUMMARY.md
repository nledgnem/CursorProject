# Drawdown Investigation & Fix Summary

## Problem Identified
Max drawdown of -96% was caused by **excessive gross exposure** in the position sizing.

## Root Causes

### 1. Position Sizing Creates 200% Gross Exposure
- **ALT basket weights**: Sum to 1.0 (100% of notional short)
- **Major weights**: Sized to ~1.0 (100% of notional long) for dollar-neutrality
- **Total gross exposure**: 200% (100% short + 100% long)
- **Impact**: A -10% move in alts with 200% gross = -20% return (amplified 2x)

### 2. Extreme Daily Returns
- **Worst day**: -18.98% return on 2024-11-07
- **Cause**: High gross exposure amplifies market moves
- **Compounding**: Multiple large losses in Nov-Dec 2024 compound to -96% drawdown

### 3. Equity Curve Calculation
- **Issue**: Started at 1.0288 instead of 1.0
- **Fix**: Changed to `equity = 1.0 * cumprod(1 + returns)`

## Fixes Applied

### 1. Scaled Position Sizing
- **Before**: ALT weights = 100% short, Major weights = 100% long (200% gross)
- **After**: ALT weights = 50% short, Major weights = 50% long (100% gross)
- **Result**: Maintains dollar-neutrality while capping gross exposure at 100%

### 2. Fixed Equity Curve
- Start at 1.0, then compound returns correctly

### 3. Improved Major Sizing Logic
- Cap individual major weights at 50%
- Better beta-neutrality calculation

## Expected Impact

With 100% gross exposure (instead of 200%):
- Daily returns should be ~50% smaller
- Drawdown should be more reasonable (<30-40% max)
- Strategy risk profile more appropriate for long-short

## Next Steps

1. Verify new backtest results show improved drawdown
2. Check that gross exposure is actually capped at 100%
3. Validate that returns are more reasonable
4. Consider adding position sizing validation/logging
