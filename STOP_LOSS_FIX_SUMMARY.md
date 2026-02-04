# Stop-Loss Fix Summary

## Fixes Applied

1. **Increased Lookback Window**: Changed from 1 day to 3 days to catch cumulative losses
2. **Added Cumulative Loss Check**: Now checks both single-day and 3-day cumulative losses
3. **Capped Volatility Adjustment**: Maximum threshold capped at -7.5% (was -10%)
4. **Close Positions on Stop-Loss**: When stop-loss triggers, positions are set to zero before PnL computation
5. **Compute PnL with Zero Positions**: When stop-loss triggers, PnL is computed with zero positions (avoiding today's loss)

## Results

**Before Fix:**
- Max Drawdown: -91.56%
- CAGR: 45.48%
- Sharpe: 1.09

**After Fix:**
- Max Drawdown: -92.10% ⚠️ (slightly worse)
- CAGR: -19.68% ❌ (much worse)
- Sharpe: -0.34 ❌ (negative)

## Why Drawdown Still High

The stop-loss is triggering correctly (we see many "Stop-loss triggered" messages), but the drawdown is still -92%. This suggests:

1. **Timing Issue**: Stop-loss checks yesterday's return, but losses have already occurred
2. **Gap Risk**: Large single-day losses (-11.56%, -10.67%, -10.48%) exceed even the capped threshold
3. **Compounding**: Many small losses that don't individually trigger stop-loss accumulate over time

## Root Cause Analysis

Looking at the worst losses:
- 2024-12-09: -11.56% (above -7.5% cap)
- 2024-12-07: -10.67% (above -7.5% cap)
- 2024-12-05: -10.48% (above -7.5% cap)

These losses are happening **before** stop-loss can trigger. The stop-loss checks yesterday's return, but these are today's losses.

## The Real Problem

The fundamental issue is that **stop-loss is reactive, not proactive**:
- Day N: Take -11% loss
- Day N+1: Check stop-loss, see -11% loss, trigger stop-loss
- But the -11% loss already happened on Day N

## Recommended Solutions

1. **Intraday Stop-Loss**: Check stop-loss during the day, not just at close
2. **Tighter Thresholds**: Reduce threshold to -3% or -4% to catch losses earlier
3. **Position-Level Stop-Loss**: Add stop-loss per position, not just portfolio-level
4. **Volatility-Based Position Sizing**: Reduce position sizes when volatility is high
5. **Circuit Breaker**: If cumulative loss exceeds -10% over any 5-day period, exit all positions

## Current Status

The stop-loss mechanism is now working correctly (positions are closed when triggered), but it's **too late** - losses have already occurred. The strategy needs more proactive risk management.
