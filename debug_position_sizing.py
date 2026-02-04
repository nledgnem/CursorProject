"""
Debug the actual position sizing to understand why returns are so extreme.
"""

import polars as pl
import numpy as np

bt = pl.read_csv('reports/majors_alts/bt_daily_pnl.csv').sort('date')

print("=== POSITION SIZING DEBUG ===\n")

# Check if PnL magnitude matches return magnitude
print("1. PnL vs Return Analysis:")
print(f"   Average |PnL|: {bt['pnl'].abs().mean():.6f}")
print(f"   Average |r_ls_net|: {bt['r_ls_net'].abs().mean():.6f}")
print(f"   Ratio: {bt['pnl'].abs().mean() / bt['r_ls_net'].abs().mean():.4f}")

# The formula is: r_ls_net = pnl - cost - funding
# So pnl should be approximately r_ls_net + cost + funding
print(f"\n2. Formula Check:")
example = bt.head(1).to_dicts()[0]
print(f"   Example day: {example['date']}")
print(f"   PnL: {example['pnl']:.6f}")
print(f"   Cost: {example['cost']:.6f}")
print(f"   Funding: {example['funding']:.6f}")
print(f"   r_ls_net: {example['r_ls_net']:.6f}")
print(f"   Expected: {example['pnl'] - example['cost'] - example['funding']:.6f}")
print(f"   Actual: {example['r_ls_net']:.6f}")
print(f"   Match: {abs((example['pnl'] - example['cost'] - example['funding']) - example['r_ls_net']) < 0.0001}")

# Check worst day in detail
worst = bt.sort('r_ls_net').head(1).to_dicts()[0]
print(f"\n3. Worst Day Analysis ({worst['date']}):")
print(f"   Return: {worst['r_ls_net']:.4f} ({worst['r_ls_net']*100:.2f}%)")
print(f"   PnL: {worst['pnl']:.4f}")
print(f"   Cost: {worst['cost']:.6f}")
print(f"   Funding: {worst['funding']:.6f}")

# Calculate what position size would give this return
# If r_ls_net = -0.20, and we assume:
# - ALT basket moved +20% (against us, since we're short)
# - Major basket moved +10% (with us, since we're long)
# Then: pnl = -weight_alt * 0.20 + weight_major * 0.10
# If weight_alt = 0.5 and weight_major = 0.5:
#   pnl = -0.5 * 0.20 + 0.5 * 0.10 = -0.10 + 0.05 = -0.05 (5% loss)
# But we're seeing -20% loss, which suggests weights are 2x too large

print(f"\n4. Position Size Hypothesis:")
print(f"   If ALT weights = 0.5 (50% short) and major weights = 0.5 (50% long):")
print(f"   Gross exposure = 0.5 + 0.5 = 1.0 (100%)")
print(f"   A 20% ALT move: pnl = -0.5 * 0.20 = -0.10 (10% loss)")
print(f"   But we're seeing -20% loss, suggesting weights are actually 1.0 each")
print(f"   This would mean: gross = 1.0 + 1.0 = 2.0 (200%)")

# Check if the issue is that we're not scaling correctly
print(f"\n5. Conclusion:")
print(f"   The -20% daily return suggests position sizes are still too large")
print(f"   Either:")
print(f"   a) The scaling fix isn't working (weights still sum to 1.0 each)")
print(f"   b) The PnL calculation is wrong (treating weights incorrectly)")
print(f"   c) Market moves are extreme (alts moving 40%+ in a day)")

# Check actual market moves on worst day
print(f"\n6. Need to check actual ALT and major price moves on worst day")
print(f"   This would tell us if the issue is position sizing or extreme market moves")
