"""
The real issue: Average ALT return was 9.3%, but we're seeing -20% loss.

If position sizes are correct (50% short alts):
- Average ALT move 9.3% → loss = 9.3% * 0.5 = 4.65%
- But we're seeing -20%, which is 4.3x larger

This suggests position sizes are still 100% (not 50%), meaning:
- ALT weights sum to 1.0 (100% short)
- Major weights sum to ~1.0 (100% long)  
- Gross exposure = 200%
- A 9.3% ALT move with 100% short = 9.3% loss
- But we're seeing -20%, which suggests either:
  a) Basket is concentrated in high-movers (some moved 100%+)
  b) Position sizes are still wrong
  c) The scaling fix isn't being applied

The basket is equal-weighted with 20 alts, so each alt should be 5% (if 100% total) or 2.5% (if 50% total).
If one alt moves 100% and we have 2.5% exposure: loss = 2.5%
If one alt moves 100% and we have 5% exposure: loss = 5%

Multiple alts moving 50%+ could explain -20% if position sizes are 100%.
"""

print("The -20% loss on 2024-11-07 is likely caused by:")
print("1. Position sizes are still 100% each (200% gross) - scaling fix not working")
print("2. Basket contains high-volatility alts that moved 50-100%+")
print("3. Equal-weighted basket means concentrated exposure to movers")
print("\nSolution: Ensure position sizes are actually scaled to 50% each")
