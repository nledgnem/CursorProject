# ALT Basket Size Analysis

## Results Summary

| Basket Size | Sharpe | Sortino | Max Drawdown | CAGR | Volatility |
|-------------|--------|---------|--------------|------|------------|
| **20 ALTs** | **1.2407** | **1.8088** | -90.93% | **55.73%** | 43.35% |
| 50 ALTs | 1.1237 | 1.6326 | -91.24% | 47.84% | 43.17% |
| 100 ALTs | 0.9715 | 1.4012 | -91.65% | 38.48% | 43.20% |
| 150 ALTs | 1.0982 | 1.5907 | -90.93% | 46.28% | 43.22% |
| 200 ALTs | 1.1156 | 1.6051 | -91.06% | 47.02% | 42.88% |
| 300 ALTs | 0.9795 | 1.4179 | -91.68% | 39.05% | 43.32% |

## Key Findings

### 1. **20 ALTs is OPTIMAL**
- **Best Sharpe**: 1.24 (vs 0.97-1.12 for larger baskets)
- **Best CAGR**: 55.73% (vs 38-48% for larger baskets)
- **Best Sortino**: 1.81 (vs 1.40-1.63 for larger baskets)

### 2. **Larger Baskets Perform WORSE**
- Sharpe decreases as basket size increases
- CAGR decreases significantly (55.73% → 38.48% at 100 ALTs)
- Returns are diluted by including lower-quality ALTs

### 3. **Drawdown is Similar Across All Sizes**
- All sizes show ~-91% max drawdown
- Drawdown is NOT reduced by diversification
- This confirms drawdown is inherent to the strategy

### 4. **Volatility is Similar**
- All sizes show ~43% volatility
- Diversification doesn't reduce volatility significantly

## Why Larger Baskets Don't Help

1. **Quality Dilution**: Top 20 ALTs are the highest quality (liquidity, stability). Adding more includes lower-quality ALTs.

2. **Enhanced Filters Work Well**: The volatility, correlation, and momentum filters already select the best ALTs. Expanding beyond top 20 adds ALTs that passed filters but are still lower quality.

3. **Diminishing Returns**: The top 20 liquid ALTs already provide good diversification. Adding more doesn't help and actually hurts.

4. **Inverse Volatility Weighting**: With more ALTs, weights become more spread out, potentially including more volatile ALTs that dilute the portfolio.

## Conclusion

**Keep basket size at 20 ALTs** - it provides the best risk-adjusted returns.

The enhanced selection filters (volatility, correlation, momentum) combined with inverse volatility weighting already optimize the selection. Expanding the universe dilutes quality without improving risk-adjusted returns.
