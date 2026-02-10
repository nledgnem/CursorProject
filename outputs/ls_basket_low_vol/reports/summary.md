# LS Basket Low-Vol Report

## Universe QC
- Period: 2023-01-01 to 2024-12-31
- Universe size: 383
- Min mcap USD: 10,000,000
- Min 14d avg volume USD: 1,000,000

## Top 3 Baskets

### Rank 1: Method A (params: {'G': 1.0, 'alpha': 0.5, 'beta': 0.1, 'max_w_abs': 0.1})
- Realized Vol (ann): 6.96%
- Kurtosis: 31.99
- CVaR 95%: 1.10%
- CVaR 99%: 2.32%
- Max Drawdown: -16.04%
- Avg Turnover: 0.40%
- Long/Short Corr: -0.93

## Recommendation
**Recommended: Method A** with params {'G': 1.0, 'alpha': 0.5, 'beta': 0.1, 'max_w_abs': 0.1}. 
*Note: Long/short correlation (-0.93) is below target (>=0.8). No candidate met all constraints; this is the lowest-volatility basket. Consider relaxing constraints or adjusting method parameters.*