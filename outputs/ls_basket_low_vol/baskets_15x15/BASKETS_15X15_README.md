# 15+15 Long/Short Baskets

**10 candidate baskets** (5 equal-weight + 5 optimized), each with **15 longs + 15 shorts**, dollar-neutral, ranked by realized volatility.

## Top 5 Baskets (by Volatility)

| Rank | Type       | Strategy         | Vol (ann) | Max DD | Turnover |
|------|------------|------------------|-----------|--------|----------|
| 1    | optimized  | momentum_rank    | 24.88%    | -36.2% | 3.31%    |
| 2    | equal_weight | momentum_rank  | 30.07%    | -30.1% | 3.27%    |
| 3    | optimized  | greedy_seq       | 34.70%    | -46.3% | 2.18%    |
| 4    | optimized  | correlation_pairs| 35.03%   | -46.4% | 1.88%    |
| 5    | optimized  | minvar_rank      | 35.03%    | -46.4% | 1.88%    |

## Strategies

1. **momentum_rank** — Bottom 15 by 30d return = longs (mean reversion), top 15 = shorts
2. **minvar_rank** — Lowest 15 vol = longs, highest 15 vol = shorts
3. **correlation_pairs** — Cluster by return correlation, 1 long + 1 short per cluster
4. **greedy_seq** — Greedily add (long, short) pairs to minimize basket variance
5. **factor_neutral** — Pick 15+15 by PCA loadings to neutralize first factor

## Outputs

- `runs/rankN_{type}_{strategy}_weights.csv` — 15 longs + 15 shorts with weights
- `runs/rankN_{type}_{strategy}_daily_pnl.csv` — Daily P&L series
- `runs/rankN_{type}_{strategy}_summary.csv` — Metrics
- `summary.md` — Full ranking table
- `metadata.json` — Run metadata

## Charts (for presentation)

- `chart_00_executive_summary.png` — One-page overview: top 3 equity curves, vol ranking, key metrics
- `chart_01_equity_curves.png` — Portfolio value over time for top 5 baskets
- `chart_02_volatility_comparison.png` — Bar chart: volatility by basket (lower = better)
- `chart_03_risk_metrics.png` — Volatility, max drawdown, turnover for top 5
- `chart_04_best_basket_detail.png` — Best basket: equity curve + rolling volatility
- `chart_05_best_basket_weights.png` — Best basket: top 10 longs and shorts

Generate charts: `python scripts/ls_basket_low_vol/generate_basket_charts.py`

## Run

```bash
python run_baskets_15x15.py
```
