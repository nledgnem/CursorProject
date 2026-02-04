# Monitor Evaluation Summary - 2-Year Baseline

> **For detailed technical explanation**: See `DETAILED_EXPLANATION_FOR_MADS.md` which covers:
> - What LS returns are and how they're calculated
> - What regime history represents
> - Complete backtest pipeline process
> - Monitor evaluation methodology
> - Technical implementation details

## Overview
Ran baseline monitor evaluation on **2 years of LS returns** (2024-01-07 to 2025-12-31) with **extended horizons** [5, 10, 20, 40, 60] trading days.

## Data Coverage
- **LS Returns**: 725 days (from backtest pipeline run `20260106_032842_453e33332daace30`)
- **Regime History**: 743 days (from `OwnScripts/regime_backtest/regime_history.csv`)
- **Aligned Dates**: 670 common trading days

## Key Results

### Edge Statistics (Regime 5=BEST vs Regime 1=WORST)

| Horizon | Spread (5-1) | P-value | Edge Best (5 vs All) | Edge Worst (1 vs All) | n1 | n5 | n_all |
|---------|--------------|---------|---------------------|----------------------|----|----|-------|
| H=5     | **-0.0038**  | 0.72    | -0.0022            | 0.0017               | 190| 133| 666   |
| H=10    | **-0.0004**  | 0.98    | 0.0048              | 0.0053               | 190| 130| 661   |
| H=20    | **-0.0116**  | 0.61    | -0.0010             | 0.0106               | 189| 129| 651   |
| H=40    | **-0.0242**  | 0.52    | -0.0096             | 0.0145               | 189| 125| 631   |
| H=60    | **-0.0217**  | 0.64    | 0.0004              | 0.0222               | 181| 125| 611   |

### Key Findings

1. **Signal is weak/inconsistent**: Regime 5 (BEST) does NOT consistently outperform Regime 1 (WORST)
2. **Direction flips**: At H=10, spread is essentially zero; at other horizons, Regime 1 often outperforms
3. **No statistical significance**: All p-values > 0.5 (bootstrap with block_size=10, n_boot=300)
4. **Sample sizes are adequate**: n1=181-190, n5=125-133 (well above minimum threshold of 30)

### Interpretation

The existing monitor's regime classification (1=WORST/RED, 5=BEST/GREEN) is **not aligned with actual forward LS return performance**. The monitor does not appear to have meaningful predictive power for forward returns at any horizon tested.

## Files Included

1. `regime_bucket_stats.csv` - Statistics (n, mean, median, std, sharpe_like, min, max) per regime bucket
2. `regime_edges.csv` - Edge statistics with bootstrap significance (p-values, 95% CIs)
3. `run_receipt.json` - Full run metadata, config snapshot, input hashes, date coverage
4. `configs/monitor_eval.yaml` - Configuration used (horizons, bootstrap settings, etc.)

## Configuration Used

- **Horizons**: [5, 10, 20, 40, 60] trading days
- **Block Bootstrap**: enabled, block_size=10, n_boot=300
- **Calendar**: inner join (drop_missing=true)
- **Forward Returns**: Explicitly excludes same-day (t+1..t+H only)

## Next Steps (if iterating)

1. Try weekly evaluation points (reduce overlap for H=5)
2. Experiment with different lookbacks/thresholds/smoothing in monitor
3. Consider alternative regime definitions or scoring methods

## Question for Review

**Is this monitor promising enough to iterate on, or should it be deprioritized?**

Given the weak signal and lack of statistical significance across all horizons, my recommendation is to **deprioritize** this monitor unless there's a strong hypothesis for why the regime scoring should work differently.

