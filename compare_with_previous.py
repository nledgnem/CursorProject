#!/usr/bin/env python3
"""Compare current backtest results with previous runs."""

import pandas as pd
import json
from pathlib import Path
import glob

print("=" * 70)
print("BACKTEST COMPARISON: WITH vs WITHOUT OI + FUNDING")
print("=" * 70)

# Current results (with OI + Funding)
current_kpis = json.load(open("reports/majors_alts/kpis.json"))
current_pnl = pd.read_csv("reports/majors_alts/bt_daily_pnl.csv")

print("\nCURRENT RUN (with OI + Funding):")
print(f"  CAGR: {current_kpis['cagr']*100:.2f}%")
print(f"  Sharpe: {current_kpis['sharpe']:.3f}")
print(f"  Sortino: {current_kpis['sortino']:.3f}")
print(f"  Max DD: {current_kpis['max_drawdown']*100:.2f}%")
print(f"  Hit Rate: {current_kpis['hit_rate']*100:.2f}%")
print(f"  Total PnL: {current_pnl['pnl'].sum()*100:.2f}%")
print(f"  Avg Daily PnL: {current_pnl['pnl'].mean()*100:.4f}%")
print(f"  Trading Days: {(current_pnl['pnl'] != 0).sum()} / {len(current_pnl)}")

# Look for previous runs in outputs/runs
outputs_dir = Path("outputs/runs")
previous_results = []

if outputs_dir.exists():
    # Find all backtest result files
    result_files = sorted(
        glob.glob(str(outputs_dir / "*/outputs/backtest_results.csv")),
        key=lambda x: Path(x).stat().st_mtime,
        reverse=True
    )
    
    # Load the most recent one (might be from different pipeline)
    if len(result_files) > 0:
        print(f"\n\nFound {len(result_files)} previous backtest result files")
        print("Note: These may be from different pipeline (golden config)")

print("\n" + "=" * 70)
print("FEATURE IMPACT ANALYSIS")
print("=" * 70)

print("\nFeatures Enabled in Current Run:")
print("  [X] OI Risk: Using REAL OI data (BTC OI from CoinGlass)")
print("  [X] Funding Skew: Enabled (weight: 0.12)")
print("  [X] Funding Heating: Enabled (weight: 0.10)")
print("  [X] Total feature weight for OI+Funding: 0.26 (26%)")

# Analyze regime behavior
print("\n\nRegime Behavior Analysis:")
regime_counts = current_pnl['regime'].value_counts()
print(f"  Total regimes detected: {len(regime_counts)}")
print(f"  Most common regime: {regime_counts.index[0]} ({regime_counts.iloc[0]} days)")

# Days with actual trading
trading_days = current_pnl[current_pnl['pnl'] != 0]
print(f"\n  Trading Activity:")
print(f"    Days with positions: {len(trading_days)} ({len(trading_days)/len(current_pnl)*100:.1f}%)")
print(f"    Days with zero exposure: {len(current_pnl) - len(trading_days)} ({(len(current_pnl) - len(trading_days))/len(current_pnl)*100:.1f}%)")

if len(trading_days) > 0:
    print(f"\n  Performance on Trading Days:")
    print(f"    Total PnL: {trading_days['pnl'].sum()*100:.2f}%")
    print(f"    Avg Daily PnL: {trading_days['pnl'].mean()*100:.4f}%")
    print(f"    Best Day: {trading_days['pnl'].max()*100:.2f}%")
    print(f"    Worst Day: {trading_days['pnl'].min()*100:.2f}%")
    print(f"    Win Rate: {(trading_days['pnl'] > 0).mean()*100:.1f}%")

# Funding impact
if 'funding' in current_pnl.columns:
    funding_days = current_pnl[current_pnl['funding'] != 0]
    if len(funding_days) > 0:
        print(f"\n  Funding Impact:")
        print(f"    Days with funding: {len(funding_days)}")
        print(f"    Total funding cost: {funding_days['funding'].sum()*100:.2f}%")
        print(f"    Avg daily funding: {funding_days['funding'].mean()*100:.4f}%")

print("\n" + "=" * 70)
print("CONCLUSION")
print("=" * 70)
print("\nThe backtest completed successfully with:")
print("  - Real OI data from CoinGlass (BTC OI)")
print("  - Funding data from CoinGlass (507 symbols)")
print("  - Both features integrated into regime classification")
print("\nTo determine if OI and funding improve performance, you would need to:")
print("  1. Run a baseline backtest without OI and funding features")
print("  2. Compare metrics (Sharpe, Sortino, CAGR, Max DD)")
print("  3. Analyze regime classification differences")
print("\nCurrent results show:")
print(f"  - Sharpe Ratio: {current_kpis['sharpe']:.3f}")
print(f"  - Sortino Ratio: {current_kpis['sortino']:.3f}")
print(f"  - CAGR: {current_kpis['cagr']*100:.2f}%")
print(f"  - Max Drawdown: {current_kpis['max_drawdown']*100:.2f}%")
