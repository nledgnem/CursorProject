#!/usr/bin/env python3
"""Compare baseline (no OI/funding) vs enhanced (with OI/funding) backtest results."""

import pandas as pd
import json
from pathlib import Path

print("=" * 70)
print("BASELINE vs ENHANCED BACKTEST COMPARISON")
print("=" * 70)

# Load enhanced results (with OI + Funding)
enhanced_kpis_path = Path("reports/majors_alts/kpis.json")
baseline_kpis_path = Path("reports/majors_alts_baseline/kpis.json")

if enhanced_kpis_path.exists():
    enhanced_kpis = json.load(open(enhanced_kpis_path))
    enhanced_pnl = pd.read_csv("reports/majors_alts/bt_daily_pnl.csv")
    
    print("\nENHANCED RUN (with OI + Funding):")
    print(f"  CAGR: {enhanced_kpis['cagr']*100:.2f}%")
    print(f"  Sharpe: {enhanced_kpis['sharpe']:.3f}")
    print(f"  Sortino: {enhanced_kpis['sortino']:.3f}")
    print(f"  Max DD: {enhanced_kpis['max_drawdown']*100:.2f}%")
    print(f"  Calmar: {enhanced_kpis['calmar']:.3f}")
    print(f"  Hit Rate: {enhanced_kpis['hit_rate']*100:.2f}%")
    print(f"  Total PnL: {enhanced_pnl['pnl'].sum()*100:.2f}%")
    print(f"  Trading Days: {(enhanced_pnl['pnl'] != 0).sum()} / {len(enhanced_pnl)}")
else:
    print("\n[ERROR] Enhanced results not found")
    enhanced_kpis = None
    enhanced_pnl = None

if baseline_kpis_path.exists():
    baseline_kpis = json.load(open(baseline_kpis_path))
    baseline_pnl = pd.read_csv("reports/majors_alts_baseline/bt_daily_pnl.csv")
    
    print("\n\nBASELINE RUN (no OI, no Funding):")
    print(f"  CAGR: {baseline_kpis['cagr']*100:.2f}%")
    print(f"  Sharpe: {baseline_kpis['sharpe']:.3f}")
    print(f"  Sortino: {baseline_kpis['sortino']:.3f}")
    print(f"  Max DD: {baseline_kpis['max_drawdown']*100:.2f}%")
    print(f"  Calmar: {baseline_kpis['calmar']:.3f}")
    print(f"  Hit Rate: {baseline_kpis['hit_rate']*100:.2f}%")
    print(f"  Total PnL: {baseline_pnl['pnl'].sum()*100:.2f}%")
    print(f"  Trading Days: {(baseline_pnl['pnl'] != 0).sum()} / {len(baseline_pnl)}")
    
    # Comparison
    if enhanced_kpis:
        print("\n\n" + "=" * 70)
        print("COMPARISON: ENHANCED vs BASELINE")
        print("=" * 70)
        
        metrics = [
            ("CAGR", "cagr", lambda x: x*100, "%"),
            ("Sharpe Ratio", "sharpe", lambda x: x, ""),
            ("Sortino Ratio", "sortino", lambda x: x, ""),
            ("Max Drawdown", "max_drawdown", lambda x: x*100, "%"),
            ("Calmar Ratio", "calmar", lambda x: x, ""),
            ("Hit Rate", "hit_rate", lambda x: x*100, "%"),
        ]
        
        print("\nMetric Comparison:")
        print(f"{'Metric':<20} {'Enhanced':>12} {'Baseline':>12} {'Delta':>12} {'Better':>10}")
        print("-" * 70)
        
        for metric_name, key, transform, unit in metrics:
            enhanced_val = transform(enhanced_kpis[key])
            baseline_val = transform(baseline_kpis[key])
            delta = enhanced_val - baseline_val
            
            # Determine which is better
            if key in ["cagr", "sharpe", "sortino", "calmar", "hit_rate"]:
                better = "Enhanced" if delta > 0 else "Baseline"
            else:  # max_drawdown (lower is better)
                better = "Enhanced" if delta < 0 else "Baseline"
            
            print(f"{metric_name:<20} {enhanced_val:>11.2f}{unit:>1} {baseline_val:>11.2f}{unit:>1} {delta:>+11.2f}{unit:>1} {better:>10}")
        
        # Trading activity comparison
        enhanced_trading = (enhanced_pnl['pnl'] != 0).sum()
        baseline_trading = (baseline_pnl['pnl'] != 0).sum()
        print(f"\nTrading Activity:")
        print(f"  Enhanced: {enhanced_trading} days ({enhanced_trading/len(enhanced_pnl)*100:.1f}%)")
        print(f"  Baseline: {baseline_trading} days ({baseline_trading/len(baseline_pnl)*100:.1f}%)")
        print(f"  Delta: {enhanced_trading - baseline_trading} days")
        
        # Regime distribution comparison
        if 'regime' in enhanced_pnl.columns and 'regime' in baseline_pnl.columns:
            print(f"\nRegime Distribution:")
            enhanced_regimes = enhanced_pnl['regime'].value_counts()
            baseline_regimes = baseline_pnl['regime'].value_counts()
            
            all_regimes = set(enhanced_regimes.index) | set(baseline_regimes.index)
            print(f"{'Regime':<25} {'Enhanced':>12} {'Baseline':>12} {'Delta':>12}")
            print("-" * 70)
            for regime in sorted(all_regimes):
                e_count = enhanced_regimes.get(regime, 0)
                b_count = baseline_regimes.get(regime, 0)
                delta = e_count - b_count
                print(f"{regime:<25} {e_count:>12} {b_count:>12} {delta:>+12}")
        
        print("\n" + "=" * 70)
        print("CONCLUSION")
        print("=" * 70)
        
        # Determine overall winner
        sharpe_improvement = enhanced_kpis['sharpe'] - baseline_kpis['sharpe']
        sortino_improvement = enhanced_kpis['sortino'] - baseline_kpis['sortino']
        cagr_improvement = enhanced_kpis['cagr'] - baseline_kpis['cagr']
        dd_improvement = baseline_kpis['max_drawdown'] - enhanced_kpis['max_drawdown']  # Lower is better
        
        improvements = []
        if sharpe_improvement > 0:
            improvements.append(f"Sharpe +{sharpe_improvement:.3f}")
        if sortino_improvement > 0:
            improvements.append(f"Sortino +{sortino_improvement:.3f}")
        if cagr_improvement > 0:
            improvements.append(f"CAGR +{cagr_improvement*100:.2f}%")
        if dd_improvement > 0:
            improvements.append(f"Max DD {dd_improvement*100:.2f}% better")
        
        if improvements:
            print(f"\nEnhanced version shows improvements:")
            for imp in improvements:
                print(f"  - {imp}")
        else:
            print(f"\nBaseline version performs better or equal")
        
        print(f"\nRecommendation:")
        if sharpe_improvement > 0.1 or sortino_improvement > 0.1:
            print("  -> Use Enhanced version (OI + Funding features add value)")
        elif sharpe_improvement < -0.1 or sortino_improvement < -0.1:
            print("  -> Use Baseline version (OI + Funding features not helpful)")
        else:
            print("  -> Features have minimal impact, consider other factors")
else:
    print("\n[WAITING] Baseline results not yet available")
    print("  The baseline backtest is still running...")
    print("  Check back later or monitor the process")

print("\n" + "=" * 70)
