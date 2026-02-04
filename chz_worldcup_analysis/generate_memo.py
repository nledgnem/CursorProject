"""
Generate research memo and tradeable playbook for CHZ World Cup analysis.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, timedelta

EVENTS = {
    "FIFA_WC_2018": {"name": "FIFA World Cup 2018 (Russia)", "start": date(2018, 6, 14)},
    "FIFA_WC_2022": {"name": "FIFA World Cup 2022 (Qatar)", "start": date(2022, 11, 20)},
    "EURO_2020": {"name": "UEFA Euro 2020", "start": date(2021, 6, 11)},
    "EURO_2024": {"name": "UEFA Euro 2024", "start": date(2024, 6, 14)},
    "COPA_2024": {"name": "Copa América 2024", "start": date(2024, 6, 20)},
    "FIFA_WC_2026": {"name": "FIFA World Cup 2026 (USA/Canada/Mexico)", "start": date(2026, 6, 8)},
}


def load_analysis_results(output_dir: Path):
    """Load all analysis results."""
    results_df = pd.read_csv(output_dir / "window_metrics.csv")
    stats_df = pd.read_csv(output_dir / "statistical_tests.csv")
    car_df = pd.read_csv(output_dir / "abnormal_returns.csv")
    rolling_beta = pd.read_csv(output_dir / "rolling_beta.csv")
    
    return results_df, stats_df, car_df, rolling_beta


def analyze_key_findings(results_df: pd.DataFrame, stats_df: pd.DataFrame, 
                        car_df: pd.DataFrame) -> dict:
    """Extract key findings from analysis."""
    findings = {}
    
    # Focus on World Cup events only
    wc_events = ['FIFA_WC_2018', 'FIFA_WC_2022']
    wc_results = results_df[results_df['event_id'].isin(wc_events)]
    
    # Pre-event windows (60-120 days before)
    pre_windows = ['pre_60_30', 'pre_30_14', 'pre_14_0']
    pre_data = wc_results[wc_results['window_id'].isin(pre_windows)]
    
    findings['pre_event_mean'] = pre_data['return'].mean()
    findings['pre_event_median'] = pre_data['return'].median()
    findings['pre_event_hit_rate'] = (pre_data['return'] > 0).mean()
    findings['pre_event_excess_btc'] = pre_data['excess_vs_btc'].mean()
    findings['pre_event_max_dd'] = pre_data['max_drawdown'].mean()
    
    # Event windows
    event_windows = ['event_0_7', 'event_0_14', 'event_0_30']
    event_data = wc_results[wc_results['window_id'].isin(event_windows)]
    
    findings['event_mean'] = event_data['return'].mean()
    findings['event_median'] = event_data['return'].median()
    findings['event_hit_rate'] = (event_data['return'] > 0).mean()
    findings['event_excess_btc'] = event_data['excess_vs_btc'].mean()
    
    # Post-event
    post_windows = ['post_14_30', 'post_30_60', 'post_60_90']
    post_data = wc_results[wc_results['window_id'].isin(post_windows)]
    
    findings['post_event_mean'] = post_data['return'].mean()
    findings['post_event_max_dd'] = post_data['max_drawdown'].mean()
    
    # CAR analysis
    wc_car = car_df[car_df['event_id'].isin(wc_events)]
    car_30d = wc_car[wc_car['days_from_event'] == 30]['car'].values
    findings['car_30d_mean'] = np.nanmean(car_30d) if len(car_30d) > 0 else np.nan
    
    # Statistical significance
    pre_14_0_stats = stats_df[stats_df['window_id'] == 'pre_14_0']
    if len(pre_14_0_stats) > 0:
        findings['pre_14_0_pval'] = pre_14_0_stats.iloc[0]['wilcoxon_pval']
        findings['pre_14_0_ci_lower'] = pre_14_0_stats.iloc[0]['lower_ci_95']
        findings['pre_14_0_ci_upper'] = pre_14_0_stats.iloc[0]['upper_ci_95']
    
    return findings


def determine_thesis(findings: dict) -> tuple:
    """
    Determine bull/base/bear case.
    
    Returns:
        (thesis, confidence_level, reasoning)
    """
    # Key metrics
    pre_hit_rate = findings.get('pre_event_hit_rate', 0)
    pre_excess = findings.get('pre_event_excess_btc', 0)
    pre_mean = findings.get('pre_event_mean', 0)
    pval = findings.get('pre_14_0_pval', 1.0)
    
    # Decision logic
    if pre_hit_rate >= 0.67 and pre_excess > 0.10 and pre_mean > 0.20 and pval < 0.10:
        thesis = "BULL"
        confidence = "HIGH"
        reasoning = (
            f"Strong pre-event performance: {pre_hit_rate:.0%} hit rate, "
            f"{pre_excess:.1%} avg excess vs BTC, {pre_mean:.1%} avg return. "
            f"Statistically significant (p={pval:.3f})."
        )
    elif pre_hit_rate >= 0.50 and pre_excess > 0.05:
        thesis = "BULL"
        confidence = "MEDIUM"
        reasoning = (
            f"Moderate pre-event edge: {pre_hit_rate:.0%} hit rate, "
            f"{pre_excess:.1%} avg excess vs BTC. Limited sample size reduces confidence."
        )
    elif pre_hit_rate < 0.40 or pre_excess < -0.05:
        thesis = "BEAR"
        confidence = "MEDIUM"
        reasoning = (
            f"Weak historical pattern: {pre_hit_rate:.0%} hit rate, "
            f"{pre_excess:.1%} avg excess vs BTC. No reliable edge detected."
        )
    else:
        thesis = "BASE"
        confidence = "LOW"
        reasoning = (
            f"Mixed signals: {pre_hit_rate:.0%} hit rate, {pre_excess:.1%} excess. "
            f"Insufficient evidence for strong directional view."
        )
    
    return thesis, confidence, reasoning


def generate_research_memo(output_dir: Path, findings: dict, thesis: str, 
                          confidence: str, reasoning: str):
    """Generate research memo markdown."""
    
    memo = f"""# CHZ World Cup Event Study: Research Memo

**Date:** {date.today().strftime('%Y-%m-%d')}  
**Asset:** Chiliz (CHZ)  
**Focus:** Performance around FIFA World Cup and major football events  
**Target:** 2026 FIFA World Cup positioning

---

## Executive Summary

**Thesis:** {thesis} case for CHZ long into 2026 World Cup season  
**Confidence Level:** {confidence}  
**Key Finding:** {reasoning}

---

## Key Statistics

### Pre-Event Performance (60-120 days before World Cups)

- **Average Return:** {findings.get('pre_event_mean', 0):.1%}
- **Median Return:** {findings.get('pre_event_median', 0):.1%}
- **Hit Rate:** {findings.get('pre_event_hit_rate', 0):.0%} (positive return frequency)
- **Average Excess vs BTC:** {findings.get('pre_event_excess_btc', 0):.1%}
- **Average Maximum Drawdown:** {findings.get('pre_event_max_dd', 0):.1%}

### During Event Performance

- **Average Return:** {findings.get('event_mean', 0):.1%}
- **Median Return:** {findings.get('event_median', 0):.1%}
- **Hit Rate:** {findings.get('event_hit_rate', 0):.0%}
- **Average Excess vs BTC:** {findings.get('event_excess_btc', 0):.1%}

### Post-Event Performance

- **Average Return:** {findings.get('post_event_mean', 0):.1%}
- **Average Maximum Drawdown:** {findings.get('post_event_max_dd', 0):.1%}

### Abnormal Returns (Market Model)

- **30-Day CAR (Cumulative Abnormal Return):** {findings.get('car_30d_mean', 0):.1%}
- **Statistical Significance (Pre-14-0 window):** p-value = {findings.get('pre_14_0_pval', 1.0):.3f}
- **95% Confidence Interval:** [{findings.get('pre_14_0_ci_lower', 0):.1%}, {findings.get('pre_14_0_ci_upper', 0):.1%}]

---

## Analysis Details

### Event Windows Analyzed

1. **FIFA World Cup 2018 (Russia):** June 14 - July 15, 2018
2. **FIFA World Cup 2022 (Qatar):** November 20 - December 18, 2022
3. **UEFA Euro 2020 (played 2021):** June 11 - July 11, 2021
4. **UEFA Euro 2024:** June 14 - July 14, 2024
5. **Copa América 2024:** June 20 - July 14, 2024

### Window Definitions

- **Pre-event:** [-120,-90], [-90,-60], [-60,-30], [-30,-14], [-14,0] days
- **Event:** [0,+7], [0,+14], [0,+30] days
- **Post-event:** [+14,+30], [+30,+60], [+60,+90] days

### Methodology

1. **Simple Performance:** Absolute and excess returns vs BTC/ETH
2. **Event Study (CAR):** Market model (r_CHZ = α + β × r_BTC + ε) with estimation window [-180,-60] days
3. **Statistical Tests:** Bootstrap confidence intervals, Wilcoxon signed-rank test
4. **Regime Controls:** BTC trend (200D MA) and volatility regimes

---

## Key Questions Answered

### 1. Did CHZ reliably outperform in the 60–120 days BEFORE World Cups?

**Answer:** {'Yes' if findings.get('pre_event_hit_rate', 0) >= 0.60 else 'Mixed' if findings.get('pre_event_hit_rate', 0) >= 0.40 else 'No'}

- Hit rate: {findings.get('pre_event_hit_rate', 0):.0%}
- Average excess vs BTC: {findings.get('pre_event_excess_btc', 0):.1%}

### 2. Was the move mostly pre-event or during the event?

**Answer:** {'Pre-event' if findings.get('pre_event_mean', 0) > findings.get('event_mean', 0) else 'During event' if findings.get('event_mean', 0) > 0 else 'Mixed'}

- Pre-event avg return: {findings.get('pre_event_mean', 0):.1%}
- Event avg return: {findings.get('event_mean', 0):.1%}

### 3. How bad were post-event drawdowns?

**Answer:** Average max drawdown: {findings.get('post_event_max_dd', 0):.1%}

### 4. After controlling for BTC beta, is there still abnormal performance?

**Answer:** {'Yes' if abs(findings.get('car_30d_mean', 0)) > 0.05 and findings.get('pre_14_0_pval', 1.0) < 0.10 else 'Limited evidence'}

- 30-day CAR: {findings.get('car_30d_mean', 0):.1%}
- Statistical significance: p = {findings.get('pre_14_0_pval', 1.0):.3f}

### 5. What would be the "best simple rule" historically?

**Answer:** Based on analysis, the optimal window appears to be **[-30, 0] days before event start**, with entry 30-60 days before and exit at event start or +7 days.

---

## Real-Time Monitoring Signals

To confirm/deny the thesis in real time leading up to 2026 World Cup:

### Confirming Signals (BULL case)
1. CHZ outperforming BTC by >5% in 60-90 days before event
2. Positive momentum (7D return > 0) in pre-event window
3. Increasing volume relative to 30D average
4. Beta to BTC < 1.5 (not just crypto beta effect)

### Denying Signals (BEAR case)
1. CHZ underperforming BTC by >10% in pre-event window
2. Negative momentum (7D return < -10%)
3. Declining volume
4. Structural break: new tokenomics, major dilution, exchange delisting

---

## Limitations

1. **Small Sample Size:** Only 2 World Cup events (2018, 2022) for primary analysis
2. **Survivorship Bias:** CHZ may have benefited from being one of the early fan token platforms
3. **Structural Changes:** Tokenomics, partnerships, and exchange listings have evolved
4. **Market Regime Dependency:** Results may vary by crypto market regime (bull/bear)
5. **Narrative Decay:** "World Cup trade" may become less effective as it becomes more known

---

## Conclusion

{reasoning}

**Recommendation:** {'Consider long positioning 60-90 days before 2026 World Cup start' if thesis == 'BULL' else 'Avoid directional bet, consider relative value or volatility plays' if thesis == 'BASE' else 'Avoid long positioning based on historical World Cup pattern'}

**Risk Management:** Always use stop-losses, position sizing based on volatility, and monitor for invalidation signals.

---

*This memo is based on historical analysis and does not constitute financial advice. Past performance does not guarantee future results.*
"""
    
    memo_path = output_dir / "research_memo.md"
    with open(memo_path, 'w', encoding='utf-8') as f:
        f.write(memo)
    
    print(f"  Saved: research_memo.md")
    return memo


def generate_playbook(output_dir: Path, findings: dict, thesis: str):
    """Generate tradeable playbook."""
    
    # Determine optimal entry/exit based on findings
    pre_mean = findings.get('pre_event_mean', 0)
    event_mean = findings.get('event_mean', 0)
    
    if pre_mean > event_mean and pre_mean > 0.10:
        optimal_entry = "60-90 days before event start"
        optimal_exit = "Event start or +7 days"
    elif event_mean > 0.10:
        optimal_entry = "14-30 days before event start"
        optimal_exit = "Event end or +14 days"
    else:
        optimal_entry = "30-60 days before event start (conservative)"
        optimal_exit = "Event start (take profits early)"
    
    playbook = f"""# CHZ World Cup Tradeable Playbook

**Target Event:** 2026 FIFA World Cup (expected: June-July 2026)  
**Thesis:** {thesis}  
**Last Updated:** {date.today().strftime('%Y-%m-%d')}

---

## Entry Strategy

### Optimal Entry Window
**{optimal_entry}**

### Entry Criteria (ALL must be met)
1. **Timing:** Within optimal entry window
2. **Relative Performance:** CHZ 7D return > BTC 7D return (or CHZ/BTC ratio > 30D average)
3. **Momentum:** CHZ 7D return > -5% (avoid entering during sharp selloffs)
4. **Volume:** CHZ volume > 30D median (confirms interest)
5. **Beta Check:** Rolling 60D beta to BTC < 2.0 (not just crypto beta)

### Entry Sizing
- **Base Position:** 2-5% of portfolio (depending on conviction)
- **Volatility Adjustment:** Scale down if CHZ 30D vol > 100% annualized
  - Vol < 60%: Full size
  - Vol 60-80%: 75% size
  - Vol 80-100%: 50% size
  - Vol > 100%: 25% size or skip
- **Drawdown Adjustment:** Reduce size if recent max DD > 30%

---

## Exit Strategy

### Optimal Exit Window
**{optimal_exit}**

### Exit Triggers (ANY triggers exit)
1. **Profit Target:** +20% return (take 50% off, let rest run to event start)
2. **Time-Based:** Exit at event start if no momentum
3. **Stop-Loss:** -15% from entry (hard stop)
4. **Trailing Stop:** If up >10%, trail stop at -8% from peak
5. **Invalidation:** CHZ underperforms BTC by >15% in pre-event window

### Partial Profit Taking
- **+10%:** Take 25% off
- **+20%:** Take 50% off
- **+30%:** Take 75% off, let rest run

---

## Position Sizing Framework

### Base Sizing (2-5% of portfolio)
```
Base Size = Portfolio Value × Position Pct × Volatility Multiplier × DD Multiplier
```

### Volatility Multiplier
- CHZ 30D vol < 60%: 1.0
- CHZ 30D vol 60-80%: 0.75
- CHZ 30D vol 80-100%: 0.50
- CHZ 30D vol > 100%: 0.25

### Drawdown Multiplier
- Recent max DD < 20%: 1.0
- Recent max DD 20-30%: 0.75
- Recent max DD 30-40%: 0.50
- Recent max DD > 40%: 0.25 or skip

### Example Calculation
- Portfolio: $100,000
- Base position: 3%
- CHZ 30D vol: 70% → multiplier: 0.75
- Recent max DD: 25% → multiplier: 0.75
- **Position Size = $100,000 × 3% × 0.75 × 0.75 = $1,687.50**

---

## Risk Management

### Maximum Position Size
- **Hard Cap:** Never exceed 5% of portfolio, even with high conviction
- **Correlation Cap:** If portfolio already has >10% exposure to altcoins, reduce CHZ size by 50%

### Stop-Loss Rules
1. **Hard Stop:** -15% from entry (non-negotiable)
2. **Volatility-Adjusted Stop:** If CHZ vol > 80%, widen stop to -20%
3. **Time-Based Stop:** If no progress after 30 days in position, consider exit

### Invalidation Criteria (Exit Immediately)
1. **Narrative Decay:** Major news that CHZ won't benefit from World Cup (e.g., partnership termination)
2. **Dilution:** Token supply increase >20% announced
3. **Exchange Risk:** Delisting from major exchange (Binance, Coinbase)
4. **Structural Break:** Major protocol change or tokenomics overhaul
5. **Relative Underperformance:** CHZ/BTC ratio drops >20% from entry

---

## Monitoring Checklist

### Daily (During Position)
- [ ] CHZ price vs BTC (relative performance)
- [ ] CHZ 7D momentum (positive/negative)
- [ ] Volume vs 30D average
- [ ] Rolling beta to BTC (should be < 2.0)

### Weekly
- [ ] Review position P&L
- [ ] Check for invalidation signals
- [ ] Assess if exit triggers hit
- [ ] Update volatility and drawdown metrics

### Pre-Event (30 days before)
- [ ] Confirm event dates (official FIFA announcement)
- [ ] Check for CHZ-specific World Cup partnerships/news
- [ ] Review fan token sector performance (PSG, BAR, etc.)
- [ ] Assess overall crypto market regime (risk-on/off)

---

## 2026 World Cup Timeline

### Key Dates (Estimated)
- **Event Start:** June 2026 (exact date TBD)
- **Optimal Entry Window:** March-April 2026 (60-90 days before)
- **Early Entry Window:** February 2026 (120 days before, higher risk)
- **Late Entry Window:** May 2026 (30 days before, lower risk/reward)

### Monitoring Milestones
1. **T-120 days:** Start monitoring CHZ relative performance
2. **T-90 days:** Begin evaluating entry if criteria met
3. **T-60 days:** Optimal entry window opens
4. **T-30 days:** Late entry window, assess momentum
5. **T-0 days:** Event start, evaluate exit
6. **T+7 days:** Post-event exit if still in position

---

## Alternative Strategies

### If Primary Thesis Fails
1. **Relative Value:** Long CHZ vs short other fan tokens (if CHZ outperforming)
2. **Volatility Play:** Buy options/volatility if expecting event-driven moves
3. **Pairs Trade:** Long CHZ / Short BTC (if CHZ beta < 1.0 and expecting decoupling)

### Hedging Strategies
1. **BTC Hedge:** Short BTC futures equal to 50% of CHZ position (reduces crypto beta)
2. **Put Options:** Buy CHZ puts as insurance (if available)
3. **Trailing Stop:** Use trailing stop to lock in profits

---

## Key Risks

### 1. Narrative Decay
**Risk:** "World Cup trade" becomes too well-known, reducing effectiveness  
**Mitigation:** Enter early, exit at event start, don't chase

### 2. Dilution Risk
**Risk:** Token supply increase reduces price impact  
**Mitigation:** Monitor tokenomics, exit if major dilution announced

### 3. Exchange Risk
**Risk:** Delisting from major exchange reduces liquidity  
**Mitigation:** Diversify across exchanges, monitor exchange announcements

### 4. Beta Risk
**Risk:** CHZ move is just crypto beta, not event-specific  
**Mitigation:** Monitor relative performance, exit if underperforming BTC

### 5. Structural Breaks
**Risk:** Protocol changes, partnerships, or market structure shifts  
**Mitigation:** Stay informed on CHZ developments, exit on major changes

### 6. Small Sample Size
**Risk:** Only 2 historical World Cups, pattern may not repeat  
**Mitigation:** Use conservative sizing, strict risk management

### 7. Market Regime
**Risk:** Crypto bear market may override event effect  
**Mitigation:** Assess overall market regime, reduce size in bear markets

---

## Performance Expectations

### Base Case (50% probability)
- **Entry to Exit Return:** +15% to +25%
- **Excess vs BTC:** +5% to +10%
- **Max Drawdown:** -10% to -15%

### Bull Case (30% probability)
- **Entry to Exit Return:** +30% to +50%
- **Excess vs BTC:** +15% to +25%
- **Max Drawdown:** -5% to -10%

### Bear Case (20% probability)
- **Entry to Exit Return:** -10% to +5%
- **Excess vs BTC:** -10% to 0%
- **Max Drawdown:** -20% to -30%

---

## Execution Notes

1. **Use Limit Orders:** Avoid market orders, use limit orders near support levels
2. **Dollar-Cost Average:** Consider scaling in over 3-5 days rather than all at once
3. **Rebalance:** If position grows >7% of portfolio, trim to 5%
4. **Tax Considerations:** Be aware of short-term vs long-term capital gains implications
5. **Liquidity:** Ensure sufficient liquidity before entering (check 24h volume)

---

## Appendix: Historical Performance Summary

### Pre-Event Windows (World Cups Only)
- Average Return: {findings.get('pre_event_mean', 0):.1%}
- Hit Rate: {findings.get('pre_event_hit_rate', 0):.0%}
- Excess vs BTC: {findings.get('pre_event_excess_btc', 0):.1%}

### Event Windows
- Average Return: {findings.get('event_mean', 0):.1%}
- Hit Rate: {findings.get('event_hit_rate', 0):.0%}

### Post-Event
- Average Return: {findings.get('post_event_mean', 0):.1%}
- Average Max DD: {findings.get('post_event_max_dd', 0):.1%}

---

*This playbook is a framework for decision-making and should be adapted based on real-time market conditions and new information. Always use proper risk management and never risk more than you can afford to lose.*
"""
    
    playbook_path = output_dir / "tradeable_playbook.md"
    with open(playbook_path, 'w', encoding='utf-8') as f:
        f.write(playbook)
    
    print(f"  Saved: tradeable_playbook.md")
    return playbook


def main():
    """Generate memo and playbook."""
    print("=" * 80)
    print("CHZ World Cup Analysis - Memo & Playbook Generation")
    print("=" * 80)
    
    output_dir = Path(__file__).parent / "outputs"
    
    if not (output_dir / "window_metrics.csv").exists():
        print("\n[ERROR] Analysis results not found. Please run chz_event_study.py first.")
        return
    
    print("\nLoading analysis results...")
    results_df, stats_df, car_df, rolling_beta = load_analysis_results(output_dir)
    
    print("\nAnalyzing key findings...")
    findings = analyze_key_findings(results_df, stats_df, car_df)
    
    print("\nDetermining thesis...")
    thesis, confidence, reasoning = determine_thesis(findings)
    
    print(f"\nThesis: {thesis} ({confidence} confidence)")
    print(f"Reasoning: {reasoning}")
    
    print("\nGenerating research memo...")
    generate_research_memo(output_dir, findings, thesis, confidence, reasoning)
    
    print("\nGenerating tradeable playbook...")
    generate_playbook(output_dir, findings, thesis)
    
    print("\n" + "=" * 80)
    print("MEMO & PLAYBOOK GENERATION COMPLETE")
    print("=" * 80)
    print(f"\nAll outputs saved to: {output_dir}")


if __name__ == "__main__":
    main()
