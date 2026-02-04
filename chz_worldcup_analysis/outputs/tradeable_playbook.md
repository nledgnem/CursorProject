# CHZ World Cup Tradeable Playbook

**Target Event:** 2026 FIFA World Cup (expected: June-July 2026)  
**Thesis:** BEAR  
**Last Updated:** 2026-01-14

---

## Entry Strategy

### Optimal Entry Window
**30-60 days before event start (conservative)**

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
**Event start (take profits early)**

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
- Average Return: 4.2%
- Hit Rate: 17%
- Excess vs BTC: 8.3%

### Event Windows
- Average Return: -38.8%
- Hit Rate: 0%

### Post-Event
- Average Return: 0.6%
- Average Max DD: -26.4%

---

*This playbook is a framework for decision-making and should be adapted based on real-time market conditions and new information. Always use proper risk management and never risk more than you can afford to lose.*
