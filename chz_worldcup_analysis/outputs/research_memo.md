# CHZ World Cup Event Study: Research Memo

**Date:** 2026-01-14  
**Asset:** Chiliz (CHZ)  
**Focus:** Performance around FIFA World Cup and major football events  
**Target:** 2026 FIFA World Cup positioning

---

## Executive Summary

**Thesis:** BEAR case for CHZ long into 2026 World Cup season  
**Confidence Level:** MEDIUM  
**Key Finding:** Weak historical pattern: 17% hit rate, 8.3% avg excess vs BTC. No reliable edge detected.

---

## Key Statistics

### Pre-Event Performance (60-120 days before World Cups)

- **Average Return:** 4.2%
- **Median Return:** -23.0%
- **Hit Rate:** 17% (positive return frequency)
- **Average Excess vs BTC:** 8.3%
- **Average Maximum Drawdown:** -31.2%

### During Event Performance

- **Average Return:** -38.8%
- **Median Return:** -31.9%
- **Hit Rate:** 0%
- **Average Excess vs BTC:** -39.5%

### Post-Event Performance

- **Average Return:** 0.6%
- **Average Maximum Drawdown:** -26.4%

### Abnormal Returns (Market Model)

- **30-Day CAR (Cumulative Abnormal Return):** -654.6%
- **Statistical Significance (Pre-14-0 window):** p-value = 0.125
- **95% Confidence Interval:** [-40.8%, -8.0%]

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

**Answer:** No

- Hit rate: 17%
- Average excess vs BTC: 8.3%

### 2. Was the move mostly pre-event or during the event?

**Answer:** Pre-event

- Pre-event avg return: 4.2%
- Event avg return: -38.8%

### 3. How bad were post-event drawdowns?

**Answer:** Average max drawdown: -26.4%

### 4. After controlling for BTC beta, is there still abnormal performance?

**Answer:** Limited evidence

- 30-day CAR: -654.6%
- Statistical significance: p = 0.125

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

Weak historical pattern: 17% hit rate, 8.3% avg excess vs BTC. No reliable edge detected.

**Recommendation:** Avoid long positioning based on historical World Cup pattern

**Risk Management:** Always use stop-losses, position sizing based on volatility, and monitor for invalidation signals.

---

*This memo is based on historical analysis and does not constitute financial advice. Past performance does not guarantee future results.*
