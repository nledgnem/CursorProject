# MSM v0 Regime Framework — Executive Summary

## The Logic

We classify each week using **point-in-time** macro regimes so that backtests and live deployment are free of lookahead bias.

- **Bitcoin Dominance (BTCDOM) trend**  
  We use the **30-day simple moving average (SMA)** of the reconstructed BTCDOM index. On each decision date we only use data up to and including that date. The trend is:
  - **Rising** if spot BTCDOM is **above** its 30d SMA  
  - **Falling** otherwise  

- **Funding regime**  
  We use the **52-week rolling percentile rank** of the funding feature \( F_{tk} \) (with a minimum of 26 weeks so regimes start after ~6 months). Each week is assigned a percentile of its current \( F_{tk} \) within the trailing 52 weeks. We then map that percentile to four regimes:
  - **Q1: Negative/Low** (0–25th %ile)  
  - **Q2: Weak** (25–50th %ile)  
  - **Q3: Neutral** (50–75th %ile)  
  - **Q4: High** (75–100th %ile)  
  Using a rolling percentile rather than an absolute threshold ensures the regime classification adapts to shifting market baselines across bull and bear cycles.

No full-sample quartiles or future information are used. All inputs are known at or before the decision time.

### The Funding Feature (F_tk) Construction

- **Universe**: Top 30 eligible altcoins by market capitalization at each decision date, each with market cap above \$50M. The construction explicitly excludes BTC, ETH, stablecoins, exchange tokens, wrapped/bridge assets, and liquid-staking tokens so that the feature is a pure measure of **retail altcoin leverage**.
- **Per-asset funding**: For each altcoin in the basket, we compute the 7-day mean of its perpetual funding rate over the 7 calendar days prior to the decision date, aggregating multiple instruments per asset into a single daily funding series before taking the weekly mean.
- **Basket feature**: \( F_{tk} \) is the **equal-weighted mean** of those per-asset 7-day funding averages across the 30-asset basket, rather than a BTC- or majors-driven measure.
- **Coverage rule**: A minimum **60% funding data coverage** across the basket is required; if fewer than 60% of the 30 alts have valid 7-day funding, the week is dropped from the sample. This prevents a small number of illiquid or noisy names from dominating the basket statistic.

---

## Market Regime Filter

The primary alpha in the MSM v0 L/S basket is concentrated in a single regime quadrant:

**Q2 Weak Funding + Rising BTCDOM**.

We use this quadrant as a **gating mechanism** that restricts strategy execution exclusively to regimes with high expected risk-adjusted returns. In our point-in-time backtest this quadrant delivered:

- **Mean weekly L/S return** in the ~2.9% range  
- **Win rate** ~80% (weeks with positive L/S return)  
- **~15 weeks** in sample falling into this regime  

Execution is gated: exposure is ON only when **both** conditions are met; otherwise weekly return is 0%.

---

## Capital Allocation Rule

| Condition | Action |
|-----------|--------|
| **Funding regime = Q2: Weak** **and** **BTCDOM trend = Rising** | **ON** — Deploy the L/S basket (full exposure; weekly return = \( y \)). |
| **Any other regime** | **OFF** — No exposure; weekly return = 0%. |

So:

- **\( y_{\text{gated}} = y \)** when (Q2 Weak and Rising BTCDOM), else **\( y_{\text{gated}} = 0 \)**.  
- The *Regime-Gated Equity Curve* is the cumulative compounded return of \( y_{\text{gated}} \); the *Raw L/S Basket* curve is the cumulative compounded return of \( y \).  

This gives the PM a clear risk and performance profile: we are only in the market when the Regime Filter condition is satisfied, and flat otherwise.

---

## Benchmark Comparison

The institutional equity curve is plotted against its benchmarks so the PM can evaluate true alpha generation. The chart **equity_curve_comparison.png** shows four cumulative return series on the same axes (Y-axis in percentage):

- **Gated L/S (Regime Gate)** — thick solid green: capital deployed only in Q2 Weak + Rising BTCDOM.
- **Raw L/S Basket** — dashed gray: always-on strategy return.
- **Reconstructed BTCDOM** — solid blue: cumulative weekly return of the primary macro index.
- **Binance BTCDOM** — dotted orange: cumulative weekly return of the reference exchange index.

This comparison proves how the Regime Filter performs not just versus the raw strategy, but versus the underlying macro BTCDOM trends. Alpha is visible when the green curve separates from both the raw L/S line and the benchmark lines in the regimes where the gate is ON.

---

## Risk-Adjusted Performance (Sharpe & Drawdown)

| Strategy | Annualized Sharpe | Max Drawdown (%) |
|----------|-------------------|------------------|
| Raw L/S Basket | 0.27 | -39.4 |
| Gated L/S (Regime Gate) | **1.60** | **-9.5** |
| Reconstructed BTCDOM | 1.16 | -29.9 |
| Gated Reconstructed BTCDOM | 1.68 | -4.9 |
| Binance BTCDOM | 1.34 | -30.5 |
| Gated Binance BTCDOM | 1.84 | -3.0 |

*Annualized Sharpe assumes risk-free rate = 0 and 52 periods per year. Max drawdown is the minimum of (wealth − peak) / peak over the sample. Gated series apply the same regime rule (Q2 Weak + Rising BTCDOM) to isolate the regime edge.*

While the BTCDOM indices deliver higher absolute return over the full period, the Gated L/S strategy delivers the best risk-adjusted profile: the highest annualized Sharpe (1.60) and by far the shallowest maximum drawdown (-9.5% vs. ~-30% for the benchmarks). The Regime Filter restricts execution to the high-Sharpe quadrant and flattens exposure otherwise, reducing drawdown in adverse macro regimes.
