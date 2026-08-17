# Does BTC trend agreement predict returns? An independent test of Coinbase's TrendScore

**Sample:** BTC 2015-07-20 → 2026-08-17 (Coinbase Exchange spot, daily UTC closes, 4,047 days; 3,662 usable signal days after the 365-day warm-up).
**DVOL sample:** 2021-03-24 → 2026-08-16 (Deribit, 1,972 days).
**All figures/tables:** `results/figures/`, `results/tables/`. Rerun: see `README.md`.

---

## Executive conclusion

1. **The replication is essentially exact.** Our 20-day forward returns by TrendScore are **-0.44% / +1.17% / +3.55% / +7.88%** against Coinbase's published **-0.7% / +1.3% / +3.8% / +7.7%**. Every bucket lands within 0.3pp. The monotonic ordering holds at *all six* horizons tested (1, 5, 10, 20, 30, 60 days). Coinbase reported the data honestly.

2. **On the full sample the effect clears autocorrelation-robust significance, but only just, and only at the extremes.** Score 3 − Score 0 = **+8.32pp**, Newey-West *t* = **2.82** (*p* = 0.005); 14 of 20 strictly non-overlapping phase subsamples are significant at 5%; block-bootstrap 95% CI **[+2.1pp, +14.5pp]**, P(spread ≤ 0) = 0.6%. Naive t-stats overstate significance by **3.1–3.7×** — anyone quoting an unadjusted *t* ≈ 13 here is wrong by a factor of ~3.7.

3. **Only Score 3 is individually distinguishable from zero** (HAC *t* = 3.54). Scores 0 and 1 are statistically indistinguishable from the unconditional mean (*t* = −0.21 and +0.79). The bootstrap puts the probability of a *strictly* monotonic 0<1<2<3 ordering at only **56%**. This is a "full agreement is different" result wearing the costume of a four-step ladder.

4. **It survives nearby lookbacks extremely well.** All 36 combinations of short ∈ {20,30,40} × medium ∈ {60,90,120} × long ∈ {250,300,365,400} produce a positive Score3−Score0 spread, ranging only **+6.3pp to +9.9pp**, with Spearman ρ between 0.089 and 0.153. Coinbase's 30/90/365 sits at the **67th percentile** of its own grid — a genuine plateau, not a lucky cell. This is the single strongest robustness result in the study.

5. **It does not survive out of sample.** Split at 2022-01-01: train spread **+14.17pp** (*t* = 2.15) → OOS spread **+2.76pp** (*t* = 0.99, *p* = 0.32), bootstrap CI **[−3.3pp, +8.5pp]** spans zero. By era: 2015-2019 +15.8pp, 2020-2022 +9.5pp, **2023-present +0.5pp** (ρ = 0.025, *p* = 0.37). The relationship has decayed monotonically across BTC's cycles and is currently absent.

6. **A single 200-day moving average does the same job on BTC.** Head-to-head (§9b): OOS conditional spread +3.53% vs SMA200's +3.14%, both insignificant; TrendScore adds nothing beyond the MA in an incremental HAC regression (OOS *p* = 0.311); the two agree on 86% of days. TrendScore's better OOS strategy Sharpe (0.97 vs 0.59) comes entirely from 2024–2025 — it *lost* to the MA in three of five OOS years. **For BTC, use the moving average.** On alts the ranking reverses — TrendScore beats the MA 2-3x in every era (bullet 13) — but that edge is not statistically established either.

7. **"Agreement" adds only modestly over a single trend filter.** Any one horizon alone gives a +4.3pp to +4.7pp spread; TrendScore ≥ 2 gives +5.2pp and TrendScore = 3 gives +5.9pp. In a joint HAC regression *none* of the three indicators is individually significant (*t* = 1.70 / 1.44 / 1.32) and each contributes ~+2.9pp. The composite is a mild noise-reduction over its parts, not a distinct phenomenon.

8. **BTC TrendScore predicts ETH and SOL better than it predicts BTC — and better than their own TrendScores do.** 20-day Score3−Score0: **ETH +12.1pp** (*t* = 2.29), **SOL +18.3pp** (*t* = 2.59) vs BTC's +8.3pp; each altcoin's *own* score manages only +7.7pp (*t* = 1.65) and +6.3pp (*t* = 0.89). At 60 days: ETH +44.5pp, SOL +75.3pp. **But the era decay is identical** (ETH +14.0pp → +2.4pp, SOL +24.5pp → +6.4pp OOS), so this is a *larger* effect, not a *more robust* one — and the mechanism that would have explained it is rejected in bullet 12.

9. **The DVOL overlay at Coinbase's implied threshold is a no-op.** DVOL exceeded 90 on only **8.5%** of days in its history. At threshold 90 the overlay moves Strategy B's Sharpe 0.766 → 0.759 and max drawdown −45.30% → −45.29%. Lower thresholds do help (60 → Sharpe 0.83, MaxDD −37.6%), but that is the best cell of a 7-point grid chosen on the full sample and must not be read as out-of-sample evidence.

10. **Coinbase's specific claim that implied vol handles acute crashes better than slow bears is correct — and is the overlay's main problem.** In the May-2021 crash the brake was active on **67%** of days (mean DVOL 98). In the Nov-2021 → Nov-2022 bear (−77%) it was active on **11%**. In the three drawdowns since 2024 (−26%, −28%, −53%) it was active on **0%** of days, because DVOL has structurally declined (median 55.7, 95th percentile 94). A fixed 90 threshold is decaying into a dead switch.

11. **The drawdown brake reliably cuts drawdown; its risk-adjusted benefit is sample-dependent.** Full sample it takes Strategy B from −77.9% to −51.5% max drawdown and Calmar 0.96 → 1.17 (Sharpe 1.33 → 1.42) at a cost of 14pp of CAGR. On the 2021+ subsample it *hurts*: Sharpe 0.766 → 0.629, Calmar 0.633 → 0.538, though max drawdown still improves (−45.3% → −39.1%).

12. **The crypto-beta mechanism was tested on a broad alt basket and rejected** (§7b). On 384 point-in-time Binance pairs the effect does *not* scale with beta — the rank correlation between beta quintile and spread is **−0.65** full-sample and **−0.97** in training, with the *lowest*-beta quintile showing the *largest* spread. Betas span 0.81–1.51, so proportional scaling would have put Q5 at ~1.86× Q1; observed ≈0.7×. This was the pre-registered failure condition, so the SOL result in §9b is treated as multiple-testing noise.

13. **What did survive the basket test is narrow but consistent: TrendScore delivers 2–3× the SMA200 spread on alts in every era and every basket size** (+6.07% vs +2.09% full sample; +4.32% vs +1.83% OOS), and SMA200 is never significant on alts. But TrendScore is not significant out of sample either (*p* = 0.14), so this is a directional edge, not an established one.

**Recommendation: _Interesting but insufficient evidence_ — monitor it, do not gate anything on it.** The replication is exact and the parameter plateau is real, but nothing in this study is statistically significant out of sample once the beta mechanism is rejected. For BTC trend de-risking specifically, a 200-day moving average is the better engineering choice. See §12.

---

## 1. Method and conventions

**Signal.** For each date *t*, using closes up to and including 23:59:59 UTC on *t*:

```
R30(t)  = P(t)/P(t-30)  - 1
R90(t)  = P(t)/P(t-90)  - 1
R365(t) = P(t)/P(t-365) - 1
TrendScore(t) = 1[R30>0] + 1[R90>0] + 1[R365>0]
```

Every price series is reindexed onto a complete daily calendar and forward-filled before differencing, so `shift(30)` means exactly 30 calendar days, not 30 observations. Coinbase BTC-USD required **zero** fills over 4,047 days; ETH-USD required 2.

**Forward returns.** `F_h(t) = P(t+h)/P(t) - 1`. For the descriptive study this is the standard event-study convention Coinbase reports. It contains no look-ahead in the *signal*, but it is not directly tradable — you cannot transact at the instant of the close you used.

**Execution.** For all backtests, the signal from close(*t*) is executed at close(*t*+1) and first earns the return of day *t*+2 (`exposure.shift(2)`). That is one full day more conservative than the theoretical minimum. Sensitivities at lag 1 and lag 3 are in `15_strategy_lag_and_cash_sensitivity.csv`. Every strategy still beats buy-and-hold on Sharpe at all three lags, and D ranks first at each; the A-vs-C ordering does flip between lag 1 (C 1.36 > A 1.35) and lag 2 (A 1.38 > C 1.27), so fine-grained rankings among the maps are not stable to execution assumptions.

**Look-ahead controls.** `verify.py` runs five traps, all passing: (a) TrendScore(*t*) is unchanged when all prices after *t* are deleted; (b) shuffling only the future leaves every past score identical; (c) executed weight equals target weight lagged by exactly `SIGNAL_LAG`; (d) the drawdown multiplier on day *i* is reproducible from equity through *i*−1 alone (max abs diff 0.00e+00); (e) a randomly shuffled signal returns 3.2× against buy-and-hold's 225×.

**Data.** Coinbase Exchange BTC-USD/ETH-USD and Binance SOLUSDT as primaries, each cross-checked against the other venue. Daily-return correlations 0.991 / 0.994 / 0.9999; median absolute level differences 0.057% / 0.058% / 0.043%. Coinbase and Binance produce **identical TrendScores on 99.04%** of overlapping days, and the same 3−0 spread to within 0.15pp on the common window.

The project's own data lake was used as a third check only — `fact_price` carries BTC only from 2024-01-07 and the local copy is stale at 2026-01-05. That check surfaced a real issue documented in `README.md`: **`fact_price.date` is stamped one day later than the UTC close it represents.**

---

## 2. Core replication

`03_coinbase_replication_20d.csv` · `fig01_replication_20d.png`

| TrendScore | Coinbase claimed | Ours (mean) | Difference | Ours (median) | n | % of sample | Win rate |
|---|---|---|---|---|---|---|---|
| 0 | −0.70% | **−0.44%** | +0.26pp | +0.52% | 415 | 11.3% | 53.0% |
| 1 | +1.30% | **+1.17%** | −0.13pp | −0.16% | 902 | 24.6% | 49.3% |
| 2 | +3.80% | **+3.55%** | −0.25pp | +2.76% | 1,069 | 29.2% | 58.0% |
| 3 | +7.70% | **+7.88%** | +0.18pp | +3.03% | 1,276 | 34.8% | 58.7% |

This is as close to an exact replication as one could reasonably expect from a different price source over a slightly different window.

**But look at the median column.** Score 3's mean is +7.88% while its median is +3.03%, and its standard deviation (21.6%) is 60% higher than Score 0's (13.5%). The win-rate edge is only 58.7% vs 53.0% — 5.7pp. So roughly half the headline mean effect is right-tail skew, and Score 3 is also the **highest-risk** bucket. You are being paid partly for holding more volatility, not purely for better odds.

`fig02_multi_horizon_heatmap.png` and `08_multi_horizon_hac.csv` show the ordering is monotone at 1, 5, 10, 20, 30 and 60 days, with Score3−Score0 HAC *t* between 2.27 and 3.06 throughout. The consistency across horizons is real evidence — this is not a 20-day artefact.

---

## 3. Statistical rigour

Overlapping 20-day returns are mechanically MA(19) even under the null. Three independent routes were run and kept separate.

**(A) Are means different across scores?** Joint HAC Wald test that all score dummies are zero: *F* = 2.96, **p = 0.031**. Yes, marginally.

**(B) Is Score 3 materially better than Score 0?** Yes, by every route:

| Route | Estimate | Inference |
|---|---|---|
| HAC OLS (maxlags = 20) | +8.32pp | *t* = 2.82, *p* = 0.0048 |
| Non-overlapping, 20 phases | median +8.44pp (range +4.5 to +10.7) | median *p* = 0.034; 14/20 phases significant at 5% |
| Circular block bootstrap (60d blocks, 2,000 reps) | +8.09pp | 95% CI [+2.06pp, +14.47pp]; P(≤0) = 0.6% |

**(C) Is the relationship monotonic?** Much weaker. Per-score HAC means:

| Score | Mean | HAC *t* | Naive *t* | Inflation |
|---|---|---|---|---|
| 0 | −0.44% | −0.21 | −0.66 | 3.1× |
| 1 | +1.17% | +0.79 | +2.51 | 3.2× |
| 2 | +3.55% | +2.06 | +6.76 | 3.3× |
| 3 | +7.88% | **+3.54** | +13.05 | 3.7× |

Only Score 3 is individually significant. Bootstrap P(strictly monotonic) = **0.561** — barely better than a coin flip. Bootstrap Spearman ρ = 0.110, 95% CI **[−0.006, +0.222]**, i.e. the CI touches zero at the 2.5% tail (one-sided P(ρ ≤ 0) = 3.2%).

**Honest reading:** the *extremes* differ; the *ladder* is decorative. The defensible claim is "full trend agreement is followed by better returns than full disagreement", not "each additional agreeing horizon adds return".

### Does *agreement* add anything?

`12b_agreement_vs_single_horizon.csv` · `12c_horizon_horse_race_hac.csv`

| Signal | Mean(on) − Mean(off), 20D | HAC *t* |
|---|---|---|
| R30 > 0 alone | +4.35pp | 2.36 |
| R90 > 0 alone | +4.73pp | 2.37 |
| R365 > 0 alone | +4.25pp | 1.90 |
| TrendScore ≥ 2 | **+5.24pp** | **2.70** |
| TrendScore = 3 | **+5.92pp** | 2.52 |

In a joint regression with all three indicators, none is individually significant (*t* = 1.70 / 1.44 / 1.32) and each carries ~+2.9pp. The horizons contain roughly equal, partly-independent information; combining them raises the *t*-stat above any component. That is a real but modest benefit — TrendScore is a noise-reduced average of three trend filters, not a new phenomenon.

---

## 4. Robustness

### Subperiods — the effect is decaying

`09_subperiod_20d.csv` · `fig03_subperiods.png`

| Era | n | Score 0 | Score 1 | Score 2 | Score 3 | 3−0 | Monotone? | Spearman ρ (*p*) |
|---|---|---|---|---|---|---|---|---|
| 2015–2019 | 1,261 | −3.81% | +2.09% | +4.53% | +11.98% | **+15.79pp** | Yes | 0.159 (1e-8) |
| 2020–2022 | 1,096 | −1.25% | +0.64% | +2.55% | +8.20% | **+9.45pp** | Yes | 0.148 (8e-7) |
| 2023–present | 1,305 | +2.63% | +0.94% | +3.09% | +3.17% | **+0.54pp** | **No** | 0.025 (0.37) |

Not one cycle carrying the result — three cycles with a monotonically shrinking effect that has now reached zero. That is the most important negative finding in this study.

The calendar-year detail (`10_by_calendar_year_20d.csv`) is blunter still: 2023 has a Score3−Score0 spread of **−31.7pp** (Score 0 averaged +36.2% forward), and 2026 year-to-date has ρ = −0.36.

### Ex-post regimes — descriptive only

`11_expost_regime_20d.csv`. Labelling each day bull/bear/sideways by its **centred ±90-day** return (which uses future information and is therefore for description only, never a backtest), the score→return relationship *reverses* inside every regime (3−0 spreads of −8.8pp, −13.6pp, −7.6pp).

This is largely mechanical — the label incorporates the same forward window being predicted — so it must not be read as "the signal has no alpha within regimes". What it does establish is the **mechanism**: Score 3 occurs on 1,003 of 1,801 bull days and Score 0 on 242 of 700 bear days. TrendScore is a regime *identifier*, and its apparent predictive power is the persistence of crypto regimes.

### Parameter robustness — a genuine plateau

`12_parameter_grid_20d.csv` · `fig04_parameter_robustness.png`

All 36 pre-specified combinations:

- **36/36** have a positive Score3−Score0 spread, range **+6.31pp to +9.95pp** (median +7.93pp)
- **27/36** are strictly monotonic across all four buckets
- Spearman ρ range 0.089 → 0.153
- Coinbase's 30/90/365 sits at the **67th percentile** of the grid

30/90/365 is a broad, flat plateau, not an isolated lucky combination. Whatever the effect is, it is not a lookback artefact.

---

## 5. Strategy performance

`14_strategy_performance_base.csv` · `fig05_equity_curves.png` · `fig06_drawdowns.png` · `fig07_timeline.png`
Full sample, 5 bps per exposure change, 2-day execution lag, residual capital at 0%.

| | CAGR | Ann vol | Sharpe | Sortino | MaxDD | Calmar | Cum. return | Worst yr | Best yr | Avg exp | Turnover/yr | Up capture | Down capture |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| BTC buy & hold | 57.0% | 0.68 | 0.84 | 1.15 | −83.8% | 0.68 | 93.5× | −73.4% (2018) | +1324% (2017) | 1.00 | 0.10 | 1.00 | 1.00 |
| A — linear | 68.0% | 0.49 | 1.38 | 1.74 | −65.5% | 1.04 | 185.7× | −48.2% | +1065% | 0.62 | 14.7 | 0.66 | 0.62 |
| B — threshold (≥2) | **74.5%** | 0.56 | 1.33 | 1.46 | −77.9% | 0.96 | **273.6×** | −65.4% | +1324% | 0.64 | 15.9 | 0.69 | 0.62 |
| C — strong (=3 only) | 51.7% | 0.41 | 1.27 | 1.08 | −57.2% | 0.90 | 65.7× | −21.4% | +628% | 0.35 | 17.9 | 0.40 | 0.34 |
| D — long veto | 65.9% | 0.45 | **1.46** | 1.60 | −63.4% | **1.04** | 163.4× | −45.6% | +945% | 0.49 | 16.9 | 0.54 | 0.48 |

All four pre-specified maps beat buy-and-hold on Sharpe and Calmar. The mechanism is visible in the capture ratios: they keep **~65% of upside** while taking **~62% of downside** — a modest asymmetry, amplified by compounding and lower volatility drag.

**Costs are not a threat** (`13_strategy_cost_sensitivity.csv`): going 0 → 10 bps costs Strategy B 2.8pp of CAGR (75.9% → 73.2%). Turnover of ~16/year is high for a "slow" signal — it comes from the 30-day return whipsawing around zero — but at these cost levels it does not break the result. Adding a 4%/yr cash rate on uninvested capital *improves* every strategy by 2–3pp of CAGR.

---

## 6. Out-of-sample evidence

This is where the study turns.

### The signal (`16_oos_signal_split.csv`)

| Sample | n | Score 0 | Score 3 | 3−0 | HAC *t* | *p* | Bootstrap 95% CI |
|---|---|---|---|---|---|---|---|
| Train (pre-2022) | 1,992 | −3.81% | +10.37% | **+14.17pp** | 2.15 | 0.032 | [+0.6pp, +42.0pp] |
| **OOS (2022+)** | 1,670 | +0.42% | +3.17% | **+2.76pp** | **0.99** | **0.32** | **[−3.3pp, +8.5pp]** |

The conditional-return effect **does not replicate out of sample.** Point estimate down 80%, not statistically distinguishable from zero, bootstrap CI spanning zero.

### The strategy (`17_oos_strategy_split.csv`)

| Sample | Strategy | CAGR | Vol | Sharpe | MaxDD | Calmar |
|---|---|---|---|---|---|---|
| Train | BTC B&H | 118.8% | 0.79 | 1.50 | −83.8% | 1.42 |
| Train | B — threshold | 122.0% | 0.70 | 1.74 | −77.9% | 1.57 |
| **OOS** | BTC B&H | **6.9%** | 0.51 | **0.14** | **−67.0%** | 0.10 |
| **OOS** | B — threshold | **31.5%** | 0.32 | **0.97** | **−26.1%** | **1.20** |
| OOS | D — long veto | 27.0% | 0.26 | 1.04 | −20.7% | 1.30 |

**The strategy outperformed out of sample even though the signal's conditional-return edge did not.** That is not a contradiction — it identifies where the value actually is. OOS volatility fell from 51% to 32% and max drawdown from −67% to −26%. What survived is **risk reduction and volatility drag avoidance**, which is what any trend filter delivers; what did not survive is the **score-conditional expected-return premium**, which is what Coinbase's table is actually claiming.

Caveat: this is one 4.6-year window in one asset, in which buy-and-hold happened to do badly. De-risking anything in that window looks good. Do not over-read it.

### Honest walk-forward on parameters (`18_walk_forward_parameter_selection.csv`)

Selecting the best triple **on training data only**, freezing it, then evaluating OOS:

| Triple | Train 3−0 | OOS 3−0 | OOS monotone? |
|---|---|---|---|
| Coinbase 30/90/365 (pre-specified) | +14.17pp | +2.76pp | No |
| Train-selected 40/120/365 | +14.27pp | +5.38pp | Yes |

Selection bought almost nothing in training (+0.1pp over the pre-specified triple) and both degrade badly OOS. Mild evidence that the plateau is real rather than overfit — and no rescue for the OOS decay.

---

## 7. Cross-asset: BTC TrendScore as a crypto-beta filter

`20_cross_asset_btc_score_conditioning.csv` · `19_cross_asset_own_trendscore.csv` · `fig08_cross_asset.png`

**20-day forward returns conditioned on BTC's TrendScore:**

| Target | Score 0 | Score 1 | Score 2 | Score 3 | 3−0 | HAC *t* | ρ |
|---|---|---|---|---|---|---|---|
| BTC (own) | −0.44% | +1.17% | +3.55% | +7.88% | +8.32pp | 2.82 | 0.117 |
| **ETH** | +2.67% | −0.95% | +2.36% | **+14.78%** | **+12.11pp** | 2.29 | 0.168 |
| **SOL** | +1.72% | −4.65% | +7.26% | **+20.03%** | **+18.30pp** | 2.59 | 0.195 |

**Each altcoin conditioned on its OWN TrendScore:** ETH 3−0 = +7.69pp (*t* = 1.65), SOL 3−0 = +6.27pp (*t* = 0.89).

Three things follow:

1. **BTC's TrendScore is a better predictor of ETH and SOL than of BTC**, in both magnitude and rank correlation.
2. **BTC's TrendScore beats each altcoin's own TrendScore at predicting that altcoin.** This is the strongest support in the study for the crypto-beta / risk-on-filter framing: alt returns are dominated by a common risk-on factor for which BTC trend is a cleaner proxy than the alt's own noisy price history.
3. **The pattern is not monotonic — it is binary.** Score 1 is *worse* than Score 0 for both ETH (−0.95% vs +2.67%) and SOL (−4.65% vs +1.72%). The signal is "full agreement vs everything else", which argues for a Strategy-C-shaped rule, not a linear one.

Tail behaviour supports the risk-on reading: at Score 3, SOL's 5th-percentile 20-day return is −26.6% versus −42.3% at Score 1. At 60 days the spreads are ETH +44.5pp (*t* = 2.70) and SOL +75.3pp (*t* = 2.20).

### But it decays the same way (`20b_cross_asset_era_split.csv`)

| Target | Train (pre-2022) 3−0 | OOS (2022+) 3−0 | OOS HAC *t* |
|---|---|---|---|
| BTC | +14.17pp | +2.76pp | 0.99 |
| ETH | +14.01pp | +2.38pp | 0.55 |
| SOL | +24.51pp | +6.41pp | 1.08 |

The cross-asset effect is **larger, not more durable.** Every one of these is insignificant out of sample. The correct conclusion is that BTC TrendScore is the *better-shaped* version of this idea — a crypto-beta filter rather than a BTC timing tool — but it inherits the same decay and cannot be called validated.

---

## 7b. Broad alt-basket test — the crypto-beta mechanism is rejected

`30`–`34_*.csv` · `fig12_alt_basket_beta.png` · run via `python alt_basket.py --sweep`

§7 and §9b left one live hypothesis: that TrendScore works as a **crypto-beta risk-on filter**, which is why it predicted SOL better than BTC and why the 200-day MA missed it. That story makes a prediction that can fail, so it was pre-registered and tested:

- **H1** — a broad point-in-time alt basket shows a larger spread than BTC, surviving 2022+
- **H2** — the spread **scales with beta** across trailing-beta quintiles ← *the real test*
- **H3** — SMA200 does not capture it as well

**Universe:** 384 Binance USDT spot pairs (stables, BTC, wrapped and leveraged tokens excluded). Eligible at *t* if listed ≥180 days before *t*; monthly rebalance ranking eligible coins by **trailing 30-day quote volume**, top N equal-weighted; trailing 180-day beta vs BTC computed from past data only. Every selection input is knowable at the time.

### Methodology note: ticker reuse silently destroys this test

The first run produced a basket spread of **−288%** — impossible for a 20-day return. Cause: Binance reuses a ticker after a token swap or redenomination, splicing two different units into one series. `LUNAUSDT` shows **+17,739,900%** on 2022-05-31 because old LUNA (crashed to $0.00005) and Terra 2.0 LUNA ($8.87) share the symbol.

These breaks are invisible in daily returns — the rescaling happens during a trading halt, so the series just has a *gap*. Detection has to run on the ratio between consecutive **available** closes plus gap length, not on daily returns. Four symbols were affected (**LUNA, QUICK, STRAX, SUN**) and each was **truncated at the break**, which preserves genuine catastrophic crashes (LUNA's real −99.97% collapse stays) while discarding fake resurrections. Details in `33_redenomination_breaks.csv`.

### H1 — supported in-sample, not significant out of sample

20-day on−off spread, stable across basket sizes (`34_*.csv`):

| Era | TrendScore ≥ 2 (N=25 / 50 / 100) | HAC *p* (N=50) | SMA200 (N=25 / 50 / 100) | HAC *p* (N=50) |
|---|---|---|---|---|
| Full sample | +4.97% / **+6.07%** / +6.13% | **0.029** | +1.19% / +2.09% / +2.14% | 0.491 |
| Train (pre-2022) | +1.04% / +2.75% / +2.79% | 0.582 | −3.68% / −1.75% / −1.64% | 0.743 |
| OOS (2022+) | +4.16% / **+4.32%** / +4.08% | **0.140** | +1.98% / +1.83% / +1.63% | 0.552 |

The basket spread (+6.07%) marginally exceeds BTC's own (+5.24%), and unlike BTC it does *not* decay out of sample — OOS (+4.32%) is larger than train (+2.75%). But **it is not statistically significant out of sample** (*p* = 0.14). H1 is half-met.

### H3 — supported

TrendScore delivers **2–3× the SMA200 spread in every era and every basket size**, and SMA200 is never significant on alts (best *p* = 0.49). Whatever separated the two signals on SOL generalises to the broad basket. This is the one durable differentiator found in the whole study.

### H2 — rejected

The beta sort works (realised mean trailing beta Q1 = 0.81 → Q5 = 1.51). The spreads do not follow it:

| Era | Q1 | Q2 | Q3 | Q4 | Q5 | rank-corr |
|---|---|---|---|---|---|---|
| Full sample | **+8.50%** | +6.57% | +6.19% | +6.13% | +6.72% | **−0.65** |
| Train | **+7.16%** | +5.54% | +4.84% | +2.62% | +2.72% | **−0.97** |
| OOS | **+5.45%** | +3.25% | +3.57% | +4.45% | +5.13% | +0.09 |

The relationship is flat-to-*inverted*: the **lowest**-beta quintile has the **highest** spread in almost every configuration. Across all N × era cells the rank correlation is negative in 7 of 9.

This is not a power problem. Betas span 0.81 → 1.51, so a spread proportional to beta would put Q5 at ~1.86× Q1. Observed Q5/Q1 ≈ **0.7**. The test had the range to detect proportional scaling and found the opposite sign.

**The pre-registered failure condition is triggered. The crypto-beta mechanism is rejected, and the SOL result from §9b is therefore treated as multiple-testing noise rather than the visible tip of a market-wide risk-on effect.**

### Breadth looks impressive and mostly is not

128 of 132 coins (97%) show a positive spread full-sample, 123 of 132 (93%) out of sample, median OOS spread +4.15%. That reads as overwhelming confirmation and is not: alts are mutually correlated, so 132 coins are close to **one** observation, not 132. The tell is in the same table — only **8 of 132** reach HAC *t* > 1.96 out of sample. Consistent sign, weak significance: exactly what one broad correlated factor with a modest, statistically unproven edge looks like.

### Survivorship bias, and why it does not rescue the result

Binance's `exchangeInfo` lists only currently-trading symbols, so dead coins are missing, and no public endpoint fixes this (the repo's own `single_coin_panel` is likewise survivor-only and starts 2024-03). The **direction** matters more than the existence: coins die disproportionately during risk-off periods when TrendScore is low, so excluding them removes catastrophic returns mostly from the *low*-score bucket, making it look better and **shrinking** the measured spread. Survivorship bias therefore works *against* H1 — the +4.32% OOS figure is conservative. It does not, however, affect H2, which compares beta buckets that are all equally survivor-selected.

---

## 8. DVOL overlay

**Methodology note:** Coinbase describes a volatility brake but not its formula. We implement `vol_multiplier = min(1, threshold/DVOL)`, threshold 90 — **our interpretation, explicitly not a claim about their exact method.** It is one-way: it can only reduce exposure.

### The default threshold barely does anything

`21_dvol_threshold_grid.csv` · `21b_dvol_binding_frequency.csv`, Strategy B on the DVOL sample (2021-03-24 →):

| Overlay | CAGR | Vol | Downside vol | Sharpe | MaxDD | Calmar | Worst month | Worst 5d | Worst 20d | Avg exp | Turnover |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Buy & hold | 2.8% | 0.55 | 0.39 | 0.05 | −76.7% | 0.04 | −37.1% | −28.2% | −40.6% | 1.00 | 0.18 |
| Trend only | 28.7% | 0.375 | 0.339 | 0.766 | −45.30% | 0.633 | −21.9% | −18.2% | −24.8% | 0.535 | 16.3 |
| + DVOL 90 (default) | 28.3% | 0.372 | 0.338 | **0.759** | **−45.29%** | 0.624 | −21.9% | −17.9% | −24.8% | 0.533 | 16.5 |
| + DVOL 80 | 27.7% | 0.362 | 0.325 | 0.765 | −43.9% | 0.631 | −21.1% | −16.4% | −23.2% | 0.525 | 16.8 |
| + DVOL 70 | 28.0% | 0.347 | 0.306 | 0.807 | −40.9% | 0.684 | −18.6% | −16.4% | −20.5% | 0.512 | 16.7 |
| + DVOL 60 | 27.3% | 0.327 | 0.284 | **0.834** | **−37.6%** | **0.725** | −16.1% | −16.4% | **−17.7%** | 0.494 | 16.8 |

At the default the overlay is **indistinguishable from doing nothing** — max drawdown moves by 0.01pp. Reason: **DVOL exceeded 90 on only 8.5% of days** (min 32.4, median 55.7, p95 94.0, max 156.2). Thresholds of 100/110/120 are literally inert.

Lower thresholds do improve risk-adjusted results, and improve them *smoothly* as the brake binds harder — 80 → 70 → 60 raises Sharpe 0.765 → 0.807 → 0.834 while CAGR stays ~28%. That smoothness is mildly reassuring. But 60 is the best cell of a 7-point grid evaluated on the full DVOL sample, and roughly half the improvement is simply "less average exposure (0.535 → 0.494) in a bad period". This is **exploratory, not out-of-sample evidence.**

### Acute crashes vs slow bears — Coinbase is right, and that is the problem

`22_dvol_crash_speed_episodes.csv` · `fig09_dvol_panel.png`. All BTC drawdowns ≥20% in the DVOL era:

| Episode | Depth | Days to trough | Mean DVOL | Mean multiplier | % days braked | Worst 20d in episode | Mean DVOL in that 20d |
|---|---|---|---|---|---|---|---|
| 2021-04-14 → 2021-07-20 | −53.1% | 97 | **97.9** | 0.911 | **67%** | −40.6% | **114.6** |
| 2021-11-09 → 2022-11-21 | −76.7% | 377 | 76.8 | 0.992 | **11%** | −35.7% | 83.8 |
| 2024-03-14 → 2024-09-06 | −26.2% | 176 | 58.2 | 1.000 | **0%** | −17.0% | 57.5 |
| 2025-01-22 → 2025-04-08 | −28.2% | 76 | 53.4 | 1.000 | **0%** | −17.8% | 53.2 |
| 2025-10-07 → 2026-06-30 | −53.1% | 266 | 46.1 | 1.000 | **0%** | −34.3% | 43.9 |

The May-2021 crash is exactly the case DVOL is built for: implied vol spiked to 156, the brake cut exposure to 0.58 and was active two days in three. The **Nov-2021 → May-2022 slow bear** — the period specifically flagged in the brief — is exactly the case it misses: BTC lost 77% over 377 days while DVOL averaged 76.8 and the brake engaged on 11% of days.

The forward-looking problem is worse than "it misses slow bears": **DVOL has structurally declined.** Across three separate drawdowns since 2024 totalling 26%, 28% and 53%, the brake never engaged once. As BTC's options market has matured and implied vol has compressed, a fixed 90 threshold has become a switch that no longer fires. Any DVOL overlay used going forward needs a *relative* threshold (e.g. a rolling percentile of DVOL), not an absolute one.

---

## 9. Drawdown control and decomposition

`23_decomposition_full_sample.csv` · `24_decomposition_dvol_sample.csv` · `fig10_decomposition.png`

Brake: DD ≤ 20% → ×1.00; 20–40% → linear taper to ×0.25; ≥40% → ×0.25. Computed from the strategy's own equity through *t*−1 only (verified causal).

**Full sample (no DVOL available pre-2021):**

| Stage (Strategy B) | CAGR | Sharpe | MaxDD | Calmar | Avg exp |
|---|---|---|---|---|---|
| 1. BTC buy & hold | 57.0% | 0.84 | −83.8% | 0.68 | 1.00 |
| 2. Trend only | 74.5% | 1.33 | −77.9% | 0.96 | 0.64 |
| 3. Trend + DD brake | 60.4% | **1.42** | **−51.5%** | **1.17** | 0.46 |

**DVOL sample (2021-03-24 →), the full four-way ladder:**

| Stage (Strategy B) | CAGR | Sharpe | MaxDD | Calmar | Downside vol | Avg exp | Turnover |
|---|---|---|---|---|---|---|---|
| 1. BTC buy & hold | 2.8% | 0.05 | −76.7% | 0.04 | 0.394 | 1.00 | 0.18 |
| 2. Trend only | **28.7%** | **0.766** | −45.3% | **0.633** | 0.339 | 0.535 | 16.3 |
| 3. + DVOL (90) | 28.3% | 0.759 | −45.3% | 0.624 | 0.338 | 0.533 | 16.5 |
| 4. + DD brake | 21.0% | 0.629 | **−39.1%** | 0.538 | 0.318 | 0.450 | 13.6 |

**Where the performance actually comes from:**

- **Step 1 → 2 (the trend signal) is the entire story.** +25.9pp of CAGR, Sharpe 0.05 → 0.77, drawdown −76.7% → −45.3%. Everything else is second-order.
- **Step 2 → 3 (DVOL) contributes nothing at the default threshold.** −0.4pp CAGR, −0.007 Sharpe, +0.01pp drawdown.
- **Step 3 → 4 (drawdown brake) buys drawdown with return.** Reliably reduces max drawdown in both samples (−26.4pp full sample, −6.2pp DVOL sample), but on the recent sample it costs 7.3pp of CAGR and 0.13 of Sharpe. It is a *risk-mandate* tool, not an alpha tool, and it is path-dependent: it de-risks after losses, so it hurts in sharp V-shaped recoveries.

---

## 9b. Head-to-head vs a single 200-day moving average

`25`–`29_*.csv` · `fig11_ma200_head_to_head.png` · run via `python ma_benchmark.py`

The horse race in §3 showed no individual lookback is significant alone and each contributes ~equally — the signature of a signal whose content is just "is BTC trending up", measured three times. So the real null hypothesis is the industry-default single-parameter trend filter: **price > 200-day SMA**. It has the same epistemic status as 30/90/365 (externally specified by convention, not chosen by us after seeing this data), so it is a fair benchmark rather than a straw man.

**Decision rule fixed before running:** TrendScore earns its complexity only if it beats SMA200 on *both* the OOS conditional spread *and* OOS risk-adjusted performance.

**(a) 20-day conditional spread (on − off):**

| Signal | Full sample | Train (pre-2022) | **OOS (2022+)** |
|---|---|---|---|
| SMA200 | +4.59% (t=2.20) | +4.52% (t=1.36) | +3.14% (t=1.51) |
| TrendScore ≥ 2 | +5.24% (t=2.70) | +4.83% (t=1.54) | **+3.53% (t=1.80)** |
| TrendScore = 3 | +5.92% (t=2.52) | +6.84% (t=2.00) | +2.69% (t=1.24) |

**(b) Strategy, identical engine (5 bps, 2-day lag):**

| | Full CAGR / Sharpe / MaxDD | **OOS CAGR / Sharpe / Calmar / MaxDD** | Turnover |
|---|---|---|---|
| Buy & hold | 56.8% / 0.84 / −83.8% | 6.9% / 0.14 / 0.10 / −67.0% | 0.1 |
| SMA200 | 61.1% / 1.15 / −64.9% | 19.8% / 0.59 / 0.56 / −35.4% | **6.9** |
| TrendScore ≥ 2 | 74.2% / 1.32 / −77.9% | **31.5% / 0.97 / 1.20 / −26.1%** | 15.6 |
| TrendScore = 3 | 51.7% / 1.27 / −57.2% | 21.3% / 0.90 / 0.98 / −21.7% | 14.3 |

**(c) Incremental HAC regression — `fwd20 ~ SMA200_on + TrendScore_on`:** TrendScore adds **nothing statistically significant beyond the MA in any era** — full sample *p* = 0.096, train *p* = 0.451, **OOS *p* = 0.311**. Neither term dominates; they are 86.2% the same exposure (corr 0.71).

**(d) The OOS gap is two years, not a trend.** TrendScore≥2 minus SMA200 by calendar year: 2022 **−15.7pp**, 2023 **−15.5pp**, 2024 **+51.7pp**, 2025 **+49.6pp**, 2026 **−5.4pp**. TrendScore *lost* to the moving average in three of five out-of-sample years.

**Verdict on BTC: TrendScore does not clearly earn its complexity.** It passes the letter of the pre-specified rule via the strategy metric, but that metric is driven by two years, the cleaner incremental test says no, and the two signals agree 86% of the time. For BTC trend de-risking, **SMA200 is the better engineering choice** — one parameter instead of three, less than half the turnover (6.9 vs 15.6), statistically indistinguishable information.

### But the cross-asset case reverses this

BTC signal → altcoin 20-day forward return, on − off:

| Target | Signal | Full sample | Train | **OOS (2022+)** |
|---|---|---|---|---|
| ETH | SMA200 | +8.99% (p=0.005) | +11.03% (p=0.034) | +3.94% (p=0.164) |
| ETH | **TrendScore ≥ 2** | +8.93% (p=0.004) | +8.81% (p=0.095) | **+5.04% (p=0.068)** |
| SOL | SMA200 | +9.80% (p=0.055) | +4.52% (p=0.798) | +5.36% (p=0.210) |
| SOL | **TrendScore ≥ 2** | **+16.92% (p=0.001)** | **+29.50% (p=0.010)** | **+9.16% (p=0.031)** |

**BTC TrendScore ≥ 2 → SOL is significant in all three eras; SMA200 is significant in none.** This is the only out-of-sample-significant result in the entire study, and the moving average does not capture it.

Two things to keep in proportion: this section runs 18 tests (2 assets × 3 signals × 3 eras), so a single *p* = 0.031 is roughly what chance would deliver — what makes it more than that is *consistency across all three eras*, not the p-value. And SOL is one asset with history only from 2020-08.

The binary ≥2 contrast is also stronger than the score3−score0 contrast reported in §7 (SOL OOS +9.2pp, *t* = 2.15 vs +6.4pp, *t* = 1.08) simply because it splits the whole sample instead of comparing two thin end buckets. The usable form of this signal is binary.

**This yields a falsifiable prediction rather than a fitted result:** if TrendScore works because it is a crypto-beta risk-on filter, the effect should *scale with asset beta* — largest in SOL, smaller in ETH, smallest in BTC. That is exactly the observed ordering (+16.9% / +8.9% / +5.2% full sample). Testing it on a broad altcoin basket is now the highest-value next step, because the prediction can fail.

---

## 10. Perpetual futures — discussion only, not modelled

The primary study models desired **net BTC exposure**; no perp mechanics were simulated, deliberately, so that implementation assumptions could not distort the signal research. Holding spot and shorting perps to reduce net exposure is economically *not* the same as selling spot:

- **Funding.** The dominant term. Short perp receives funding when the perp trades above index — historically the common state in risk-on regimes. Since TrendScore de-risks precisely when trend is *weak*, the hedge would often be applied when funding is neutral or negative, i.e. you would frequently be *paying* to be short. This could systematically invert the expected carry benefit and deserves explicit measurement against `fact_funding` before anyone assumes it is free.
- **Fees and slippage.** Two legs instead of one. At ~16 exposure changes/year the perp route roughly doubles the transaction-cost drag modelled here.
- **Basis.** The hedge is imperfect: perp-index basis moves, so a "flat" book still carries basis P&L, largest exactly during the stress episodes the overlay exists for.
- **Collateral and liquidation.** A short perp needs margin. In a violent up-move — common right after Score 0/1 regimes end — the short leg loses while spot gains, and the margin call can arrive before the spot gain is monetisable. Liquidation risk is not symmetric with simply being flat.
- **Counterparty and venue risk.** Spot-and-short concentrates assets at a derivatives venue. Selling spot does not.
- **Tax and custody.** Selling spot realises gains; a perp hedge generally does not, which may be the whole point — or may create mark-to-market income treatment depending on jurisdiction. Jurisdiction-specific; get advice.

**Net:** the perp overlay changes the *cost and risk* of expressing the signal, not the signal's information content. Since the signal itself did not validate out of sample, the perp question is premature.

---

## 11. Failure modes

- **Look-ahead.** Actively controlled and tested (`verify.py`, five traps). The residual risk is the descriptive tables using `F_h(t)` from the signal close, which is standard for an event study but not tradable; the backtests carry a 2-day lag.
- **Overlapping returns.** The central statistical hazard. Naive t-stats here inflate by 3.1–3.7×. Handled three ways; all three agree on the 3−0 spread and all three are weaker on monotonicity.
- **Overfitting.** Low for the core test — the 30/90/365 triple and the A/B/C strategy maps were externally specified before we saw data, and the parameter grid was fixed in advance. **Moderate** for the DVOL threshold conclusion, which is a best-of-grid full-sample result and is labelled as such.
- **Independent events, not observations. This is the deepest problem, and it is worse than "three cycles".** Score 0 occurs on 421 days, but those days form only **four distinct market episodes**: the late-2018/early-2019 bear (84 days, one contiguous Oct-2018 → Feb-2019 block), the 2022 bear (196 days), late-2025 (38 days) and 2026 (103 days). The entire +15.8pp spread of the 2015-2019 bucket rests on **a single bear market**. The effective sample size for anything involving Score 0 is ~4, not 415. Every confidence interval in this report — including the HAC and bootstrap ones, which correct for serial correlation but not for there being only four events — is therefore optimistic.
- **Sample start.** The signal sample begins **2016-07-19**, not 2015, because of the 365-day warm-up; the "2015-2019" bucket is really Jul-2016 → Dec-2019. Binance cross-check data only begins 2017-08-17, so ~13 months of the earliest window has no second-source verification — though it contains **zero** Score-0 days, so it does not affect the extremes comparison.
- **DVOL history.** 5.4 years, one crash regime and one slow bear. Any DVOL conclusion rests on ~2 events.
- **Regime-identification vs prediction.** Score 3 occurs in bull markets and Score 0 in bear markets essentially by construction. The "prediction" is largely the persistence of crypto regimes. When regimes stop persisting — as 2023–2026 suggests — the signal stops working.
- **Structural change.** Spot ETFs, institutional participation and compressed implied vol have all changed BTC's return distribution. Realised DVOL levels have halved. Any parameter calibrated to 2016–2021 dynamics — including a fixed 90 DVOL threshold — is calibrated to a market that no longer exists.
- **Transaction costs.** Modelled at 0/5/10 bps; not a threat at those levels, but ~16 exposure changes/year would be materially worse in size or in perp form.
- **Survivorship / venue.** None for BTC. ETH and SOL results begin at their exchange listings; SOL's usable sample is only 6 years.

---

## 12. Practical recommendation

### **Interesting but insufficient evidence.** Monitor it; do not gate anything on it.

This is a downgrade from the interim read, and the reason is specific: the crypto-beta mechanism — the one story that made the cross-asset result more than a coincidence — was pre-registered, tested on a broad basket, and **failed** (§7b).

**What is solid.** The replication is exact. The parameter plateau is broad and flat across all 36 tested combinations. The result holds across two independent exchange feeds (99.04% identical scores) and is present at six horizons. The economics are intelligible — it is a regime identifier, and crypto regimes have historically persisted.

**What is not.** Nothing in this study is statistically significant out of sample. BTC OOS: +2.76pp, *t* = 0.99. Alt basket OOS: +4.32pp, *p* = 0.14. Beta scaling: rank-corr −0.65, wrong sign. Monotonicity: a 56% bootstrap proposition. Score-0 evidence: four market episodes, not 415 observations. On BTC, a 200-day MA matches it with one parameter and half the turnover. The DVOL overlay is inert at the published threshold. The drawdown brake trades return for drawdown and hurts Sharpe on recent data.

**Therefore:**

- ✅ **Do** log it daily as a regime state variable alongside existing macro-regime fields. It costs nothing, and forward observations are the only evidence that can still settle this.
- ✅ **Do** note the one durable differentiator for future work: on alts, TrendScore consistently returns 2–3× the SMA200 spread (+6.07% vs +2.09% full, +4.32% vs +1.83% OOS) in every era and basket size. It is directional, not significant — worth watching, not worth trading.
- ⚠️ **Do not** gate alt exposure on it. That was the interim recommendation and the basket test does not support it.
- ⚠️ **Prefer SMA200 for BTC trend de-risking.** Same information (86% identical exposure, incremental HAC *p* = 0.311), one parameter instead of three, 6.9 vs 15.6 annual turnover.
- ❌ **Do not** size positions off the published +7.7% Score-3 number. Full-sample mean, +3.0% median, 21.6% standard deviation, no out-of-sample support.
- ❌ **Do not** implement the DVOL overlay at threshold 90 — it has not engaged during any drawdown since 2024. A rolling-percentile threshold would need its own test.
- ❌ **Do not** deploy live.

**What would change this verdict.** Not more backtesting — the OOS failure is precisely the thing more backtesting cannot fix. Three things could, in order of value:

1. **Forward-logged observations.** Every day logged from today is clean out-of-sample evidence. Given the 2023–2026 flatline, a year or more of the alt spread holding near its +4.3% OOS point estimate would be meaningful; a year near zero would close the question.
2. **An explanation for why TrendScore beats SMA200 on alts but not BTC.** That asymmetry is consistent across every era and basket size and is currently unexplained. Beta is not the answer. A mechanism that predicts something testable would be worth more than another backtest.
3. **A rolling-percentile DVOL specification**, if a volatility brake is wanted for other reasons. The absolute-threshold version is dead.

If forward observations flatten, the honest conclusion is that Coinbase published a real in-sample regularity that stopped working around 2022, and that the tradable residue is ordinary trend-following available from a single moving average.
