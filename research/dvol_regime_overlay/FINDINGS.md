# Should DVOL join the regime monitor?

**Sample:** 2023-04-19 → 2026-08-16, 1,216 daily observations.
**Binding constraint is funding, not DVOL** — silver funding starts 2023-04-19, DVOL 2021-03-24. Weekly `y` has only 105 rows.
Rerun: `python build_panel.py && python study.py`

---

## Answer in one paragraph

DVOL is **not redundant** with what you already track, but it **does not predict** either the risk or the return of the book you're gating. A DVOL brake improves the backtest, and beats *randomly* cutting the same exposure at p≈0.04 — but against the comparison that actually matters, **not braking at all**, the Sharpe gain has a 95% CI of [−0.64, +1.37] and a 23% chance of being negative. Resolving that would take ~13 more years of data. **Recommendation: don't add it, in any form.** The study did surface something more actionable: `Fragmentation_Spread` is the strongest predictor in your panel and is currently spent as a binary switch.

---

## Correction to something I told you earlier

I previously described `Fragmentation_Spread` as a *"tail guard, rarely binds, low day-to-day information."* That was wrong, and I based it on a single snapshot (0.000021 on 2026-08-17) rather than the distribution.

Over 1,216 days:

- The toxic ceiling (`7.5e-05`) is breached on **37.4% of days** — 455 of 1,216. It is not a rare tail guard; it is one of the main things turning your gate off.
- As a **continuous** variable it is the **strongest predictor in the entire panel**: 10-day forward return `t = -2.61, p = 0.0091`, and `t = -4.34` at the 5-day horizon — stronger than `Environment_APR` (`t = +1.95`) and far stronger than DVOL (`t = -1.27`).

| Spread quintile | n | mean 10d forward return |
|---|---|---|
| Q1 (least fragmented) | 242 | +1.84% |
| Q2 | 340 | **+3.59%** |
| Q3 | 142 | +1.48% |
| Q4 | 241 | −0.03% |
| Q5 (most fragmented) | 241 | **−0.99%** |

Not perfectly monotone at the bottom, but the top half is clearly and significantly worse. **You have a strong continuous signal being spent as a binary on/off.** That is probably a bigger opportunity than DVOL, and it's already in your data.

---

## Q1 — Should we incorporate DVOL at all?

### 1a. Is it orthogonal? **Yes.**

Spearman ρ of DVOL's 365-day percentile against existing fields:

| vs | ρ |
|---|---|
| `Environment_APR` | +0.30 |
| `Fragmentation_Spread` | +0.20 |
| `w_risk` | −0.21 |

Max |ρ| = 0.30. DVOL carries information the monitor doesn't currently have — it isn't a repackaging of funding.

### 1b. Does it predict the book's forward RISK? **No.**

Regressing forward realised volatility of the L/S book on APR + Spread, then adding DVOL:

| Horizon | R² (APR+Spread) | R² (+DVOL) | DVOL t | p |
|---|---|---|---|---|
| 5d | 0.012 | 0.017 | −1.10 | 0.27 |
| 10d | 0.013 | 0.020 | −1.09 | 0.28 |
| 20d | 0.026 | 0.043 | −1.29 | 0.20 |

Nothing. Which is economically coherent: DVOL is **directional BTC implied vol**, and the book is a **relative-value spread**. There was no reason to assume one forecasts the other, and it doesn't.

### 1c. Does it predict the book's forward RETURN? **No.**

| Horizon | DVOL coef | DVOL t | p | APR t | Spread t |
|---|---|---|---|---|---|
| 5d | −1.08% | −1.27 | 0.20 | +1.95 | **−4.34** |
| 10d | −1.76% | −1.16 | 0.25 | +1.35 | **−3.03** |
| 20d | −2.28% | −0.80 | 0.42 | +1.02 | **−2.53** |

DVOL adds nothing once APR and Spread are in. Note where the signal actually lives.

---

## Q2 — How should it be expressed?

**Percentile, not level.** Over the full DVOL history the index halved (median 55.7, and yearly means fell from 91.8 in 2021 to 43.9 in 2026), so a fixed threshold decays into a dead switch — the finding from the trend study, where a level-90 brake fired on 0% of days across all three drawdowns since 2024. Within *this* 3.3-year window the range is only 32–83, so the drift is milder, but percentile is still the safer construction and matches your existing `funding_pct_rank` convention.

**One-way brake** (reduce exposure only, never lever up).

Threshold scan, 10-day forward return, high-DVOL minus low-DVOL:

| Threshold | return diff | t | p |
|---|---|---|---|
| ≥ p60 | −1.03% | −1.06 | 0.29 |
| ≥ p70 | −1.58% | −1.50 | 0.13 |
| ≥ p80 | −1.42% | −1.20 | 0.23 |
| ≥ p90 | −1.80% | −1.08 | 0.28 |

Consistently negative, never significant. On forward *volatility* there's no pattern at all.

---

## Q3 — In conjunction with APR and Spread

### Interaction: noise-like

| APR regime | n | high-DVOL effect on 10d return | on 10d vol |
|---|---|---|---|
| Recovery Ramp | 260 | **+1.40%** (t=+1.20) | −0.082 (t=−1.80) |
| Golden Pocket | 710 | **−2.76%** (t=−1.73) | +0.007 (t=+0.16) |
| Leverage Exhaustion | 93 | +1.56% (t=+0.39) | **−0.188 (t=−2.89)** |

The sign flips between regimes with small n. The one significant cell (vol reduction in Leverage Exhaustion) rests on 93 observations. I would not build a rule on this.

### Backtest: the brake looks good

| Variant | CAGR | Sharpe | MaxDD | Calmar | Avg exp | % braked |
|---|---|---|---|---|---|---|
| `w_risk` only (current) | +66.9% | 1.88 | −24.5% | 2.74 | 0.83 | 0% |
| × brake @ DVOL p70 | +65.3% | **2.25** | **−17.8%** | **3.67** | 0.57 | 34% |
| × brake @ DVOL p80 | +62.1% | 2.00 | −19.4% | 3.20 | 0.64 | 25% |
| × brake @ DVOL p90 | +62.6% | 1.91 | −24.5% | 2.56 | 0.69 | 18% |
| × soft(0.5) @ p80 | +71.2% | 2.14 | −21.4% | 3.33 | 0.77 | 18% |

Nearly the same CAGR for meaningfully less risk. That's the shape you want.

### But is it timing, or just less exposure?

Cutting exposure 34% of the time will lower drawdown whatever the trigger. So: 2,000 **placebo** brakes, same fraction of braked days, placed at random in 10-day blocks to preserve persistence:

```
base (no brake)            Sharpe 1.88   MaxDD -24.45%
real DVOL p70 brake        Sharpe 2.25   MaxDD -17.78%
random brakes, same dose   Sharpe 1.51   [p5 0.86, p95 2.18]
→ 4.0% of random brakes match or beat the real one
```

So DVOL's *timing* is doing something beyond generic de-risking — p ≈ 0.04.

**Two caveats that matter as much as the result:**

1. **p70 was the best of three thresholds** tested on the full sample. Adjusting for that, the effective p is nearer 0.12 than 0.04.
2. **The effect more than halves out of sample:**

| | no brake | p70 brake | Δ Sharpe |
|---|---|---|---|
| Train (→2024-12-17) | 0.99 | 1.45 | **+0.46** |
| Test (2024-12-17→) | 2.82 | 3.02 | **+0.20** |

And the drawdown benefit vanishes entirely in the test half (−17.78% both). Note also the base book goes from Sharpe 0.99 to 2.82 across halves — the test period was extremely kind, so *any* conclusion from it is fragile.

---

## Recommendation

**Do not add DVOL to the monitor.**

An earlier draft of this document said "log it, don't gate on it," on the
argument that forward observations would eventually settle the question. That
argument does not survive being checked.

### The placebo test was the flattering test

Two different comparisons, both true, and I led with the wrong one:

| Question | Result |
|---|---|
| Is the DVOL brake better than **randomly** cutting the same exposure? | yes, p ≈ 0.04 |
| Is the DVOL brake better than **not braking at all**? | **not distinguishable** |

Block-bootstrap on the Sharpe difference vs no brake:

```
observed ΔSharpe          +0.374   (1.88 → 2.25)
95% CI                    [-0.638, +1.368]
P(ΔSharpe ≤ 0)            0.229
```

A 23% chance the "improvement" is actually negative. The decision-relevant
comparison is the second one, and it says nothing has been established.

### What the brake mechanically is

```
volatility     0.357 → 0.290   (19% lower)
return cost    −3.1%/yr
```

A straight risk/return trade. Whether it is a *good* trade is exactly what the
CI above cannot answer.

### Forward logging would not resolve it

The CI width is ~2.0 Sharpe points. Halving it needs 4× the data — roughly
**13 more years**. "Log it and accumulate out-of-sample evidence" is not a real
plan on that timescale; it is a way of deferring the decision while still paying
the cost of another field on the dashboard, another producer that can break, and
another thing to keep fresh.

That cost is not hypothetical. This project has just spent substantial effort
removing exactly one such field — BTCDOM — which was logged, not gated, quietly
wrong for six months, and dragged a fatal pipeline guard and a 200-line runbook
behind it.

### What is and isn't being claimed

DVOL is **not proven to be noise**. It is *unresolvable* with the data that
exists — 3.3 years, bounded by funding history, in a single regime where the
base book's Sharpe nearly triples between halves. That distinction matters
intellectually, but for a build decision it lands in the same place: there is no
evidential basis for putting it in the monitor.

The one honest reason to add it anyway would be non-statistical: a human reading
"options are pricing ±10% over the next month" versus "±33%" may find that
useful context when interpreting everything else. That is a UI preference, not a
research finding, and it should be argued on its own terms rather than dressed
up as a signal.

### Where the effort should go instead

`Fragmentation_Spread` — the strongest predictor in the panel (t = −2.61
continuous, −4.34 at 5 days), already collected, already trusted, and currently
spent as a binary switch that fires 37% of the time. Making it continuous
improves something that is already known to work, rather than adding something
that isn't.

## Limitations

- **3.3 years, one regime.** Funding history caps the sample. The base book's Sharpe nearly triples between halves, so era effects dominate.
- **The daily L/S proxy is a proxy** — corr +0.872 with the real weekly `y` (84.6% sign agreement), which is good, but it is a top-50-by-volume Binance basket, not your production universe.
- **No transaction costs** in the backtest. The p70 brake drives average exposure from 0.83 to 0.57, which is real turnover.
- **Weekly `y` has 105 observations.** Anything tested on it directly is underpowered; that's why the daily proxy exists.
