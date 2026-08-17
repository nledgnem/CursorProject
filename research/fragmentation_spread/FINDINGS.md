# Fragmentation_Spread: should the binary gate become continuous?

**Sample:** 2023-04-19 → 2026-08-16, 1,216 daily observations (bounded by funding history).
**Baseline:** the current production rule — `gate_on = APR ≥ 2.0 AND spread < 7.5e-05`.
Rerun: `python study.py`

---

## Answer: no. Leave it alone.

**My hypothesis was wrong.** I said you had "a strong continuous signal spent as a binary switch," implying waste. Every continuous variant tested is **worse** than the binary you already run, and the ceiling turns out to sit almost exactly where the break in the data is.

| Variant | CAGR | Sharpe | MaxDD | Calmar | Avg exp |
|---|---|---|---|---|---|
| no spread gate | +66.9% | 1.88 | −24.5% | 2.74 | 0.83 |
| **A — current binary @ 7.5e-05** | **+69.2%** | **2.18** | **−20.4%** | **3.40** | 0.61 |
| B — binary at matched percentile | +59.7% | 1.87 | −22.0% | 2.72 | 0.60 |
| C — linear taper on percentile | +48.9% | 2.00 | −18.2% | 2.69 | 0.53 |
| D — taper floored at 0.25 | +49.1% | 2.00 | −17.6% | 2.79 | 0.53 |
| E — taper, top quartile off | +62.6% | 2.08 | −21.2% | 2.95 | 0.64 |

Bootstrap of ΔSharpe **versus the current rule** — all three tapers are negative and none is distinguishable from zero:

| Variant | ΔSharpe | 95% CI | P(Δ ≤ 0) |
|---|---|---|---|
| C linear taper | −0.184 | [−0.970, +0.435] | 0.733 |
| D floored taper | −0.180 | [−0.921, +0.464] | 0.729 |
| E top-quartile off | −0.097 | [−0.767, +0.488] | 0.628 |

The best taper (E) also fails its placebo — 7.5% of value-shuffled versions match or beat it — and flips sign across the train/test split (worse in train, better in test).

---

## Why the hypothesis was wrong

The regression that motivated this (10-day forward return on spread, `t = −2.61`) is real. My inference from it was not.

Forward 10-day return by spread decile:

| Decile | spread ≤ | n | mean 10d fwd |
|---|---|---|---|
| D1 | 7.8e-06 | 121 | +1.59% |
| D2 | 3.97e-05 | 121 | +2.10% |
| D3 | 5.0e-05 | 340 | **+3.59%** |
| D4 | 5.28e-05 | 21 | +0.97% |
| D5 | 7.21e-05 | 121 | +1.57% |
| D6 | 9.22e-05 | 120 | +0.14% |
| D7 | 0.000115 | 121 | −0.20% |
| D8 | 0.000158 | 120 | **−2.02%** |
| D9 | 0.000717 | 121 | +0.02% |

That is a **step function, not a gradient**. Flat-and-positive through D5, then it falls off. There is no ordering *within* the good region that a taper could exploit — D1 is worse than D3 — so scaling exposure proportionally to spread just throws away exposure in the states that pay.

A linear regression will report significance on a step function. That significance says "this variable matters," not "a linear scaler is the right implementation." I conflated those, and the decile table is what separates them.

**The ceiling is also well-placed.** 7.5e-05 sits at the **63rd percentile**, which is the D6/D7 boundary — right where returns cross into negative. Whether that was design or luck, moving it is not indicated.

---

## The uncomfortable part: the incumbent doesn't clear the bar either

I held the challengers to the standard that killed DVOL — placebo at matched dose, bootstrap CI, train/test. Intellectual honesty requires applying it to the incumbent too.

Current binary gate vs no spread gate at all:

```
Sharpe                    2.18  vs  1.88      (ΔSharpe +0.30)
placebo (random gating,
  same 37.4% dose)        5.2% of random gates match or beat it
bootstrap ΔSharpe         95% CI [-0.397, +1.080]   P(Δ ≤ 0) = 0.186
```

Compare against the DVOL brake we rejected:

| | placebo | P(Δ ≤ 0) |
|---|---|---|
| DVOL p70 brake (rejected) | 4.0% | 0.229 |
| **Fragmentation_Spread gate (in production)** | **5.2%** | **0.186** |

**Statistically these are the same situation.** 3.3 years is simply not enough to establish an effect of this size, for either variable.

### Why I still say keep the spread gate

Three reasons that are about evidence quality, not about it being the status quo:

1. **It raises return while cutting exposure.** CAGR goes 66.9% → 69.2% with average exposure falling 0.83 → 0.61. The DVOL brake *cost* 3.1%/yr for its risk reduction. Getting more return from less exposure is a materially different signature from buying risk reduction with return — it is what a real signal looks like.
2. **The underlying regression evidence is 2–3× stronger** — spread `t = −2.61` (10d) and `−4.34` (5d), versus DVOL's `−1.27` and `−0.80`.
3. **The decile structure is coherent** and matches an economic story: high funding dispersion means alt moves are idiosyncratic rather than factor-driven, which is exactly when a broad short-alt basket carries name-specific risk it is not being paid for.

And the asymmetry matters: the bar for **removing** something already running and apparently working should be higher than the bar for **adding** something new. On evidence this weak, that asymmetry is doing real work in the recommendation, and it should be stated rather than hidden.

---

## Recommendation

1. **Change nothing.** Keep `spread < 7.5e-05` as a binary gate at its current level.
2. **Do not pursue a continuous scaler.** Tested, worse, and the decile structure explains why.
3. **Log the caveat honestly:** on 3.3 years, the spread gate's benefit is not statistically established (P(Δ ≤ 0) = 0.19). It is kept on the strength of coherent structure, stronger regression evidence, and a favourable return/exposure signature — not on a p-value.
4. **If you ever revisit it**, the question worth asking is not "binary or continuous" — it's whether the *ceiling level* should move as the funding market matures, the same way DVOL's absolute threshold decayed. That is a genuinely open question this sample cannot answer.

---

## Limitations

- **3.3 years, one regime.** The base book's Sharpe roughly doubles between halves (1.59 → 2.72 with the current gate), so era effects dominate everything measured here.
- **The daily L/S proxy** correlates +0.872 with the real weekly `y` (84.6% sign agreement) but is a top-50-by-volume Binance basket, not the production universe.
- **No transaction costs.** The gate drives ~78 units of annual turnover.
- **Deciles are uneven** (D3 has n=340, D4 n=21) because spread has heavy ties; the step-function reading is robust to that but the exact boundary is not.
