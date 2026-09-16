# Decision Memo — Trend Confirmation and Structural Alpha

*Lead quant research, 2026-09-15. Scope: Track A (Gerhard SMA120 / LL Pro 3-close execution) and Track B (crypto
structural-alpha pipeline).*

**Supporting documents**
- `DATA_AND_SYSTEM_AUDIT.md`
- `TREND_CONFIRMATION_RESEARCH.md`
- `STRUCTURAL_ALPHA_MAP.md`
- `STRUCTURAL_ALPHA_RESULTS.md`
- `DATA_GAP_ANALYSIS.md`
- `RESEARCH_BACKLOG.md`

## Summary

1. **No production change. Paper-trade faster BTC Gerhard confirmation only.** Run the two frozen candidates
   (1-close, and the 0.5σ fast-track) side by side. Everything else keeps the 3-close rule.
   - This is downgraded from "change it" after an external review and the audit in the Trend Q9 section below.
   - About 60% of BTC's gain comes from *exiting* faster, not entering faster, and all of the gain is from 2022 onwards.
   - The same faster exit *hurts* ETH and SOL.
2. **No structural signal is ready for capital.** Nothing survives FDR control under any variant (the lowest
   adjusted p is 0.13–0.175). Q4 and O1 are momentum-conditioned (a like-for-like price control leaves ≤ +0.7%). C1
   is the only forward-test candidate: large, but dominated by five events and untestable against the current
   control. See `REVIEW_RESPONSE.md` for the post-review fixes.
3. **Spend the next month on data that cannot be recovered later** (liquidation stream, contract-spec snapshots) and
   on a daily shadow log, not on more backtests of the same daily data.

No production trading code was modified. No systematic Gerhard or LL implementation exists in the repo; the
proposed rule change is documented below for a human to implement after the replay in backlog P0-3.

---

## Trend system

**Q1. Is the 3-close rule justified?**

| System | Verdict | Evidence |
|---|---|---|
| BTC Gerhard | **Questionable since 2022, not before** | Acting on the first close improved Sharpe by +0.23 [+0.05, +0.48] over 2019–26 (long/flat, 10bp costs), but by −0.05 over 2019–21. About 62% of the gain comes from faster exits (see Q9) |
| ETH Gerhard | **Yes** | Acting on close 1 lowered Sharpe by −0.11; 45% of first closes fail |
| SOL Gerhard | Inconclusive | Too few cycles |
| LL Pro, all assets | **Yes, harmlessly** | 96–100% of first colour closes confirm, so the rule costs and saves little; ETH walk-forward is negative for faster variants |

**Q2. How much return is sacrificed by waiting?**
- BTC Gerhard: **+1.9% per signal [+0.3, +3.8]** of incremental P&L from acting after 1 close rather than 3.
  P(confirm) is 0.71; acting early breaks even at 0.40.
- ETH: waiting *earns* +1.9% per signal [+0.1, +4.1].
- SOL: ≈ 0.

**Q3. How much loss is avoided?**
- ETH: failed first closes cost ≈ 5.4% each, and acting early needs P(confirm) ≥ 0.86 to break even (observed 0.55).
- BTC: the rule avoids less than it gives up, because BTC false breaks are shallow relative to the continuation from
  real breaks.
- Both effects are concentrated after 2021. BTC's gain from acting early is ≈ 0 in 2019–21.

**Q4. Does day-1 strength predict success?**
- **It predicts confirmation:** break/ATR AUC 0.70, break/σ 0.69, return z 0.66, perp volume z 0.65. A pooled
  logistic model scores OOS AUC 0.705.
- **It barely predicts P&L beyond that.** BTC first closes with z ≥ 3 returned +6.3% over 10 days (16 events,
  1.6/year). ETH's returned −2.1% (12 events).
- **Funding, basis, OI, liquidations and breadth add nothing** (all BH-insignificant).
- **August-2026-like events are not rare:** about 1–2 per year per asset. There is no evidence for a general
  "don't chase big day-1 moves" rule.

**Q5. Is there a robust threshold?**
- **No evidence for a special threshold.** BTC chose 0.5σ in 8 of 8 folds, but 0.5 is the *lowest value on the
  grid*, so this is a boundary solution: the optimiser wanted as much early action as it was allowed.
- Plain 1-close (+0.23) beats the 0.5σ rule (+0.21) over 2019–26.
- The one thing in the threshold's favour: in 2019–21 the 0.5σ rule was flat (+0.01) while 1-close lost (−0.05),
  so it may filter early-era whipsaws.
- The evidence says "faster was better on BTC since 2022". It does not say "0.5σ is special".
- Do not search below 0.5; that would be more data mining. Compare the two frozen rules on paper going forward.

**Q6. Is staged entry better?**
- M4 strength staging (isotonic P(confirm) vs break-even; partial size on close 1, full on confirmation) is the only
  model that is non-negative OOS on all three assets: BTC +0.15, ETH +0.03, SOL +0.11.
- Apart from BTC, its CIs include zero. Linear 3-step staging (M3) adds +0.13 on BTC and nothing elsewhere.
- Staging is the right *shape* for a cross-asset rule but is not proven. **Paper-trade it.**

**Q7. Exact rule proposed (documented, not implemented)**

> **Live (all assets, all systems): unchanged — 3 consecutive closes.**
>
> **Paper only, frozen 2026-09-15 (BTC Gerhard SMA120, long/flat).** Two shadow books run next to the live rule,
> in both directions:
> - **P1, 1-close:** act at the next open after the first close across the SMA.
> - **P2, 0.5σ fast-track:** act on the first close only if `b = (close / SMA120 − 1) / σ20 ≥ 0.5` in the new
>   direction. Otherwise wait for 3 closes, and unwind at the next open if price closes back before close 3.
>
> Each shadow book logs its **entry and exit decisions separately**, so the faster-exit and faster-entry legs can
> be judged on their own.
>
> **Review** after 12 BTC transitions or 18 months, whichever is later. No production change before the real-fill
> replay (P0-3) and that forward sample.
>
> **Also paper-trade** M4 strength staging (all assets).

**Q8. Confidence**
- **LOW–MEDIUM** that faster confirmation helps BTC (downgraded from MEDIUM):
  - it survives leaving out any single year or cycle, and the transition-segment bootstrap;
  - but it is absent in 2019–21 and comes mostly from exits;
  - it is research-period, not untouched, out-of-sample (the question was posed after seeing 2026);
  - it opposes the ETH and SOL evidence.
- **HIGH** that the ETH and LL 3-close rule should not be loosened (the ETH faster-exit leg is negative in every
  leave-out).
- **LOW** for M4 staging.

**Q9. Audit after external review (2026-09-15; frozen rules, no new parameters)**

Source: `track_a/robustness.py`, output in `results/tables/track_a/robustness_*.csv`.

**Sharpe difference vs 3-close:**

| Asset / rule | 2019–26 | 2019–21 | 2022+ | No 2026 | No Aug-2026 episode |
|---|---:|---:|---:|---:|---:|
| BTC fast, both directions | **+0.23** | −0.05 | +0.57 | +0.19 | +0.20 |
| BTC fast **exits only** | +0.15 | −0.03 | +0.34 | +0.12 | +0.15 |
| BTC fast **entries only** | +0.09 | −0.02 | +0.22 | +0.07 | +0.05 |
| BTC 0.5σ, both directions | +0.21 | +0.01 | +0.46 | +0.16 | +0.18 |
| ETH fast exits only | **−0.15** | −0.23 | −0.07 | −0.16 | −0.15 |
| ETH fast entries only | +0.03 | −0.04 | +0.14 | +0.01 | +0.01 |
| SOL fast exits only | −0.08 | −0.45 | −0.04 | −0.10 | −0.08 |

**Inference, BTC fast both directions, 2019–26:**

| Method | 95% CI | p (≤ 0) |
|---|---|---:|
| 60-day block bootstrap (original) | [+0.05, +0.48] | — |
| Transition-segment bootstrap (51 segments) | [+0.04, +0.60] | 0.006 |
| Leave-one-year-out | min +0.16, all 8 positive | — |
| Leave-one-cycle-out | min +0.15, all 5 positive | — |

- The largest single segment is 32% of the gain.
- Neither leg is significant on its own: exits-only CI [−0.01, +0.42], entries-only [−0.01, +0.25].

**Reading**
1. **The review was right about attribution.** Faster *exits* carry about 62% of BTC's gain, and removing the
   August-2026 episode leaves the entry leg at +0.05. The August-2026 motivation (enter strong breaks sooner) is the
   weaker half of the evidence.
2. **The asymmetry flips across assets.** Faster exits help BTC but hurt ETH (every leave-out negative) and SOL.
   Faster entries are mildly positive on ETH. No rule generalises, which is exactly why a BTC-only change needs
   forward evidence.
3. **Excluding 2026 or the August episode shrinks the effect (+0.19 / +0.20) but does not remove it.** The problem is
   the 2019–21 null, not 2026.
4. **Segment-level inference is about as tight as the daily block bootstrap at the lower bound.** The concern about an
   overconfident CI is reasonable, but it does not overturn the BTC result. Being research-period out-of-sample does.

---

## Structural alpha

**Q1. Top 5 families (by research promise, not by proven edge)**

| Rank | Family | Why |
|---|---|---|
| 1 | **Funding mechanics** (C1, interval shortened at the cap) | Largest cost-surviving OOS effect; forced payers plus squeeze convexity; needs point-in-time spec data and a comparable control |
| 2 | **Liquidation forced flow** (Q4, pooled short-liquidation continuation) | Q4 replicated on a larger asset set but is mostly momentum; causality needs intraday liquidation timestamps |
| 3 | **Listings** (L1 short new perps) | 67% win rate, median +13%, but brutal tails |
| 4 | **OI/price quadrants** (O1) | Momentum amplifier; best capacity |
| 5 | **Cross-venue funding dispersion** | Untested but data is already cached; delta-neutral |

**Q2. Strongest effects (v2, after review fixes)**

| Effect | Estimate | Status |
|---|---|---|
| C1 per event, OOS | +15.6% [+4.4, +29.8] | Median −0.4%; top 5 events = 81% of P&L; control not comparable |
| Q4 per event, OOS | +1.96% [+0.17, +3.48] | Portfolio Sharpe 1.11; only +0.7% [−0.5, +1.9] over a balanced price control |
| Shorting after liquidation spikes | Q2 −4.8% [−8.9, −1.2]; Q3 −3.8% | Bad trades; the v1 claim of being worse than the price effect is withdrawn (Q3 excess CI now [−7.8, +1.0]) |
| Shorting >100% annualised funding (F4) | −4.9% per event | Negative |

**Q3. Who is the counterparty?**

| Family | Counterparty |
|---|---|
| C1 | Crowded short payers forced to pay capped funding every 1–4h, and short sellers squeezed out of thin alts |
| Liquidations | Margin engines closing leveraged positions regardless of price, then late shorts fading the move |
| Listings | Attention-driven buyers, with early holders and market makers supplying stock |
| Funding contrarian (failed) | The supposed counterparty, crowded payers, is *not* systematically wrong over 5 days; funding pays for momentum risk |

**Q4. What survives costs and OOS?**
- Mean CI > 0 after all costs, including the BTC hedge leg, OOS: **Q4 and C1 only.**
- Also beating a *comparable* price-only control: **none.**
  - Q4's balanced control leaves +0.7% [−0.5, +1.9].
  - C1's control is not comparable.
- Also not outlier-driven: **Q4 only** (top 5 = 29%), and Q4 fails the control test.
- Surviving Benjamini-Hochberg across 23 hypotheses: **none** (min adjusted p 0.13 wild / 0.14 t / 0.175
  directional).

**Q5. What should be paper-traded?**
- **Formally, nothing clears the PAPER TRADE gate.**
- **Shadow-log at zero cost** (next-open hypothetical fills, logged daily before outcomes):
  - C1, with median and ex-top-5 statistics as primary;
  - Q4, as the benchmark for liquidation signals;
  - the pre-registered pooled short-liquidation continuation (P1-3);
  - BTC P1/P2 books and M4 trend staging.
- Review after 90 days.

**Q6. What needs more data?**

| Idea | Data needed |
|---|---|
| Liquidation ideas | Intraday liquidation timestamps (P0-1) |
| C1 | Point-in-time funding caps and intervals (P0-2) |
| Listings | Announcement timestamps and short availability (P1-2) |
| Token unlocks (strongest untested prior) | Point-in-time unlock schedules (P1-4) |
| Positioning | Binance archive metrics (backfilling now, P1-1) |

**Q7. What next?**
1. Start the liquidation collector and spec snapshotter this week (P0-1, P0-2).
2. Replay BTC Gerhard fast-track on real fills (P0-3).
3. Schedule the daily shadow log (P0-4).
4. Resolve the lake bronze regression and mount Drive (P0-5).
5. Open the positioning metrics only after writing their three hypotheses down (P1-1).

---

## Portfolio

**Q1. What diversifies the trend book?**
- The Gerhard 3-close book is ρ 0.72 to BTC.
- Every Track B event portfolio has |ρ| ≤ 0.14 to both BTC and Gerhard, and ≤ 0.10 to the majors/alts L/S book.
- The genuinely independent candidates are **C1, L1 and X4** (ρ ≈ 0 to everything). The liquidation/OI cluster is
  also near zero to BTC.
- **Diversification is available; positive expectancy is not yet proven.**

**Q2. What just replicates beta or momentum?**
- **O1 and Q4:** 0.7–0.9 portfolio correlation to the "buy anything up 1σ" control. Against a balanced control their
  event excess is +0.4% [−0.2, +0.8] and +0.7% [−0.5, +1.9].
- **Q1, X3, O4:** 0.57–0.8 to the "buy anything down 1σ" control. Short-term reversal.
- **F2, P2:** 0.6 to each other and to the controls. Funding and basis are momentum/reversal seen through a different
  lens.
- **CB1, CB2:** BTC beta (ρ ±0.3).
- **Faster BTC trend entry:** more BTC beta at trend turns. That is fine; it is what the book is for.

**Q3. Highest research ROI**
1. **Liquidation event stream and contract-spec snapshots.** Cheap, time-sensitive, and they unblock both of the
   only two families with a plausible forced-flow counterparty.
2. **Real-fill replay of the BTC fast-track.** Highest probability of a production improvement.
3. **Daily shadow log.** The only clean OOS evidence available.
4. **Point-in-time unlock data.** Highest prior, highest data cost.

Lowest ROI:
- more daily-bar mining of funding and basis;
- ML models;
- intraday seasonality.

---

## Caveats

- **Data**
  - Daily data only for Track B signals. CoinGlass liquidations and OI are daily cross-venue aggregates (Binance,
    OKX, Bybit, HTX); pre-2021 values are treated as missing.
  - Funding intervals for C1 are inferred.
- **OOS window:** 2024–26 is mostly a BTC bull regime. Short-side failures may be partly regime.
- **Costs** are modelled, not measured. The 100bp alt tier is a guess at stressed small-cap execution.
- **Lake:** the lake bronze regression is unresolved and Drive is unmounted. Track A/B used exchange-native caches,
  not the lake.
- **LL Pro:** the reconstruction is approximate (colour flips can run 0–4 days late) and is for personal research use
  only.
