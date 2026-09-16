# Crypto Quant Research — Executive Summary

*2026-09-16. Two workstreams: (A) trend-confirmation execution, (B) a structural-alpha research pipeline.
Revised after an external falsification review. Research only: nothing here trades.*

## The two decisions

### A. Trend confirmation (Gerhard SMA120 / LL Pro 3-close rule) — no production change

- Keep the **3-consecutive-close rule live on every asset and system**.
- **Paper-trade two frozen BTC rules** next to it: act on close 1, and act on close 1 only when the break is at
  least 0.5σ through the SMA. Log entry and exit decisions **separately**.
- Review after 12 BTC transitions or 18 months, alongside a replay on real fills.

**Why not adopt it, despite a positive backtest** (+0.23 Sharpe, CI [+0.05, +0.48] over 2019–26):

| Finding | Number |
|---|---|
| Gain before 2022 | −0.05 (i.e. none) |
| Share of the gain from exiting faster, not entering faster | ≈ 62% (+0.15 of +0.23) |
| Entry leg without the August-2026 episode that motivated the study | +0.05 |
| The same faster-exit rule on ETH | −0.15, negative in every leave-one-year and leave-one-cycle test |
| Evidence status | Research-period, not untouched, out-of-sample |

The effect does survive leaving out any single year or cycle, a transition-level bootstrap, and dropping 2026
(+0.19). That is enough to track it forward, not to trade it.

### B. Structural alpha (23 pre-registered signals on Binance perps) — nothing is ready for capital

- **Nothing survives multiple-testing control.** The lowest adjusted p for a positive signal is 0.13–0.175,
  depending on the method.
- **C1 (Binance shortens the funding interval): forward test only.** +15.6% per event out-of-sample
  [+4.4%, +29.8%] after all costs, but the median event is −0.4% and 5 of 113 events make up 81% of the profit.
- **Q4 and O1 are momentum in disguise.** Against a price-only control matched on move size, liquidity, beta and
  year, Q4 adds +0.7% [−0.5%, +1.9%] and O1 adds +0.4% [−0.2%, +0.8%].
- **Rejected as trades:** fading funding extremes, fading perp premium, the delta-neutral carry trade (−0.62% per
  event after costs), and shorting after liquidation spikes.
- **Diversification is available but unproven:** every signal portfolio has |correlation| ≤ 0.13 to BTC, to the
  Gerhard trend book and to the majors/alts long-short book.

## What the external review changed

Six of its seven findings were real. All are fixed, with unit tests (30 pass).

| Issue | Consequence |
|---|---|
| Short-signal controls flipped a return that already had costs deducted | The claim "shorting after liquidations is worse than the price move" is withdrawn |
| BTC hedge leg paid no trading costs and no funding | Returns fall 0.2–0.5pp per trade; O1's portfolio Sharpe 0.79 → 0.35 |
| Missing prices were booked as 0% returns | Positions are now skipped or closed at the last valid price |
| p-values assumed many independent clusters | Now uses small-sample and bootstrap methods |
| Price controls were counted as hypotheses in the multiple-testing family | Family 25 → 23 |
| Rankings could follow the scorecard rather than the evidence | Rule-based verdicts now cap every recommendation |

The seventh (possible look-ahead in C1) was checked and is clean: funding intervals are measured from the previous
print, and entry is the next day's open.

**Found while fixing, not raised by the review:** the data changed mid-review (a download fix replaced empty TradFi
contracts with real crypto, so liquidation signals now cover 53 assets rather than 36, and Q4 held up on the larger
set), and a cost bug I introduced in the carry test was caught before publication.

## What to do next

1. **Start collecting data that cannot be recovered later:** the liquidation event stream, and daily contract-spec
   snapshots (funding intervals and caps).
2. **Replay the BTC rules on real fills** and start the two paper books.
3. **Run the daily watchlist as a shadow log** — hypothetical fills recorded before outcomes are known. This is the
   only genuinely untouched evidence available.
4. **Build a price-only control on the full universe**, so C1, funding and basis signals become testable.
5. **Decide on the data lake:** the bronze regression is unresolved and the authoritative Drive copy is unmounted.

**Do not** spend more time on threshold grids, more daily-bar combinations of funding, open interest and
liquidations, basis fades, hour-of-day effects, or machine-learning models.

## Caveats

- Daily data; liquidations and open interest are cross-venue daily aggregates with no intraday ordering.
- Costs are modelled, not measured (majors 20bp round trip, illiquid alts 100bp, plus the hedge leg).
- The out-of-sample window (2024–26) is mostly a BTC bull regime.
- The LL Pro indicator is an approximate reconstruction, for personal research use only.
- The trend systems are reconstructions; no production trading code was read or modified.

## Documents in this pack

| Document | Contents |
|---|---|
| Decision memo | Every question answered, with the recommended rules |
| Trend confirmation research | Track A in full, including the post-review audit (§8a) |
| Structural alpha results | Track B v2: ranked signals, verdicts, controls, costs, correlations |
| Review response | Point-by-point reply to the external review |
| Research backlog | P0–P3 work, ranked by expected research value |
