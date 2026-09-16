# Structural Alpha Map

*2026-09-15 · Track B planning document.*

For each anomaly family this sets out:
- the economic hypothesis and who is forced or induced to trade against us;
- what data it needs and whether we have it (see `DATA_AND_SYSTEM_AUDIT.md`);
- a feasibility grade and a pre-registered test plan.

It was written **before** any Track B result was computed, so the hypotheses and tests can't be tuned to the outcomes.

**Feasibility**
- **A:** testable now or after the free backfill already running.
- **B:** partially testable (proxy data, short history, or a single venue).
- **C:** needs new data (see `DATA_GAP_ANALYSIS.md`).

**Priority** is expected research value (mechanism strength × probability of a real edge × capacity × difficulty to replicate) divided by data/engineering cost.

---

## Summary

| # | Family | Core mechanism | Counterparty | Prior class | Data we have | Feasibility | Priority |
|---|---|---|---|---|---|---|---|
| 1 | Funding dislocations | Leverage demand exceeds arbitrage capital; funding is the price of that imbalance | Levered directional longs (or panicking shorts) paying to hold | Risk premium + forced flow | Binance funding prints, 843 perps incl. delisted, 2019→; Bybit/HL for the top 60 (backfill) | **A** | **P0** |
| 2 | Basis dislocations | Perp premium vs index reflects the same imbalance; cash-and-carry capital is slow and costly | Same levered longs; basis traders earn the carry | Risk premium | Binance daily premium index, all perps, 2020→; no dated futures curve | **A** (perp/spot), **C** (term structure) | **P1** |
| 3 | Cross-venue price dislocations | Fragmented liquidity, transfer frictions, regional USD access | Local flow that can't cross venues (KRW, USD-only, on-chain) | Structural / microstructure | Coinbase vs Binance daily closes only | **B** daily premium / **C** intraday | P2 |
| 4 | Listings / delistings | Attention shock, new supply unlocking, short-selling unavailable at launch | Retail buying on attention; early holders selling into new liquidity | Behavioural + structural | Perp first/last-trade dates for 843 perps (survivorship-free); no announcement timestamps | **B** (listing-date drift) / **C** (announcement effect) | **P1** |
| 5 | Token unlocks / vesting | Insiders and investors receive supply and sell; the market underprices the anticipated flow | Unlock recipients with low cost basis | Forced / predictable flow | None | **C** | P1 (data-gated) |
| 6 | Liquidation / forced flow | Margin calls force market orders regardless of price; the move overshoots, then reverts or cascades | Liquidated traders and ADL counterparties | Forced flow | CoinGlass liquidations: BTC per venue 2020→; top-60 assets aggregated (backfill, 2021→); per-asset lake copy 2024-01 → 2026-06 | **A** | **P0** |
| 7 | OI / price divergence | New leverage built into a move signals crowding; OI unwinds mark exhaustion | Late levered momentum traders | Behavioural + forced | CoinGlass OI (top 60); Binance archive OI/long-short (top 30, 2020-09→) | **A** | **P1** |
| 8 | Funding + OI + liquidation interactions | Crowding (funding + OI) plus fuel (liquidations) creates asymmetric reversal risk | Crowded side | Forced flow | 1 + 6 + 7 | **A** (top 60) | **P0** after 1/6/7 |
| 9 | Weekend / time-of-day | Liquidity varies by session; TradFi-linked flow is absent on weekends; funding settlements create hourly flow | Liquidity-constrained flow at settlement windows | Microstructure | Daily all perps; 1h BTC/ETH/SOL perp and spot (backfill) | **A** (weekend, sessions for 3 majors) | P2 |
| 10 | Oracle / contract mechanics | Funding caps and floors, interval changes, mark/index construction create predictable payments or basis behaviour | Positions trapped by caps; mechanical rebalancers | Structural | Funding prints (caps visible), funding-interval changes (inferable from print spacing); HL/Variational specs (snapshots) | **B** | P2 |
| 11 | New products / venues | Early price discovery is thin: few market makers, uncalibrated funding, retail-dominated flow | Early retail; venue incentive farmers | Structural (decaying) | Listing first days (Binance); HL HIP-3 metadata (forward only) | **B** framework / **C** HIP-3 history | P2 (framework) |
| + | Coinbase / USD premium | US-regulated USD demand (ETF creations, institutions) vs offshore USDT liquidity | US spot buyers who can't use offshore venues | Structural | Coinbase and Binance daily BTC/ETH/SOL | **A** (daily) | P2 |
| + | Delisting / collapse (short side) | Delisted or collapsing perps: forced exits, no dip buyers | Holders forced out by delisting deadlines | Forced flow | Last-trade dates from frozen-tail detection (145 delisted) | **B** (no announcement dates) | P3 |

---

## Family details and pre-registered tests

### 1. Funding dislocations — P0
- **Hypothesis.** Extreme positive funding means levered longs pay heavily to hold. Two outcomes are possible:
  - (a) *mean reversion*: crowded longs are the marginal seller when momentum stalls, so returns are weak after extreme funding;
  - (b) *risk premium*: short perp plus long spot earns the funding.
- **Who pays, and why they accept it.** Leverage-constrained retail and trend followers who value convexity and access over carry.
- **Why the edge persists.** Arbitrage capital is balance-sheet and venue constrained. Carry trades suffer violent squeezes: the short perp leg can be liquidated before funding accrues.
- **What kills it.** Deep, cheap basis capital (ETF-era institutions), lower leverage caps, funding caps.
- **Tests (Binance, all perps).** Signals use funding known at time t: daily sum of prints; 3-day and 7-day mean annualised; cross-sectional and time-series z-scores. For each, measure:
  - forward 1–10 day returns of the perp (excess vs the date-matched baseline) by funding decile;
  - funding *persistence* (autocorrelation, half-life) and the carry P&L of a delta-neutral short perp / long spot, net of fees, basis change and the gap to next open;
  - cross-sectional quintile spread (long low funding / short high funding), liquidity-filtered;
  - *funding despite reversal* (price down ≥ 1σ over 3 days while funding stays in the top decile).
- **Controls.** Momentum (7/30-day return), size (volume rank), BTC beta; the monthly-cluster bootstrap; Benjamini-Hochberg across definitions.

### 2. Basis dislocations — P1
- **Hypothesis.** Perp premium vs index is a real-time imbalance measure. Extreme premium mean-reverts, and the perp underperforms the index.
- **Tests.**
  - Daily premium close deciles, and premium z (90-day) per asset.
  - Forward perp returns; premium half-life; premium–funding consistency. When funding is capped but premium is extreme, forced payments are coming.
  - A cross-sectional premium spread.
- **Limits.** Spot/futures term structure needs Deribit, CME or quarterly curves (COIN-M BTC/ETH quarterlies are fetchable; not in this pass).

### 3. Cross-venue price dislocations — P2
- **Hypothesis.** A USD vs USDT (Coinbase vs Binance) premium reflects US institutional demand that offshore markets absorb with a lag.
- **Tests.**
  - Daily Coinbase − Binance close premium z; forward BTC/ETH returns.
  - **Caveat:** daily closes can't separate stale pricing from real divergence. Intraday synchronized 1m data is needed to judge executability.
- **Kept as a watch item.** Executable arbitrage is a latency game we are not built for.

### 4. Listings / delistings — P1
- **Hypothesis.** Newly listed perps carry attention-driven long bias and supply overhang, so early days drift negative. Shorting is only possible once the perp exists.
- **Tests.**
  - Event = first trading day of each Binance USDT perp (survivorship-free).
  - Forward returns from day 1–2 open over 5–60 days, excess vs the BTC/market basket.
  - Conditioning: listing-day volume, market regime, whether spot pre-existed (proxy: coin age via first CoinGecko price, when available).
- **Limits.** No announcement timestamps, so the announcement effect can't be separated from the listing effect → data gap.

### 5. Token unlocks — P1, data-gated
- **Hypothesis.** Unlocks above ~1–2% of circulating supply or ~5 days of ADV produce pre-event underperformance (anticipatory shorting and hedging) and post-event selling by low-cost-basis recipients.
- **Not testable** without point-in-time unlock schedules and circulating supply. Specified in the data-gap analysis.

### 6. Liquidation / forced flow — P0
- **Hypothesis.** Large liquidations are uninformed forced trades.
  - **Overshoot → reversion** is expected when liquidations are large relative to OI but OI is reset (fuel exhausted).
  - **Cascade → continuation** is expected when liquidations are large but OI is still elevated or rebuilding.
- **Tests (BTC deep; top 60 aggregated).**
  - Long-liquidation and short-liquidation intensity: USD, % of prior OI, and z-scores. Forward returns in both directions.
  - Split by same-day OI change (reset vs rebuild) and funding sign.
  - Declustered at 7 days; calendar-matched null; separate from the prior BTC short-squeeze study, which treated only the long-side continuation case.
- **Known data issues.** Venue splice: use the consistent 4-venue panel only. Pre-2021 zeros are excluded.

### 7. OI / price divergence — P1
- **Hypothesis.** The four quadrants of (price Δ, OI Δ) mean different things:
  - price up / OI up: new longs, fragile continuation;
  - price up / OI down: short covering, weaker continuation;
  - price down / OI up: new shorts, squeeze fuel;
  - price down / OI down: long capitulation, reversion.
- **Tests.**
  - 1-day and 3-day price z × OI change z quadrants (thresholds ±1σ); forward returns.
  - Binance archive long/short ratios as crowding confirmation (top 30).

### 8. Interactions — P0 (only after 1, 6, 7)
Pre-registered combinations, each with a mechanism:
- **(a) Crowded long.** Top-decile funding + OI 7-day build ≥ 1σ + price 3-day ≥ 2σ → reversal.
- **(b) Short squeeze.** Negative funding + short liquidations ≥ 1σ of OI + OI falling → short-horizon continuation.
- **(c) Capitulation.** Long liquidations ≥ 2σ + OI reset ≤ −1σ + funding ≤ 0 → reversion upward.
- **(d) Funding despite reversal.** Top-decile funding + price 3-day ≤ −1σ → continuation down.

No other combinations will be tested. Benjamini-Hochberg is applied across all B6–B8 tests.

### 9. Weekend / time-of-day — P2
- **Hypotheses.**
  - Weekend returns and volatility differ as TradFi flow disappears, while Monday reopening flow is concentrated.
  - Returns in the hour around funding settlements (00/08/16 UTC) reflect pre-settlement positioning by funding-sensitive traders.
- **Tests.** 1h BTC/ETH/SOL perp and spot returns by hour-of-day and day-of-week, with block-bootstrap CIs. Settlement-hour return and volume vs adjacent hours.
- **Bar.** Only effects with a mechanism and stable sign across years are retained.

### 10. Contract mechanics — P2
- **Tests.**
  - Detect funding-cap hits (prints at ±cap) and funding-interval changes (8h → 4h → 1h, inferred from print spacing; Binance shortens the interval during extreme funding).
  - Forward returns and funding persistence after a cap hit or interval change.
- **Rationale.** Payments are forced and partly predictable; interval changes are exchange responses to stress.

### 11. New products / venues — P2 (framework)
- **Framework.** `qlib.events` plus a listing registry. Any new instrument (Binance perp, HL perp, HIP-3 market, tokenised equity on Variational) gets the same template:
  - day 1–30 drift vs benchmark;
  - funding level and decay;
  - spread and volume ramp;
  - basis to reference.
- **Forward collection needed.** HL `perpDexs`/`meta` and Variational stats snapshots (the Render pipeline already snapshots listings daily).

---

## Rules applied to every Track B test
1. **Economic hypothesis first.** The mechanism above is written before looking at outcomes.
2. **No look-ahead.** Signal known at the close of bar t; entry at the next open; baseline is date-matched; declustered events; calendar-cluster bootstrap CIs.
3. **Costs.** BTC/ETH/SOL 5+5 bp per side. Alt perps ≥ 10 bp slippage + 5 bp half-spread + square-root impact, stress-tested at 2–3×. Funding is included for perp holding periods.
4. **Sample splits.** In-sample ≤ 2023-12-31; out-of-sample 2024-01-01 → present. No threshold is re-chosen on OOS data.
5. **Multiple testing.** Every tested variant counts. Results carry Benjamini-Hochberg adjusted p-values across the family.
6. **Scoring and survival.** Survivors are scored with `qlib.scoring` (AQS). A high Sharpe without a mechanism is capped.
