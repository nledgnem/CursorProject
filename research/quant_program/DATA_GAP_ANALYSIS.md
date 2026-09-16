# Data Gap Analysis & Acquisition Roadmap

*2026-09-15 · builds on `DATA_AND_SYSTEM_AUDIT.md` and live API probes run the same day.*

**Decision rule.** Every dataset must answer: *"What specific decision becomes better if we have it?"*
Datasets without a clear decision are rejected, however easy they are to get.

**Priority score** (qualitative, 1–5 each):

```
(research value × P(real edge) × capacity of the edge × difficulty for competitors)
÷ (cost × engineering burden × data-quality risk)
```

**Build-vs-buy codes**

| Code | Meaning |
|---|---|
| A | Already available internally |
| B | Freely downloadable |
| C | Reconstructable internally |
| D | Paid vendor preferable |
| E | Unavailable or unreliable historically |

**Actions:** IGNORE · NICE TO HAVE · COLLECT GOING FORWARD · BACKFILL HISTORY · BUILD PIPELINE · PURCHASE / EVALUATE VENDOR

---

## 1. Master table

| # | Dataset | Current availability | Research enabled | Current limitation | Frequency | History needed | Point-in-time? | Potential source | Build vs buy | Cost / effort | Research value | Priority | Action |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | **Per-venue funding history (Binance, Bybit, OKX, Hyperliquid, Deribit)** | Binance full (backfilled 2019→, 843 perps). Bybit + HL for top-60 (backfilling). OKX ~3 months only. | Funding dislocations, cross-venue funding divergence, carry capacity, HL-specific crowding | OKX history unavailable free; HL only since 2023-05 | per print (1h/4h/8h) | full available | No (prints are immutable), but interval changes must be stored | Exchange APIs; Binance archive; CoinGlass (key present) | B (+A for Binance) | Low | HIGH | **P0** | BACKFILL HISTORY (Bybit, HL all perps); COLLECT GOING FORWARD (OKX, Deribit) |
| 2 | **Per-venue open interest + long/short ratios** | CoinGlass aggregate OI (top-60, backfilling); Binance archive `metrics` 5-min OI / top-trader / global / taker ratios 2020-09→ (free, not yet pulled beyond a sample) | OI/price divergence, crowding, liquidation-fuel conditioning, venue-specific positioning | Binance API gives only 30 days; the archive is 1 file per symbol-day (heavy); no per-venue OI outside Binance | 5-min → daily | 2020-09→ | No | data.binance.vision (B); Bybit OI 1d API (B); CoinGlass (A) | B | Medium (≈2,200 files/symbol) | HIGH | **P0** | BACKFILL HISTORY for top-100 Binance perps; COLLECT GOING FORWARD hourly for all venues |
| 3 | **Liquidation events with side, venue and timestamp** | BTC daily per venue (CoinGlass 2020→); top-60 daily aggregates (backfilling); lake per-asset 2024-01→2026-06 (worktree only) | Forced-flow reversion vs cascade, liquidation/OI thresholds, intraday timing of flushes | Daily aggregates hide timing; venue coverage spliced (Hyperliquid/Gate/Bitget added late); Binance liquidation history endpoint removed | event (WebSocket) → 1m | forward from now; history only via vendor | No | Exchange WebSocket `forceOrder` streams (Binance/Bybit/OKX), HL trades/liquidations (B, forward only); CoinGlass / Kaiko / Amberdata (D) | B forward + D history | Low to collect; Medium to buy | VERY HIGH | **P0** | COLLECT GOING FORWARD immediately; EVALUATE VENDOR for 1m history (top-50) |
| 4 | **Historical token unlock / vesting schedules + point-in-time circulating supply** | None | Token-unlock flows (anticipation and post-unlock selling), supply-adjusted valuation, listing-supply overhang | Nothing in lake; DefiLlama emissions API paywalled; `fact_markets_snapshot` supply only accumulating since 2026-01 | event + daily | 3+ years | **Yes, critical** (schedules get revised; today's schedule applied historically is look-ahead) | Tokenomist / TokenUnlocks, DefiLlama Pro, Messari (D); on-chain vesting-contract reconstruction per token (C, expensive) | D (C only for top-20 tokens) | Medium–High | HIGH | **P1** | PURCHASE / EVALUATE VENDOR (require point-in-time snapshots); COLLECT GOING FORWARD daily supply snapshots now |
| 5 | **Exchange listing / delisting announcements with timestamps** | Listing *dates* inferable (first kline); delisting inferred from frozen tails; no announcement times | Separate announcement effect from listing effect; delisting forced-exit shorts; spot vs perp listing sequencing | No announcement timestamps anywhere; Binance exchangeInfo shows only current state | event (minute) | 2020→ | Yes (announcement time must be the first public time) | Binance announcements API / RSS / X scraping (C); Kaiko events, The Tie, CryptoPanic (D) | C | Low–Medium | HIGH | **P1** | BUILD PIPELINE (scraper + archive) + backfill from announcement pages |
| 6 | **Synchronized 1-minute OHLCV for top-50 perps across Binance, Bybit, OKX, Hyperliquid, Coinbase, Kraken** | 1h BTC/ETH/SOL (Binance perp + spot, backfilled); daily for all Binance perps | Cross-venue dislocation, session effects, funding-window flow, liquidation timing, realistic execution slippage for Track A | Daily bars can't separate stale prints from executable divergence | 1m | 2–3 years | No | Exchange APIs / Binance archive (B); HL candles only last 5,000 bars → collect forward (B) | B | Medium storage (≈50 GB compressed for 50 × 6 venues × 3y) | MEDIUM–HIGH | **P1** | BACKFILL HISTORY (Binance/Bybit/OKX/Coinbase); COLLECT GOING FORWARD (HL, Kraken) |
| 7 | **Perp premium / mark / index prices + dated futures curves** | Binance daily premium index all perps (backfilled 2020→); COIN-M quarterlies fetchable | Basis dislocations, term-structure carry, mark/index anomalies around oracle updates | No term structure; no index constituent history | 1h (premium), daily (curve) | 2020→ | Index constituents: yes | Binance `premiumIndexKlines`, COIN-M, Deribit futures, CME (B / D for CME) | B | Low | MEDIUM | **P1** | BACKFILL HISTORY (1h premium top-100; BTC/ETH curves) |
| 8 | **Options surface: IV, skew, term structure, OI by strike (BTC/ETH)** | DVOL daily only | Trend-confirmation conditioning (vol regime), vol risk premium, dealer-gamma pinning around expiries | No chains; skew/term structure unavailable | 1h snapshots | 2+ years | No | Deribit API snapshots forward (B); Tardis / Amberdata / Laevitas historical (D) | B forward + D history | Low forward; Medium buy | MEDIUM | P2 | COLLECT GOING FORWARD (hourly Deribit summary); evaluate history only if vol-premium research is prioritised |
| 9 | **L2 order-book depth (±1%/±2% bands) + spread for top-50 perps** | Binance `bookDepth` archive 2023-01→ (not pulled); Variational spread tiers (snapshots) | Capacity estimates, slippage/impact models for alt strategies, liquidity-regime filters, order-book imbalance | No depth history in lake; costs are currently assumed, not measured | 1m depth bands (not full L2) | 1–2 years | No | Binance archive `bookDepth` (B); Tardis / Kaiko full L2 (D) | B for depth bands; D for full L2 | Medium; full L2 is **very** heavy (TB-scale) | HIGH for capacity/costs, LOW for signals | **P1** (depth bands) / P3 (full L2) | BACKFILL depth bands; IGNORE full L2 until a microstructure strategy survives |
| 10 | **Trades / tick data** | None (archive available: spot trades 2017→, perp aggTrades 2020→) | CVD, taker aggression, true intraday liquidation-cascade reconstruction | Heavy; daily taker volume already captures much of the signal | tick | 6–12 months for studies | No | Binance archive (B); Tardis (D) | B | High storage (hundreds of GB) | MEDIUM | P3 | NICE TO HAVE; pull only for specific event windows (±1 day around liquidation events) |
| 11 | **Stablecoin supply, mints/burns, exchange balances** | None | Liquidity-regime conditioning (net new dollars), pre-rally mint signals | Not in lake | daily | 2020→ | Supply: no; exchange balances: attribution changes (yes) | DefiLlama stablecoins (B, verified free 2017→); on-chain mints via Etherscan/Dune (C) | B | Low | MEDIUM | P2 | BACKFILL HISTORY (DefiLlama total + per-chain) |
| 12 | **Exchange net flows (BTC/ETH/stables to and from exchanges)** | None | Selling-pressure anticipation around unlocks, whale deposits before dumps | Wallet attribution is vendor-specific and revised; noisy | daily / hourly | 3 years | **Yes** (attribution changes) | CryptoQuant / Glassnode (D); Dune labels (C, partial) | D | Medium | MEDIUM | P2 | EVALUATE VENDOR only if unlock research is funded; otherwise IGNORE |
| 13 | **Hyperliquid HIP-3 markets, deployer params, oracle updates** | Forward snapshots possible (`perpDexs`, `meta`); none stored | New-market price discovery, oracle-lag anomalies, tokenised equity/commodity perps on HL | Brand new; no history exists | event + 1m | forward only | Yes (params change) | Hyperliquid info API (B) | B | Low | MEDIUM–HIGH (new, uncrowded) | **P1** | COLLECT GOING FORWARD now (hourly snapshots + candles + funding) |
| 14 | **Contract spec / rule-change history (leverage brackets, funding caps, interval changes, delisting notices, margin changes)** | Current snapshots only; funding-interval changes inferable from print spacing | Contract-mechanics anomalies; explaining regime breaks in funding / liquidation data | Exchanges overwrite specs; no archive | event | forward + inferred history | **Yes** | exchangeInfo / leverageBracket daily snapshots (B), announcement scraper (C) | B/C | Low | MEDIUM | P2 | COLLECT GOING FORWARD (daily spec snapshots, diffed) |
| 15 | **ETF flows, CME futures OI and basis, Coinbase premium** | ETF flows 2024-01→2026-08 (CoinGlass, BTC study); Coinbase vs Binance daily (derivable) | Institutional-demand regime, CME basis as institutional carry signal, weekend-gap effects | CME not in repo; ETF coverage BTC only | daily | 2021→ (CME), 2024→ (ETF) | No | CoinGlass (A); CME via vendor or CFTC COT (B, weekly) | A/B | Low | MEDIUM | P2 | BACKFILL HISTORY (CFTC COT weekly, ETH ETF flows) |
| 16 | **Macro (SPX, NDX, VIX, DXY, rates, liquidity)** | SPX/NDX/DXY/10Y daily (yfinance, BTC study) | Regime conditioning for Track A/B only | Weekend NaNs; no VIX | daily | full | No | yfinance / FRED (B) | B | Trivial | LOW–MEDIUM | P3 | NICE TO HAVE (add VIX, FRED liquidity) |
| 17 | **Point-in-time market cap / rank / universe membership** | `universe_eligibility` frozen 2025-12-01 with ETH/SOL mis-mapped; CoinGecko market cap history (bronze regression locally) | Survivorship-free cross-sectional research beyond Binance perps; any size-sorted strategy | Frozen producer; mapping errors; current rank ≠ historical rank | daily | 3+ years | **Yes** | CoinGecko history (A/B, fix producer); CoinMetrics reference data (D) | C | Medium (engineering) | HIGH (foundational) | **P1** | BUILD PIPELINE (restore producer, point-in-time snapshots) |
| 18 | **DEX prices / volumes / DEX–CEX divergence** | None | New-token price discovery before CEX listing; DEX–CEX arbitrage | Chain-by-chain complexity | 1m–1h | 1 year | No | DefiLlama DEX volume (B); Dune (C); GeckoTerminal (B) | B/C | Medium | MEDIUM | P3 | IGNORE until a listing strategy survives |
| 19 | **Social / search / sentiment** | None | Attention proxy for listing drift | Noisy, easily faked, poor historical consistency | daily | 2+ years | Yes | Google Trends, LunarCrush (D) | D | Medium | LOW | P3 | IGNORE |
| 20 | **Prediction-market probabilities** | None | Event-risk conditioning; new-market framework | Short, thin history | 1h | forward | No | Polymarket API (B) | B | Low | LOW–MEDIUM | P3 | NICE TO HAVE (forward collection only) |

---

## 2. The five highest-value datasets we do NOT have

| Rank | Dataset | Why it matters | Strategies / anomalies it unlocks |
|---|---|---|---|
| 1 | **Real-time liquidation event stream (side, venue, size, timestamp)** + vendor 1m history for the top-50 | Forced flow is the cleanest mechanism in crypto (margin engines sell regardless of price). Daily aggregates can't tell a 2-minute cascade from a slow bleed, which is exactly what separates *reversion* from *continuation*. | Liquidation-flush reversion; cascade continuation; intraday entry timing for Track A fast-tracks; liquidation-conditioned funding trades |
| 2 | **Point-in-time token unlock schedules + circulating supply** | The most predictable non-price flow in crypto (dated, sized, recipient-known). It can't be researched at all today, and building it wrong (today's schedule applied historically) guarantees false edges. | Pre-unlock short/hedge drift; post-unlock selling; unlock × funding/OI crowding; supply-adjusted cross-sectional value |
| 3 | **Binance archive positioning metrics (OI, top-trader and global long/short, taker ratio) for the top-100 perps** | Free, deep (2020-09→), exchange-native. Converts funding-only crowding signals into *who* is crowded (top traders vs retail accounts). | OI/price divergence; crowded-long reversal; retail-vs-whale positioning splits; interaction effects (B8) |
| 4 | **Listing / delisting announcement timestamps** | Separates attention effects (announcement) from supply/liquidity effects (listing). Without it, listing studies measure a blend and any tradeable pre-listing edge is invisible. | Announcement drift; pre-listing run-up and post-listing fade; delisting forced exits; perp-before-spot sequencing |
| 5 | **Synchronized 1m cross-venue bars + depth bands (±1%/±2%)** | Turns assumed costs into measured costs and tells executable dislocations from stale prints. Every alt-perp strategy's capacity estimate depends on it. | Cross-venue dislocation; realistic capacity and slippage for B1/B6/B7; session/funding-window effects; realistic Track A execution |

---

## 3. Answers to the roadmap questions

**Q4. What to start collecting immediately, even without historical backfill?**
1. **Liquidation WebSocket streams:** Binance, Bybit and OKX `forceOrder`/liquidation channels, plus Hyperliquid trades flagged as liquidations. It's cheap (≈ a few hundred MB/month) and history can't be recovered free later.
2. **Hyperliquid snapshots, hourly:** `meta`, `perpDexs` (HIP-3), funding, OI, mark/oracle prices, 1m candles (history is capped at 5,000 bars).
3. **OKX funding and OI, hourly:** the free API keeps only ~3 months.
4. **Daily contract-spec snapshots, diffed:** Binance exchangeInfo + leverage brackets, HL meta, Variational stats.
5. **Daily circulating-supply snapshots** for the top-300 assets: point-in-time supply starts accruing now.
6. **Deribit option summary, hourly** (BTC/ETH): IV, skew and term structure build history for later vol-regime work.

**Q5. Which historical datasets are worth purchasing?**
- **Unlock schedules with point-in-time revisions** (Tokenomist or DefiLlama Pro). Only buy if the vendor can prove historical snapshots rather than a current table; otherwise it is look-ahead by construction.
- **1m liquidation history for the top-50** (CoinGlass higher tier, Kaiko or Amberdata). Evaluate on a 3-month trial against our BTC per-venue data first.
- **Not yet:** full L2 or tick history (Tardis), until a microstructure strategy survives on cheaper data.

**Q6. What can we reconstruct ourselves?**
- **Positioning history:** Binance archive metrics (OI, long/short, taker) and `bookDepth` depth bands.
- **Funding and premium:** funding for every venue with a history API, and premium-index basis.
- **Listing and delisting dates:** first kline and frozen-tail detection (already built).
- **Funding-interval change history:** from print spacing (already built).
- **Stablecoin supply:** DefiLlama.
- **Announcement timestamps:** a scraper over exchange announcement archives.
- **Point-in-time universe membership:** by restoring the decommissioned `universe_eligibility` producer with daily snapshots.

**Q7. Which apparent datasets are likely not worth the cost?**
- **Full L2 / tick archives** at this stage: TB-scale storage, and our edges are daily-horizon.
- **Social sentiment feeds:** noisy, revisable, easily gamed.
- **Generic on-chain dashboards** (active addresses, TVL) without a specific flow hypothesis.
- **Exchange-flow data** unless the unlock programme is funded: attribution revisions make history unreliable.
- **Macro vendor feeds:** free sources suffice for regime conditioning.

**Q8. Blind spots in the existing data lake**
1. **Survivorship and frozen data.** Binance serves frozen candles for 145 delisted perps. Any study that treats them as live prices fakes zero returns. There is no lake-level delisting registry.
2. **No point-in-time anything.** Universe, mappings (`valid_from` is a build date) and supply are all current-state. The one point-in-time artifact (`universe_eligibility`) is frozen and mis-maps ETH/SOL.
3. **Venue concentration.** Funding and OI history is Binance-only in the lake, yet the firm trades on Variational and Hyperliquid, where funding and liquidation mechanics differ.
4. **Execution venues are unmeasured.** No historical Variational or Hyperliquid prices, spreads or funding, so live-vs-backtest gaps (e.g. Apathy's funding −7.6%/cohort vs a 1–2.5% assumption) can't be diagnosed.
5. **No intraday data at all** in the lake: every "confirmation" and "event" study is forced into daily granularity.
6. **Units and timestamp conventions vary by table** (funding decimal vs percent; CoinGecko one-day stamp offset). There is no automated unit or offset test in the pipeline.
7. **No realised trade and position history** for LL Pro / danlongshort, so strategy-vs-live attribution and portfolio correlation work is impossible.
8. **Data versioning.** The Drive export overwrites in place, and the Aug-2026 bronze regression went unnoticed. Content hashes per nightly export (as `qlib.governance` does for research runs) would catch this.

---

## 4. Suggested sequencing (value ÷ effort)

| Week | Action | Owner-type | Unlocks |
|---|---|---|---|
| 1 | Forward collectors: liquidation WebSockets, HL hourly snapshots, OKX funding/OI, spec snapshots, supply snapshots | data engineering (small services on Render) | Irrecoverable history starts accruing |
| 1–2 | Backfill Binance archive metrics (top-100) + bookDepth bands (top-50) | research (reuse `qlib.data`) | Positioning (B7/B8), measured costs and capacity |
| 2 | Backfill Bybit + HL funding for all perps; 1h premium for the top-100 | research | Cross-venue funding divergence, basis at 1h |
| 2–3 | Restore the point-in-time universe producer; nightly content-hash manifest on the Drive export | data engineering | Foundational hygiene |
| 3–4 | Announcement scraper + backfill | data engineering | Listing announcement effects |
| 4–6 | Vendor trials: unlock schedules (point-in-time), 1m liquidation history | research lead | Unlock programme; intraday forced flow |
