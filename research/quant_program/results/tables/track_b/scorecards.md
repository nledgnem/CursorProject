# Track B scorecards (auto-generated)

### Q4_short_liq_OI_rebuild_long
*Family:* liquidation · **AQS 79.0** · **WATCH** · Classification: FORCED FLOW · Confidence: MEDIUM

| Field | Assessment |
|---|---|
| Mechanism | Margin engines liquidate regardless of price: flushes overshoot when fuel is exhausted (OI reset) and cascade when leverage keeps rebuilding. |
| Counterparty | Liquidated leveraged traders and the insurance fund / ADL counterparties. |
| Why they trade | Forced: margin calls, not choice. |
| Why the opportunity exists | Liquidity providers widen during cascades; risk capital is scarce exactly when needed. |
| Why it persists | Catching liquidation flushes is psychologically and operationally hard (gap risk). |
| What would kill it | Lower leverage, better liquidation engines (partial liquidations), more market-making capital. |
| Expected half-life | hours to days |
| Capacity / liquidity | medium (flush depth is large for majors); median event ADV $118m / stressed at entry |
| Execution difficulty | high (enters during volatility) |
| Venue / counterparty risk | exchange outages during cascades |
| Data quality | medium: CoinGlass aggregated daily, venue coverage spliced; pre-2021 zeros excluded |
| Statistics | n 1811; win_OOS 0.4191; mae_med_OOS -0.06996; mfe_med_OOS 0.07575; breakeven_rt_bp 233; median_rt_cost_bp 72.47 |
| In-sample | n 718; net_mean 0.01038; median -0.01415; t 1.54; p_bh 0.4362 |
| Out-of-sample | n 1088; net_mean 0.01964; median -0.01544; t 2.264; ci [0.0017, 0.0348]; p_bh 0.1402 |
| Cost sensitivity | break-even round trip 233 bp vs median modelled 72 bp (ratio 3.22) |
| Regime dependence | {'bear': (-0.0002, 911), 'bull': (0.0324, 900)} | years positive share 0.83 |
| Component scores (0-5) | mechanism 4, oos_significance 4, regime_robust 2, param_stability 4, cost_resilience 5, capacity 4, sample_size 5, decay 5, implementation 3, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.14 (>=0.10) → max PAPER TRADE; excess over price-only control +0.67% [-0.55%, +1.85%] not above 0 → max WATCH; verdict: NO INCREMENTAL STRUCTURAL ALPHA → max WATCH |

**Research verdict: NO INCREMENTAL STRUCTURAL ALPHA**

### C1_funding_interval_shortened
*Family:* mechanics · **AQS 72.4** · **WATCH** · Classification: STRUCTURAL ALPHA · Confidence: MEDIUM

| Field | Assessment |
|---|---|
| Mechanism | Exchanges shorten funding intervals under extreme funding, accelerating payments by the crowded side. |
| Counterparty | Crowded side forced to pay more frequently. |
| Why they trade | Positions already on; switching costs. |
| Why the opportunity exists | Rule change is mechanical and public but rarely monitored. |
| Why it persists | Few participants track contract-spec changes systematically. |
| What would kill it | Rule changes; wider adoption of spec monitoring. |
| Expected half-life | days |
| Capacity / liquidity | low-medium; median event ADV $15m / alt perps |
| Execution difficulty | medium |
| Venue / counterparty risk | exchange rule changes |
| Data quality | medium: interval changes inferred from print spacing |
| Statistics | n 136; win_OOS 0.4956; mae_med_OOS -0.2056; mfe_med_OOS 0.1899; breakeven_rt_bp 1452; median_rt_cost_bp 105.5 |
| In-sample | n 21; net_mean 0.0246; median 0.03294; t 1.887; p_bh 0.4362 |
| Out-of-sample | n 113; net_mean 0.1556; median -0.003843; t 2.427; ci [0.0435, 0.2980]; p_bh 0.1402 |
| Cost sensitivity | break-even round trip 1452 bp vs median modelled 106 bp (ratio 13.76) |
| Regime dependence | {'bear': (0.1379, 106), 'bull': (0.1253, 30)} | years positive share 0.67 |
| Component scores (0-5) | mechanism 3, oos_significance 4, regime_robust 4, param_stability 3, cost_resilience 5, capacity 2, sample_size 4, decay 5, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.14 (>=0.10) → max PAPER TRADE; excess over price-only control +15.25% [-0.23%, +33.89%] not above 0 → max WATCH; top-5 events = 81% of OOS P&L → max WATCH |

**Research verdict: FORWARD TEST ONLY**

### O1_price_up_OI_up
*Family:* oi_price · **AQS 66.6** · **WATCH** · Classification: BEHAVIORAL · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Price/OI quadrants separate new-leverage-driven moves from covering/capitulation. |
| Counterparty | Late levered momentum traders or capitulating longs. |
| Why they trade | Momentum chasing; margin pressure. |
| Why the opportunity exists | Crowding is observable but slow to resolve. |
| Why it persists | Signal is noisy and regime-dependent. |
| What would kill it | Wider use of OI dashboards. |
| Expected half-life | days |
| Capacity / liquidity | medium; median event ADV $139m / perp |
| Execution difficulty | medium |
| Venue / counterparty risk | exchange |
| Data quality | medium: aggregated OI across venues (spliced) |
| Statistics | n 3579; win_OOS 0.4208; mae_med_OOS -0.07107; mfe_med_OOS 0.06983; breakeven_rt_bp 180.2; median_rt_cost_bp 71.75 |
| In-sample | n 1272; net_mean 0.01173; median -0.01427; t 1.607; p_bh 0.4362 |
| Out-of-sample | n 2291; net_mean 0.01025; median -0.01588; t 1.522; ci [-0.0026, 0.0222]; p_bh 0.3965 |
| Cost sensitivity | break-even round trip 180 bp vs median modelled 72 bp (ratio 2.51) |
| Regime dependence | {'bear': (0.0031, 1549), 'bull': (0.0167, 2030)} | years positive share 0.83 |
| Component scores (0-5) | mechanism 2, oos_significance 2, regime_robust 4, param_stability 4, cost_resilience 4, capacity 4, sample_size 5, decay 4, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.40 (>=0.10) → max PAPER TRADE; excess over price-only control +0.37% [-0.15%, +0.84%] not above 0 → max WATCH; verdict: NO INCREMENTAL STRUCTURAL ALPHA → max WATCH |

**Research verdict: NO INCREMENTAL STRUCTURAL ALPHA**

### X2_short_squeeze_continuation
*Family:* interaction · **AQS 62.2** · **WATCH** · Classification: FORCED FLOW · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Crowding (funding + OI) combined with fuel (liquidations / extension) creates asymmetric reversal or continuation risk. |
| Counterparty | The crowded, levered side. |
| Why they trade | Late momentum / trapped positions. |
| Why the opportunity exists | Requires combining several data sources few monitor jointly. |
| Why it persists | Rare events; hard to size; painful to hold against crowded momentum. |
| What would kill it | Wider multi-factor monitoring; lower leverage. |
| Expected half-life | days |
| Capacity / liquidity | low-medium (rare events); median event ADV $154m / perp |
| Execution difficulty | medium-high |
| Venue / counterparty risk | exchange |
| Data quality | medium (inherits CoinGlass limitations) |
| Statistics | n 427; win_OOS 0.4465; mae_med_OOS -0.0524; mfe_med_OOS 0.06841; breakeven_rt_bp 118.9; median_rt_cost_bp 70.47 |
| In-sample | n 212; net_mean -0.0001085; median -0.004455; t -0.01177; p_bh 0.9907 |
| Out-of-sample | n 215; net_mean 0.009501; median -0.006893; t 0.8398; ci [-0.0120, 0.0333]; p_bh 0.6408 |
| Cost sensitivity | break-even round trip 119 bp vs median modelled 70 bp (ratio 1.69) |
| Regime dependence | {'bear': (0.0059, 320), 'bull': (0.0012, 107)} | years positive share 0.67 |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 4, param_stability 3, cost_resilience 3, capacity 4, sample_size 5, decay 3, implementation 3, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.64 (>=0.10) → max PAPER TRADE; top-5 events = 145% of OOS P&L → max WATCH |

**Research verdict: WATCH**

### F2_extreme_neg_funding_long
*Family:* funding · **AQS 61.0** · **REJECT** · Classification: PERSISTENT RISK PREMIUM · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Leverage demand exceeds arbitrage capital; funding prices the imbalance and crowded payers become marginal sellers (or buyers) when momentum stalls. |
| Counterparty | Levered directional traders (mostly retail / trend followers) paying funding to hold exposure. |
| Why they trade | Access to leverage and convexity matters more to them than carry cost. |
| Why the opportunity exists | Delta-neutral capital is balance-sheet, venue and collateral constrained; the short-perp leg can be squeezed before funding accrues. |
| Why it persists | Carry trades are unpleasant (squeeze risk, exchange risk, capital lock-up across venues). |
| What would kill it | Cheap institutional basis capital, lower leverage caps, funding caps, fee changes. |
| Expected half-life | days to weeks |
| Capacity / liquidity | high for majors; limited by alt-perp liquidity; median event ADV $21m / perp-dependent |
| Execution difficulty | medium (daily rebalance, perp margin management) |
| Venue / counterparty risk | single-exchange perp exposure (Binance); funding-cap rule changes |
| Data quality | good: exchange-native funding prints 2019-> (all perps incl. delisted) |
| Statistics | n 2406; win_OOS 0.3945; mae_med_OOS -0.1153; mfe_med_OOS 0.08465; breakeven_rt_bp 139.2; median_rt_cost_bp 103 |
| In-sample | n 921; net_mean 0.002628; median -0.02904; t 0.2678; p_bh 0.9086 |
| Out-of-sample | n 1465; net_mean 0.004433; median -0.02749; t 0.488; ci [-0.0147, 0.0216]; p_bh 0.7569 |
| Cost sensitivity | break-even round trip 139 bp vs median modelled 103 bp (ratio 1.35) |
| Regime dependence | {'bear': (0.0142, 1186), 'bull': (-0.0064, 1220)} | years positive share 0.71 |
| Component scores (0-5) | mechanism 4, oos_significance 1, regime_robust 2, param_stability 4, cost_resilience 2, capacity 2, sample_size 5, decay 5, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.76 (>=0.10) → max PAPER TRADE; top-5 events = 239% of OOS P&L → max WATCH; positive event mean but OOS portfolio Sharpe -0.53 → max WATCH; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### X3_capitulation_rebound
*Family:* interaction · **AQS 58.6** · **WATCH** · Classification: FORCED FLOW · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Crowding (funding + OI) combined with fuel (liquidations / extension) creates asymmetric reversal or continuation risk. |
| Counterparty | The crowded, levered side. |
| Why they trade | Late momentum / trapped positions. |
| Why the opportunity exists | Requires combining several data sources few monitor jointly. |
| Why it persists | Rare events; hard to size; painful to hold against crowded momentum. |
| What would kill it | Wider multi-factor monitoring; lower leverage. |
| Expected half-life | days |
| Capacity / liquidity | low-medium (rare events); median event ADV $156m / perp |
| Execution difficulty | medium-high |
| Venue / counterparty risk | exchange |
| Data quality | medium (inherits CoinGlass limitations) |
| Statistics | n 483; win_OOS 0.4878; mae_med_OOS -0.09273; mfe_med_OOS 0.06269; breakeven_rt_bp 105.9; median_rt_cost_bp 71.62 |
| In-sample | n 195; net_mean 0.004634; median -0.006133; t 0.3388; p_bh 0.8929 |
| Out-of-sample | n 287; net_mean 0.002415; median -0.001641; t 0.1915; ci [-0.0204, 0.0290]; p_bh 0.8681 |
| Cost sensitivity | break-even round trip 106 bp vs median modelled 72 bp (ratio 1.48) |
| Regime dependence | {'bear': (-0.0007, 307), 'bull': (0.0103, 176)} | years positive share 0.50 |
| Component scores (0-5) | mechanism 4, oos_significance 1, regime_robust 2, param_stability 2, cost_resilience 2, capacity 4, sample_size 5, decay 4, implementation 3, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.87 (>=0.10) → max PAPER TRADE; excess over price-only control +1.44% [-0.57%, +3.57%] not above 0 → max WATCH; top-5 events = 221% of OOS P&L → max WATCH; verdict: NO INCREMENTAL STRUCTURAL ALPHA → max WATCH |

**Research verdict: NO INCREMENTAL STRUCTURAL ALPHA**

### Q1_long_liq_flush_OI_reset_long
*Family:* liquidation · **AQS 58.2** · **WATCH** · Classification: FORCED FLOW · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Margin engines liquidate regardless of price: flushes overshoot when fuel is exhausted (OI reset) and cascade when leverage keeps rebuilding. |
| Counterparty | Liquidated leveraged traders and the insurance fund / ADL counterparties. |
| Why they trade | Forced: margin calls, not choice. |
| Why the opportunity exists | Liquidity providers widen during cascades; risk capital is scarce exactly when needed. |
| Why it persists | Catching liquidation flushes is psychologically and operationally hard (gap risk). |
| What would kill it | Lower leverage, better liquidation engines (partial liquidations), more market-making capital. |
| Expected half-life | hours to days |
| Capacity / liquidity | medium (flush depth is large for majors); median event ADV $164m / stressed at entry |
| Execution difficulty | high (enters during volatility) |
| Venue / counterparty risk | exchange outages during cascades |
| Data quality | medium: CoinGlass aggregated daily, venue coverage spliced; pre-2021 zeros excluded |
| Statistics | n 1475; win_OOS 0.4739; mae_med_OOS -0.08794; mfe_med_OOS 0.07526; breakeven_rt_bp 98.02; median_rt_cost_bp 69.93 |
| In-sample | n 629; net_mean -0.005853; median -0.01167; t -0.6188; p_bh 0.8289 |
| Out-of-sample | n 844; net_mean 0.009086; median -0.003577; t 0.821; ci [-0.0112, 0.0315]; p_bh 0.6408 |
| Cost sensitivity | break-even round trip 98 bp vs median modelled 70 bp (ratio 1.40) |
| Regime dependence | {'bear': (0.0016, 591), 'bull': (0.0035, 884)} | years positive share 0.50 |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 4, param_stability 2, cost_resilience 2, capacity 4, sample_size 5, decay 3, implementation 3, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.64 (>=0.10) → max PAPER TRADE; excess over price-only control +1.73% [-0.34%, +3.58%] not above 0 → max WATCH; top-5 events = 61% of OOS P&L → max WATCH; verdict: NO INCREMENTAL STRUCTURAL ALPHA → max WATCH |

**Research verdict: NO INCREMENTAL STRUCTURAL ALPHA**

### CB2_coinbase_premium_low_short_BTC
*Family:* cross_venue · **AQS 52.0** · **WATCH** · Classification: STRUCTURAL ALPHA · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | US USD spot demand (Coinbase) vs offshore USDT liquidity; premium indicates regional flow. |
| Counterparty | Offshore liquidity providers slow to reprice US flow. |
| Why they trade | Regional access constraints. |
| Why the opportunity exists | Capital controls / USD rails / USDT FX. |
| Why it persists | Transfer frictions. |
| What would kill it | Faster cross-venue arbitrage; ETF plumbing. |
| Expected half-life | hours to days |
| Capacity / liquidity | high (BTC); median event ADV $13,248m / deep |
| Execution difficulty | low |
| Venue / counterparty risk | low |
| Data quality | medium: daily closes cannot separate stale prints; USDT FX component |
| Statistics | n 50; win_OOS 0.6296; mae_med_OOS -0.02188; mfe_med_OOS 0.03936; breakeven_rt_bp 40.24; median_rt_cost_bp 20 |
| In-sample | n 23; net_mean -0.009655; median -0.01648; t -0.405; p_bh 0.8858 |
| Out-of-sample | n 27; net_mean 0.01197; median 0.005986; t 1.182; ci [-0.0076, 0.0301]; p_bh 0.4923 |
| Cost sensitivity | break-even round trip 40 bp vs median modelled 20 bp (ratio 2.01) |
| Regime dependence | {'bear': (0.0129, 19), 'bull': (-0.0046, 31)} | years positive share 0.57 |
| Component scores (0-5) | mechanism 2, oos_significance 0, regime_robust 2, param_stability 2, cost_resilience 4, capacity 5, sample_size 3, decay 3, implementation 5, counterparty_risk 4 |
| Evidence gates | OOS BH p=0.49 (>=0.10) → max PAPER TRADE; top-5 events = 137% of OOS P&L → max WATCH; verdict: INSUFFICIENT EVIDENCE → max WATCH |

**Research verdict: INSUFFICIENT EVIDENCE**

### CB1_coinbase_premium_high_long_BTC
*Family:* cross_venue · **AQS 51.6** · **WATCH** · Classification: STRUCTURAL ALPHA · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | US USD spot demand (Coinbase) vs offshore USDT liquidity; premium indicates regional flow. |
| Counterparty | Offshore liquidity providers slow to reprice US flow. |
| Why they trade | Regional access constraints. |
| Why the opportunity exists | Capital controls / USD rails / USDT FX. |
| Why it persists | Transfer frictions. |
| What would kill it | Faster cross-venue arbitrage; ETF plumbing. |
| Expected half-life | hours to days |
| Capacity / liquidity | high (BTC); median event ADV $13,520m / deep |
| Execution difficulty | low |
| Venue / counterparty risk | low |
| Data quality | medium: daily closes cannot separate stale prints; USDT FX component |
| Statistics | n 26; win_OOS 0.3333; mae_med_OOS -0.0266; mfe_med_OOS 0.02836; breakeven_rt_bp 151.9; median_rt_cost_bp 20 |
| In-sample | n 17; net_mean 0.02176; median -0.01063; t 0.994; p_bh 0.6013 |
| Out-of-sample | n 9; net_mean -0.002987; median -0.01273; t -0.2519; ci [-0.0238, 0.0174]; p_bh 0.8681 |
| Cost sensitivity | break-even round trip 152 bp vs median modelled 20 bp (ratio 7.60) |
| Regime dependence | {'bear': (-0.0059, 8), 'bull': (0.0217, 18)} | years positive share 0.43 |
| Component scores (0-5) | mechanism 2, oos_significance 0, regime_robust 4, param_stability 1, cost_resilience 5, capacity 5, sample_size 2, decay 1, implementation 5, counterparty_risk 4 |
| Evidence gates | OOS BH p=0.87 (>=0.10) → max PAPER TRADE; verdict: INSUFFICIENT EVIDENCE → max WATCH |

**Research verdict: INSUFFICIENT EVIDENCE**

### U1_control_price_up_1sd_long
*Family:* control · **AQS 51.6** · **REJECT** · Classification: CONTROL (price-only baseline, not a strategy) · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Price/OI quadrants separate new-leverage-driven moves from covering/capitulation. |
| Counterparty | Late levered momentum traders or capitulating longs. |
| Why they trade | Momentum chasing; margin pressure. |
| Why the opportunity exists | Crowding is observable but slow to resolve. |
| Why it persists | Signal is noisy and regime-dependent. |
| What would kill it | Wider use of OI dashboards. |
| Expected half-life | days |
| Capacity / liquidity | medium; median event ADV $137m / perp |
| Execution difficulty | medium |
| Venue / counterparty risk | exchange |
| Data quality | medium: aggregated OI across venues (spliced) |
| Statistics | n 6238; win_OOS 0.3955; mae_med_OOS -0.06876; mfe_med_OOS 0.06354; breakeven_rt_bp 99.69; median_rt_cost_bp 71.51 |
| In-sample | n 2424; net_mean 0.003429; median -0.01287; t 0.5283; p_bh nan |
| Out-of-sample | n 3790; net_mean 0.002272; median -0.01899; t 0.4177; ci [-0.0081, 0.0129]; p_bh nan |
| Cost sensitivity | break-even round trip 100 bp vs median modelled 72 bp (ratio 1.39) |
| Regime dependence | {'bear': (-0.0022, 2811), 'bull': (0.0068, 3427)} | years positive share 0.50 |
| Component scores (0-5) | mechanism 2, oos_significance 1, regime_robust 2, param_stability 2, cost_resilience 2, capacity 4, sample_size 5, decay 4, implementation 4, counterparty_risk 3 |
| Evidence gates | control signal (baseline only) → max REJECT; verdict: CONTROL → max REJECT |

**Research verdict: CONTROL**

### L1_new_perp_listing_short
*Family:* listings · **AQS 50.6** · **WATCH** · Classification: BEHAVIORAL · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | New listings draw attention-driven buying while early holders and market makers sell into new liquidity; shorting only becomes possible once the perp exists. |
| Counterparty | Attention-driven retail buyers and listing momentum chasers. |
| Why they trade | Salience of new listings; FOMO; exchange promotion. |
| Why the opportunity exists | Short supply is scarce at launch; unlock/airdrop holders need exit liquidity. |
| Why it persists | Shorting new listings is dangerous (thin books, squeezes, funding spikes). |
| What would kill it | Pre-listing price discovery elsewhere (DEX, other CEX perps), tighter listing standards. |
| Expected half-life | weeks |
| Capacity / liquidity | low-medium (thin early books) / thin in first days |
| Execution difficulty | high (day-1 volatility, funding spikes) |
| Venue / counterparty risk | exchange + delisting risk |
| Data quality | medium: listing dates from first kline; no announcement timestamps |
| Statistics | n 589; win_OOS 0.6643; mae_med_OOS -0.2179; mfe_med_OOS 0.2866; breakeven_rt_bp 191.8; median_rt_cost_bp 120 |
| In-sample | n 175; net_mean 0.01074; median 0.07982; t 0.4795; p_bh 0.8591 |
| Out-of-sample | n 414; net_mean 0.005676; median 0.1297; t 0.1675; ci [-0.0622, 0.0696]; p_bh 0.8681 |
| Cost sensitivity | break-even round trip 192 bp vs median modelled 120 bp (ratio 1.60) |
| Regime dependence | {'bear': (0.0255, 206), 'bull': (-0.0027, 383)} | years positive share 0.50 |
| Component scores (0-5) | mechanism 3, oos_significance 1, regime_robust 2, param_stability 2, cost_resilience 3, capacity 1, sample_size 5, decay 4, implementation 3, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.87 (>=0.10) → max PAPER TRADE; positive event mean but OOS portfolio Sharpe -0.02 → max WATCH |

**Research verdict: WATCH**

### F1_extreme_pos_funding_short
*Family:* funding · **AQS 49.2** · **REJECT** · Classification: PERSISTENT RISK PREMIUM · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Leverage demand exceeds arbitrage capital; funding prices the imbalance and crowded payers become marginal sellers (or buyers) when momentum stalls. |
| Counterparty | Levered directional traders (mostly retail / trend followers) paying funding to hold exposure. |
| Why they trade | Access to leverage and convexity matters more to them than carry cost. |
| Why the opportunity exists | Delta-neutral capital is balance-sheet, venue and collateral constrained; the short-perp leg can be squeezed before funding accrues. |
| Why it persists | Carry trades are unpleasant (squeeze risk, exchange risk, capital lock-up across venues). |
| What would kill it | Cheap institutional basis capital, lower leverage caps, funding caps, fee changes. |
| Expected half-life | days to weeks |
| Capacity / liquidity | high for majors; limited by alt-perp liquidity; median event ADV $19m / perp-dependent |
| Execution difficulty | medium (daily rebalance, perp margin management) |
| Venue / counterparty risk | single-exchange perp exposure (Binance); funding-cap rule changes |
| Data quality | good: exchange-native funding prints 2019-> (all perps incl. delisted) |
| Statistics | n 998; win_OOS 0.5787; mae_med_OOS -0.1309; mfe_med_OOS 0.1384; breakeven_rt_bp 105.9; median_rt_cost_bp 104.3 |
| In-sample | n 348; net_mean -0.01099; median 0.01102; t -1.125; p_bh 0.5458 |
| Out-of-sample | n 629; net_mean 0.006816; median 0.02904; t 0.4941; ci [-0.0253, 0.0290]; p_bh 0.7569 |
| Cost sensitivity | break-even round trip 106 bp vs median modelled 104 bp (ratio 1.02) |
| Regime dependence | {'bear': (-0.0332, 206), 'bull': (0.009, 792)} | years positive share 0.38 |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 2, param_stability 1, cost_resilience 2, capacity 2, sample_size 5, decay 3, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.76 (>=0.10) → max PAPER TRADE; top-5 events = 87% of OOS P&L → max WATCH; positive event mean but OOS portfolio Sharpe -0.36 → max WATCH; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### X4_funding_despite_reversal_OI
*Family:* interaction · **AQS 42.2** · **REJECT** · Classification: FORCED FLOW · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Crowding (funding + OI) combined with fuel (liquidations / extension) creates asymmetric reversal or continuation risk. |
| Counterparty | The crowded, levered side. |
| Why they trade | Late momentum / trapped positions. |
| Why the opportunity exists | Requires combining several data sources few monitor jointly. |
| Why it persists | Rare events; hard to size; painful to hold against crowded momentum. |
| What would kill it | Wider multi-factor monitoring; lower leverage. |
| Expected half-life | days |
| Capacity / liquidity | low-medium (rare events); median event ADV $79m / perp |
| Execution difficulty | medium-high |
| Venue / counterparty risk | exchange |
| Data quality | medium (inherits CoinGlass limitations) |
| Statistics | n 85; win_OOS 0.5116; mae_med_OOS -0.05406; mfe_med_OOS 0.13; breakeven_rt_bp 61.69; median_rt_cost_bp 79.45 |
| In-sample | n 41; net_mean 0.01399; median 0.0349; t 0.896; p_bh 0.6249 |
| Out-of-sample | n 43; net_mean -0.0171; median 0.002341; t -0.4488; ci [-0.1002, 0.0425]; p_bh 0.7569 |
| Cost sensitivity | break-even round trip 62 bp vs median modelled 79 bp (ratio 0.78) |
| Regime dependence | {'bear': (-0.0025, 52), 'bull': (-0.001, 33)} | years positive share 0.67 |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 0, param_stability 3, cost_resilience 1, capacity 3, sample_size 3, decay 1, implementation 3, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.76 (>=0.10) → max PAPER TRADE; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### O4_price_down_OI_down
*Family:* oi_price · **AQS 40.4** · **REJECT** · Classification: BEHAVIORAL · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Price/OI quadrants separate new-leverage-driven moves from covering/capitulation. |
| Counterparty | Late levered momentum traders or capitulating longs. |
| Why they trade | Momentum chasing; margin pressure. |
| Why the opportunity exists | Crowding is observable but slow to resolve. |
| Why it persists | Signal is noisy and regime-dependent. |
| What would kill it | Wider use of OI dashboards. |
| Expected half-life | days |
| Capacity / liquidity | medium; median event ADV $151m / perp |
| Execution difficulty | medium |
| Venue / counterparty risk | exchange |
| Data quality | medium: aggregated OI across venues (spliced) |
| Statistics | n 3367; win_OOS 0.3839; mae_med_OOS -0.08525; mfe_med_OOS 0.06069; breakeven_rt_bp 53.91; median_rt_cost_bp 71.72 |
| In-sample | n 1331; net_mean 0.003365; median -0.008893; t 0.5077; p_bh 0.8591 |
| Out-of-sample | n 2024; net_mean -0.005301; median -0.01835; t -0.7506; ci [-0.0173, 0.0098]; p_bh 0.6589 |
| Cost sensitivity | break-even round trip 54 bp vs median modelled 72 bp (ratio 0.75) |
| Regime dependence | {'bear': (0.0007, 1458), 'bull': (-0.0039, 1909)} | years positive share 0.17 |
| Component scores (0-5) | mechanism 2, oos_significance 0, regime_robust 2, param_stability 1, cost_resilience 1, capacity 4, sample_size 5, decay 1, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.66 (>=0.10) → max PAPER TRADE; excess over price-only control +0.23% [-0.25%, +0.69%] not above 0 → max WATCH; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### F4_high_funding_ann_100pct_short
*Family:* funding · **AQS 39.0** · **REJECT** · Classification: LIKELY NOISE · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Leverage demand exceeds arbitrage capital; funding prices the imbalance and crowded payers become marginal sellers (or buyers) when momentum stalls. |
| Counterparty | Levered directional traders (mostly retail / trend followers) paying funding to hold exposure. |
| Why they trade | Access to leverage and convexity matters more to them than carry cost. |
| Why the opportunity exists | Delta-neutral capital is balance-sheet, venue and collateral constrained; the short-perp leg can be squeezed before funding accrues. |
| Why it persists | Carry trades are unpleasant (squeeze risk, exchange risk, capital lock-up across venues). |
| What would kill it | Cheap institutional basis capital, lower leverage caps, funding caps, fee changes. |
| Expected half-life | days to weeks |
| Capacity / liquidity | high for majors; limited by alt-perp liquidity; median event ADV $41m / perp-dependent |
| Execution difficulty | medium (daily rebalance, perp margin management) |
| Venue / counterparty risk | single-exchange perp exposure (Binance); funding-cap rule changes |
| Data quality | good: exchange-native funding prints 2019-> (all perps incl. delisted) |
| Statistics | n 1058; win_OOS 0.585; mae_med_OOS -0.1455; mfe_med_OOS 0.1757; breakeven_rt_bp -320.4; median_rt_cost_bp 76.78 |
| In-sample | n 755; net_mean -0.03694; median 0.01248; t -4.075; p_bh 0.02566 |
| Out-of-sample | n 294; net_mean -0.04873; median 0.04121; t -1.292; ci [-0.1345, 0.0216]; p_bh 0.4355 |
| Cost sensitivity | break-even round trip -320 bp vs median modelled 77 bp (ratio -4.17) |
| Regime dependence | {'bear': (-0.0241, 112), 'bull': (-0.0421, 946)} | years positive share 0.17 |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 0, param_stability 1, cost_resilience 0, capacity 3, sample_size 5, decay 0, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.44 (>=0.10) → max PAPER TRADE; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### Q3_short_liq_squeeze_OI_reset_short
*Family:* liquidation · **AQS 38.4** · **REJECT** · Classification: LIKELY NOISE · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Margin engines liquidate regardless of price: flushes overshoot when fuel is exhausted (OI reset) and cascade when leverage keeps rebuilding. |
| Counterparty | Liquidated leveraged traders and the insurance fund / ADL counterparties. |
| Why they trade | Forced: margin calls, not choice. |
| Why the opportunity exists | Liquidity providers widen during cascades; risk capital is scarce exactly when needed. |
| Why it persists | Catching liquidation flushes is psychologically and operationally hard (gap risk). |
| What would kill it | Lower leverage, better liquidation engines (partial liquidations), more market-making capital. |
| Expected half-life | hours to days |
| Capacity / liquidity | medium (flush depth is large for majors); median event ADV $154m / stressed at entry |
| Execution difficulty | high (enters during volatility) |
| Venue / counterparty risk | exchange outages during cascades |
| Data quality | medium: CoinGlass aggregated daily, venue coverage spliced; pre-2021 zeros excluded |
| Statistics | n 248; win_OOS 0.3487; mae_med_OOS -0.1264; mfe_med_OOS 0.04917; breakeven_rt_bp -218.4; median_rt_cost_bp 71.78 |
| In-sample | n 96; net_mean -0.0151; median -0.002067; t -1.104; p_bh 0.5458 |
| Out-of-sample | n 152; net_mean -0.03779; median -0.02064; t -1.927; ci [-0.0660, -0.0013]; p_bh 0.2201 |
| Cost sensitivity | break-even round trip -218 bp vs median modelled 72 bp (ratio -3.04) |
| Regime dependence | {'bear': (-0.0338, 162), 'bull': (-0.0199, 86)} | years positive share 0.17 |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 0, param_stability 1, cost_resilience 0, capacity 4, sample_size 4, decay 0, implementation 3, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.22 (>=0.10) → max PAPER TRADE; excess over price-only control -3.96% [-7.79%, +1.04%] not above 0 → max WATCH; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### Q2_long_liq_OI_rebuild_short
*Family:* liquidation · **AQS 38.0** · **REJECT** · Classification: LIKELY NOISE · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Margin engines liquidate regardless of price: flushes overshoot when fuel is exhausted (OI reset) and cascade when leverage keeps rebuilding. |
| Counterparty | Liquidated leveraged traders and the insurance fund / ADL counterparties. |
| Why they trade | Forced: margin calls, not choice. |
| Why the opportunity exists | Liquidity providers widen during cascades; risk capital is scarce exactly when needed. |
| Why it persists | Catching liquidation flushes is psychologically and operationally hard (gap risk). |
| What would kill it | Lower leverage, better liquidation engines (partial liquidations), more market-making capital. |
| Expected half-life | hours to days |
| Capacity / liquidity | medium (flush depth is large for majors); median event ADV $96m / stressed at entry |
| Execution difficulty | high (enters during volatility) |
| Venue / counterparty risk | exchange outages during cascades |
| Data quality | medium: CoinGlass aggregated daily, venue coverage spliced; pre-2021 zeros excluded |
| Statistics | n 567; win_OOS 0.4676; mae_med_OOS -0.09774; mfe_med_OOS 0.08166; breakeven_rt_bp -308.3; median_rt_cost_bp 73.38 |
| In-sample | n 225; net_mean -0.02308; median 0.002589; t -2.072; p_bh 0.3519 |
| Out-of-sample | n 340; net_mean -0.04834; median -0.009124; t -2.575; ci [-0.0886, -0.0119]; p_bh 0.1268 |
| Cost sensitivity | break-even round trip -308 bp vs median modelled 73 bp (ratio -4.20) |
| Regime dependence | {'bear': (-0.0288, 221), 'bull': (-0.0443, 346)} | years positive share 0.17 |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 0, param_stability 1, cost_resilience 0, capacity 3, sample_size 5, decay 0, implementation 3, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.13 (>=0.10) → max PAPER TRADE; excess over price-only control -3.11% [-8.33%, +1.72%] not above 0 → max WATCH; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### F3_funding_despite_reversal_short
*Family:* funding · **AQS 37.0** · **REJECT** · Classification: LIKELY NOISE · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Leverage demand exceeds arbitrage capital; funding prices the imbalance and crowded payers become marginal sellers (or buyers) when momentum stalls. |
| Counterparty | Levered directional traders (mostly retail / trend followers) paying funding to hold exposure. |
| Why they trade | Access to leverage and convexity matters more to them than carry cost. |
| Why the opportunity exists | Delta-neutral capital is balance-sheet, venue and collateral constrained; the short-perp leg can be squeezed before funding accrues. |
| Why it persists | Carry trades are unpleasant (squeeze risk, exchange risk, capital lock-up across venues). |
| What would kill it | Cheap institutional basis capital, lower leverage caps, funding caps, fee changes. |
| Expected half-life | days to weeks |
| Capacity / liquidity | high for majors; limited by alt-perp liquidity; median event ADV $20m / perp-dependent |
| Execution difficulty | medium (daily rebalance, perp margin management) |
| Venue / counterparty risk | single-exchange perp exposure (Binance); funding-cap rule changes |
| Data quality | good: exchange-native funding prints 2019-> (all perps incl. delisted) |
| Statistics | n 3021; win_OOS 0.5185; mae_med_OOS -0.09349; mfe_med_OOS 0.115; breakeven_rt_bp -11.36; median_rt_cost_bp 103.2 |
| In-sample | n 1273; net_mean -0.01077; median -0.00317; t -1.26; p_bh 0.522 |
| Out-of-sample | n 1734; net_mean -0.01174; median 0.005045; t -1.362; ci [-0.0288, 0.0068]; p_bh 0.4204 |
| Cost sensitivity | break-even round trip -11 bp vs median modelled 103 bp (ratio -0.11) |
| Regime dependence | {'bear': (-0.0193, 1385), 'bull': (-0.0046, 1636)} | years positive share 0.12 |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 0, param_stability 1, cost_resilience 0, capacity 2, sample_size 5, decay 0, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.42 (>=0.10) → max PAPER TRADE; excess over price-only control -0.33% [-2.79%, +1.81%] not above 0 → max WATCH; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### F1c_delta_neutral_carry
*Family:* funding · **AQS 37.0** · **REJECT** · Classification: LIKELY NOISE · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Delta-neutral: short perp / long spot to collect extreme funding minus premium change and four legs of costs. |
| Counterparty | Levered directional traders (mostly retail / trend followers) paying funding to hold exposure. |
| Why they trade | Access to leverage and convexity matters more to them than carry cost. |
| Why the opportunity exists | Delta-neutral capital is balance-sheet, venue and collateral constrained; the short-perp leg can be squeezed before funding accrues. |
| Why it persists | Carry trades are unpleasant (squeeze risk, exchange risk, capital lock-up across venues). |
| What would kill it | Cheap institutional basis capital, lower leverage caps, funding caps, fee changes. |
| Expected half-life | days to weeks |
| Capacity / liquidity | high for majors; limited by alt-perp liquidity / perp-dependent |
| Execution difficulty | high (two legs, spot inventory, perp margin, squeeze risk) |
| Venue / counterparty risk | single-exchange perp exposure (Binance); funding-cap rule changes |
| Data quality | good: exchange-native funding prints 2019-> (all perps incl. delisted) |
| Statistics | n 977; win_OOS 0.1479; mae_med_OOS nan; mfe_med_OOS nan; breakeven_rt_bp nan; median_rt_cost_bp 20 |
| In-sample | n 348; net_mean -0.003521; median nan; t -1.236; p_bh 0.522 |
| Out-of-sample | n 629; net_mean -0.007697; median nan; t -7.62; ci [-0.0096, -0.0058]; p_bh 3.894e-07 |
| Cost sensitivity | n/a |
| Regime dependence | nan | years positive share n/a |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 2, param_stability 1, cost_resilience 0, capacity 1, sample_size 5, decay 0, implementation 2, counterparty_risk 3 |
| Evidence gates | verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### U2_control_price_down_1sd_long
*Family:* control · **AQS 36.4** · **REJECT** · Classification: CONTROL (price-only baseline, not a strategy) · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Price/OI quadrants separate new-leverage-driven moves from covering/capitulation. |
| Counterparty | Late levered momentum traders or capitulating longs. |
| Why they trade | Momentum chasing; margin pressure. |
| Why the opportunity exists | Crowding is observable but slow to resolve. |
| Why it persists | Signal is noisy and regime-dependent. |
| What would kill it | Wider use of OI dashboards. |
| Expected half-life | days |
| Capacity / liquidity | medium; median event ADV $136m / perp |
| Execution difficulty | medium |
| Venue / counterparty risk | exchange |
| Data quality | medium: aggregated OI across venues (spliced) |
| Statistics | n 6247; win_OOS 0.3765; mae_med_OOS -0.07304; mfe_med_OOS 0.05753; breakeven_rt_bp 42.58; median_rt_cost_bp 71.98 |
| In-sample | n 2425; net_mean 0.004123; median -0.007112; t 0.7708; p_bh nan |
| Out-of-sample | n 3803; net_mean -0.00758; median -0.01832; t -1.597; ci [-0.0155, 0.0017]; p_bh nan |
| Cost sensitivity | break-even round trip 43 bp vs median modelled 72 bp (ratio 0.59) |
| Regime dependence | {'bear': (-0.0001, 2982), 'bull': (-0.0058, 3265)} | years positive share 0.33 |
| Component scores (0-5) | mechanism 2, oos_significance 0, regime_robust 0, param_stability 1, cost_resilience 1, capacity 4, sample_size 5, decay 1, implementation 4, counterparty_risk 3 |
| Evidence gates | control signal (baseline only) → max REJECT; verdict: CONTROL → max REJECT |

**Research verdict: CONTROL**

### O2_price_up_OI_down
*Family:* oi_price · **AQS 35.2** · **REJECT** · Classification: BEHAVIORAL · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Price/OI quadrants separate new-leverage-driven moves from covering/capitulation. |
| Counterparty | Late levered momentum traders or capitulating longs. |
| Why they trade | Momentum chasing; margin pressure. |
| Why the opportunity exists | Crowding is observable but slow to resolve. |
| Why it persists | Signal is noisy and regime-dependent. |
| What would kill it | Wider use of OI dashboards. |
| Expected half-life | days |
| Capacity / liquidity | medium; median event ADV $64m / perp |
| Execution difficulty | medium |
| Venue / counterparty risk | exchange |
| Data quality | medium: aggregated OI across venues (spliced) |
| Statistics | n 86; win_OOS 0.3462; mae_med_OOS -0.04868; mfe_med_OOS 0.044; breakeven_rt_bp 58.42; median_rt_cost_bp 74.06 |
| In-sample | n 34; net_mean 0.003621; median -0.02054; t 0.1449; p_bh 0.9271 |
| Out-of-sample | n 52; net_mean -0.005608; median -0.01951; t -0.5323; ci [-0.0251, 0.0137]; p_bh 0.7569 |
| Cost sensitivity | break-even round trip 58 bp vs median modelled 74 bp (ratio 0.79) |
| Regime dependence | {'bear': (-0.0113, 38), 'bull': (0.0055, 48)} | years positive share 0.33 |
| Component scores (0-5) | mechanism 2, oos_significance 0, regime_robust 2, param_stability 1, cost_resilience 1, capacity 3, sample_size 3, decay 1, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.76 (>=0.10) → max PAPER TRADE; excess over price-only control -0.71% [-2.35%, +0.94%] not above 0 → max WATCH; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### X1_crowded_long_reversal
*Family:* interaction · **AQS 35.2** · **REJECT** · Classification: LIKELY NOISE · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Crowding (funding + OI) combined with fuel (liquidations / extension) creates asymmetric reversal or continuation risk. |
| Counterparty | The crowded, levered side. |
| Why they trade | Late momentum / trapped positions. |
| Why the opportunity exists | Requires combining several data sources few monitor jointly. |
| Why it persists | Rare events; hard to size; painful to hold against crowded momentum. |
| What would kill it | Wider multi-factor monitoring; lower leverage. |
| Expected half-life | days |
| Capacity / liquidity | low-medium (rare events); median event ADV $115m / perp |
| Execution difficulty | medium-high |
| Venue / counterparty risk | exchange |
| Data quality | medium (inherits CoinGlass limitations) |
| Statistics | n 46; win_OOS 0.2857; mae_med_OOS -0.216; mfe_med_OOS 0.1124; breakeven_rt_bp -1676; median_rt_cost_bp 72.28 |
| In-sample | n 25; net_mean -0.07518; median -0.02517; t -1.429; p_bh 0.5079 |
| Out-of-sample | n 21; net_mean -0.2941; median -0.06757; t -2.014; ci [-0.6492, -0.0633]; p_bh 0.2201 |
| Cost sensitivity | break-even round trip -1676 bp vs median modelled 72 bp (ratio -23.19) |
| Regime dependence | {'bear': (-0.359, 11), 'bull': (-0.1174, 35)} | years positive share 0.17 |
| Component scores (0-5) | mechanism 4, oos_significance 0, regime_robust 0, param_stability 1, cost_resilience 0, capacity 4, sample_size 2, decay 0, implementation 3, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.22 (>=0.10) → max PAPER TRADE; too few events for a price-only control test → max WATCH; verdict: INSUFFICIENT EVIDENCE → max WATCH |

**Research verdict: INSUFFICIENT EVIDENCE**

### P1_premium_z_high_short
*Family:* basis · **AQS 33.0** · **REJECT** · Classification: LIKELY NOISE · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Perp premium vs index is a real-time imbalance gauge; extremes mean-revert as arbitrage closes it. |
| Counterparty | Aggressive perp takers paying up (or dumping) relative to spot index. |
| Why they trade | Speed and leverage preference; perp is the default retail instrument. |
| Why the opportunity exists | Spot-perp arbitrage needs spot inventory/borrow and two-venue capital. |
| Why it persists | Capital friction and short-horizon squeeze risk. |
| What would kill it | More arbitrage capital, index methodology changes. |
| Expected half-life | hours to days |
| Capacity / liquidity | medium; median event ADV $26m / perp + spot needed for pure basis |
| Execution difficulty | medium |
| Venue / counterparty risk | exchange + index methodology |
| Data quality | good: Binance premiumIndexKlines 2020-> |
| Statistics | n 6767; win_OOS 0.5243; mae_med_OOS -0.1014; mfe_med_OOS 0.0968; breakeven_rt_bp -72.67; median_rt_cost_bp 90.33 |
| In-sample | n 3320; net_mean -0.02261; median 9.491e-05; t -4.031; p_bh 0.004761 |
| Out-of-sample | n 3416; net_mean -0.01122; median 0.007117; t -1.368; ci [-0.0230, 0.0052]; p_bh 0.4204 |
| Cost sensitivity | break-even round trip -73 bp vs median modelled 90 bp (ratio -0.80) |
| Regime dependence | {'bear': (-0.0196, 1267), 'bull': (-0.0162, 5500)} | years positive share 0.14 |
| Component scores (0-5) | mechanism 3, oos_significance 0, regime_robust 0, param_stability 1, cost_resilience 0, capacity 2, sample_size 5, decay 0, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.42 (>=0.10) → max PAPER TRADE; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### P2_premium_z_low_long
*Family:* basis · **AQS 33.0** · **REJECT** · Classification: LIKELY NOISE · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Perp premium vs index is a real-time imbalance gauge; extremes mean-revert as arbitrage closes it. |
| Counterparty | Aggressive perp takers paying up (or dumping) relative to spot index. |
| Why they trade | Speed and leverage preference; perp is the default retail instrument. |
| Why the opportunity exists | Spot-perp arbitrage needs spot inventory/borrow and two-venue capital. |
| Why it persists | Capital friction and short-horizon squeeze risk. |
| What would kill it | More arbitrage capital, index methodology changes. |
| Expected half-life | hours to days |
| Capacity / liquidity | medium; median event ADV $20m / perp + spot needed for pure basis |
| Execution difficulty | medium |
| Venue / counterparty risk | exchange + index methodology |
| Data quality | good: Binance premiumIndexKlines 2020-> |
| Statistics | n 10643; win_OOS 0.4041; mae_med_OOS -0.09012; mfe_med_OOS 0.07652; breakeven_rt_bp 31.79; median_rt_cost_bp 108.2 |
| In-sample | n 3975; net_mean -0.01118; median -0.01913; t -1.918; p_bh 0.3532 |
| Out-of-sample | n 6637; net_mean -0.004856; median -0.01894; t -0.9036; ci [-0.0154, 0.0054]; p_bh 0.6408 |
| Cost sensitivity | break-even round trip 32 bp vs median modelled 108 bp (ratio 0.29) |
| Regime dependence | {'bear': (-0.0019, 5842), 'bull': (-0.0137, 4801)} | years positive share 0.29 |
| Component scores (0-5) | mechanism 3, oos_significance 0, regime_robust 0, param_stability 1, cost_resilience 0, capacity 2, sample_size 5, decay 0, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.64 (>=0.10) → max PAPER TRADE; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**

### O3_price_down_OI_up
*Family:* oi_price · **AQS 29.4** · **REJECT** · Classification: LIKELY NOISE · Confidence: LOW

| Field | Assessment |
|---|---|
| Mechanism | Price/OI quadrants separate new-leverage-driven moves from covering/capitulation. |
| Counterparty | Late levered momentum traders or capitulating longs. |
| Why they trade | Momentum chasing; margin pressure. |
| Why the opportunity exists | Crowding is observable but slow to resolve. |
| Why it persists | Signal is noisy and regime-dependent. |
| What would kill it | Wider use of OI dashboards. |
| Expected half-life | days |
| Capacity / liquidity | medium; median event ADV $80m / perp |
| Execution difficulty | medium |
| Venue / counterparty risk | exchange |
| Data quality | medium: aggregated OI across venues (spliced) |
| Statistics | n 177; win_OOS 0.3269; mae_med_OOS -0.05585; mfe_med_OOS 0.04677; breakeven_rt_bp -44.98; median_rt_cost_bp 74.05 |
| In-sample | n 73; net_mean -0.001754; median -0.00752; t -0.149; p_bh 0.9271 |
| Out-of-sample | n 104; net_mean -0.01923; median -0.0217; t -2.544; ci [-0.0338, -0.0037]; p_bh 0.1268 |
| Cost sensitivity | break-even round trip -45 bp vs median modelled 74 bp (ratio -0.61) |
| Regime dependence | {'bear': (-0.0059, 105), 'bull': (-0.021, 72)} | years positive share 0.33 |
| Component scores (0-5) | mechanism 2, oos_significance 0, regime_robust 0, param_stability 1, cost_resilience 0, capacity 3, sample_size 4, decay 0, implementation 4, counterparty_risk 3 |
| Evidence gates | OOS BH p=0.13 (>=0.10) → max PAPER TRADE; excess over price-only control -1.12% [-3.03%, +0.65%] not above 0 → max WATCH; verdict: REJECT AS TRADE → max REJECT |

**Research verdict: REJECT AS TRADE**
