# Additional Data Recommendations for Market State Monitor (MSM v0)

Based on your MSM v0 strategy requirements, here are additional data points that would enhance regime detection:

## Current MSM v0 Features (from STRATEGY_EXPLANATION.md)

1. **ALT Breadth** - How many alts are moving up vs down
2. **BTC Dominance** - BTC market cap relative to total crypto market
3. **Funding Skew** - Difference between ALT funding rates and major funding rates
4. **Funding Heating** - Short-term vs long-term funding spread acceleration
5. **OI Risk** - Open interest changes (using real OI data from CoinGlass)
6. **Liquidity** - Trading volume and flow proxies
7. **Volatility Spread** - ALT volatility vs BTC volatility
8. **Momentum** - Cross-sectional momentum (ALT vs major performance)

---

## 🎯 High-Value Additional Data Points

### 1. **Global Market Data (BTC Dominance)**
**Why:** Directly feeds BTC Dominance feature
- **Endpoint:** `/global` and `/global/market_cap_chart`
- **Data:** 
  - Total market cap (BTC, ETH, USD)
  - BTC dominance percentage
  - Total volume
  - Active cryptocurrencies count
- **Output:** `fact_global_market.parquet`
- **API Calls:** 1 call (daily) + historical chart data
- **Value:** 
  - Direct BTC dominance metric (no calculation needed)
  - Market-wide context for regime detection
  - Historical dominance trends

**Implementation Priority:** ⭐⭐⭐⭐⭐ (Critical for BTC Dominance feature)

---

### 2. **Coin Categories & Sectors**
**Why:** Better ALT basket construction and sector rotation analysis
- **Endpoint:** `/coins/categories` and `/coins/categories/list`
- **Data:**
  - Asset categories (DeFi, NFT, Layer 1, Layer 2, etc.)
  - Sector market caps
  - Sector performance
- **Output:** `dim_asset_categories.parquet`, `fact_sector_performance.parquet`
- **API Calls:** ~2-5 calls (one-time + periodic updates)
- **Value:**
  - Better ALT basket selection (avoid correlated sectors)
  - Sector rotation signals
  - Diversification metrics

**Implementation Priority:** ⭐⭐⭐⭐ (High - improves basket construction)

---

### 3. **Historical Global Market Cap Chart**
**Why:** Long-term market context and dominance trends
- **Endpoint:** `/global/market_cap_chart` (Analyst tier exclusive)
- **Data:**
  - Historical total market cap
  - Historical BTC dominance
  - Historical total volume
- **Output:** `fact_global_market_history.parquet`
- **API Calls:** 1 call (10 years of data in one call)
- **Value:**
  - Extended BTC dominance history
  - Market cycle identification
  - Long-term trend analysis

**Implementation Priority:** ⭐⭐⭐⭐⭐ (Critical - extends dominance data to 10 years)

---

### 4. **Exchange-Specific Data**
**Why:** Better liquidity analysis and exchange flow tracking
- **Endpoints:** 
  - `/exchanges/{id}` - Exchange details
  - `/exchanges/{id}/tickers` - Exchange tickers
  - `/exchanges/{id}/volume_chart` - Volume charts (already implemented)
- **Data:**
  - Exchange trust scores
  - Exchange trading pairs
  - Exchange-specific volume breakdown
- **Output:** `dim_exchanges.parquet`, `fact_exchange_tickers.parquet`
- **API Calls:** ~10-20 calls (one-time + periodic)
- **Value:**
  - Exchange quality metrics
  - Liquidity concentration analysis
  - Exchange flow tracking

**Implementation Priority:** ⭐⭐⭐ (Medium - enhances liquidity feature)

---

### 5. **Trending Coins**
**Why:** Momentum and sentiment indicators
- **Endpoint:** `/search/trending`
- **Data:**
  - Currently trending coins
  - Search volume trends
  - Social media mentions
- **Output:** `fact_trending_coins.parquet`
- **API Calls:** 1 call (daily)
- **Value:**
  - Early momentum signals
  - Sentiment indicators
  - FOMO detection

**Implementation Priority:** ⭐⭐⭐ (Medium - complements momentum feature)

---

### 6. **Derivative Data (Futures/Perpetuals)**
**Why:** Additional funding/OI data source (complement CoinGlass)
- **Endpoints:**
  - `/derivatives` - List all derivatives
  - `/derivatives/exchanges` - Exchange derivatives
  - `/derivatives/exchanges/{id}` - Exchange-specific derivatives
- **Data:**
  - Derivative volumes
  - Open interest (alternative to CoinGlass)
  - Funding rates (alternative to CoinGlass)
- **Output:** `fact_derivative_volume.parquet`, `fact_derivative_oi.parquet`
- **API Calls:** ~5-10 calls (daily)
- **Value:**
  - Cross-validate CoinGlass data
  - Broader exchange coverage
  - Redundancy for critical features

**Implementation Priority:** ⭐⭐⭐⭐ (High - backup for funding/OI features)

---

### 7. **Coin Historical Data (Extended)**
**Why:** Better volatility and momentum calculations
- **Endpoint:** `/coins/{id}/history` (with date parameter)
- **Data:**
  - Historical market data snapshots
  - Price, market cap, volume at specific dates
- **Output:** Already covered by existing price/mcap/volume tables
- **API Calls:** Covered by existing backfill
- **Value:**
  - More accurate historical calculations
  - Point-in-time snapshots

**Implementation Priority:** ⭐⭐ (Low - already have this via market_chart/range)

---

### 8. **NFT Market Data** (Optional)
**Why:** Broader market sentiment (NFT market cycles correlate with crypto)
- **Endpoints:** `/nft/list`, `/nft/{id}`
- **Data:**
  - NFT market cap
  - NFT trading volume
  - NFT floor prices
- **Output:** `fact_nft_market.parquet`
- **API Calls:** ~5-10 calls (daily)
- **Value:**
  - Risk-on/risk-off indicator
  - Market sentiment proxy

**Implementation Priority:** ⭐⭐ (Low - nice to have, not critical)

---

## 🎯 Recommended Implementation Order

### Phase 1: Critical for MSM v0 (Do First)
1. ✅ **Global Market Data** - BTC Dominance (direct feature input)
2. ✅ **Historical Global Market Cap Chart** - Extended dominance history
3. ✅ **Derivative Data** - Backup for funding/OI features

### Phase 2: High Value Enhancements
4. ✅ **Coin Categories** - Better ALT basket construction
5. ✅ **Exchange-Specific Data** - Enhanced liquidity analysis

### Phase 3: Nice to Have
6. ✅ **Trending Coins** - Momentum signals
7. ✅ **NFT Market Data** - Sentiment indicators

---

## 📊 Estimated API Usage

- **Global Market Data:** 1 call/day = 30/month
- **Global Market Cap Chart:** 1 call (one-time, 10 years) = 1 total
- **Derivative Data:** 10 calls/day = 300/month
- **Coin Categories:** 5 calls (one-time) = 5 total
- **Exchange Data:** 20 calls (one-time) = 20 total
- **Trending Coins:** 1 call/day = 30/month
- **NFT Data:** 10 calls/day = 300/month (optional)

**Total Monthly:** ~660 calls/month (well within 500k limit)

---

## 🔧 Implementation Scripts Needed

1. `fetch_global_market_data.py` - Global market + BTC dominance
2. `fetch_derivative_data.py` - Derivative volumes/OI
3. `fetch_coin_categories.py` - Asset categories
4. `fetch_trending_coins.py` - Trending coins
5. `fetch_nft_data.py` - NFT market (optional)

---

## 💡 Key Insights for MSM v0

### Most Critical Additions:
1. **Global Market Data** - Directly provides BTC dominance (currently you calculate it)
2. **Historical Global Chart** - 10 years of dominance history for better backtesting
3. **Derivative Data** - Backup source for funding/OI (critical features)

### Enhancement Opportunities:
- **Categories** - Better ALT basket diversification
- **Exchange Data** - Better liquidity metrics
- **Trending** - Early momentum signals

All of these integrate with your existing data lake structure and enhance your 8 MSM v0 features!
