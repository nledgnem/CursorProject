# CoinGecko API Endpoints Inventory

## ✅ Currently Implemented (Already in Use)

### Core Data (Basic Tier)
1. **`/coins/{id}/market_chart/range`** - Historical price, market cap, volume (10 years with Analyst tier)
   - Used in: `src/providers/coingecko.py`
   - Data: `fact_price.parquet`, `fact_marketcap.parquet`, `fact_volume.parquet`

### Analyst Tier - Already Implemented
2. **`/coins/{id}/ohlc/range`** - OHLC data with date range
   - Used in: `src/providers/coingecko_analyst.py`
   - Data: `fact_ohlc.parquet`
   - Status: ✅ Implemented, backfilling in progress

3. **`/coins/top_gainers_losers`** - Top 30 gainers/losers by duration
   - Used in: `src/providers/coingecko_analyst.py`
   - Data: `fact_market_breadth.parquet`
   - Status: ✅ Implemented

4. **`/coins/list/new`** - Latest 200 newly listed coins
   - Used in: `src/providers/coingecko_analyst.py`
   - Data: `dim_new_listings.parquet`
   - Status: ✅ Implemented

5. **`/exchanges/{id}/volume_chart`** - Exchange volume chart
   - Used in: `src/providers/coingecko_analyst.py`
   - Data: `fact_exchange_volume.parquet`
   - Status: ✅ Implemented

6. **`/global`** - Global market data (BTC dominance)
   - Used in: `scripts/fetch_global_market_data.py`
   - Data: `fact_global_market.parquet`
   - Status: ✅ Implemented

7. **`/global/market_cap_chart`** - Historical global market cap
   - Used in: `scripts/fetch_global_market_data.py`
   - Data: `fact_global_market_history.parquet`
   - Status: ✅ Implemented (but timed out - needs retry)

8. **`/derivatives`** - All derivative tickers
   - Used in: `scripts/fetch_derivative_data.py`
   - Data: `fact_derivative_volume.parquet`
   - Status: ✅ Implemented

9. **`/derivatives/exchanges`** - Derivative exchanges list
   - Used in: `scripts/fetch_derivative_data.py`
   - Data: `dim_derivative_exchanges.parquet`
   - Status: ✅ Implemented

10. **`/key`** - API usage check
    - Used in: `src/providers/coingecko_analyst.py`
    - Status: ✅ Implemented

---

## 🔍 Available but NOT Yet Implemented

### Analyst Tier Endpoints (💼) - High Priority for MSM

#### Market Data & Trends
11. **`/coins/markets`** - All coins with price, market cap, volume
    - **Use Case**: Real-time snapshot of all markets
    - **MSM Value**: Could enhance ALT Breadth calculations
    - **Complexity**: Low - single endpoint call

12. **`/coins/{id}/tickers`** - Coin tickers on CEX/DEX
    - **Use Case**: Exchange-specific price data, arbitrage opportunities
    - **MSM Value**: Could enhance Liquidity feature
    - **Complexity**: Medium - requires per-coin calls

13. **`/coins/{id}/history`** - Historical data at specific date
    - **Use Case**: Point-in-time snapshots
    - **MSM Value**: Historical regime analysis
    - **Complexity**: Low - similar to existing endpoints

14. **`/search/trending`** - Trending search coins, NFTs, categories (last 24h)
    - **Use Case**: Sentiment indicator, early trend detection
    - **MSM Value**: Could enhance Momentum feature
    - **Complexity**: Low - single endpoint call

15. **`/exchanges/{id}/volume_chart/range`** - Exchange volume by date range (💼)
    - **Use Case**: Historical exchange volume analysis
    - **MSM Value**: Enhanced Liquidity feature with historical context
    - **Complexity**: Medium - per-exchange calls

#### Categories & Classification
16. **`/coins/categories/list`** - All coin categories
    - **Use Case**: Asset classification
    - **MSM Value**: Category-based analysis (DeFi, Layer 1, etc.)
    - **Complexity**: Low - single endpoint call

17. **`/coins/categories`** - Categories with market data
    - **Use Case**: Category-level market cap, volume
    - **MSM Value**: Sector rotation analysis
    - **Complexity**: Low - single endpoint call

#### Exchange Data
18. **`/exchanges`** - All exchanges with trading volumes
    - **Use Case**: Exchange rankings, volume distribution
    - **MSM Value**: Market structure analysis
    - **Complexity**: Low - single endpoint call

19. **`/exchanges/{id}`** - Exchange details and tickers
    - **Use Case**: Exchange-specific analysis
    - **MSM Value**: Exchange-level liquidity metrics
    - **Complexity**: Medium - per-exchange calls

20. **`/exchanges/{id}/tickers`** - Exchange tickers (paginated)
    - **Use Case**: All trading pairs on an exchange
    - **MSM Value**: Exchange-level market breadth
    - **Complexity**: Medium - pagination required

#### Derivative Data
21. **`/derivatives/exchanges/{id}`** - Specific derivative exchange data
    - **Use Case**: Exchange-specific derivative metrics
    - **MSM Value**: Exchange-level OI/funding analysis
    - **Complexity**: Low - per-exchange calls

22. **`/derivatives/exchanges/list`** - Derivative exchanges list
    - **Use Case**: Complete list of derivative exchanges
    - **MSM Value**: Exchange universe expansion
    - **Complexity**: Low - single endpoint call

#### NFT Data (💼) - Lower Priority for MSM
23. **`/nfts/markets`** - NFT collections with market data
24. **`/nfts/{id}/market_chart`** - NFT historical data
25. **`/nfts/{id}/tickers`** - NFT marketplace floor prices

#### Public Treasury Data
26. **`/entities/list`** - Public companies/governments
27. **`/{entity}/public_treasury/{coin_id}`** - Entity crypto holdings
28. **`/public_treasury/{entity_id}/holding_chart`** - Historical holdings

#### Onchain DEX Data (GeckoTerminal) - 💼
29. **`/onchain/networks`** - Supported blockchain networks
30. **`/onchain/networks/{network}/dexes`** - DEXes on a network
31. **`/onchain/networks/{network}/pools`** - Top pools on a network
32. **`/onchain/networks/{network}/tokens/{address}`** - Token data by contract
33. **`/onchain/networks/{network}/pools/{address}/ohlcv`** - Pool OHLCV
34. **`/onchain/pools/megafilter`** - Advanced pool filtering (💼)
35. **`/onchain/networks/{network}/tokens/{address}/top_traders`** - Top traders (💼)
36. **`/onchain/networks/{network}/tokens/{address}/top_holders`** - Top holders (💼)

#### Enterprise Tier Only (👑) - Not Available
- `/coins/{id}/circulating_supply_chart` - Historical circulating supply
- `/coins/{id}/total_supply_chart` - Historical total supply

---

## 🎯 Recommended for Market State Monitor (MSM v0)

### High Priority (Direct MSM Feature Enhancement)

1. **`/search/trending`** ⭐⭐⭐⭐⭐
   - **Why**: Sentiment/trend indicator
   - **MSM Feature**: Momentum
   - **API Calls**: 1/day
   - **Complexity**: Low

2. **`/coins/categories`** ⭐⭐⭐⭐
   - **Why**: Sector rotation, category-level analysis
   - **MSM Feature**: ALT Breadth (category breakdown)
   - **API Calls**: 1/day
   - **Complexity**: Low

3. **`/exchanges/{id}/volume_chart/range`** ⭐⭐⭐⭐
   - **Why**: Historical exchange volume for major exchanges
   - **MSM Feature**: Liquidity
   - **API Calls**: ~10-20 (for major exchanges)
   - **Complexity**: Medium

4. **`/coins/markets`** ⭐⭐⭐
   - **Why**: Real-time snapshot of all markets
   - **MSM Feature**: ALT Breadth (broader coverage)
   - **API Calls**: 1/day
   - **Complexity**: Low

### Medium Priority (Nice to Have)

5. **`/exchanges`** ⭐⭐⭐
   - **Why**: Exchange rankings, market structure
   - **MSM Feature**: Liquidity (exchange distribution)
   - **API Calls**: 1/day
   - **Complexity**: Low

6. **`/derivatives/exchanges/{id}`** ⭐⭐
   - **Why**: Exchange-specific derivative metrics
   - **MSM Feature**: OI Risk (exchange-level)
   - **API Calls**: ~20 (for major derivative exchanges)
   - **Complexity**: Low

### Lower Priority (Future Enhancement)

7. **Onchain DEX Data** ⭐
   - **Why**: DEX liquidity, token-level data
   - **MSM Feature**: Liquidity (DEX component)
   - **API Calls**: Many (network-specific)
   - **Complexity**: High

8. **NFT Data** ⭐
   - **Why**: NFT market sentiment (correlation with crypto)
   - **MSM Feature**: Sentiment indicator
   - **API Calls**: Variable
   - **Complexity**: Medium

---

## 📊 Summary

### Already Using: **10 endpoints**
- Core: 1 (market_chart/range)
- Analyst: 9 (OHLC, gainers/losers, new listings, exchanges, global, derivatives)

### Recommended to Add: **4-6 endpoints**
1. `/search/trending` - Trending searches (sentiment)
2. `/coins/categories` - Category market data
3. `/exchanges/{id}/volume_chart/range` - Historical exchange volumes
4. `/coins/markets` - All markets snapshot
5. `/exchanges` - Exchange rankings
6. `/derivatives/exchanges/{id}` - Exchange-specific derivatives

### Total Available: **~50+ endpoints**
- Analyst tier: ~15-20 exclusive endpoints
- Basic tier: ~30 endpoints
- Enterprise tier: ~5 endpoints (not available)

---

## 💡 Next Steps

1. **Implement trending searches** (`/search/trending`) - Quick win, 1 API call
2. **Add category data** (`/coins/categories`) - Sector analysis, 1 API call
3. **Historical exchange volumes** (`/exchanges/{id}/volume_chart/range`) - Enhanced liquidity
4. **All markets snapshot** (`/coins/markets`) - Broader ALT Breadth coverage

These would add valuable data points for MSM v0 with minimal API usage!
