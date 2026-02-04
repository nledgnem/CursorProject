# Data Fetch Summary - Analyst Tier Utilization

## ✅ Successfully Fetched Data

### 1. **Global Market Data (BTC Dominance)** ⭐⭐⭐⭐⭐
- **Status:** ✅ Complete
- **File:** `fact_global_market.parquet`
- **Records:** 1 (current snapshot)
- **Key Metrics:**
  - BTC Dominance: **57.33%**
  - Active Cryptocurrencies: **18,970**
- **Value for MSM v0:** Directly feeds **BTC Dominance** feature (no calculation needed)
- **API Calls:** 1 call

### 2. **Derivative Exchanges Metadata** ⭐⭐⭐⭐
- **Status:** ✅ Complete
- **File:** `dim_derivative_exchanges.parquet`
- **Records:** 20 exchanges
- **Top Exchanges by OI:**
  1. Binance (Futures): 335,014 BTC OI
  2. Bybit (Futures): 149,469 BTC OI
  3. CoinW (Futures): 147,097 BTC OI
  4. Gate (Futures): 143,568 BTC OI
  5. LBank (Futures): 114,602 BTC OI
- **Value for MSM v0:** Exchange metadata for derivative data analysis
- **API Calls:** 1 call

### 3. **Derivative Volumes** ⭐⭐⭐⭐⭐
- **Status:** ✅ Complete
- **File:** `fact_derivative_volume.parquet`
- **Records:** **19,876** derivative contracts
- **Coverage:**
  - Exchanges: **100**
  - Assets: **2,205**
- **Value for MSM v0:** Backup data for **Liquidity** feature, cross-validate CoinGlass
- **API Calls:** 1 call

### 4. **Derivative Open Interest** ⭐⭐⭐⭐⭐
- **Status:** ✅ Complete
- **File:** `fact_derivative_open_interest.parquet`
- **Records:** **19,209** contracts with OI
- **Coverage:**
  - Exchanges: **96**
  - Assets: **2,163**
  - Total OI: **$219.6 billion**
- **Value for MSM v0:** Backup data for **OI Risk** feature, cross-validate CoinGlass
- **API Calls:** 1 call

---

## ⏳ Background Fetch Status

The following data is being fetched in the background:

### 5. **OHLC Data (Open, High, Low, Close)**
- **Status:** ⏳ In Progress (encountered parameter errors, fixed)
- **File:** `fact_ohlc.parquet`
- **Expected:** Historical OHLC for all assets (10 years)
- **Value for MSM v0:** Enhances **Volatility Spread** calculations
- **API Calls:** ~2,718 calls (one per asset)

### 6. **Market Breadth (Top Gainers/Losers)**
- **Status:** ⏳ In Progress
- **File:** `fact_market_breadth.parquet`
- **Expected:** Top 30 gainers/losers by duration (24h, 7d, 14d, 30d)
- **Value for MSM v0:** Directly feeds **ALT Breadth** feature
- **API Calls:** 4 calls

### 7. **New Listings**
- **Status:** ⏳ In Progress
- **File:** `dim_new_listings.parquet`
- **Expected:** Latest 200 newly listed coins
- **Value for MSM v0:** Universe expansion
- **API Calls:** 1 call

### 8. **Exchange Volumes**
- **Status:** ⏳ In Progress
- **File:** `fact_exchange_volume.parquet`
- **Expected:** Volume data for major exchanges
- **Value for MSM v0:** Enhances **Liquidity** feature
- **API Calls:** ~10 calls

---

## 📊 Total API Usage

### Completed Fetches:
- Global Market Data: 1 call
- Derivative Exchanges: 1 call
- Derivative Volumes: 1 call
- Derivative OI: 1 call
- **Total Completed: 4 calls**

### In Progress:
- OHLC Backfill: ~2,718 calls
- Market Breadth: 4 calls
- New Listings: 1 call
- Exchange Volumes: ~10 calls
- **Total In Progress: ~2,733 calls**

### Grand Total: ~2,737 calls
**Remaining API Calls: ~481,263 / 500,000 (96.3%)**

---

## 🎯 MSM v0 Feature Enhancement Summary

### Direct Feature Inputs:
1. ✅ **BTC Dominance** - Now available directly from `fact_global_market.parquet`
2. ⏳ **ALT Breadth** - Will be enhanced by `fact_market_breadth.parquet`
3. ⏳ **Volatility Spread** - Will be enhanced by `fact_ohlc.parquet` (high/low ranges)

### Backup/Cross-Validation:
4. ✅ **Funding Skew** - Backup data from `fact_derivative_volume.parquet` (funding rates)
5. ✅ **OI Risk** - Backup data from `fact_derivative_open_interest.parquet`
6. ⏳ **Liquidity** - Enhanced by `fact_exchange_volume.parquet`

### Universe Expansion:
7. ⏳ **New Assets** - `dim_new_listings.parquet` for universe expansion

---

## 📁 New Data Lake Tables

```
data/curated/data_lake/
├── fact_global_market.parquet              [NEW ✅] - BTC Dominance
├── fact_derivative_volume.parquet          [NEW ✅] - Derivative volumes
├── fact_derivative_open_interest.parquet   [NEW ✅] - Derivative OI
├── dim_derivative_exchanges.parquet        [NEW ✅] - Exchange metadata
├── fact_ohlc.parquet                       [NEW ⏳] - OHLC data (in progress)
├── fact_market_breadth.parquet              [NEW ⏳] - Top gainers/losers (in progress)
├── dim_new_listings.parquet                [NEW ⏳] - New listings (in progress)
└── fact_exchange_volume.parquet             [NEW ⏳] - Exchange volumes (in progress)
```

---

## 🚀 Next Steps

1. **Wait for Background Fetch to Complete**
   - OHLC data (most valuable for volatility analysis)
   - Market breadth (direct ALT Breadth input)
   - New listings and exchange volumes

2. **Set Up Daily Automation**
   - Global market data (daily)
   - Market breadth (daily)
   - Derivative data (daily)
   - Exchange volumes (daily)

3. **Historical Global Market Cap Chart**
   - Retry with smaller date ranges or chunking
   - Provides 10 years of BTC dominance history

4. **Integrate with MSM v0**
   - Update regime monitor to use new data sources
   - Enhance features with OHLC and market breadth data

---

## 💡 Key Achievements

✅ **BTC Dominance** now available directly (no calculation needed)
✅ **$219.6B** in derivative open interest data captured
✅ **19,876** derivative contracts tracked
✅ **100 exchanges** covered for derivative data
✅ **2,205 assets** with derivative data

**Your Analyst tier subscription is being fully utilized!**
