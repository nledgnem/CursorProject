# Maximizing Your CoinGecko Analyst Tier Subscription

## ✅ Current Status

**API Usage Check Results:**
- Plan: **Analyst** ✅
- Rate Limit: **500 calls/minute** ✅
- Monthly Credit: **500,000 calls**
- Used: **16,827 calls** (3.4%)
- Remaining: **483,173 calls** (96.6%)

**You have plenty of capacity to fetch all the data!**

---

## 🎯 Priority Data Points to Fetch

### ⭐⭐⭐⭐⭐ **CRITICAL - Do These First**

#### 1. **OHLC Data (Open, High, Low, Close)**
**Why:** Essential for better volatility analysis and MSM v0 enhancements
- **Endpoint:** `/coins/{id}/ohlc` with time range
- **Historical Range:** 10 years (Analyst tier exclusive)
- **Output:** `fact_ohlc.parquet`
- **API Calls:** ~2,718 calls (one per asset)
- **Value:** 
  - Better Volatility Spread calculations
  - True high/low ranges for drawdown analysis
  - Gap analysis between days

**Command:**
```bash
python scripts/fetch_analyst_tier_data.py --ohlc
```

#### 2. **Top Gainers/Losers (Market Breadth)**
**Why:** Directly feeds ALT Breadth feature in MSM v0
- **Endpoint:** `/coins/top_gainers_losers` (Analyst tier exclusive)
- **Output:** `fact_market_breadth.parquet`
- **API Calls:** 4 calls (one per duration: 24h, 7d, 14d, 30d)
- **Value:** 
  - Real-time market breadth metrics
  - Regime detection enhancement
  - Momentum analysis

**Command:**
```bash
python scripts/fetch_analyst_tier_data.py --market-breadth
```

### ⭐⭐⭐⭐ **HIGH PRIORITY**

#### 3. **New Listings (Universe Expansion)**
**Why:** Automatically discover new assets
- **Endpoint:** `/coins/list/new` (Analyst tier exclusive)
- **Output:** `dim_new_listings.parquet`
- **API Calls:** 1 call (returns latest 200 coins)
- **Value:** 
  - Keep universe current
  - Early entry opportunities
  - Automated universe expansion

**Command:**
```bash
python scripts/fetch_analyst_tier_data.py --new-listings
```

#### 4. **Exchange Volume Data**
**Why:** Exchange-level liquidity analysis
- **Endpoint:** `/exchanges/{id}/volume_chart`
- **Output:** `fact_exchange_volume.parquet`
- **API Calls:** ~10 calls (one per major exchange)
- **Value:** 
  - Understand volume concentration
  - Liquidity patterns
  - Market structure analysis

**Command:**
```bash
python scripts/fetch_analyst_tier_data.py --exchange-volumes
```

---

## 📊 Implementation Summary

### Quick Start (Fetch All):
```bash
# Fetch everything at once
python scripts/fetch_analyst_tier_data.py --all

# Or fetch individually
python scripts/fetch_analyst_tier_data.py --ohlc
python scripts/fetch_analyst_tier_data.py --market-breadth
python scripts/fetch_analyst_tier_data.py --new-listings
python scripts/fetch_analyst_tier_data.py --exchange-volumes
```

### Estimated API Usage:
- **OHLC Backfill:** ~2,718 calls (one-time)
- **Market Breadth:** 4 calls (daily)
- **New Listings:** 1 call (daily/weekly)
- **Exchange Volumes:** ~10 calls (daily)
- **Total Initial:** ~2,733 calls
- **Monthly Ongoing:** ~450 calls/month

**Well within your 500k monthly limit!**

---

## 📁 New Data Lake Tables

After running the scripts, you'll have:

```
data/curated/data_lake/
├── fact_ohlc.parquet                    [NEW] - OHLC data
├── fact_market_breadth.parquet          [NEW] - Top gainers/losers
├── dim_new_listings.parquet             [NEW] - Newly listed coins
├── fact_exchange_volume.parquet         [NEW] - Exchange volumes
└── ... (existing tables)
```

---

## 🔄 Daily/Weekly Automation

### Recommended Schedule:

1. **Daily (Market Breadth):**
   ```bash
   python scripts/fetch_analyst_tier_data.py --market-breadth
   ```
   - 4 API calls/day
   - ~120 calls/month

2. **Daily (Exchange Volumes):**
   ```bash
   python scripts/fetch_analyst_tier_data.py --exchange-volumes
   ```
   - ~10 API calls/day
   - ~300 calls/month

3. **Weekly (New Listings):**
   ```bash
   python scripts/fetch_analyst_tier_data.py --new-listings
   ```
   - 1 API call/week
   - ~4 calls/month

4. **One-Time (OHLC Backfill):**
   ```bash
   python scripts/fetch_analyst_tier_data.py --ohlc
   ```
   - ~2,718 API calls (one-time)
   - Then incremental updates as needed

---

## 🎯 Integration with MSM v0

### Enhanced Features:

1. **ALT Breadth (Enhanced):**
   - Current: Count alts moving up/down from price data
   - Enhanced: Use `fact_market_breadth.parquet` for accurate rankings
   - Benefit: More reliable regime detection

2. **Volatility Spread (Enhanced):**
   - Current: Uses close prices only
   - Enhanced: Use OHLC high/low for true volatility ranges
   - Benefit: More accurate volatility calculations

3. **Momentum (Enhanced):**
   - Current: Calculated from price returns
   - Enhanced: Use top gainers/losers rankings
   - Benefit: Cross-validate momentum signals

---

## ✅ Next Steps

1. **Test Market Breadth (Quick Win):**
   ```bash
   python scripts/fetch_analyst_tier_data.py --market-breadth
   ```

2. **Fetch New Listings (Quick Win):**
   ```bash
   python scripts/fetch_analyst_tier_data.py --new-listings
   ```

3. **Backfill OHLC (High Value):**
   ```bash
   python scripts/fetch_analyst_tier_data.py --ohlc
   ```

4. **Fetch Exchange Volumes:**
   ```bash
   python scripts/fetch_analyst_tier_data.py --exchange-volumes
   ```

5. **Check API Usage Anytime:**
   ```bash
   python scripts/fetch_analyst_tier_data.py --check-usage
   ```

---

## 💡 Pro Tips

1. **Start with Quick Wins:** Market breadth and new listings are fast (5 calls total)
2. **OHLC is High Value:** This will significantly improve your volatility analysis
3. **Monitor Usage:** Use `--check-usage` regularly to track consumption
4. **Automate Daily:** Set up cron/scheduled tasks for daily market breadth updates
5. **Incremental Updates:** After initial OHLC backfill, only fetch new dates

---

## 📈 Expected Benefits

- **Better Regime Detection:** Market breadth data directly enhances ALT Breadth
- **Improved Volatility Analysis:** OHLC data provides true high/low ranges
- **Universe Expansion:** Automatically discover new assets
- **Liquidity Insights:** Exchange volume data for market structure analysis
- **Extended Backtesting:** 10 years of OHLC data for robust validation

**All of this data will integrate seamlessly with your existing data lake structure!**
