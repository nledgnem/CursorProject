# Maximizing Your CoinGecko Analyst Tier Subscription

## Priority Data Points to Fetch (Before Subscription Expires)

### ⭐⭐⭐⭐⭐ **HIGHEST PRIORITY**

#### 1. **OHLC Data (Open, High, Low, Close)**
**Why:** Essential for better volatility analysis, drawdown calculations, and enhanced MSM v0 features
- **Endpoint:** `/coins/{id}/ohlc` with time range
- **Historical Range:** 10 years (Analyst tier exclusive)
- **Output:** `fact_ohlc.parquet`
- **Value:** Directly improves Volatility Spread calculations in MSM v0

#### 2. **Top Gainers/Losers (Market Breadth)**
**Why:** Directly feeds into ALT Breadth feature in MSM v0 regime monitor
- **Endpoint:** `/coins/top_gainers_losers` (Analyst tier exclusive)
- **Output:** `fact_market_breadth.parquet`
- **Value:** Real-time market breadth metrics for regime detection

#### 3. **Historical OHLC Backfill (10 years)**
**Why:** Complete historical OHLC data for all existing assets
- **Endpoint:** `/coins/{id}/ohlc` with date range
- **Range:** 2013-01-01 to present
- **Output:** Extend `fact_ohlc.parquet` with historical data
- **Value:** Extended backtesting with high/low data

### ⭐⭐⭐⭐ **HIGH PRIORITY**

#### 4. **New Listings (Universe Expansion)**
**Why:** Automatically discover new assets for your universe
- **Endpoint:** `/coins/list/new` (Analyst tier exclusive)
- **Output:** `dim_new_listings.parquet`
- **Value:** Keep universe current with newly listed coins

#### 5. **Exchange Volume Data**
**Why:** Exchange-level liquidity analysis
- **Endpoint:** `/exchanges/{id}/volume_chart`
- **Output:** `fact_exchange_volume.parquet`
- **Value:** Understand volume concentration and liquidity patterns

### ⭐⭐⭐ **MEDIUM PRIORITY**

#### 6. **Derivative Data (Futures/Perpetuals)**
**Why:** Complement CoinGlass data, cross-validation
- **Endpoints:** `/derivatives`, `/derivatives/exchanges`
- **Output:** `fact_derivative_volume.parquet`, `fact_derivative_open_interest.parquet`
- **Value:** Additional source for funding/OI data

#### 7. **Trending Coins**
**Why:** Identify momentum and trending assets
- **Endpoint:** `/search/trending`
- **Output:** `fact_trending_coins.parquet`
- **Value:** Momentum analysis for regime detection

#### 8. **Global Market Data**
**Why:** Market-wide metrics for regime context
- **Endpoint:** `/global`
- **Output:** `fact_global_market.parquet`
- **Value:** Overall market cap, volume, BTC dominance

---

## Implementation Plan

### Phase 1: Critical Data (Do First)
1. ✅ **OHLC Historical Backfill** - Fetch 10 years of OHLC for all assets
2. ✅ **Top Gainers/Losers** - Daily market breadth data
3. ✅ **OHLC Daily Updates** - Set up incremental OHLC fetching

### Phase 2: Universe Expansion
4. ✅ **New Listings** - Fetch and track newly listed coins
5. ✅ **Exchange Volume** - Major exchanges volume data

### Phase 3: Enhanced Features
6. ✅ **Derivative Data** - Futures/perpetuals data
7. ✅ **Trending Coins** - Momentum indicators
8. ✅ **Global Market** - Market-wide metrics

---

## Estimated API Calls

### OHLC Historical Backfill:
- ~2,718 assets × 1 call = **2,718 calls**
- Can fetch 10 years in single call per asset

### Top Gainers/Losers:
- 1 call per day × 30 days = **30 calls/month**
- Historical: Can fetch daily for past 90 days = **90 calls**

### New Listings:
- 1 call = **1 call** (returns latest 200)

### Exchange Volume:
- ~10 major exchanges × 1 call = **10 calls**
- Historical: 10 exchanges × 90 days = **900 calls**

### Total Estimated:
- **Initial Backfill:** ~3,700 calls
- **Monthly Ongoing:** ~40 calls/month
- **Well within 500k monthly limit!**

---

## Quick Wins (Can Do Today)

1. **Fetch Top Gainers/Losers** - 1 API call, immediate value
2. **Fetch New Listings** - 1 API call, discover new assets
3. **Test OHLC Endpoint** - Verify it works for your assets
4. **Fetch Exchange Volume** - 10 calls, understand liquidity

---

## Next Steps

I'll create scripts to fetch all of these data points. Start with the highest priority items first!
