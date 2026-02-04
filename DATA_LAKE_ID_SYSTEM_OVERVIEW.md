# Data Lake ID System Overview

## 🎯 Universal Bridge IDs

### 1. **asset_id** - THE PRIMARY UNIVERSAL BRIDGE ⭐

**Purpose:** Canonical identifier that bridges ALL asset-related data across the entire data lake.

**Format:**
- Native coins: Uppercase symbol (e.g., `"BTC"`, `"ETH"`, `"SOL"`)
- Tokens: `"CHAIN_ADDRESS"` format (e.g., `"ERC20_0xabc..."`, `"SOL_abc..."`)

**Where it's used:**
- ✅ `dim_asset.asset_id` - Master asset dimension table
- ✅ `fact_price.asset_id` - Daily prices
- ✅ `fact_marketcap.asset_id` - Market capitalization
- ✅ `fact_volume.asset_id` - Trading volumes
- ✅ `fact_ohlc.asset_id` - OHLC data
- ✅ `fact_market_breadth.asset_id` - Top gainers/losers
- ✅ `fact_markets_snapshot.asset_id` - All markets snapshot
- ✅ `dim_new_listings.asset_id` - New coin listings
- ✅ `fact_funding.asset_id` - Funding rates (base asset)
- ✅ `dim_instrument.asset_id` - Links instruments to assets
- ✅ `universe_eligibility.asset_id` - Universe eligibility
- ✅ `basket_snapshots.asset_id` - Basket selections

**How to bridge provider IDs to asset_id:**
```python
# Via mapping table
map_provider_asset.asset_id  <-  map_provider_asset.provider_asset_id
```

**Example:**
- CoinGecko ID `"bitcoin"` → `asset_id` `"BTC"` (via `map_provider_asset`)
- All fact tables use `asset_id = "BTC"` for Bitcoin data

---

### 2. **instrument_id** - INSTRUMENT BRIDGE ⭐

**Purpose:** Canonical identifier for trading instruments (perpetuals, futures, spot pairs).

**Format:** `"venue_type_symbol"` (e.g., `"binance_perp_BTCUSDT"`)

**Where it's used:**
- ✅ `dim_instrument.instrument_id` - Master instrument dimension table
- ✅ `fact_funding.instrument_id` - Funding rates (instrument-specific)
- ✅ `map_provider_instrument.instrument_id` - Provider instrument mappings

**Links to asset_id:**
- `dim_instrument.asset_id` - Links instrument to its base asset
- Example: `instrument_id = "binance_perp_BTCUSDT"` → `asset_id = "BTC"`

**Example:**
- Binance symbol `"BTCUSDT"` → `instrument_id` `"binance_perp_BTCUSDT"` (via `map_provider_instrument`)
- `fact_funding` uses both `asset_id` and `instrument_id` for flexibility

---

## 🔷 Domain-Specific IDs (No Universal Bridge)

These IDs are consistent within their domain but don't directly bridge to assets.

### 3. **exchange_id** - Exchange Identifier

**Purpose:** Identifies exchanges across exchange-related tables.

**Format:** Lowercase exchange name (e.g., `"binance"`, `"coinbase"`, `"binance_futures"`)

**Where it's used:**
- ✅ `dim_exchanges.exchange_id` - Exchange dimension table
- ✅ `fact_exchange_volume.exchange_id` - Exchange trading volumes
- ✅ `fact_exchange_volume_history.exchange_id` - Historical exchange volumes
- ✅ `fact_derivative_exchange_details.exchange_id` - Derivative exchange metrics
- ✅ `dim_derivative_exchanges.exchange_id` - Derivative exchange dimension

**Note:** Does NOT bridge to assets directly. Exchange data is separate from asset data.

**Example:**
- `exchange_id = "binance"` appears in all Binance-related tables
- To get Binance's BTC volume, you'd join `fact_exchange_volume` with asset data via other means

---

### 4. **category_id** - Category Identifier

**Purpose:** Identifies cryptocurrency categories (DeFi, Layer-1, etc.).

**Format:** CoinGecko category ID (e.g., `"defi"`, `"layer-1"`)

**Where it's used:**
- ✅ `fact_category_market.category_id` - Category market data

**Note:** Does NOT directly bridge to assets. Categories contain multiple assets, but the relationship is not stored in a mapping table.

---

### 5. **item_id** - Trending Item Identifier

**Purpose:** Identifies trending items (coins, NFTs, categories).

**Format:** CoinGecko ID or category ID (depends on `item_type`)

**Where it's used:**
- ✅ `fact_trending_searches.item_id` - Trending searches

**Note:** 
- If `item_type = "coin"`, then `item_id` is a `coingecko_id` which can be mapped to `asset_id` via `map_provider_asset`
- If `item_type = "category"`, then `item_id` is a `category_id` (no direct asset bridge)

---

## 📝 Provider-Specific IDs (Mapped to Canonical IDs)

### 6. **coingecko_id** - CoinGecko Provider ID

**Purpose:** CoinGecko's internal asset identifier.

**Format:** CoinGecko ID (e.g., `"bitcoin"`, `"ethereum"`, `"solana"`)

**Where it's stored:**
- ✅ `dim_asset.coingecko_id` - Asset dimension (for reference)
- ✅ `dim_new_listings.coingecko_id` - New listings
- ✅ `fact_markets_snapshot.coingecko_id` - Markets snapshot

**How to bridge to asset_id:**
```python
# Via mapping table
map_provider_asset.asset_id  <-  map_provider_asset.provider_asset_id
WHERE map_provider_asset.provider = 'coingecko'
AND map_provider_asset.provider_asset_id = coingecko_id
```

**Example:**
- `coingecko_id = "bitcoin"` → `asset_id = "BTC"` (via `map_provider_asset`)

---

### 7. **base_asset** - Base Asset Symbol (Legacy/Inconsistent)

**Purpose:** Base asset symbol in derivative tables.

**Format:** Uppercase symbol (e.g., `"BTC"`, `"ETH"`)

**Where it's used:**
- ⚠️ `fact_derivative_volume.base_asset` - Derivative volumes
- ⚠️ `fact_derivative_open_interest.base_asset` - Open interest

**Note:** This is a **symbol**, not an `asset_id`. Should ideally be converted to `asset_id` for full integration.

**Recommendation:** These tables should use `asset_id` instead of `base_asset` for consistency.

---

## 🔗 ID Relationships & Bridging

### Primary Bridge: asset_id

```
Provider IDs (coingecko_id, etc.)
    |
    | (via map_provider_asset)
    v
asset_id ← THE UNIVERSAL BRIDGE
    |
    | (used in all fact tables)
    v
fact_price, fact_marketcap, fact_volume, fact_ohlc,
fact_market_breadth, fact_markets_snapshot, etc.
```

### Secondary Bridge: instrument_id

```
Provider Instrument IDs (binance symbols, etc.)
    |
    | (via map_provider_instrument)
    v
instrument_id ← INSTRUMENT BRIDGE
    |
    | (used in instrument fact tables)
    v
fact_funding (has both asset_id AND instrument_id)
    |
    | (links to asset via)
    v
dim_instrument.asset_id → asset_id
```

### Domain-Specific IDs (No Universal Bridge)

```
exchange_id → Used across exchange-related tables
    - dim_exchanges.exchange_id
    - fact_exchange_volume.exchange_id
    - fact_exchange_volume_history.exchange_id
    - fact_derivative_exchange_details.exchange_id

category_id → Used in category tables
    - fact_category_market.category_id

item_id → Used in trending searches
    - fact_trending_searches.item_id
    - May be coingecko_id (which can map to asset_id) or category_id
```

---

## 📊 Summary Table

| ID Type | Universal Bridge? | Format | Primary Use |
|---------|------------------|--------|-------------|
| **asset_id** | ✅ **YES** | `"BTC"` or `"ERC20_0x..."` | All asset-related fact tables |
| **instrument_id** | ✅ **YES** | `"binance_perp_BTCUSDT"` | Instrument-related fact tables |
| **exchange_id** | ❌ No | `"binance"` | Exchange-related tables |
| **category_id** | ❌ No | `"defi"` | Category tables |
| **item_id** | ⚠️ Partial | CoinGecko ID or category ID | Trending searches |
| **coingecko_id** | ✅ Via mapping | `"bitcoin"` | Provider reference, maps to asset_id |
| **base_asset** | ⚠️ Should be asset_id | `"BTC"` | Legacy derivative tables |

---

## 🎯 How to Join Data Across Tables

### Example 1: Get prices with asset metadata

```python
import polars as pl

# Load fact table
prices = pl.read_parquet('data/curated/data_lake/fact_price.parquet')

# Load dimension table
assets = pl.read_parquet('data/curated/data_lake/dim_asset.parquet')

# Join using asset_id (THE UNIVERSAL BRIDGE)
result = prices.join(
    assets.select(['asset_id', 'symbol', 'name', 'chain']),
    on='asset_id',
    how='left'
)
```

### Example 2: Convert CoinGecko ID to asset_id

```python
# Load mapping table
mappings = pl.read_parquet('data/curated/data_lake/map_provider_asset.parquet')

# Get asset_id for CoinGecko ID "bitcoin"
asset_id = mappings.filter(
    (pl.col('provider') == 'coingecko') &
    (pl.col('provider_asset_id') == 'bitcoin')
).select('asset_id').item()
# Returns: "BTC"
```

### Example 3: Get funding rates with asset and instrument info

```python
# Load fact table
funding = pl.read_parquet('data/curated/data_lake/fact_funding.parquet')

# Load dimension tables
instruments = pl.read_parquet('data/curated/data_lake/dim_instrument.parquet')
assets = pl.read_parquet('data/curated/data_lake/dim_asset.parquet')

# Join using both asset_id and instrument_id
result = funding.join(
    instruments.select(['instrument_id', 'instrument_symbol', 'base_asset_symbol', 'asset_id']),
    on='instrument_id',
    how='left'
).join(
    assets.select(['asset_id', 'symbol', 'name']),
    on='asset_id',
    how='left'
)
```

### Example 4: Get exchange volumes (no asset bridge)

```python
# Exchange data is separate from asset data
exchange_volumes = pl.read_parquet('data/curated/data_lake/fact_exchange_volume.parquet')
exchanges = pl.read_parquet('data/curated/data_lake/dim_exchanges.parquet')

# Join using exchange_id (domain-specific ID)
result = exchange_volumes.join(
    exchanges.select(['exchange_id', 'exchange_name', 'country']),
    on='exchange_id',
    how='left'
)
```

---

## ⚠️ Known Issues & Recommendations

### Issue 1: base_asset instead of asset_id

**Tables affected:**
- `fact_derivative_volume.base_asset`
- `fact_derivative_open_interest.base_asset`

**Recommendation:** Convert `base_asset` to `asset_id` for full data lake integration.

**How to fix:**
```python
# Map base_asset (symbol) to asset_id via dim_asset
derivative_vol = pl.read_parquet('fact_derivative_volume.parquet')
assets = pl.read_parquet('dim_asset.parquet')

# Join to get asset_id
derivative_vol = derivative_vol.join(
    assets.select(['asset_id', 'symbol']),
    left_on='base_asset',
    right_on='symbol',
    how='left'
)
# Then add asset_id column and eventually deprecate base_asset
```

### Issue 2: category_id doesn't bridge to assets

**Tables affected:**
- `fact_category_market.category_id`

**Recommendation:** Create a mapping table `map_category_assets` to link categories to their constituent assets.

---

## ✅ Best Practices

1. **Always use asset_id for asset-related joins** - It's the universal bridge
2. **Use instrument_id for instrument-specific data** - Links to asset_id via dim_instrument
3. **Use exchange_id for exchange-related data** - Domain-specific, no asset bridge
4. **Convert provider IDs to asset_id via mapping tables** - Don't use provider IDs directly in joins
5. **Prefer asset_id over symbols** - Symbols can be ambiguous (e.g., multiple tokens with same symbol)

---

## 📚 Key Takeaways

1. **asset_id is THE universal bridge** - Use it to join all asset-related data
2. **instrument_id bridges instruments** - Links to asset_id via dim_instrument
3. **exchange_id, category_id are domain-specific** - No universal bridge, but consistent within domain
4. **Provider IDs map to asset_id** - Use `map_provider_asset` to convert
5. **Some tables need updates** - `base_asset` should become `asset_id` for full integration

---

**Bottom Line:** `asset_id` is your universal bridge for all asset-related data. Use it consistently, and convert provider IDs to `asset_id` via mapping tables when needed.
