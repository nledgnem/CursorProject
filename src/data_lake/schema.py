"""Schema definitions for data lake tables."""

from typing import Dict, List, Optional
from datetime import date
import pandas as pd


# Dimension Tables
DIM_ASSET_SCHEMA = {
    "asset_id": str,  # Canonical internal ID (e.g., "BTC", "ETH", "ERC20_0x...")
    "symbol": str,  # Primary symbol (e.g., "BTC", "ETH")
    "name": Optional[str],  # Full name
    "chain": Optional[str],  # Blockchain (e.g., "ethereum", "bitcoin", "solana")
    "contract_address": Optional[str],  # Contract address if token
    "coingecko_id": Optional[str],  # CoinGecko ID
    "is_stable": bool,  # Is stablecoin
    "is_wrapped_stable": bool,  # Is wrapped stablecoin
    "metadata_json": Optional[str],  # JSON string for additional metadata
}

DIM_INSTRUMENT_SCHEMA = {
    "instrument_id": str,  # Canonical internal ID
    "venue": str,  # Exchange (e.g., "binance", "coinbase")
    "instrument_symbol": str,  # Exchange symbol (e.g., "BTCUSDT")
    "instrument_type": str,  # "spot", "perpetual", "future", etc.
    "quote": str,  # Quote currency (e.g., "USDT", "USD")
    "base_asset_symbol": str,  # Base asset symbol (e.g., "BTC")
    "asset_id": Optional[str],  # Canonical asset_id (links to dim_asset.asset_id)
    "multiplier": Optional[float],  # Contract multiplier (e.g., 1000 for 1000SHIBUSDT)
    "metadata_json": Optional[str],  # JSON string for additional metadata
}

# Mapping Tables (time-valid)
MAP_PROVIDER_ASSET_SCHEMA = {
    "provider": str,  # Data provider (e.g., "coingecko", "binance")
    "provider_asset_id": str,  # Provider's ID (e.g., "bitcoin", "BTCUSDT")
    "asset_id": str,  # Our canonical asset_id
    "valid_from": date,  # When mapping becomes valid
    "valid_to": Optional[date],  # When mapping expires (None = current)
    "mapping_method": str,  # "exact_match", "manual_override", "fuzzy_match", etc.
    "confidence": float,  # 0.0-1.0 confidence score
}

MAP_PROVIDER_INSTRUMENT_SCHEMA = {
    "provider": str,  # Data provider (e.g., "binance")
    "provider_instrument_id": str,  # Provider's instrument ID
    "instrument_id": str,  # Our canonical instrument_id
    "valid_from": date,  # When mapping becomes valid
    "valid_to": Optional[date],  # When mapping expires (None = current)
    "mapping_method": str,  # "exact_match", "manual_override", etc.
    "confidence": float,  # 0.0-1.0 confidence score
}

# Category to Asset Mapping
MAP_CATEGORY_ASSET_SCHEMA = {
    "asset_id": str,  # Canonical asset_id
    "category_id": str,  # Category ID from dim_categories
    "category_name": str,  # Category name (for reference, from API)
    "source": str,  # "coingecko"
}

# Fact Tables (append-only, time-series)
FACT_PRICE_SCHEMA = {
    "asset_id": str,
    "date": date,
    "close": float,  # Closing price
    "source": str,  # Data source (e.g., "coingecko")
}

FACT_MARKETCAP_SCHEMA = {
    "asset_id": str,
    "date": date,
    "marketcap": float,  # Market cap in USD
    "source": str,  # Data source
}

FACT_VOLUME_SCHEMA = {
    "asset_id": str,
    "date": date,
    "volume": float,  # Volume in USD
    "source": str,  # Data source
}

FACT_FUNDING_SCHEMA = {
    "asset_id": str,  # Base asset symbol (e.g., "BTC")
    "instrument_id": Optional[str],  # Instrument ID if available (e.g., "binance_perp_BTCUSDT")
    "date": date,
    "funding_rate": float,  # Funding rate (e.g., 0.0001 = 0.01%)
    "exchange": str,  # Exchange name (e.g., "BINANCE")
    "source": str,  # Data source (e.g., "coinglass")
}

# Analyst Tier Exclusive Fact Tables
FACT_OHLC_SCHEMA = {
    "asset_id": str,
    "date": date,
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "source": str,  # "coingecko"
}

FACT_MARKET_BREADTH_SCHEMA = {
    "date": date,
    "asset_id": str,
    "rank": int,  # Rank in top gainers/losers
    "price_change_24h": Optional[float],  # Percentage change
    "price_change_7d": Optional[float],   # Percentage change
    "price_change_14d": Optional[float],  # Percentage change
    "price_change_30d": Optional[float],  # Percentage change
    "category": str,  # "gainer" or "loser"
    "duration": str,  # "1h", "24h", "7d", "14d", "30d", etc.
    "source": str,  # "coingecko"
}

FACT_EXCHANGE_VOLUME_SCHEMA = {
    "exchange_id": str,  # "binance", "coinbase", etc.
    "date": date,
    "volume_btc": float,
    "volume_usd": float,
    "source": str,  # "coingecko"
}

# Analyst Tier Dimension Tables
DIM_NEW_LISTINGS_SCHEMA = {
    "asset_id": str,
    "symbol": str,
    "name": str,
    "listing_date": date,  # When it was listed on CoinGecko
    "coingecko_id": str,
    "source": str,  # "coingecko"
}

# Global Market Data (for BTC Dominance)
FACT_GLOBAL_MARKET_SCHEMA = {
    "date": date,
    "total_market_cap_usd": float,
    "total_market_cap_btc": float,
    "total_volume_usd": float,
    "total_volume_btc": float,
    "btc_dominance": float,  # BTC dominance percentage
    "active_cryptocurrencies": int,
    "markets": int,
    "source": str,  # "coingecko"
}

FACT_GLOBAL_MARKET_HISTORY_SCHEMA = {
    "date": date,
    "market_cap_btc": float,
    "market_cap_usd": float,
    "source": str,  # "coingecko"
}

# Derivative Data (backup for funding/OI)
FACT_DERIVATIVE_VOLUME_SCHEMA = {
    "date": date,
    "exchange": str,
    "base_asset": str,
    "target": str,
    "volume_usd": float,
    "open_interest_usd": float,
    "funding_rate": float,
    "source": str,  # "coingecko"
}

FACT_DERIVATIVE_OPEN_INTEREST_SCHEMA = {
    "date": date,
    "exchange": str,
    "base_asset": str,
    "target": str,
    "open_interest_usd": float,
    "open_interest_btc": float,
    "funding_rate": float,
    "source": str,  # "coingecko"
}

# Trending & Sentiment Data
FACT_TRENDING_SEARCHES_SCHEMA = {
    "date": date,
    "item_type": str,  # "coin", "nft", "category"
    "item_id": str,  # CoinGecko ID or category ID
    "item_name": str,  # Name of the item
    "item_symbol": Optional[str],  # Symbol (for coins)
    "rank": int,  # Rank in trending list
    "source": str,  # "coingecko"
}

# Category Market Data
FACT_CATEGORY_MARKET_SCHEMA = {
    "date": date,
    "category_id": str,  # Category ID
    "category_name": str,  # Category name
    "market_cap_usd": float,
    "market_cap_btc": float,
    "volume_24h_usd": float,
    "volume_24h_btc": float,
    "market_cap_change_24h": Optional[float],  # Percentage
    "top_3_coins": Optional[str],  # JSON string of top 3 coins
    "source": str,  # "coingecko"
}

# All Markets Snapshot
FACT_MARKETS_SNAPSHOT_SCHEMA = {
    "date": date,
    "asset_id": str,  # Canonical asset_id
    "coingecko_id": str,  # CoinGecko ID
    "symbol": str,
    "name": str,
    "current_price_usd": float,
    "market_cap_usd": Optional[float],
    "market_cap_rank": Optional[int],
    "fully_diluted_valuation_usd": Optional[float],
    "total_volume_usd": Optional[float],
    "high_24h_usd": Optional[float],
    "low_24h_usd": Optional[float],
    "price_change_24h": Optional[float],
    "price_change_percentage_24h": Optional[float],
    "market_cap_change_24h": Optional[float],
    "market_cap_change_percentage_24h": Optional[float],
    "circulating_supply": Optional[float],
    "total_supply": Optional[float],
    "max_supply": Optional[float],
    "ath_usd": Optional[float],  # All-time high
    "ath_change_percentage": Optional[float],
    "ath_date": Optional[date],
    "atl_usd": Optional[float],  # All-time low
    "atl_change_percentage": Optional[float],
    "atl_date": Optional[date],
    "source": str,  # "coingecko"
}

# Historical Exchange Volume (by date range)
FACT_EXCHANGE_VOLUME_HISTORY_SCHEMA = {
    "date": date,
    "exchange_id": str,  # Exchange ID
    "volume_btc": float,
    "volume_usd": float,
    "source": str,  # "coingecko"
}

# Exchange Rankings & Details
DIM_EXCHANGES_SCHEMA = {
    "exchange_id": str,  # Exchange ID (e.g., "binance", "coinbase")
    "exchange_name": str,  # Exchange name
    "country": Optional[str],  # Country code
    "year_established": Optional[int],  # Year established
    "description": Optional[str],  # Exchange description
    "url": Optional[str],  # Exchange website URL
    "image": Optional[str],  # Exchange logo URL
    "has_trading_incentive": Optional[bool],  # Has trading incentives
    "trust_score": Optional[int],  # Trust score (1-10)
    "trust_score_rank": Optional[int],  # Trust score rank
    "trade_volume_24h_btc": Optional[float],  # 24h volume in BTC
    "trade_volume_24h_btc_normalized": Optional[float],  # Normalized 24h volume
    "tickers": Optional[int],  # Number of trading pairs
    "source": str,  # "coingecko"
}

# Derivative Exchange Details (time-series)
FACT_DERIVATIVE_EXCHANGE_DETAILS_SCHEMA = {
    "date": date,
    "exchange_id": str,  # Derivative exchange ID
    "exchange_name": str,  # Exchange name
    "open_interest_btc": float,  # Total open interest in BTC
    "trade_volume_24h_btc": float,  # 24h trading volume in BTC
    "number_of_perpetual_pairs": int,  # Number of perpetual pairs
    "number_of_futures_pairs": int,  # Number of futures pairs
    "number_of_derivatives": Optional[int],  # Total derivatives
    "source": str,  # "coingecko"
}

DIM_DERIVATIVE_EXCHANGES_SCHEMA = {
    "exchange_id": str,
    "exchange_name": str,
    "open_interest_btc": float,
    "trade_volume_24h_btc": float,
    "number_of_perpetual_pairs": int,
    "number_of_futures_pairs": int,
    "source": str,  # "coingecko"
}

# Categories List (metadata only)
DIM_CATEGORIES_SCHEMA = {
    "category_id": str,  # Category ID
    "category_name": str,  # Category name
    "source": str,  # "coingecko"
}

# Exchange Details (with tickers)
FACT_EXCHANGE_TICKERS_SCHEMA = {
    "date": date,
    "exchange_id": str,  # Exchange ID
    "ticker_base": str,  # Base asset symbol
    "ticker_target": str,  # Quote asset symbol
    "ticker_pair": str,  # Trading pair (e.g., "BTC/USDT")
    "last_price_usd": Optional[float],  # Last price in USD
    "volume_usd": Optional[float],  # Volume in USD
    "bid_ask_spread_percentage": Optional[float],  # Bid-ask spread %
    "trust_score": Optional[str],  # Trust score (e.g., "green", "yellow")
    "source": str,  # "coingecko"
}

# Output Tables (from pipeline)
UNIVERSE_ELIGIBILITY_SCHEMA = {
    "rebalance_date": date,
    "snapshot_date": date,
    "asset_id": Optional[str],  # May use symbol instead
    "symbol": str,  # For convenience
    "coingecko_id": Optional[str],  # CoinGecko ID for reference
    "venue": Optional[str],  # Exchange venue
    "exclusion_reason": Optional[str],  # Why excluded, or None if eligible
    "marketcap": Optional[float],
    "volume_14d": Optional[float],
    "is_stablecoin": Optional[bool],  # Is stablecoin flag
    "is_blacklisted": Optional[bool],  # Is blacklisted flag
    "meets_liquidity": bool,
    "meets_age": bool,
    "meets_mcap": bool,
    "perp_eligible_proxy": bool,
    "first_seen_date": Optional[date],
    "data_proxy_label": str,
    "proxy_version": Optional[str],  # Proxy version identifier
    "proxy_source": Optional[str],  # Proxy source identifier
    "source": str,  # Source of price/marketcap/volume data (e.g., "coingecko")
}

BASKET_SNAPSHOTS_SCHEMA = {
    "rebalance_date": date,
    "snapshot_date": Optional[date],  # Snapshot date for reference
    "asset_id": Optional[str],  # May use symbol instead
    "symbol": str,  # For convenience
    "coingecko_id": Optional[str],  # CoinGecko ID for reference
    "venue": Optional[str],  # Exchange venue
    "basket_name": Optional[str],  # Basket name identifier
    "selection_version": Optional[str],  # Selection version identifier
    "weight": float,
    "rank": int,
    "marketcap": float,
    "volume_14d": Optional[float],  # 14-day volume for reference
    "source": str,  # Source of price/marketcap/volume data (e.g., "coingecko")
}

# Table names (for DuckDB views)
# Table name mappings
TABLE_NAMES = {
    "dim_asset": "dim_asset",
    "dim_instrument": "dim_instrument",
    "dim_new_listings": "dim_new_listings",
    "map_provider_asset": "map_provider_asset",
    "map_provider_instrument": "map_provider_instrument",
    "map_category_asset": "map_category_asset",
    "fact_price": "fact_price",
    "fact_marketcap": "fact_marketcap",
    "fact_volume": "fact_volume",
    "fact_funding": "fact_funding",
    "fact_ohlc": "fact_ohlc",
    "fact_market_breadth": "fact_market_breadth",
    "fact_exchange_volume": "fact_exchange_volume",
    "fact_global_market": "fact_global_market",
    "fact_global_market_history": "fact_global_market_history",
    "fact_derivative_volume": "fact_derivative_volume",
    "fact_derivative_open_interest": "fact_derivative_open_interest",
    "dim_derivative_exchanges": "dim_derivative_exchanges",
    "fact_trending_searches": "fact_trending_searches",
    "fact_category_market": "fact_category_market",
    "fact_markets_snapshot": "fact_markets_snapshot",
    "fact_exchange_volume_history": "fact_exchange_volume_history",
    "dim_exchanges": "dim_exchanges",
    "fact_derivative_exchange_details": "fact_derivative_exchange_details",
    "dim_categories": "dim_categories",
    "fact_exchange_tickers": "fact_exchange_tickers",
    "universe_eligibility": "universe_eligibility",
    "basket_snapshots": "basket_snapshots",
}


def create_empty_table(schema: Dict[str, type]) -> pd.DataFrame:
    """Create an empty DataFrame with the specified schema."""
    data = {col: [] for col in schema.keys()}
    df = pd.DataFrame(data)
    # Set dtypes
    for col, dtype in schema.items():
        if dtype == date:
            df[col] = pd.Series(dtype="object")  # Will be converted to date later
        elif dtype == Optional[str] or dtype == str:
            df[col] = df[col].astype(str)
        elif dtype == Optional[float] or dtype == float:
            df[col] = df[col].astype(float)
        elif dtype == bool:
            df[col] = df[col].astype(bool)
        elif dtype == int:
            df[col] = df[col].astype(int)
    return df


def validate_schema(df: pd.DataFrame, schema: Dict[str, type], table_name: str) -> List[str]:
    """Validate DataFrame against schema. Returns list of errors (empty if valid)."""
    errors = []
    
    # Check all required columns exist
    for col in schema.keys():
        if col not in df.columns:
            errors.append(f"{table_name}: Missing column '{col}'")
    
    # Check for extra columns (warn but don't fail)
    for col in df.columns:
        if col not in schema:
            errors.append(f"{table_name}: Extra column '{col}' not in schema")
    
    return errors
