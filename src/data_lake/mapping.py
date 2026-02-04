"""Build dimension and mapping tables from raw data sources."""

import pandas as pd
import json
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import hashlib


def generate_asset_id(
    symbol: str,
    chain: Optional[str] = None,
    contract_address: Optional[str] = None,
) -> str:
    """
    Generate canonical asset_id.
    
    Priority:
    1. If chain + contract_address available: "ERC20_0x..." or "SOL_..."
    2. If native coin: use uppercase symbol (e.g., "BTC", "ETH")
    3. Otherwise: use symbol (will improve later)
    """
    if chain and contract_address:
        # Normalize contract address (lowercase, no 0x prefix issues)
        addr = contract_address.lower().strip()
        if addr.startswith("0x"):
            addr = addr[2:]
        # Create ID like "ERC20_0xabc..." or "SOL_abc..."
        chain_prefix = chain.upper()
        return f"{chain_prefix}_{addr}"
    elif symbol:
        # For native coins, use uppercase symbol
        return symbol.upper().strip()
    else:
        raise ValueError("Cannot generate asset_id: need symbol or (chain + contract_address)")


def build_dim_asset_from_coingecko(
    coingecko_data: pd.DataFrame,
    stablecoins: Set[str],
    wrapped_stables: Set[str],
) -> pd.DataFrame:
    """
    Build dim_asset table from CoinGecko data.
    
    Args:
        coingecko_data: DataFrame with columns like 'symbol', 'name', 'id', etc.
        stablecoins: Set of stablecoin symbols
        wrapped_stables: Set of wrapped stablecoin symbols
    
    Returns:
        DataFrame with dim_asset schema
    """
    rows = []
    
    for _, row in coingecko_data.iterrows():
        symbol = str(row.get("symbol", "")).upper().strip()
        if not symbol:
            continue
        
        # Generate asset_id (for now, use symbol; improve later with chain/contract)
        asset_id = generate_asset_id(symbol=symbol)
        
        rows.append({
            "asset_id": asset_id,
            "symbol": symbol,
            "name": str(row.get("name", "")),
            "chain": None,  # TODO: Extract from CoinGecko metadata if available
            "contract_address": None,  # TODO: Extract from CoinGecko metadata
            "coingecko_id": str(row.get("id", "")),
            "is_stable": symbol in stablecoins,
            "is_wrapped_stable": symbol in wrapped_stables,
            "metadata_json": json.dumps({
                "coingecko_id": row.get("id", ""),
                "platforms": row.get("platforms", {}),
            }),
        })
    
    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["asset_id"])


def build_map_provider_asset_coingecko(
    coingecko_data: pd.DataFrame,
    dim_asset: pd.DataFrame,
    valid_from: date,
) -> pd.DataFrame:
    """
    Build mapping table from CoinGecko to our asset_id.
    
    Args:
        coingecko_data: DataFrame with CoinGecko data
        dim_asset: dim_asset table
        valid_from: When this mapping becomes valid
    
    Returns:
        DataFrame with map_provider_asset schema
    """
    rows = []
    
    # Create lookup: symbol -> asset_id
    symbol_to_asset_id = dict(zip(dim_asset["symbol"], dim_asset["asset_id"]))
    
    for _, row in coingecko_data.iterrows():
        symbol = str(row.get("symbol", "")).upper().strip()
        coingecko_id = str(row.get("id", ""))
        
        if not symbol or not coingecko_id:
            continue
        
        asset_id = symbol_to_asset_id.get(symbol)
        if not asset_id:
            # Generate asset_id if not in dim_asset
            asset_id = generate_asset_id(symbol=symbol)
        
        rows.append({
            "provider": "coingecko",
            "provider_asset_id": coingecko_id,  # CoinGecko uses IDs like "bitcoin"
            "asset_id": asset_id,
            "valid_from": valid_from,
            "valid_to": None,  # Current mapping
            "mapping_method": "exact_match" if symbol in symbol_to_asset_id else "symbol_fallback",
            "confidence": 1.0 if symbol in symbol_to_asset_id else 0.8,
        })
    
    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["provider", "provider_asset_id", "valid_from"])


def build_dim_instrument_from_binance_perps(
    perp_listings: pd.DataFrame,
    dim_asset: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Build dim_instrument table from Binance perpetual listings.
    
    Args:
        perp_listings: DataFrame with columns like 'symbol', 'onboardDate', etc.
        dim_asset: Optional dim_asset table to link asset_id. If None, asset_id will be None.
    
    Returns:
        DataFrame with dim_instrument schema
    """
    rows = []
    
    # Create lookup: symbol -> asset_id from dim_asset
    symbol_to_asset_id = {}
    if dim_asset is not None and len(dim_asset) > 0:
        # Try matching base_asset_symbol to asset_id or symbol in dim_asset
        if "asset_id" in dim_asset.columns:
            if "symbol" in dim_asset.columns:
                # Match by symbol first
                symbol_to_asset_id = dict(zip(dim_asset["symbol"], dim_asset["asset_id"]))
            # Also match asset_id to itself (in case symbol doesn't match)
            for asset_id in dim_asset["asset_id"]:
                if asset_id not in symbol_to_asset_id:
                    symbol_to_asset_id[asset_id] = asset_id
    
    for _, row in perp_listings.iterrows():
        binance_symbol = str(row.get("symbol", ""))  # e.g., "BTCUSDT", "1000SHIBUSDT"
        if not binance_symbol:
            continue
        
        # Parse symbol to extract base and quote
        # Handle multipliers like "1000SHIBUSDT" -> base="1000SHIB", quote="USDT"
        quote = "USDT"  # Default for USD-M futures
        if binance_symbol.endswith("USDT"):
            base_symbol = binance_symbol[:-4]
        elif binance_symbol.endswith("USD"):
            base_symbol = binance_symbol[:-3]
            quote = "USD"
        else:
            base_symbol = binance_symbol
            quote = "USDT"  # Default
        
        # Extract multiplier (e.g., "1000" from "1000SHIB")
        # Must do this BEFORE looking up asset_id, since we need the clean base_symbol
        multiplier = None
        clean_base_symbol = base_symbol
        if base_symbol and base_symbol[0].isdigit():
            # Extract leading digits
            multiplier_str = ""
            for char in base_symbol:
                if char.isdigit():
                    multiplier_str += char
                else:
                    break
            if multiplier_str:
                multiplier = float(multiplier_str)
                clean_base_symbol = base_symbol[len(multiplier_str):]
        
        # Look up asset_id from dim_asset using clean_base_symbol
        asset_id = symbol_to_asset_id.get(clean_base_symbol.upper(), None)
        
        # Generate instrument_id
        instrument_id = f"binance_perp_{binance_symbol}"
        
        rows.append({
            "instrument_id": instrument_id,
            "venue": "binance",
            "instrument_symbol": binance_symbol,
            "instrument_type": "perpetual",
            "quote": quote,
            "base_asset_symbol": clean_base_symbol.upper(),
            "asset_id": asset_id,  # Link to dim_asset.asset_id
            "multiplier": multiplier,
            "metadata_json": json.dumps({
                "onboard_date": str(row.get("onboard_date", "")),
                "contract_type": "PERPETUAL",
            }),
        })
    
    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["instrument_id"])


def build_map_provider_instrument_binance(
    perp_listings: pd.DataFrame,
    dim_instrument: pd.DataFrame,
    valid_from: date,
) -> pd.DataFrame:
    """
    Build mapping table from Binance instrument symbols to our instrument_id.
    
    Args:
        perp_listings: DataFrame with Binance perp listings
        dim_instrument: dim_instrument table
        valid_from: When this mapping becomes valid
    
    Returns:
        DataFrame with map_provider_instrument schema
    """
    rows = []
    
    # Create lookup: instrument_symbol -> instrument_id
    symbol_to_instrument_id = dict(zip(
        dim_instrument["instrument_symbol"],
        dim_instrument["instrument_id"]
    ))
    
    for _, row in perp_listings.iterrows():
        binance_symbol = str(row.get("symbol", ""))
        if not binance_symbol:
            continue
        
        instrument_id = symbol_to_instrument_id.get(binance_symbol)
        if not instrument_id:
            # Generate if not found
            instrument_id = f"binance_perp_{binance_symbol}"
        
        rows.append({
            "provider": "binance",
            "provider_instrument_id": binance_symbol,
            "instrument_id": instrument_id,
            "valid_from": valid_from,
            "valid_to": None,
            "mapping_method": "exact_match" if binance_symbol in symbol_to_instrument_id else "symbol_fallback",
            "confidence": 1.0 if binance_symbol in symbol_to_instrument_id else 0.8,
        })
    
    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["provider", "provider_instrument_id", "valid_from"])


def load_manual_mapping_overrides(override_file: Optional[Path]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Load manual mapping overrides from a CSV or JSON file.
    
    Expected format:
    - asset_overrides.csv: provider,provider_asset_id,asset_id
    - instrument_overrides.csv: provider,provider_instrument_id,instrument_id
    
    Returns:
        Tuple of (asset_overrides_dict, instrument_overrides_dict)
    """
    asset_overrides = {}
    instrument_overrides = {}
    
    if not override_file or not override_file.exists():
        return asset_overrides, instrument_overrides
    
    # Try CSV first
    if override_file.suffix == ".csv":
        df = pd.read_csv(override_file)
        if "provider" in df.columns and "provider_asset_id" in df.columns and "asset_id" in df.columns:
            for _, row in df.iterrows():
                key = (row["provider"], row["provider_asset_id"])
                asset_overrides[key] = row["asset_id"]
        
        if "provider" in df.columns and "provider_instrument_id" in df.columns and "instrument_id" in df.columns:
            for _, row in df.iterrows():
                key = (row["provider"], row["provider_instrument_id"])
                instrument_overrides[key] = row["instrument_id"]
    
    return asset_overrides, instrument_overrides
