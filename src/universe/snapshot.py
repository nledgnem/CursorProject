"""Point-in-time universe snapshot builder."""

import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
import yaml


def get_rebalance_dates(start_date: date, end_date: date, frequency: str, day: int = 1) -> List[date]:
    """Generate rebalance dates."""
    dates = []
    current = start_date
    
    if frequency == "monthly":
        # First rebalance on start_date (or first day of month if day != 1)
        if current.day != day:
            # Move to first day of next month with specified day
            if current.month == 12:
                current = date(current.year + 1, 1, day)
            else:
                current = date(current.year, current.month + 1, day)
        
        while current <= end_date:
            dates.append(current)
            # Move to next month
            if current.month == 12:
                current = date(current.year + 1, 1, day)
            else:
                current = date(current.year, current.month + 1, day)
    
    elif frequency == "quarterly":
        # Quarters: Jan 1, Apr 1, Jul 1, Oct 1
        quarter_months = [1, 4, 7, 10]
        # Find first quarter date >= start_date
        year = start_date.year
        month = start_date.month
        for qm in quarter_months:
            if qm >= month:
                current = date(year, qm, day)
                break
        else:
            current = date(year + 1, 1, day)
        
        while current <= end_date:
            dates.append(current)
            # Move to next quarter
            if current.month == 10:
                current = date(current.year + 1, 1, day)
            elif current.month == 7:
                current = date(current.year, 10, day)
            elif current.month == 4:
                current = date(current.year, 7, day)
            else:  # month == 1
                current = date(current.year, 4, day)
    
    else:
        raise ValueError(f"Unknown frequency: {frequency}")
    
    return dates


def calculate_weights(
    symbols: List[str],
    market_caps: pd.Series,
    scheme: str,
    max_weight: float,
) -> pd.Series:
    """
    Calculate portfolio weights with iterative max weight capping.
    
    Args:
        symbols: List of symbols to weight
        market_caps: Series of market caps (symbol -> mcap)
        scheme: 'cap_weighted', 'sqrt_cap_weighted', or 'equal_weight_capped'
        max_weight: Maximum weight per asset
    
    Returns:
        Series of weights (symbol -> weight)
    """
    if scheme == "cap_weighted":
        weights = market_caps / market_caps.sum()
    
    elif scheme == "sqrt_cap_weighted":
        sqrt_mcaps = np.sqrt(market_caps)
        weights = sqrt_mcaps / sqrt_mcaps.sum()
    
    elif scheme == "equal_weight_capped":
        n = len(symbols)
        equal_weight = 1.0 / n
        weights = pd.Series(equal_weight, index=symbols)
    
    else:
        raise ValueError(f"Unknown weighting scheme: {scheme}")
    
    # Iterative weight capping: keep capping and redistributing until no asset exceeds max_weight
    max_iterations = 100
    for iteration in range(max_iterations):
        capped = weights.clip(upper=max_weight)
        excess = (weights - capped).sum()
        
        if excess < 1e-10:  # No excess to redistribute
            break
        
        # Redistribute excess proportionally to uncapped assets
        uncapped_mask = capped < max_weight
        if not uncapped_mask.any():
            # All assets are capped, just normalize
            capped = capped / capped.sum()
            break
        
        uncapped_weights = capped[uncapped_mask]
        if uncapped_weights.sum() > 0:
            redistribution = excess * (uncapped_weights / uncapped_weights.sum())
            capped[uncapped_mask] += redistribution
        
        # Check if any asset still exceeds max_weight after redistribution
        if (capped > max_weight + 1e-10).any():
            weights = capped
            continue
        else:
            weights = capped
            break
    
    # Final normalization to ensure sum = 1.0
    weights = weights / weights.sum()
    
    return weights


def build_snapshots(
    config_path: Path,
    prices_path: Path,
    mcaps_path: Path,
    volumes_path: Path,
    allowlist_path: Path,
    output_path: Path,
    blacklist_path: Optional[Path] = None,
    stablecoins_path: Optional[Path] = None,
    wrapped_path: Optional[Path] = None,
    perp_listings_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Build point-in-time universe snapshots.
    
    For each rebalance date:
    1. Determine eligible coins (allowlist, not excluded, listing age, mcap, volume)
    2. Rank by market cap and select top N
    3. Calculate weights
    4. Save snapshot with full metadata
    """
    print(f"Loading config from {config_path}")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    print(f"Loading data...")
    
    # Check if paths point to fact tables (data lake format) or wide format
    prices_path_str = str(prices_path)
    use_data_lake = 'fact_price' in prices_path_str or 'data_lake' in prices_path_str
    
    if use_data_lake:
        # Load from fact tables and convert to wide format
        from src.utils.data_loader import load_data_lake_wide
        data_lake_dir = prices_path.parent
        data = load_data_lake_wide(data_lake_dir)
        prices_df = data['prices']
        mcaps_df = data['marketcap']
        volumes_df = data['volume']
        print(f"  Loaded from data lake format and converted to wide format for processing")
    else:
        # Load from wide format files (legacy)
        prices_df = pd.read_parquet(prices_path)
        mcaps_df = pd.read_parquet(mcaps_path)
        volumes_df = pd.read_parquet(volumes_path)
    
    allowlist_df = pd.read_csv(allowlist_path)
    
    # Load blacklist, stablecoins, and wrapped/synthetic assets
    repo_root = config_path.parent.parent
    if blacklist_path is None:
        blacklist_path = repo_root / "data" / "blacklist.csv"
    if stablecoins_path is None:
        stablecoins_path = repo_root / "data" / "stablecoins.csv"
    if wrapped_path is None:
        wrapped_path = repo_root / "data" / "wrapped.csv"
    
    # Load Binance perp listings dataset (optional, for point-in-time eligibility)
    perp_listings_df = None
    symbol_to_onboard_date = {}
    if perp_listings_path is None:
        # Try default locations
        for default_path in [
            repo_root / "data" / "raw" / "perp_listings_binance.parquet",
            repo_root / "data" / "curated" / "perp_listings_binance.parquet",
            repo_root / "outputs" / "perp_listings_binance.parquet",
        ]:
            if default_path.exists():
                perp_listings_path = default_path
                break
    
    if perp_listings_path and perp_listings_path.exists():
        try:
            perp_listings_df = pd.read_parquet(perp_listings_path)
            # Create symbol -> onboard_date mapping
            # Normalize Binance symbols (BTCUSDT -> BTC, 1000SHIBUSDT -> 1000SHIB)
            for _, row in perp_listings_df.iterrows():
                binance_symbol = row["symbol"]
                onboard_date = row["onboard_date"]
                if isinstance(onboard_date, str):
                    onboard_date = date.fromisoformat(onboard_date)
                elif isinstance(onboard_date, pd.Timestamp):
                    onboard_date = onboard_date.date()
                
                # Normalize: remove "USDT" suffix if present
                # Handle both "BTCUSDT" -> "BTC" and "1000SHIBUSDT" -> "1000SHIB"
                if binance_symbol.endswith("USDT"):
                    normalized_symbol = binance_symbol[:-4]  # Remove "USDT"
                else:
                    normalized_symbol = binance_symbol
                
                # Only add if we have this symbol in our price data
                # (We'll check against mcap_series.index later, but this avoids cluttering the dict)
                symbol_to_onboard_date[normalized_symbol] = onboard_date
            print(f"  Loaded {len(symbol_to_onboard_date)} Binance perp listings from {perp_listings_path}")
            print(f"  Note: Binance symbols normalized (e.g., BTCUSDT -> BTC)")
        except Exception as e:
            print(f"  [WARN] Failed to load perp listings from {perp_listings_path}: {e}")
            print(f"  [WARN] Falling back to allowlist-only perp eligibility check")
    else:
        if perp_listings_path:
            print(f"  [WARN] Perp listings file not found: {perp_listings_path}")
        print(f"  [INFO] Using allowlist-only perp eligibility (static check)")
    
    blacklisted_symbols = set()
    stablecoin_symbols = set()
    wrapped_symbols = set()
    
    if blacklist_path.exists():
        blacklist_df = pd.read_csv(blacklist_path)
        blacklisted_symbols = set(blacklist_df["symbol"].values)
        print(f"  Loaded {len(blacklisted_symbols)} blacklisted symbols from {blacklist_path}")
    else:
        print(f"  [WARN] Blacklist file not found: {blacklist_path}")
    
    if stablecoins_path.exists():
        stablecoins_df = pd.read_csv(stablecoins_path)
        stablecoin_symbols = set(stablecoins_df[stablecoins_df["is_stable"] == 1]["symbol"].values)
        print(f"  Loaded {len(stablecoin_symbols)} stablecoin symbols from {stablecoins_path}")
    else:
        print(f"  [WARN] Stablecoins file not found: {stablecoins_path}")
    
    if wrapped_path.exists():
        wrapped_df = pd.read_csv(wrapped_path)
        wrapped_symbols = set(wrapped_df["symbol"].values)
        print(f"  Loaded {len(wrapped_symbols)} wrapped/synthetic symbols from {wrapped_path}")
    else:
        print(f"  [WARN] Wrapped/synthetic assets file not found: {wrapped_path}")
    
    # Create symbol -> coingecko_id mapping from allowlist
    symbol_to_cg_id = dict(zip(allowlist_df["symbol"], allowlist_df["coingecko_id"]))
    symbol_to_venue = dict(zip(allowlist_df["symbol"], allowlist_df["venue"]))
    
    # Convert index to date if datetime
    if isinstance(prices_df.index, pd.DatetimeIndex):
        prices_df.index = prices_df.index.date
    if isinstance(mcaps_df.index, pd.DatetimeIndex):
        mcaps_df.index = mcaps_df.index.date
    if isinstance(volumes_df.index, pd.DatetimeIndex):
        volumes_df.index = volumes_df.index.date
    
    # Get config parameters
    # Handle both string and date objects from YAML
    start_date_val = config["start_date"]
    if isinstance(start_date_val, str):
        start_date = date.fromisoformat(start_date_val)
    elif isinstance(start_date_val, date):
        start_date = start_date_val
    else:
        start_date = date.fromisoformat(str(start_date_val))
    
    end_date_val = config["end_date"]
    if isinstance(end_date_val, str):
        end_date = date.fromisoformat(end_date_val)
    elif isinstance(end_date_val, date):
        end_date = end_date_val
    else:
        end_date = date.fromisoformat(str(end_date_val))
    rebalance_freq = config["rebalance_frequency"]
    rebalance_day = config.get("rebalance_day", 1)
    top_n = config["top_n"]
    base_asset = config["base_asset"]
    strategy_name = config.get("strategy_name", "unknown")
    
    eligibility = config["eligibility"]
    must_have_perp = eligibility.get("must_have_perp", True)
    min_listing_days = eligibility.get("min_listing_days", 90)
    min_mcap = eligibility.get("min_mcap_usd")
    min_volume = eligibility.get("min_volume_usd")
    # Point-in-time data requirements
    require_price = eligibility.get("require_price", True)  # Default: require price data
    require_volume = eligibility.get("require_volume", False)
    require_marketcap = eligibility.get("require_marketcap", False)
    
    weighting = config["weighting"]
    max_weight = config["max_weight_per_asset"]
    
    # Combine config excluded_assets with blacklist, stablecoins, and wrapped/synthetic
    config_excluded = set(config.get("excluded_assets", []))
    excluded = config_excluded | blacklisted_symbols | stablecoin_symbols | wrapped_symbols
    print(f"  Total excluded symbols: {len(excluded)} (config: {len(config_excluded)}, blacklist: {len(blacklisted_symbols)}, stablecoins: {len(stablecoin_symbols)}, wrapped/synthetic: {len(wrapped_symbols)})")
    
    # Log active filters
    print(f"\nActive filters:")
    print(f"  - Base asset excluded: {base_asset}")
    print(f"  - Must have perp (allowlist): {must_have_perp}")
    print(f"  - Blacklist: {len(blacklisted_symbols)} symbols")
    print(f"  - Stablecoins: {len(stablecoin_symbols)} symbols")
    print(f"  - Config excluded: {len(config_excluded)} symbols")
    print(f"  - Min listing days: {min_listing_days}")
    print(f"  - Min market cap: ${min_mcap:,.0f}" if min_mcap else "  - Min market cap: None")
    print(f"  - Min volume (14d avg): ${min_volume:,.0f}" if min_volume else "  - Min volume: None")
    print(f"  - Require price data (point-in-time): {require_price}")
    print(f"  - Require volume data (point-in-time): {require_volume}")
    print(f"  - Require marketcap data (point-in-time): {require_marketcap}")
    print(f"  - Weighting scheme: {weighting}")
    print(f"  - Max weight per asset: {max_weight:.1%}")
    
    # Basket metadata
    basket_name = f"{strategy_name}_TOP{top_n}"
    selection_version = "v1"
    venue = "BINANCE"  # Default, can be overridden per asset from allowlist
    
    # Data source tracking (for price/marketcap/volume data)
    # Currently all data comes from CoinGecko via curated parquet files
    data_source = "coingecko"
    
    # Build proxy label (versioned for future schema evolution)
    # Use Binance onboard dates if available, otherwise fall back to allowlist
    if symbol_to_onboard_date:
        proxy_version = "v0"  # Binance exchangeInfo version
        proxy_source = "binance_exchangeInfo"
        data_proxy_label = "perp_eligible_proxy_binance_v0"
    else:
        proxy_version = "v1"
        proxy_source = "perp_allowlist.csv" if must_have_perp else "none"
        data_proxy_label = f"perp_eligible_proxy_{proxy_version}" if must_have_perp else "no_perp_filter"
    
    # Get allowlisted symbols (still used for venue/coingecko_id mapping)
    allowlisted_symbols = set(allowlist_df["symbol"].values)
    
    # Get rebalance dates
    rebalance_dates = get_rebalance_dates(start_date, end_date, rebalance_freq, rebalance_day)
    print(f"Found {len(rebalance_dates)} rebalance dates")
    
    # Prepare metadata structure (will be returned regardless of success/failure)
    metadata_template = {
        "filter_thresholds": {
            "must_have_perp": must_have_perp,
            "min_listing_days": min_listing_days,
            "min_mcap_usd": min_mcap,
            "min_volume_usd": min_volume,
            "top_n": top_n,
            "weighting_scheme": weighting,
            "max_weight_per_asset": max_weight,
            "base_asset": base_asset,
            "blacklist_count": len(blacklisted_symbols),
            "stablecoin_count": len(stablecoin_symbols),
            "wrapped_count": len(wrapped_symbols),
            "config_excluded_count": len(config_excluded),
            "perp_allowlist_proxy_version": proxy_version,
            "perp_allowlist_proxy_source": proxy_source,
            "perp_allowlist_proxy_label": data_proxy_label,
            "perp_listings_used": bool(symbol_to_onboard_date),
            "perp_listings_count": len(symbol_to_onboard_date) if symbol_to_onboard_date else 0,
        },
        "date_range": {
            "start_date": str(start_date),
            "end_date": str(end_date),
        },
    }
    
    # Build snapshots
    universe_eligibility_rows = []  # All candidates with eligibility flags
    basket_snapshot_rows = []  # Selected top-N with weights
    
    for rebal_date in rebalance_dates:
        print(f"\nProcessing rebalance date: {rebal_date}")
        
        # Get data as-of this date (use most recent available data <= rebal_date)
        available_dates = [d for d in mcaps_df.index if d <= rebal_date]
        if not available_dates:
            print(f"  [SKIP] No data available for {rebal_date}")
            continue
        
        snapshot_date = max(available_dates)
        print(f"  Using snapshot date: {snapshot_date} (data as-of)")
        
        # Get market caps and volumes at snapshot date
        mcap_series = mcaps_df.loc[snapshot_date]
        vol_series = volumes_df.loc[snapshot_date] if snapshot_date in volumes_df.index else pd.Series(dtype=float)
        price_series = prices_df.loc[snapshot_date] if snapshot_date in prices_df.index else pd.Series(dtype=float)
        
        # Calculate 14d rolling average volume if needed
        if min_volume is not None:
            vol_window_start = max(available_dates[0], snapshot_date - timedelta(days=14))
            vol_window_dates = [d for d in volumes_df.index if vol_window_start <= d <= snapshot_date]
            if vol_window_dates:
                vol_avg = volumes_df.loc[vol_window_dates].mean()
            else:
                vol_avg = vol_series
        else:
            vol_avg = None
        
        # Build universe eligibility table (all candidates with flags)
        eligible = []
        filter_stats = {
            "total_candidates": 0,
            "excluded_base": 0,
            "excluded_not_in_allowlist": 0,
            "excluded_perp_not_listed_yet": 0,  # Track Binance perp not listed yet separately
            "excluded_blacklist": 0,
            "excluded_no_price": 0,
            "excluded_listing_age": 0,
            "excluded_mcap": 0,
            "excluded_volume": 0,
        }
        
        # Get first_seen_date for all symbols (compute once)
        symbol_first_seen_all = {}
        for symbol in mcap_series.index:
            first_price_date = prices_df[symbol].first_valid_index()
            if first_price_date is not None:
                if isinstance(first_price_date, pd.Timestamp):
                    first_price_date = first_price_date.date()
                symbol_first_seen_all[symbol] = first_price_date
            else:
                symbol_first_seen_all[symbol] = None
        
        for symbol in mcap_series.index:
            filter_stats["total_candidates"] += 1
            
            # Compute eligibility flags for this candidate
            first_seen = symbol_first_seen_all.get(symbol)
            listing_days = (snapshot_date - first_seen).days if first_seen else None
            
            # Point-in-time data availability flags (on snapshot_date)
            has_price_val = symbol in price_series.index and pd.notna(price_series[symbol])
            has_volume_val = symbol in vol_series.index and pd.notna(vol_series[symbol])
            has_marketcap_val = symbol in mcap_series.index and pd.notna(mcap_series[symbol])
            
            # Provider filters (blacklist, stablecoins, wrapped/synthetic, config excluded)
            is_stablecoin_val = symbol in stablecoin_symbols
            is_blacklisted_val = symbol in blacklisted_symbols
            is_wrapped_val = symbol in wrapped_symbols
            meets_provider_filters_val = (
                symbol != base_asset and
                symbol not in excluded
            )
            
            # Listing filters (age, perp eligibility)
            # Perp eligibility: use Binance onboard dates if available, otherwise fall back to allowlist
            if must_have_perp:
                if symbol_to_onboard_date:
                    # Point-in-time check: perp must have been onboarded by rebalance_date
                    onboard_date = symbol_to_onboard_date.get(symbol)
                    perp_eligible_proxy_val = (onboard_date is not None and snapshot_date >= onboard_date)
                else:
                    # Fallback to static allowlist check
                    perp_eligible_proxy_val = symbol in allowlisted_symbols
            else:
                perp_eligible_proxy_val = True
            
            meets_age_val = first_seen is not None and listing_days is not None and listing_days >= min_listing_days
            meets_listing_filters_val = meets_age_val and perp_eligible_proxy_val
            
            # Threshold checks
            mcap_val = mcap_series[symbol] if symbol in mcap_series.index else None
            meets_mcap_val = (min_mcap is None) or (pd.notna(mcap_val) and mcap_val >= min_mcap)
            vol_val = vol_avg[symbol] if (vol_avg is not None and symbol in vol_avg.index) else (vol_series[symbol] if symbol in vol_series.index else None)
            meets_liquidity_val = (min_volume is None) or (pd.notna(vol_val) and vol_val >= min_volume)
            
            # Determine exclusion reason (check in order of filter application)
            # Order: base_asset -> excluded (blacklist/stablecoin/config) -> data requirements -> 
            #        listing_age -> mcap/volume -> perp eligibility
            exclusion_reason = None
            
            # 1. Check base asset (highest priority)
            if symbol == base_asset:
                filter_stats["excluded_base"] += 1
                exclusion_reason = "base_asset"
            # 2. Check excluded (blacklist/stablecoins/wrapped/config) - before perp check
            elif symbol in excluded:
                filter_stats["excluded_blacklist"] += 1
                # Use more specific reason if available
                if symbol in wrapped_symbols:
                    exclusion_reason = "wrapped_or_synthetic"
                else:
                    exclusion_reason = "blacklist_or_stablecoin"
            # 3. Point-in-time data requirements (must be checked on snapshot_date)
            elif require_price and not has_price_val:
                filter_stats["excluded_no_price"] += 1
                exclusion_reason = "no_price_data"
            elif require_volume and not has_volume_val:
                filter_stats["excluded_no_volume"] = filter_stats.get("excluded_no_volume", 0) + 1
                exclusion_reason = "no_volume_data"
            elif require_marketcap and not has_marketcap_val:
                filter_stats["excluded_no_marketcap"] = filter_stats.get("excluded_no_marketcap", 0) + 1
                exclusion_reason = "no_marketcap_data"
            # 4. Check listing age
            elif first_seen is None or listing_days is None or listing_days < min_listing_days:
                filter_stats["excluded_listing_age"] += 1
                exclusion_reason = "insufficient_listing_age"
            # 5. Check market cap threshold
            elif min_mcap is not None:
                if not pd.notna(mcap_val) or mcap_val < min_mcap:
                    filter_stats["excluded_mcap"] += 1
                    exclusion_reason = "below_min_mcap"
            # 6. Check volume threshold
            elif min_volume is not None and vol_avg is not None:
                if not pd.notna(vol_val) or vol_val < min_volume:
                    filter_stats["excluded_volume"] += 1
                    exclusion_reason = "below_min_volume"
            # 7. Must have perp (point-in-time check if Binance data available, otherwise allowlist)
            #    This is checked LAST so other exclusion reasons take precedence
            elif must_have_perp:
                if symbol_to_onboard_date:
                    # Point-in-time check using Binance onboard dates
                    onboard_date = symbol_to_onboard_date.get(symbol)
                    if onboard_date is None or snapshot_date < onboard_date:
                        filter_stats["excluded_perp_not_listed_yet"] += 1
                        exclusion_reason = "perp_not_listed_yet"
                else:
                    # Fallback to static allowlist check
                    if symbol not in allowlisted_symbols:
                        filter_stats["excluded_not_in_allowlist"] += 1
                        exclusion_reason = "not_in_allowlist"
            
            # Compute final eligibility: meets provider filters AND has required data AND meets listing filters
            eligible_val = (
                meets_provider_filters_val and
                (not require_price or has_price_val) and
                (not require_volume or has_volume_val) and
                (not require_marketcap or has_marketcap_val) and
                meets_listing_filters_val and
                meets_mcap_val and
                meets_liquidity_val
            )
            
            # Add to eligible list if eligible
            if eligible_val and exclusion_reason is None:
                eligible.append(symbol)
            
            # Add to universe eligibility table (all candidates, whether eligible or not)
            universe_eligibility_rows.append({
                "rebalance_date": rebal_date,
                "snapshot_date": snapshot_date,
                "symbol": symbol,
                "coingecko_id": symbol_to_cg_id.get(symbol, ""),
                "venue": symbol_to_venue.get(symbol, venue),
                "marketcap": mcap_val,
                "volume_14d": vol_val,
                # Point-in-time data availability flags
                "has_price": has_price_val,
                "has_volume": has_volume_val,
                "has_marketcap": has_marketcap_val,
                # Provider filter flags
                "is_stablecoin": is_stablecoin_val,
                "is_blacklisted": is_blacklisted_val,
                "is_wrapped": is_wrapped_val,
                "meets_provider_filters": meets_provider_filters_val,
                # Listing filter flags
                "perp_eligible_proxy": perp_eligible_proxy_val,
                "meets_age": meets_age_val,
                "meets_listing_filters": meets_listing_filters_val,
                # Threshold flags
                "meets_liquidity": meets_liquidity_val,
                "meets_mcap": meets_mcap_val,
                # Final eligibility flag
                "eligible": eligible_val,
                # Metadata
                "first_seen_date": first_seen,
                "exclusion_reason": exclusion_reason,
                "data_proxy_label": data_proxy_label,
                "proxy_version": proxy_version,
                "proxy_source": proxy_source,
                "source": data_source,  # Source of price/marketcap/volume data
            })
        
        print(f"  Found {len(eligible)} eligible coins (out of {filter_stats['total_candidates']} candidates)")
        print(f"  Filter stats breakdown:")
        
        # Show individual filter counts
        if filter_stats["excluded_base"] > 0:
            print(f"    - Excluded base asset ({base_asset}): {filter_stats['excluded_base']}")
        if filter_stats["excluded_perp_not_listed_yet"] > 0:
            print(f"    - Excluded perp not listed yet (Binance): {filter_stats['excluded_perp_not_listed_yet']}")
        if filter_stats["excluded_not_in_allowlist"] > 0:
            print(f"    - Excluded not in allowlist: {filter_stats['excluded_not_in_allowlist']}")
        if filter_stats["excluded_blacklist"] > 0:
            print(f"    - Excluded blacklist/stablecoins: {filter_stats['excluded_blacklist']}")
        if filter_stats["excluded_no_price"] > 0:
            print(f"    - Excluded no price data: {filter_stats['excluded_no_price']}")
        if filter_stats.get("excluded_no_volume", 0) > 0:
            print(f"    - Excluded no volume data: {filter_stats['excluded_no_volume']}")
        if filter_stats.get("excluded_no_marketcap", 0) > 0:
            print(f"    - Excluded no marketcap data: {filter_stats['excluded_no_marketcap']}")
        if filter_stats["excluded_listing_age"] > 0:
            print(f"    - Excluded listing age < {min_listing_days}d: {filter_stats['excluded_listing_age']}")
        if filter_stats["excluded_mcap"] > 0:
            print(f"    - Excluded mcap < ${min_mcap:,.0f}: {filter_stats['excluded_mcap']}")
        if filter_stats["excluded_volume"] > 0:
            print(f"    - Excluded volume < ${min_volume:,.0f}: {filter_stats['excluded_volume']}")
        
        # Show histogram of exclusion_reason from universe_eligibility_rows (for this rebalance date only)
        if universe_eligibility_rows:
            # Filter to current rebalance date
            current_date_rows = [row for row in universe_eligibility_rows if row.get("rebalance_date") == rebal_date]
            if current_date_rows:
                exclusion_df = pd.DataFrame(current_date_rows)
                if "exclusion_reason" in exclusion_df.columns:
                    exclusion_counts = exclusion_df["exclusion_reason"].value_counts()
                    if len(exclusion_counts) > 0:
                        print(f"  Exclusion reason histogram (top 5, for this date):")
                        for reason, count in exclusion_counts.head(5).items():
                            if pd.notna(reason):  # Skip None/NaN
                                print(f"    - {reason}: {count}")
        
        if len(eligible) == 0:
            print(f"  [SKIP] No eligible coins, skipping snapshot")
            continue
        
        # Rank by market cap and select top N
        eligible_mcaps = mcap_series[eligible].dropna().sort_values(ascending=False)
        selected = eligible_mcaps.head(top_n).index.tolist()
        
        print(f"  Selected top {len(selected)} coins by market cap (eligible: {len(eligible)}, top_n: {top_n})")
        
        # Calculate weights
        selected_mcaps = mcap_series[selected]
        weights = calculate_weights(selected, selected_mcaps, weighting, max_weight)
        
        # Verify weights sum to 1 and no weight exceeds max
        assert abs(weights.sum() - 1.0) < 1e-6, f"Weights sum to {weights.sum()}, not 1.0"
        max_actual_weight = weights.max()
        if max_actual_weight > max_weight + 1e-6:
            print(f"  [WARN] Max weight {max_actual_weight:.4f} exceeds cap {max_weight:.4f}")
        else:
            print(f"  Max weight: {max_actual_weight:.4f} (cap: {max_weight:.4f})")
        
        # Get volume data for selected coins
        vol_14d_data = {}
        if vol_avg is not None:
            for symbol in selected:
                vol_14d_data[symbol] = vol_avg[symbol] if symbol in vol_avg.index else None
        else:
            for symbol in selected:
                vol_14d_data[symbol] = vol_series[symbol] if symbol in vol_series.index else None
        
        # Build basket snapshot (selected top-N with weights)
        for rank, symbol in enumerate(selected, start=1):
            basket_snapshot_rows.append({
                "rebalance_date": rebal_date,
                "snapshot_date": snapshot_date,
                "symbol": symbol,
                "coingecko_id": symbol_to_cg_id.get(symbol, ""),
                "venue": symbol_to_venue.get(symbol, venue),
                "basket_name": basket_name,
                "selection_version": selection_version,
                "rank": rank,
                "weight": weights[symbol],
                "marketcap": selected_mcaps[symbol],
                "volume_14d": vol_14d_data.get(symbol),
                "source": data_source,  # Source of price/marketcap/volume data
            })
    
    # Create DataFrames
    universe_df = pd.DataFrame(universe_eligibility_rows)
    basket_df = pd.DataFrame(basket_snapshot_rows)
    
    # Save both tables (even if empty, for audit trail)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Universe eligibility table (always save, even if empty)
    universe_path = output_path.parent / "universe_eligibility.parquet"
    if universe_df.empty:
        # Create empty DataFrame with correct schema
        universe_df = pd.DataFrame(columns=[
            "rebalance_date", "snapshot_date", "symbol", "coingecko_id", "venue",
            "marketcap", "volume_14d",
            # Point-in-time data availability flags
            "has_price", "has_volume", "has_marketcap",
            # Provider filter flags
            "is_stablecoin", "is_blacklisted", "meets_provider_filters",
            # Listing filter flags
            "perp_eligible_proxy", "meets_age", "meets_listing_filters",
            # Threshold flags
            "meets_liquidity", "meets_mcap",
            # Final eligibility flag
            "eligible",
            # Metadata
            "first_seen_date", "exclusion_reason", "data_proxy_label",
            "proxy_version", "proxy_source", "source",
        ])
    universe_df.to_parquet(universe_path, index=False)
    
    # Basket snapshots table (keep original path for backward compatibility)
    if basket_df.empty:
        print("\n[WARN] No basket snapshots created - no eligible coins found!")
        # Create empty DataFrame with correct schema
        basket_df = pd.DataFrame(columns=[
            "rebalance_date", "snapshot_date", "symbol", "coingecko_id", "venue",
            "basket_name", "selection_version", "rank", "weight",
            "marketcap", "volume_14d", "source",
        ])
        basket_df.to_parquet(output_path, index=False)
        print(f"  Created empty basket snapshots file at {output_path}")
    else:
        basket_df.to_parquet(output_path, index=False)
        print(f"\n[SUCCESS] Built {len(basket_df['rebalance_date'].unique())} basket snapshots")
        print(f"  Total basket constituents: {len(basket_df)}")
    
    print(f"  Total universe candidates: {len(universe_eligibility_rows)}")
    print(f"  Saved basket snapshots to {output_path}")
    print(f"  Saved universe eligibility to {universe_path}")
    
    # Return metadata for run_metadata.json
    return {
        "num_snapshots": len(basket_df['rebalance_date'].unique()),
        "total_constituents": len(basket_df),
        "row_count": len(basket_df),
        "universe_candidates_count": len(universe_df),
        **metadata_template,
    }

