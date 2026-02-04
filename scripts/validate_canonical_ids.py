#!/usr/bin/env python3
"""
Comprehensive canonical ID validation: 8 checks that prove your canonical IDs are "SEDOL-like".

This implements institutional-grade validation:
1. Uniqueness (hard invariant)
2. No duplicate "same-thing" assets (dedupe check)
3. Stability (no accidental remaps)
4. Coverage (does it join to real data)
5. Time-valid joins (valid_from/valid_to)
6. Symbol collision audit (crypto-specific)
7. Spot checks (human verification)
8. Run-level mapping report
"""

import sys
import argparse
from pathlib import Path
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
import json
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def check_1_uniqueness(
    map_provider_asset: pd.DataFrame,
    map_provider_instrument: pd.DataFrame,
    dim_instrument: pd.DataFrame,
) -> Dict:
    """
    Check 1: Uniqueness (hard invariant)
    
    Must be true:
    - One provider ID → one canonical asset_id (within valid time window)
    - One instrument (Binance symbol) → one instrument_id
    - One instrument_id → one underlying asset_id
    """
    violations = []
    
    # 1a: Provider asset → asset_id uniqueness (within valid_from window)
    for (provider, provider_id), group in map_provider_asset.groupby(["provider", "provider_asset_id"]):
        # Check for overlapping valid_from/valid_to windows with different asset_ids
        group_sorted = group.sort_values("valid_from")
        for i in range(len(group_sorted) - 1):
            row1 = group_sorted.iloc[i]
            row2 = group_sorted.iloc[i + 1]
            
            # Check if windows overlap
            valid_to_1 = row1.get("valid_to") if pd.notna(row1.get("valid_to")) else date(2099, 12, 31)
            valid_from_2 = row2["valid_from"]
            
            if valid_to_1 >= valid_from_2 and row1["asset_id"] != row2["asset_id"]:
                violations.append({
                    "type": "provider_asset_multiple_asset_ids",
                    "provider": provider,
                    "provider_asset_id": provider_id,
                    "asset_id_1": row1["asset_id"],
                    "asset_id_2": row2["asset_id"],
                    "overlap_period": f"{valid_from_2} to {valid_to_1}",
                })
    
    # 1b: Provider instrument → instrument_id uniqueness
    if map_provider_instrument is not None and len(map_provider_instrument) > 0:
        for (provider, provider_id), group in map_provider_instrument.groupby(["provider", "provider_instrument_id"]):
            unique_instrument_ids = group["instrument_id"].nunique()
            if unique_instrument_ids > 1:
                violations.append({
                    "type": "provider_instrument_multiple_instrument_ids",
                    "provider": provider,
                    "provider_instrument_id": provider_id,
                    "n_instrument_ids": unique_instrument_ids,
                    "instrument_ids": group["instrument_id"].unique().tolist(),
                })
    
    # 1c: Instrument → asset_id uniqueness (one instrument_id → one underlying asset_id)
    if dim_instrument is not None and len(dim_instrument) > 0:
        for instrument_id, group in dim_instrument.groupby("instrument_id"):
            unique_base_assets = group["base_asset_symbol"].nunique()
            if unique_base_assets > 1:
                violations.append({
                    "type": "instrument_multiple_base_assets",
                    "instrument_id": instrument_id,
                    "n_base_assets": unique_base_assets,
                    "base_assets": group["base_asset_symbol"].unique().tolist(),
                })
    
    return {
        "passed": len(violations) == 0,
        "violations": violations,
        "violation_count": len(violations),
    }


def check_2_no_duplicate_same_thing_assets(
    dim_asset: pd.DataFrame,
) -> Dict:
    """
    Check 2: No duplicate "same-thing" assets (dedupe check)
    
    Should not have two asset_ids that represent the same token:
    - For tokens with contract addresses: (chain, contract_address) must be unique
    - For native coins: internal rule must ensure BTC isn't duplicated
    """
    violations = []
    
    # 2a: Check contract address duplicates
    if "chain" in dim_asset.columns and "contract_address" in dim_asset.columns:
        contract_assets = dim_asset[
            dim_asset["contract_address"].notna() & 
            (dim_asset["contract_address"] != "")
        ].copy()
        
        if len(contract_assets) > 0:
            for (chain, contract), group in contract_assets.groupby(["chain", "contract_address"]):
                if len(group) > 1:
                    violations.append({
                        "type": "duplicate_contract_address",
                        "chain": chain,
                        "contract_address": contract,
                        "n_asset_ids": len(group),
                        "asset_ids": group["asset_id"].tolist(),
                        "symbols": group["symbol"].tolist(),
                    })
    
    # 2b: Check native coin duplicates (same symbol should map to one asset_id)
    symbol_counts = dim_asset.groupby("symbol")["asset_id"].nunique()
    duplicates = symbol_counts[symbol_counts > 1]
    
    for symbol, count in duplicates.items():
        asset_ids = dim_asset[dim_asset["symbol"] == symbol]["asset_id"].tolist()
        # Check if they're actually different (e.g., different chains)
        asset_rows = dim_asset[dim_asset["symbol"] == symbol]
        chains = asset_rows["chain"].unique() if "chain" in asset_rows.columns else []
        
        # If same chain or both native, this is a problem
        if len(chains) <= 1:
            violations.append({
                "type": "duplicate_native_coin",
                "symbol": symbol,
                "n_asset_ids": int(count),
                "asset_ids": asset_ids,
                "chains": chains.tolist() if len(chains) > 0 else ["native"],
            })
    
    return {
        "passed": len(violations) == 0,
        "violations": violations,
        "violation_count": len(violations),
    }


def check_3_stability(
    map_provider_asset_current: pd.DataFrame,
    map_provider_asset_previous: Optional[pd.DataFrame] = None,
    require_previous: bool = False,
) -> Dict:
    """
    Check 3: Stability (no accidental remaps)
    
    Mapping must not "flip" across runs:
    - Same provider_asset_id should map to same asset_id today vs yesterday
    - Unless intentionally corrected (logged in mapping_method/changed_reason)
    
    Args:
        require_previous: If True, fail if no previous mapping provided (for CI/CD)
    """
    if map_provider_asset_previous is None:
        if require_previous:
            return {
                "status": "FAIL",
                "tested": False,
                "passed": False,  # For backwards compatibility
                "violations": [{
                    "type": "missing_previous_mapping",
                    "message": "Stability check requires --previous-mapping file but none provided",
                }],
                "violation_count": 1,
                "note": "Previous mapping file required but not provided",
                "warning": "Stability check FAILED - baseline required but not provided",
            }
        return {
            "status": "SKIPPED",
            "tested": False,
            "passed": False,  # Do not treat SKIPPED as PASS
            "violations": [],
            "violation_count": 0,
            "note": "SKIPPED (no baseline) - No previous mapping file to compare against (use --previous-mapping for stability check)",
            "warning": "Stability check SKIPPED (no baseline) - day-1 scenario, cannot test mapping stability without prior snapshot",
        }
    
    violations = []
    
    # Compare current vs previous mappings
    current_active = map_provider_asset_current[
        map_provider_asset_current["valid_to"].isna()
    ].copy()
    
    previous_active = map_provider_asset_previous[
        map_provider_asset_previous["valid_to"].isna()
    ].copy()
    
    # Merge on provider + provider_asset_id
    merged = current_active.merge(
        previous_active,
        on=["provider", "provider_asset_id"],
        how="outer",
        suffixes=("_current", "_previous"),
    )
    
    # Find remaps (same provider ID, different asset_id)
    remaps = merged[
        merged["asset_id_current"].notna() &
        merged["asset_id_previous"].notna() &
        (merged["asset_id_current"] != merged["asset_id_previous"])
    ]
    
    for _, row in remaps.iterrows():
        # Check if there's a reason logged
        has_reason = pd.notna(row.get("changed_reason_current")) or pd.notna(row.get("mapping_method_current"))
        
        violations.append({
            "type": "mapping_remap",
            "provider": row["provider"],
            "provider_asset_id": row["provider_asset_id"],
            "asset_id_previous": row["asset_id_previous"],
            "asset_id_current": row["asset_id_current"],
            "has_reason_logged": bool(has_reason),
        })
    
    is_passed = len(violations) == 0
    return {
        "status": "PASS" if is_passed else "FAIL",
        "tested": True,
        "passed": is_passed,  # For backwards compatibility
        "violations": violations,
        "violation_count": len(violations),
        "note": "Stability check completed with baseline comparison",
    }


def check_4_coverage(
    fact_marketcap: pd.DataFrame,
    fact_price: pd.DataFrame,
    fact_volume: pd.DataFrame,
    dim_asset: pd.DataFrame,
    map_provider_asset: pd.DataFrame,
    universe_eligibility: Optional[pd.DataFrame] = None,
    basket_snapshots: Optional[pd.DataFrame] = None,
    snapshot_date: Optional[date] = None,
    raw_provider_data: Optional[pd.DataFrame] = None,
    min_rebalance_coverage_pct: float = 80.0,
    strict_rebalance_coverage: bool = False,
) -> Dict:
    """
    Check 4: Coverage (does it join to real data)
    
    Two types of coverage:
    1. Alignment coverage: % of marketcap universe that has price/volume (within curated facts)
    2. Raw provider funnel coverage: raw provider assets -> mapped -> facts (non-tautological)
    
    On rebalance dates:
    - % of eligible assets that have price data
    - % of basket constituents with full coverage
    """
    # ALIGNMENT COVERAGE: Within curated facts (mcap -> price -> volume alignment)
    # DATE-BY-DATE COVERAGE (not aggregate)
    all_dates = sorted(set(fact_marketcap["date"].unique()) | set(fact_price["date"].unique()))
    
    date_wise_coverage = []
    for check_date in all_dates:
        mcap_date = fact_marketcap[fact_marketcap["date"] == check_date]
        price_date = fact_price[fact_price["date"] == check_date]
        volume_date = fact_volume[fact_volume["date"] == check_date]
        
        mcap_assets_date = set(mcap_date["asset_id"].unique())
        price_assets_date = set(price_date["asset_id"].unique())
        volume_assets_date = set(volume_date["asset_id"].unique())
        
        mcap_with_price_date = len(mcap_assets_date & price_assets_date)
        mcap_with_volume_date = len(mcap_assets_date & volume_assets_date)
        mcap_with_both_date = len(mcap_assets_date & price_assets_date & volume_assets_date)
        
        total_mcap_date = len(mcap_assets_date)
        
        pct_price = (mcap_with_price_date / total_mcap_date * 100) if total_mcap_date > 0 else 0
        pct_volume = (mcap_with_volume_date / total_mcap_date * 100) if total_mcap_date > 0 else 0
        pct_both = (mcap_with_both_date / total_mcap_date * 100) if total_mcap_date > 0 else 0
        
        date_wise_coverage.append({
            "date": str(check_date),
            "total_mcap_assets": total_mcap_date,
            "mcap_with_price": mcap_with_price_date,
            "mcap_with_volume": mcap_with_volume_date,
            "mcap_with_both": mcap_with_both_date,
            "pct_mcap_with_price": pct_price,
            "pct_mcap_with_volume": pct_volume,
            "pct_mcap_with_both": pct_both,
        })
    
    # Aggregate stats (for backward compatibility)
    mcap_assets = set(fact_marketcap["asset_id"].unique())
    price_assets = set(fact_price["asset_id"].unique())
    volume_assets = set(fact_volume["asset_id"].unique())
    
    mcap_with_price = len(mcap_assets & price_assets)
    mcap_with_volume = len(mcap_assets & volume_assets)
    mcap_with_both = len(mcap_assets & price_assets & volume_assets)
    
    total_mcap = len(mcap_assets)
    
    # MIN/MEDIAN coverage across dates (more meaningful than aggregate)
    pct_price_values = [d["pct_mcap_with_price"] for d in date_wise_coverage if d["total_mcap_assets"] > 0]
    min_coverage = min(pct_price_values) if pct_price_values else 0
    median_coverage = sorted(pct_price_values)[len(pct_price_values) // 2] if pct_price_values else 0
    
    alignment_coverage = {
        "label": "alignment_coverage",
        "description": "Coverage within curated facts (mcap -> price -> volume alignment)",
        "total_mcap_assets": total_mcap,
        "mcap_with_price": mcap_with_price,
        "mcap_with_volume": mcap_with_volume,
        "mcap_with_both": mcap_with_both,
        "pct_mcap_with_price": (mcap_with_price / total_mcap * 100) if total_mcap > 0 else 0,
        "pct_mcap_with_volume": (mcap_with_volume / total_mcap * 100) if total_mcap > 0 else 0,
        "pct_mcap_with_both": (mcap_with_both / total_mcap * 100) if total_mcap > 0 else 0,
        "min_coverage_across_dates": min_coverage,
        "median_coverage_across_dates": median_coverage,
        "date_wise_coverage": date_wise_coverage[:50],  # First 50 dates for report
    }
    
    # RAW PROVIDER FUNNEL COVERAGE: raw -> mapped -> facts (non-tautological)
    raw_funnel_coverage = {}
    if raw_provider_data is not None:
        # Compute funnel: raw -> mapped -> facts
        raw_count = len(raw_provider_data)
        mapped_count = len(map_provider_asset)
        mapped_asset_ids = set(map_provider_asset["asset_id"].unique())
        facts_asset_ids = set(fact_marketcap["asset_id"].unique()) | set(fact_price["asset_id"].unique())
        facts_count = len(mapped_asset_ids & facts_asset_ids)
        
        raw_funnel_coverage = {
            "status": "TESTED",
            "tested": True,
            "raw_provider_assets": raw_count,
            "mapped_assets": mapped_count,
            "assets_in_facts": facts_count,
            "raw_to_mapped_pct": (mapped_count / raw_count * 100) if raw_count > 0 else 0,
            "mapped_to_facts_pct": (facts_count / mapped_count * 100) if mapped_count > 0 else 0,
            "raw_to_facts_pct": (facts_count / raw_count * 100) if raw_count > 0 else 0,
            "note": "Raw provider funnel coverage measured",
        }
    else:
        # No raw provider data - cannot measure funnel coverage
        raw_funnel_coverage = {
            "status": "SKIPPED",
            "tested": False,
            "note": "Raw provider data not supplied; funnel coverage not measured",
            "warning": "Raw provider funnel coverage SKIPPED - raw provider data not provided (coverage may be tautological)",
        }
    
    # Coverage on rebalance dates (if universe_eligibility provided)
    rebalance_coverage = {}
    rebalance_coverage_values = []
    if universe_eligibility is not None and len(universe_eligibility) > 0:
        # Use snapshot_date or rebalance_date column
        date_col = "snapshot_date" if "snapshot_date" in universe_eligibility.columns else "rebalance_date"
        rebalance_dates = universe_eligibility[date_col].unique() if date_col in universe_eligibility.columns else []
        
        for rb_date in rebalance_dates[:10]:  # Check first 10 rebalance dates
            # Convert rb_date to same type as date_col values for comparison
            if isinstance(rb_date, pd.Timestamp):
                rb_date_val = rb_date.date()
            elif isinstance(rb_date, str):
                from datetime import date as date_type
                rb_date_val = date_type.fromisoformat(rb_date)
            else:
                rb_date_val = rb_date
            
            eligible_df = universe_eligibility[universe_eligibility[date_col] == rb_date_val]
            
            # Separate candidate universe from backtest-eligible universe
            # Candidate: all assets in universe_eligibility (meets provider filters, may not have data)
            # Backtest-eligible: assets with eligible=true (has required data on date t)
            
            # Map symbols to asset_ids if needed
            if "asset_id" in eligible_df.columns:
                candidate_assets = set(eligible_df["asset_id"].unique())
                # Use eligible flag if available, otherwise check has_price
                if "eligible" in eligible_df.columns:
                    backtest_eligible_df = eligible_df[eligible_df["eligible"] == True]
                    backtest_eligible_assets = set(backtest_eligible_df["asset_id"].unique())
                elif "has_price" in eligible_df.columns:
                    backtest_eligible_df = eligible_df[eligible_df["has_price"] == True]
                    backtest_eligible_assets = set(backtest_eligible_df["asset_id"].unique())
                else:
                    # Fallback: all candidates
                    backtest_eligible_assets = candidate_assets
            elif "symbol" in eligible_df.columns:
                # Map symbols to asset_ids via dim_asset
                symbol_to_assets = dim_asset.groupby("symbol")["asset_id"].apply(set).to_dict()
                candidate_symbols = set(eligible_df["symbol"].unique())
                candidate_assets = set()
                for sym in candidate_symbols:
                    if sym in symbol_to_assets:
                        candidate_assets.update(symbol_to_assets[sym])
                
                # Get backtest-eligible assets
                if "eligible" in eligible_df.columns:
                    eligible_symbols = set(eligible_df[eligible_df["eligible"] == True]["symbol"].unique())
                elif "has_price" in eligible_df.columns:
                    eligible_symbols = set(eligible_df[eligible_df["has_price"] == True]["symbol"].unique())
                else:
                    eligible_symbols = candidate_symbols
                
                backtest_eligible_assets = set()
                for sym in eligible_symbols:
                    if sym in symbol_to_assets:
                        backtest_eligible_assets.update(symbol_to_assets[sym])
            else:
                continue
            
            if len(candidate_assets) == 0:
                continue
            
            # Use rb_date_val for fact_price lookup (convert to same type as fact_price["date"])
            price_rb = fact_price[fact_price["date"] == rb_date_val]
            price_assets_rb = set(price_rb["asset_id"].unique())
            
            # Coverage metrics
            candidate_with_price = len(candidate_assets & price_assets_rb)
            backtest_eligible_with_price = len(backtest_eligible_assets & price_assets_rb)
            
            # Primary metric: backtest-eligible coverage (should be ~100% if require_price=true)
            pct_backtest_coverage = (backtest_eligible_with_price / len(backtest_eligible_assets) * 100) if len(backtest_eligible_assets) > 0 else 0
            # Secondary metric: candidate universe coverage (shows data availability gap)
            pct_candidate_coverage = (candidate_with_price / len(candidate_assets) * 100) if len(candidate_assets) > 0 else 0
            
            rebalance_coverage_values.append(pct_backtest_coverage)
            
            rebalance_coverage[str(rb_date)] = {
                "candidate_assets": len(candidate_assets),
                "candidate_with_price": candidate_with_price,
                "pct_candidate_coverage": pct_candidate_coverage,
                "backtest_eligible_assets": len(backtest_eligible_assets),
                "backtest_eligible_with_price": backtest_eligible_with_price,
                "pct_coverage": pct_backtest_coverage,  # Primary metric (backtest-eligible)
            }
    
    # Check rebalance coverage threshold
    min_rebalance_coverage = min(rebalance_coverage_values) if rebalance_coverage_values else None
    rebalance_coverage_status = {}
    if min_rebalance_coverage is not None:
        rebalance_coverage_status = {
            "min_coverage_pct": min_rebalance_coverage,
            "threshold_pct": min_rebalance_coverage_pct,
            "passed": min_rebalance_coverage >= min_rebalance_coverage_pct,
            "dates_checked": len(rebalance_coverage_values),
            }
    
    # Basket coverage
    basket_coverage = {}
    if basket_snapshots is not None and len(basket_snapshots) > 0:
        basket_dates = basket_snapshots["snapshot_date"].unique() if "snapshot_date" in basket_snapshots.columns else []
        
        for bs_date in basket_dates[:10]:  # Check first 10 basket dates
            basket = basket_snapshots[basket_snapshots["snapshot_date"] == bs_date]
            
            # Handle both asset_id and symbol columns
            if "asset_id" in basket.columns:
                basket_assets = set(basket["asset_id"].unique())
            elif "symbol" in basket.columns:
                # Can't directly compare symbols to asset_ids without mapping
                continue
            else:
                continue
            
            price_bs = fact_price[fact_price["date"] == bs_date]
            price_assets_bs = set(price_bs["asset_id"].unique())
            
            basket_with_price = len(basket_assets & price_assets_bs)
            
            basket_coverage[str(bs_date)] = {
                "basket_assets": len(basket_assets),
                "basket_with_price": basket_with_price,
                "pct_coverage": (basket_with_price / len(basket_assets) * 100) if len(basket_assets) > 0 else 0,
            }
    
    # Pass threshold on MIN alignment coverage (not aggregate)
    alignment_passed = min_coverage >= 85.0
    
    # Overall passed: alignment must pass
    # Rebalance coverage failure only causes overall failure if strict mode is enabled
    passed = alignment_passed
    if strict_rebalance_coverage and rebalance_coverage_status and not rebalance_coverage_status.get("passed", True):
        passed = False
    
    # Collect warnings
    warnings = []
    if raw_funnel_coverage.get("status") == "SKIPPED":
        warnings.append(raw_funnel_coverage.get("warning", "Raw provider funnel coverage not measured"))
    if rebalance_coverage_status and not rebalance_coverage_status.get("passed", True):
        warnings.append(
            f"Rebalance coverage below threshold: {min_rebalance_coverage:.1f}% < {min_rebalance_coverage_pct:.1f}% "
            f"(checked {rebalance_coverage_status['dates_checked']} dates)"
        )
    
    return {
        "passed": passed,
        "alignment_coverage": alignment_coverage,
        "raw_provider_funnel_coverage": raw_funnel_coverage,
        "rebalance_coverage": rebalance_coverage,
        "rebalance_coverage_status": rebalance_coverage_status,
        "basket_coverage": basket_coverage,
        "warnings": warnings,
        "threshold_used": "min_alignment_coverage_across_dates >= 85.0%",
        # Backward compatibility
        "coverage_stats": alignment_coverage,
        "provider_universe_check": raw_funnel_coverage,
    }


def check_5_time_valid_joins(
    map_provider_asset: pd.DataFrame,
    fact_price: pd.DataFrame,
    sample_dates: Optional[List[date]] = None,
) -> Dict:
    """
    Check 5: Time-valid joins (if valid_from/valid_to)
    
    Verify that for a historical date t, the join uses mapping row where:
    valid_from <= t < valid_to (or valid_to is NULL)
    """
    if "valid_from" not in map_provider_asset.columns:
        return {
            "passed": True,
            "violations": [],
            "note": "No valid_from/valid_to columns in mapping table",
        }
    
    violations = []
    
    # Sample dates to check
    if sample_dates is None:
        sample_dates = fact_price["date"].unique()[:20]  # First 20 dates
    
    for check_date in sample_dates:
        # Get mappings valid on this date
        valid_mappings = map_provider_asset[
            (map_provider_asset["valid_from"] <= check_date) &
            (
                map_provider_asset["valid_to"].isna() |
                (map_provider_asset["valid_to"] > check_date)
            )
        ]
        
        # Check for overlapping mappings (same provider + provider_id, multiple asset_ids)
        for (provider, provider_id), group in valid_mappings.groupby(["provider", "provider_asset_id"]):
            if group["asset_id"].nunique() > 1:
                violations.append({
                    "type": "overlapping_time_valid_mappings",
                    "date": str(check_date),
                    "provider": provider,
                    "provider_asset_id": provider_id,
                    "n_asset_ids": group["asset_id"].nunique(),
                    "asset_ids": group["asset_id"].unique().tolist(),
                })
    
    return {
        "passed": len(violations) == 0,
        "violations": violations,
        "violation_count": len(violations),
        "dates_checked": len(sample_dates),
    }


def check_6_symbol_collision_audit(
    dim_asset: pd.DataFrame,
    map_provider_asset: pd.DataFrame,
) -> Dict:
    """
    Check 6: Symbol collision audit (crypto-specific)
    
    Produce a list of "danger tickers":
    - Same symbol appears for multiple asset_ids
    - Requires manual override or contract-address-based disambiguation
    """
    collisions = []
    
    # Find symbols that map to multiple asset_ids
    symbol_counts = dim_asset.groupby("symbol")["asset_id"].nunique()
    multi_asset_symbols = symbol_counts[symbol_counts > 1]
    
    for symbol, count in multi_asset_symbols.items():
        asset_rows = dim_asset[dim_asset["symbol"] == symbol]
        asset_ids = asset_rows["asset_id"].tolist()
        
        # Check if they have contract addresses (can disambiguate)
        has_contracts = asset_rows["contract_address"].notna().any() if "contract_address" in asset_rows.columns else False
        chains = asset_rows["chain"].unique().tolist() if "chain" in asset_rows.columns else []
        
        collisions.append({
            "symbol": symbol,
            "n_asset_ids": int(count),
            "asset_ids": asset_ids,
            "has_contract_addresses": bool(has_contracts),
            "chains": chains,
            "requires_manual_override": not has_contracts or len(chains) <= 1,
        })
    
    # Find provider IDs that map to multiple symbols (also dangerous)
    provider_collisions = []
    for (provider, provider_id), group in map_provider_asset.groupby(["provider", "provider_asset_id"]):
        # Get unique asset_ids for this provider ID
        asset_ids = group["asset_id"].unique()
        if len(asset_ids) > 1:
            # Get symbols for these asset_ids
            symbols = dim_asset[dim_asset["asset_id"].isin(asset_ids)]["symbol"].unique().tolist()
            provider_collisions.append({
                "provider": provider,
                "provider_asset_id": provider_id,
                "n_asset_ids": len(asset_ids),
                "asset_ids": asset_ids,
                "symbols": symbols,
            })
    
    return {
        "symbol_collisions": collisions,
        "provider_collisions": provider_collisions,
        "danger_ticker_count": len([c for c in collisions if c["requires_manual_override"]]),
    }


def check_7_spot_checks(
    dim_asset: pd.DataFrame,
    map_provider_asset: pd.DataFrame,
    fact_price: pd.DataFrame,
    anchor_symbols: Optional[List[str]] = None,
    symbol_aliases: Optional[Dict[str, str]] = None,
) -> Dict:
    """
    Check 7: Spot checks (human verification)
    
    Pick 20 "anchor" assets and verify:
    - canonical asset_id
    - provider IDs (CoinGecko id, Binance base, etc.)
    - sample price points (3 dates)
    
    Supports symbol aliases (e.g., MATIC -> POL) and coingecko_id fallback lookup.
    """
    if anchor_symbols is None:
        anchor_symbols = [
            "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "DOT", 
            "LINK", "UNI", "ATOM", "MATIC", "LTC", "BCH", "ETC",
            "ALGO", "FIL", "ICP", "NEAR", "APT",
        ]
    
    # Default symbol aliases (e.g., for renames)
    if symbol_aliases is None:
        symbol_aliases = {
            "MATIC": "POL",  # Polygon rebranded MATIC to POL
        }
    
    spot_checks = []
    failures = []
    
    for symbol in anchor_symbols:
        # Try direct symbol lookup
        asset_rows = dim_asset[dim_asset["symbol"] == symbol]
        alias_used = None
        
        # If not found, try alias
        if len(asset_rows) == 0 and symbol in symbol_aliases:
            alias_symbol = symbol_aliases[symbol]
            asset_rows = dim_asset[dim_asset["symbol"] == alias_symbol]
            if len(asset_rows) > 0:
                alias_used = alias_symbol
        
        # If still not found, try coingecko_id lookup (if available)
        if len(asset_rows) == 0 and "coingecko_id" in dim_asset.columns:
            # Try to find via map_provider_asset using coingecko provider
            coingecko_mappings = map_provider_asset[map_provider_asset["provider"] == "coingecko"]
            if len(coingecko_mappings) > 0:
                # Look for symbol in provider_asset_id (coingecko uses lowercase symbol-like IDs)
                symbol_lower = symbol.lower()
                matching_mappings = coingecko_mappings[
                    coingecko_mappings["provider_asset_id"].str.lower() == symbol_lower
                ]
                if len(matching_mappings) > 0:
                    asset_id_from_coingecko = matching_mappings.iloc[0]["asset_id"]
                    asset_rows = dim_asset[dim_asset["asset_id"] == asset_id_from_coingecko]
                    if len(asset_rows) > 0:
                        alias_used = f"coingecko_id:{symbol_lower}"
        
        if len(asset_rows) == 0:
            failure_reason = f"Symbol '{symbol}' not found in dim_asset"
            if symbol in symbol_aliases:
                failure_reason += f" (tried alias '{symbol_aliases[symbol]}' but also not found)"
            failures.append({"symbol": symbol, "reason": failure_reason, "alias_tried": alias_used})
            spot_checks.append({
                "symbol": symbol,
                "status": "NOT_FOUND_IN_DIM_ASSET",
                "asset_id": None,
                "provider_ids": {},
                "price_samples": [],
                "failure_reason": failure_reason,
                "alias_tried": alias_used,
            })
            continue
        
        asset_row = asset_rows.iloc[0]
        asset_id = asset_row["asset_id"]
        
        # Get provider IDs
        provider_mappings = map_provider_asset[map_provider_asset["asset_id"] == asset_id]
        provider_ids = {}
        for _, map_row in provider_mappings.iterrows():
            provider = map_row["provider"]
            provider_id = map_row["provider_asset_id"]
            if provider not in provider_ids:
                provider_ids[provider] = []
            provider_ids[provider].append(provider_id)
        
        # Get sample price points (3 dates)
        price_rows = fact_price[fact_price["asset_id"] == asset_id].sort_values("date")
        price_samples = []
        if len(price_rows) > 0:
            # First, middle, last
            sample_indices = [0, len(price_rows) // 2, len(price_rows) - 1]
            for idx in sample_indices:
                if idx < len(price_rows):
                    price_row = price_rows.iloc[idx]
                    price_samples.append({
                        "date": str(price_row["date"]),
                        "close": float(price_row.get("close", 0)),
                    })
        
        has_price_data = len(price_samples) > 0
        failure_reason = None
        if not has_price_data:
            failure_reason = f"Asset '{asset_id}' found but no price data in fact_price"
            failures.append({"symbol": symbol, "asset_id": asset_id, "reason": failure_reason})
        
        spot_checks.append({
            "symbol": symbol,
            "status": "FOUND" if has_price_data else "FOUND_NO_PRICE_DATA",
            "asset_id": asset_id,
            "provider_ids": provider_ids,
            "price_samples": price_samples,
            "has_price_data": has_price_data,
            "failure_reason": failure_reason,
            "alias_used": alias_used,
        })
    
    return {
        "anchor_symbols": anchor_symbols,
        "spot_checks": spot_checks,
        "found_count": sum(1 for sc in spot_checks if sc["status"] == "FOUND"),
        "with_price_data": sum(1 for sc in spot_checks if sc.get("has_price_data", False)),
        "failures": failures,
        "failure_count": len(failures),
        "symbol_aliases_used": symbol_aliases,
    }


def check_8_run_level_mapping_report(
    dim_asset: pd.DataFrame,
    map_provider_asset: pd.DataFrame,
    fact_marketcap: pd.DataFrame,
    fact_price: pd.DataFrame,
    snapshot_date: Optional[date] = None,
) -> Dict:
    """
    Check 8: Run-level mapping report (must exist)
    
    Every run must output:
    - mapping coverage %
    - list of unmapped provider IDs
    - list of conflicts (1→many)
    - list of suspected duplicates ("same contract address in multiple asset_ids")
    """
    # Filter to snapshot date if provided
    if snapshot_date:
        mcap = fact_marketcap[fact_marketcap["date"] == snapshot_date].copy()
        price = fact_price[fact_price["date"] == snapshot_date].copy()
    else:
        mcap = fact_marketcap.copy()
        price = fact_price.copy()
    
    # Coverage
    mcap_assets = set(mcap["asset_id"].unique())
    price_assets = set(price["asset_id"].unique())
    unmapped_mcap = mcap_assets - price_assets
    
    coverage_pct = (len(price_assets & mcap_assets) / len(mcap_assets) * 100) if len(mcap_assets) > 0 else 0
    
    # Unmapped provider IDs (assets in mcap but no mapping)
    unmapped_assets = dim_asset[dim_asset["asset_id"].isin(unmapped_mcap)]
    unmapped_provider_ids = []
    for _, asset_row in unmapped_assets.head(50).iterrows():  # Top 50
        mappings = map_provider_asset[map_provider_asset["asset_id"] == asset_row["asset_id"]]
        if len(mappings) == 0:
            unmapped_provider_ids.append({
                "asset_id": asset_row["asset_id"],
                "symbol": asset_row.get("symbol", "UNKNOWN"),
            })
    
    # Conflicts (1→many)
    conflicts = []
    for (provider, provider_id), group in map_provider_asset.groupby(["provider", "provider_asset_id"]):
        unique_asset_ids = group["asset_id"].nunique()
        if unique_asset_ids > 1:
            conflicts.append({
                "provider": provider,
                "provider_asset_id": provider_id,
                "n_asset_ids": unique_asset_ids,
                "asset_ids": group["asset_id"].unique().tolist(),
            })
    
    # Suspected duplicates (same contract address)
    suspected_duplicates = []
    if "chain" in dim_asset.columns and "contract_address" in dim_asset.columns:
        contract_assets = dim_asset[
            dim_asset["contract_address"].notna() & 
            (dim_asset["contract_address"] != "")
        ]
        
        for (chain, contract), group in contract_assets.groupby(["chain", "contract_address"]):
            if len(group) > 1:
                suspected_duplicates.append({
                    "chain": chain,
                    "contract_address": contract,
                    "n_asset_ids": len(group),
                    "asset_ids": group["asset_id"].tolist(),
                    "symbols": group["symbol"].tolist(),
                })
    
    return {
        "mapping_coverage_pct": coverage_pct,
        "unmapped_provider_ids": unmapped_provider_ids[:50],  # Top 50
        "unmapped_count": len(unmapped_mcap),
        "conflicts": conflicts[:50],  # Top 50
        "conflict_count": len(conflicts),
        "suspected_duplicates": suspected_duplicates[:50],  # Top 50
        "duplicate_count": len(suspected_duplicates),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive canonical ID validation (8 checks)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--data-lake-dir",
        type=Path,
        default=Path("data/curated/data_lake"),
        help="Data lake directory",
    )
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default=None,
        help="Specific date to validate (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--previous-mapping",
        type=Path,
        default=None,
        help="Previous mapping file for stability check",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON file path",
    )
    parser.add_argument(
        "--fail-on-errors",
        action="store_true",
        help="Exit with non-zero code if validation fails",
    )
    parser.add_argument(
        "--require-stability-check",
        action="store_true",
        help="Require --previous-mapping for stability check (fail if not provided)",
    )
    parser.add_argument(
        "--test-dates",
        type=str,
        nargs="+",
        default=None,
        help="Specific dates to test (YYYY-MM-DD), e.g., --test-dates 2024-01-01 2024-06-01 2025-12-22",
    )
    parser.add_argument(
        "--negative-test",
        action="store_true",
        help="Run negative test: intentionally create bad mapping and verify it fails",
    )
    parser.add_argument(
        "--write-baseline",
        type=Path,
        default=None,
        help="Export current map_provider_asset to baseline file for future stability checks",
    )
    parser.add_argument(
        "--min-rebalance-coverage-pct",
        type=float,
        default=80.0,
        help="Minimum rebalance coverage percentage threshold (default: 80.0)",
    )
    parser.add_argument(
        "--raw-provider-data",
        type=Path,
        default=None,
        help="Path to raw provider data file (parquet) for funnel coverage measurement",
    )
    parser.add_argument(
        "--strict-rebalance-coverage",
        action="store_true",
        help="Fail validation if rebalance coverage is below threshold (default: warning only)",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    data_lake_dir = (repo_root / args.data_lake_dir).resolve()
    
    if not data_lake_dir.exists():
        print(f"[ERROR] Data lake directory not found: {data_lake_dir}")
        sys.exit(1)
    
    # Parse snapshot date
    snapshot_date = None
    if args.snapshot_date:
        snapshot_date = date.fromisoformat(args.snapshot_date)
    
    print("=" * 80)
    print("CANONICAL ID VALIDATION (8 CHECKS)")
    print("=" * 80)
    print(f"\nData lake directory: {data_lake_dir}")
    if snapshot_date:
        print(f"Snapshot date: {snapshot_date}")
    print()
    
    # Load tables
    print("[Loading] Reading data lake tables...")
    dim_asset = pd.read_parquet(data_lake_dir / "dim_asset.parquet")
    map_provider_asset = pd.read_parquet(data_lake_dir / "map_provider_asset.parquet")
    
    map_provider_instrument = None
    if (data_lake_dir / "map_provider_instrument.parquet").exists():
        map_provider_instrument = pd.read_parquet(data_lake_dir / "map_provider_instrument.parquet")
    
    dim_instrument = None
    if (data_lake_dir / "dim_instrument.parquet").exists():
        dim_instrument = pd.read_parquet(data_lake_dir / "dim_instrument.parquet")
    
    fact_marketcap = pd.read_parquet(data_lake_dir / "fact_marketcap.parquet")
    fact_price = pd.read_parquet(data_lake_dir / "fact_price.parquet")
    fact_volume = pd.read_parquet(data_lake_dir / "fact_volume.parquet")
    
    universe_eligibility = None
    if (data_lake_dir.parent / "universe_eligibility.parquet").exists():
        universe_eligibility = pd.read_parquet(data_lake_dir.parent / "universe_eligibility.parquet")
    
    basket_snapshots = None
    if (data_lake_dir.parent / "universe_snapshots.parquet").exists():
        basket_snapshots = pd.read_parquet(data_lake_dir.parent / "universe_snapshots.parquet")
    
    # Load raw provider data if provided
    raw_provider_data = None
    if args.raw_provider_data:
        raw_provider_data = pd.read_parquet(args.raw_provider_data)
        print(f"[Raw Data] Loaded raw provider data from: {args.raw_provider_data}")
    
    # Write baseline if requested
    if args.write_baseline:
        baseline_path = (repo_root / args.write_baseline).resolve()
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        map_provider_asset.to_parquet(baseline_path, index=False)
        print(f"[Baseline] Exported current mapping to: {baseline_path}")
        print(f"  Use this file with --previous-mapping in future runs for stability checks")
    
    # Previous mapping for stability check
    map_provider_asset_previous = None
    if args.previous_mapping:
        map_provider_asset_previous = pd.read_parquet(args.previous_mapping)
        print(f"[Baseline] Loaded previous mapping from: {args.previous_mapping}")
    
    # Negative test: Create bad mapping and verify it fails
    if args.negative_test:
        print("\n[NEGATIVE TEST] Creating intentionally bad mapping to verify checks work...")
        test_map = map_provider_asset.copy()
        # Create duplicate: same provider ID maps to two different asset_ids
        if len(test_map) > 0:
            first_row = test_map.iloc[0].copy()
            first_row["asset_id"] = "DUPLICATE_TEST_ASSET_ID"
            test_map = pd.concat([test_map, first_row.to_frame().T], ignore_index=True)
            
            test_check1 = check_1_uniqueness(test_map, map_provider_instrument, dim_instrument)
            if test_check1["passed"]:
                print("[ERROR] Negative test FAILED - bad mapping should have been caught!")
                sys.exit(1)
            else:
                print(f"[PASS] Negative test passed - caught {test_check1['violation_count']} violations")
        print()
    
    # Run all 8 checks
    print("\n[Check 1] Uniqueness (hard invariant)...")
    check1 = check_1_uniqueness(map_provider_asset, map_provider_instrument, dim_instrument)
    
    print("[Check 2] No duplicate 'same-thing' assets...")
    check2 = check_2_no_duplicate_same_thing_assets(dim_asset)
    
    print("[Check 3] Stability (no accidental remaps)...")
    require_stability = args.require_stability_check
    check3 = check_3_stability(map_provider_asset, map_provider_asset_previous, require_previous=require_stability)
    
    print("[Check 4] Coverage (does it join to real data)...")
    check4 = check_4_coverage(
        fact_marketcap, fact_price, fact_volume, dim_asset, map_provider_asset,
        universe_eligibility, basket_snapshots, snapshot_date,
        raw_provider_data=raw_provider_data,
        min_rebalance_coverage_pct=args.min_rebalance_coverage_pct,
        strict_rebalance_coverage=args.strict_rebalance_coverage,
    )
    
    print("[Check 5] Time-valid joins...")
    # Use test dates if provided, otherwise sample
    test_dates = None
    if args.test_dates:
        test_dates = [date.fromisoformat(d) for d in args.test_dates]
    check5 = check_5_time_valid_joins(map_provider_asset, fact_price, sample_dates=test_dates)
    
    print("[Check 6] Symbol collision audit...")
    check6 = check_6_symbol_collision_audit(dim_asset, map_provider_asset)
    
    print("[Check 7] Spot checks (human verification)...")
    check7 = check_7_spot_checks(dim_asset, map_provider_asset, fact_price)
    
    print("[Check 8] Run-level mapping report...")
    check8 = check_8_run_level_mapping_report(dim_asset, map_provider_asset, fact_marketcap, fact_price, snapshot_date)
    
    # Collect warnings from all checks
    warnings = []
    if check3.get("status") == "SKIPPED":
        warnings.append(check3.get("warning", "Stability check skipped - no baseline provided"))
    if check4.get("warnings"):
        warnings.extend(check4["warnings"])
    if check7.get("failure_count", 0) > 0:
        # Provide detailed failure reasons
        failure_details = []
        for failure in check7.get("failures", []):
            reason = failure.get("reason", "Unknown")
            if failure.get("alias_tried"):
                reason += f" (tried alias: {failure['alias_tried']})"
            failure_details.append(f"{failure.get('symbol', 'UNKNOWN')}: {reason}")
        if failure_details:
            warnings.append(f"Spot checks: {check7['failure_count']} anchor assets failed - {', '.join(failure_details[:3])}")
        else:
        warnings.append(f"Spot checks: {check7['failure_count']} anchor assets failed validation")
    
    # Determine overall status
    # Required checks: 1, 2, 4, 5 (uniqueness, no dupes, coverage, time-valid joins)
    required_checks_passed = (
        check1["passed"] and
        check2["passed"] and
        check4["passed"] and
        check5["passed"]
    )
    
    # Stability check status
    stability_status = check3.get("status", "UNKNOWN")
    stability_tested = check3.get("tested", False)
    stability_passed = check3.get("passed", False) if stability_tested else None
    
    # Overall status logic
    # If required checks fail, overall is FAIL
    if not required_checks_passed:
        overall_status = "FAIL"
    # If stability was required but missing baseline, it's FAIL (not PASS_WITH_WARNINGS)
    elif stability_status == "FAIL" and not stability_tested:
        overall_status = "FAIL"
    # If stability was tested but failed, overall is FAIL
    elif stability_tested and not stability_passed:
        overall_status = "FAIL"
    # If stability was skipped (not required), downgrade to PASS_WITH_WARNINGS
    elif stability_status == "SKIPPED":
        overall_status = "PASS_WITH_WARNINGS"
    # If there are other warnings (e.g., spot check failures), downgrade to PASS_WITH_WARNINGS
    elif len(warnings) > 0:
        overall_status = "PASS_WITH_WARNINGS"
    # Everything required passed and tested
    else:
        overall_status = "PASS"
    
    # Compile report
    report = {
        "validation_timestamp": datetime.now().isoformat(),
        "snapshot_date": str(snapshot_date) if snapshot_date else None,
        "check_1_uniqueness": check1,
        "check_2_no_duplicates": check2,
        "check_3_stability": check3,
        "check_4_coverage": check4,
        "check_5_time_valid_joins": check5,
        "check_6_symbol_collisions": check6,
        "check_7_spot_checks": check7,
        "check_8_mapping_report": check8,
        "overall_status": overall_status,
        "warnings": warnings,
        "overall_passed": (overall_status == "PASS"),  # For backwards compatibility
    }
    
    # Write report
    if args.output:
        output_path = (repo_root / args.output).resolve()
    else:
        output_path = data_lake_dir / "canonical_id_validation.json"
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n[Report] Written to: {output_path}")
    
    # Print summary
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    
    print(f"\n[Check 1] Uniqueness: {'PASS' if check1['passed'] else 'FAIL'} ({check1['violation_count']} violations)")
    print(f"[Check 2] No Duplicates: {'PASS' if check2['passed'] else 'FAIL'} ({check2['violation_count']} violations)")
    
    # Stability check output
    stability_status = check3.get("status", "UNKNOWN")
    if stability_status == "SKIPPED":
        print(f"[Check 3] Stability: SKIPPED (no baseline)")
    elif stability_status == "FAIL":
        print(f"[Check 3] Stability: FAIL ({check3['violation_count']} violations)")
    else:
        print(f"[Check 3] Stability: PASS ({check3['violation_count']} violations)")
    
    # Coverage output - show both alignment and raw funnel
    alignment_cov = check4.get('alignment_coverage', check4.get('coverage_stats', {}))
    alignment_pct = alignment_cov.get('pct_mcap_with_price', 0)
    min_coverage = alignment_cov.get('min_coverage_across_dates', alignment_pct)
    
    print(f"[Check 4] Alignment Coverage: {'PASS' if check4['passed'] else 'FAIL'} (aggregate: {alignment_pct:.1f}%, min across dates: {min_coverage:.1f}%)")
    
    # Raw funnel coverage
    raw_funnel = check4.get('raw_provider_funnel_coverage', {})
    if raw_funnel.get('status') == 'SKIPPED':
        print(f"[Check 4] Raw Funnel Coverage: SKIPPED (raw provider data not provided)")
    elif raw_funnel.get('status') == 'TESTED':
        raw_to_facts = raw_funnel.get('raw_to_facts_pct', 0)
        print(f"[Check 4] Raw Funnel Coverage: raw={raw_funnel.get('raw_provider_assets', 0)} -> mapped={raw_funnel.get('mapped_assets', 0)} -> facts={raw_funnel.get('assets_in_facts', 0)} ({raw_to_facts:.1f}% raw->facts)")
    
    # Rebalance coverage status
    rebalance_status = check4.get('rebalance_coverage_status', {})
    if rebalance_status:
        min_rb = rebalance_status.get('min_coverage_pct', 0)
        threshold = rebalance_status.get('threshold_pct', 80.0)
        rb_passed = rebalance_status.get('passed', True)
        status_label = 'PASS' if rb_passed else 'WARN' if not args.strict_rebalance_coverage else 'FAIL'
        dates_checked = rebalance_status.get('dates_checked', 0)
        print(f"[Check 4] Rebalance Coverage (backtest-eligible): {status_label} (min: {min_rb:.1f}%, threshold: {threshold:.1f}%, {dates_checked} dates checked)")
        
        # Show candidate vs eligible breakdown for first date
        rebalance_cov = check4.get('rebalance_coverage', {})
        if rebalance_cov:
            first_date = list(rebalance_cov.keys())[0]
            first_date_data = rebalance_cov[first_date]
            candidate_count = first_date_data.get('candidate_assets', 0)
            eligible_count = first_date_data.get('backtest_eligible_assets', 0)
            candidate_pct = first_date_data.get('pct_candidate_coverage', 0)
            print(f"[Check 4]   Sample ({first_date}): candidate={candidate_count}, backtest-eligible={eligible_count}, candidate->price={candidate_pct:.1f}%")
    print(f"[Check 5] Time-valid Joins: {'PASS' if check5['passed'] else 'FAIL'} ({check5['violation_count']} violations, {check5.get('dates_checked', 0)} dates checked)")
    print(f"[Check 6] Symbol Collisions: {len(check6['symbol_collisions'])} collisions, {check6['danger_ticker_count']} dangerous")
    
    spot_status = f"{check7['found_count']}/{len(check7['anchor_symbols'])} found, {check7['with_price_data']} with price data"
    if check7.get('failure_count', 0) > 0:
        spot_status += f" ({check7['failure_count']} failures)"
    print(f"[Check 7] Spot Checks: {spot_status}")
    if check7.get('failures'):
        for failure in check7['failures']:
            print(f"  - {failure['symbol']}: {failure['reason']}")
    
    print(f"[Check 8] Mapping Report: {check8['mapping_coverage_pct']:.1f}% coverage, {check8['conflict_count']} conflicts, {check8['duplicate_count']} duplicates")
    
    # Print warnings if any
    if warnings:
        print(f"\n[Warnings]")
        for warning in warnings:
            print(f"  - {warning}")
    
    # Print overall status
    overall_status = report["overall_status"]
    print(f"\n[Overall] {overall_status}")
    
    if overall_status == "FAIL" and args.fail_on_errors:
        print("\n[ERROR] Validation failed. Exiting with error code.")
        sys.exit(1)
    elif overall_status == "PASS_WITH_WARNINGS":
        print("\n[INFO] Validation passed but with warnings (e.g., stability check skipped)")
    
    print("\n" + "=" * 80)
    print("VALIDATION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
