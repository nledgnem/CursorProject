"""Mapping validation: coverage, uniqueness, join sanity, and conflict reporting."""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import date
import duckdb


def check_mapping_coverage(
    fact_marketcap: pd.DataFrame,
    fact_price: pd.DataFrame,
    fact_volume: pd.DataFrame,
    snapshot_date: Optional[date] = None,
) -> Dict:
    """
    Check mapping coverage: how much of universe actually maps.
    
    Returns coverage statistics.
    """
    # Filter to snapshot date if provided
    if snapshot_date:
        mcap = fact_marketcap[fact_marketcap["date"] == snapshot_date].copy()
        price = fact_price[fact_price["date"] == snapshot_date].copy()
        volume = fact_volume[fact_volume["date"] == snapshot_date].copy()
    else:
        mcap = fact_marketcap.copy()
        price = fact_price.copy()
        volume = fact_volume.copy()
    
    # Get unique asset_ids
    mcap_assets = set(mcap["asset_id"].unique())
    price_assets = set(price["asset_id"].unique())
    volume_assets = set(volume["asset_id"].unique())
    
    # Coverage calculations
    mcap_with_price = len(mcap_assets & price_assets)
    mcap_with_volume = len(mcap_assets & volume_assets)
    all_three = len(mcap_assets & price_assets & volume_assets)
    
    total_mcap = len(mcap_assets)
    
    coverage = {
        "snapshot_date": str(snapshot_date) if snapshot_date else "all_dates",
        "total_mcap_assets": total_mcap,
        "mcap_with_price": mcap_with_price,
        "mcap_with_volume": mcap_with_volume,
        "mcap_with_both": all_three,
        "pct_mcap_with_price": (mcap_with_price / total_mcap * 100) if total_mcap > 0 else 0,
        "pct_mcap_with_volume": (mcap_with_volume / total_mcap * 100) if total_mcap > 0 else 0,
        "pct_mcap_with_both": (all_three / total_mcap * 100) if total_mcap > 0 else 0,
    }
    
    return coverage


def check_mapping_uniqueness(
    map_provider_asset: pd.DataFrame,
    map_provider_instrument: Optional[pd.DataFrame] = None,
) -> Dict:
    """
    Check mapping uniqueness: no many-to-many surprises.
    
    Returns uniqueness violations.
    """
    violations = {
        "provider_asset_duplicates": [],
        "provider_instrument_duplicates": [],
    }
    
    # Check provider_asset: each provider_asset_id should map to exactly one asset_id
    # (within a valid date range)
    for (provider, provider_id), group in map_provider_asset.groupby(["provider", "provider_asset_id"]):
        unique_asset_ids = group["asset_id"].nunique()
        if unique_asset_ids > 1:
            violations["provider_asset_duplicates"].append({
                "provider": provider,
                "provider_asset_id": provider_id,
                "n_asset_ids": unique_asset_ids,
                "asset_ids": group["asset_id"].unique().tolist(),
            })
    
    # Check provider_instrument if provided
    if map_provider_instrument is not None and len(map_provider_instrument) > 0:
        for (provider, provider_id), group in map_provider_instrument.groupby(["provider", "provider_instrument_id"]):
            unique_instrument_ids = group["instrument_id"].nunique()
            if unique_instrument_ids > 1:
                violations["provider_instrument_duplicates"].append({
                    "provider": provider,
                    "provider_instrument_id": provider_id,
                    "n_instrument_ids": unique_instrument_ids,
                    "instrument_ids": group["instrument_id"].unique().tolist(),
                })
    
    return violations


def check_join_sanity(
    dim_asset: pd.DataFrame,
    fact_marketcap: pd.DataFrame,
    fact_price: pd.DataFrame,
    fact_volume: pd.DataFrame,
    sample_size: int = 20,
) -> Dict:
    """
    Check join sanity: do mapped assets actually join to real data?
    
    Picks random sample and verifies joins work.
    """
    # Get random sample of assets
    sample_assets = dim_asset["asset_id"].sample(min(sample_size, len(dim_asset))).tolist()
    
    join_results = []
    for asset_id in sample_assets:
        # Check if asset has data in each fact table
        has_mcap = asset_id in fact_marketcap["asset_id"].values
        has_price = asset_id in fact_price["asset_id"].values
        has_volume = asset_id in fact_volume["asset_id"].values
        
        # Get symbol for readability
        symbol = dim_asset[dim_asset["asset_id"] == asset_id]["symbol"].iloc[0] if len(dim_asset[dim_asset["asset_id"] == asset_id]) > 0 else "UNKNOWN"
        
        join_results.append({
            "asset_id": asset_id,
            "symbol": symbol,
            "has_marketcap": has_mcap,
            "has_price": has_price,
            "has_volume": has_volume,
            "all_join": has_mcap and has_price and has_volume,
        })
    
    # Summary
    all_join_count = sum(1 for r in join_results if r["all_join"])
    join_summary = {
        "sample_size": len(join_results),
        "all_join_count": all_join_count,
        "all_join_pct": (all_join_count / len(join_results) * 100) if join_results else 0,
        "sample_results": join_results,
    }
    
    return join_summary


def generate_conflict_report(
    fact_marketcap: pd.DataFrame,
    fact_price: pd.DataFrame,
    dim_asset: pd.DataFrame,
    map_provider_asset: pd.DataFrame,
    snapshot_date: Optional[date] = None,
    top_n: int = 50,
) -> Dict:
    """
    Generate conflict report: what didn't map and why.
    
    Returns:
        - unmapped assets (in mcap but no price)
        - suspected duplicates
        - top missing examples
    """
    # Filter to snapshot date if provided
    if snapshot_date:
        mcap = fact_marketcap[fact_marketcap["date"] == snapshot_date].copy()
        price = fact_price[fact_price["date"] == snapshot_date].copy()
    else:
        mcap = fact_marketcap.copy()
        price = fact_price.copy()
    
    # Find assets in mcap but missing price
    mcap_assets = set(mcap["asset_id"].unique())
    price_assets = set(price["asset_id"].unique())
    missing_price_assets = mcap_assets - price_assets
    
    # Get top missing by marketcap
    missing_mcap = mcap[mcap["asset_id"].isin(missing_price_assets)]
    if len(missing_mcap) > 0:
        missing_with_symbol = missing_mcap.merge(
            dim_asset[["asset_id", "symbol"]],
            on="asset_id",
            how="left"
        )
        missing_sorted = missing_with_symbol.sort_values("marketcap", ascending=False)
        top_missing = missing_sorted.head(top_n)
        
        top_missing_list = [
            {
                "asset_id": row["asset_id"],
                "symbol": row.get("symbol", "UNKNOWN"),
                "marketcap": float(row["marketcap"]),
                "date": str(row["date"]),
            }
            for _, row in top_missing.iterrows()
        ]
    else:
        top_missing_list = []
    
    # Check for suspected duplicates (same symbol -> multiple asset_ids)
    symbol_counts = dim_asset.groupby("symbol")["asset_id"].nunique()
    duplicates = symbol_counts[symbol_counts > 1].to_dict()
    
    duplicate_list = []
    for symbol, count in duplicates.items():
        asset_ids = dim_asset[dim_asset["symbol"] == symbol]["asset_id"].tolist()
        duplicate_list.append({
            "symbol": symbol,
            "n_asset_ids": int(count),
            "asset_ids": asset_ids,
        })
    
    # Unmapped provider IDs (if we have provider data)
    # This would require knowing which provider IDs should exist
    # For now, skip this check
    
    report = {
        "snapshot_date": str(snapshot_date) if snapshot_date else "all_dates",
        "missing_price_count": len(missing_price_assets),
        "top_missing_price": top_missing_list,
        "suspected_duplicates": duplicate_list,
        "duplicate_count": len(duplicate_list),
    }
    
    return report


def validate_mapping_guardrails(
    coverage: Dict,
    uniqueness: Dict,
    min_coverage_pct: float = 85.0,
) -> Tuple[bool, List[str]]:
    """
    Validate mapping guardrails - fail if critical issues.
    
    Returns:
        (is_valid, list_of_errors)
    """
    errors = []
    
    # Guardrail 1: Coverage must be above threshold
    pct_with_price = coverage.get("pct_mcap_with_price", 0)
    if pct_with_price < min_coverage_pct:
        errors.append(
            f"Mapping coverage too low: {pct_with_price:.1f}% of marketcap assets have price "
            f"(minimum required: {min_coverage_pct}%)"
        )
    
    # Guardrail 2: No provider_asset duplicates
    if len(uniqueness["provider_asset_duplicates"]) > 0:
        errors.append(
            f"Found {len(uniqueness['provider_asset_duplicates'])} provider_asset_id(s) mapping to multiple asset_ids. "
            f"This violates uniqueness constraint."
        )
    
    # Guardrail 3: No provider_instrument duplicates
    if len(uniqueness["provider_instrument_duplicates"]) > 0:
        errors.append(
            f"Found {len(uniqueness['provider_instrument_duplicates'])} provider_instrument_id(s) mapping to multiple instrument_ids. "
            f"This violates uniqueness constraint."
        )
    
    return len(errors) == 0, errors


def run_full_mapping_validation(
    data_lake_dir: Path,
    snapshot_date: Optional[date] = None,
    min_coverage_pct: float = 85.0,
    output_path: Optional[Path] = None,
) -> Dict:
    """
    Run full mapping validation suite.
    
    Returns comprehensive validation report.
    """
    # Load fact tables
    fact_marketcap = pd.read_parquet(data_lake_dir / "fact_marketcap.parquet")
    fact_price = pd.read_parquet(data_lake_dir / "fact_price.parquet")
    fact_volume = pd.read_parquet(data_lake_dir / "fact_volume.parquet")
    
    # Load dimension and mapping tables
    dim_asset = pd.read_parquet(data_lake_dir / "dim_asset.parquet")
    map_provider_asset = pd.read_parquet(data_lake_dir / "map_provider_asset.parquet")
    
    map_provider_instrument = None
    if (data_lake_dir / "map_provider_instrument.parquet").exists():
        map_provider_instrument = pd.read_parquet(data_lake_dir / "map_provider_instrument.parquet")
    
    # Run all checks
    coverage = check_mapping_coverage(fact_marketcap, fact_price, fact_volume, snapshot_date)
    uniqueness = check_mapping_uniqueness(map_provider_asset, map_provider_instrument)
    join_sanity = check_join_sanity(dim_asset, fact_marketcap, fact_price, fact_volume)
    conflict_report = generate_conflict_report(
        fact_marketcap, fact_price, dim_asset, map_provider_asset, snapshot_date
    )
    
    # Validate guardrails
    is_valid, guardrail_errors = validate_mapping_guardrails(coverage, uniqueness, min_coverage_pct)
    
    # Compile report
    report = {
        "validation_timestamp": pd.Timestamp.now().isoformat(),
        "snapshot_date": str(snapshot_date) if snapshot_date else None,
        "is_valid": is_valid,
        "guardrail_errors": guardrail_errors,
        "coverage": coverage,
        "uniqueness": uniqueness,
        "join_sanity": join_sanity,
        "conflict_report": conflict_report,
    }
    
    # Write to file if path provided
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"  Mapping validation report written to: {output_path}")
    
    return report


def generate_sql_queries_for_duckdb() -> Dict[str, str]:
    """
    Generate SQL queries for mapping validation in DuckDB.
    
    Returns dictionary of query_name -> SQL query.
    """
    queries = {
        "coverage_by_date": """
            -- Coverage: how many marketcap rows have price on same date?
            SELECT
              date,
              COUNT(DISTINCT m.asset_id) AS mcap_assets,
              COUNT(DISTINCT p.asset_id) AS priced_assets,
              COUNT(DISTINCT CASE WHEN p.asset_id IS NOT NULL THEN m.asset_id END) AS mcap_with_price,
              ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.asset_id IS NOT NULL THEN m.asset_id END) / NULLIF(COUNT(DISTINCT m.asset_id),0), 2) AS pct_mcap_with_price
            FROM fact_marketcap m
            LEFT JOIN fact_price p
              ON m.asset_id = p.asset_id AND m.date = p.date
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 30;
        """,
        
        "uniqueness_provider_asset": """
            -- Uniqueness: provider IDs mapping to multiple asset_ids (bad)
            SELECT provider, provider_asset_id, COUNT(DISTINCT asset_id) AS n_asset_ids
            FROM map_provider_asset
            GROUP BY 1,2
            HAVING COUNT(DISTINCT asset_id) > 1
            ORDER BY n_asset_ids DESC
            LIMIT 100;
        """,
        
        "missing_price_top_offenders": """
            -- Which assets are in marketcap but missing price (top offenders)
            SELECT a.symbol, m.asset_id, COUNT(*) AS missing_days
            FROM fact_marketcap m
            LEFT JOIN fact_price p
              ON m.asset_id = p.asset_id AND m.date = p.date
            LEFT JOIN dim_asset a
              ON m.asset_id = a.asset_id
            WHERE p.asset_id IS NULL
            GROUP BY 1,2
            ORDER BY missing_days DESC
            LIMIT 50;
        """,
        
        "coverage_on_rebalance_dates": """
            -- Coverage on specific rebalance dates (join with universe_eligibility)
            SELECT
              ue.rebalance_date,
              COUNT(DISTINCT ue.asset_id) AS eligible_assets,
              COUNT(DISTINCT CASE WHEN p.asset_id IS NOT NULL THEN ue.asset_id END) AS eligible_with_price,
              ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.asset_id IS NOT NULL THEN ue.asset_id END) / NULLIF(COUNT(DISTINCT ue.asset_id),0), 2) AS pct_eligible_with_price
            FROM universe_eligibility ue
            LEFT JOIN fact_price p
              ON ue.asset_id = p.asset_id AND ue.snapshot_date = p.date
            WHERE ue.exclusion_reason IS NULL  -- Only eligible assets
            GROUP BY 1
            ORDER BY 1;
        """,
        
        "perp_coverage": """
            -- Perp coverage: how many assets have perp instruments?
            SELECT
              COUNT(DISTINCT da.asset_id) AS total_assets,
              COUNT(DISTINCT di.base_asset_symbol) AS assets_with_perp,
              ROUND(100.0 * COUNT(DISTINCT di.base_asset_symbol) / NULLIF(COUNT(DISTINCT da.asset_id),0), 2) AS pct_with_perp
            FROM dim_asset da
            LEFT JOIN dim_instrument di
              ON da.symbol = di.base_asset_symbol
            WHERE di.instrument_type = 'perpetual';
        """,
    }
    
    return queries
