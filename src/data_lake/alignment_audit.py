"""Alignment audit: Check dataset intersections and coverage."""

import pandas as pd
import json
from datetime import date
from pathlib import Path
from typing import Dict, List, Set, Tuple


def run_alignment_audit(
    snapshot_date: date,
    fact_marketcap: pd.DataFrame,
    fact_price: pd.DataFrame,
    fact_volume: pd.DataFrame,
    dim_asset: pd.DataFrame,
    map_provider_asset: pd.DataFrame,
    perp_asset_ids: Optional[Set[str]] = None,
) -> Dict:
    """
    Run alignment audit for a given snapshot date.
    
    Args:
        snapshot_date: Date to audit
        fact_marketcap: fact_marketcap table
        fact_price: fact_price table
        fact_volume: fact_volume table
        dim_asset: dim_asset table
        map_provider_asset: map_provider_asset table
        perp_asset_ids: Set of asset_ids that have perps (optional)
    
    Returns:
        Dictionary with audit results
    """
    # Filter to snapshot date
    mcap_on_date = fact_marketcap[fact_marketcap["date"] == snapshot_date]
    price_on_date = fact_price[fact_price["date"] == snapshot_date]
    volume_on_date = fact_volume[fact_volume["date"] == snapshot_date]
    
    # Get unique asset_ids
    mcap_assets = set(mcap_on_date["asset_id"].unique())
    price_assets = set(price_on_date["asset_id"].unique())
    volume_assets = set(volume_on_date["asset_id"].unique())
    
    # Intersections
    mcap_and_price = mcap_assets & price_assets
    mcap_and_volume = mcap_assets & volume_assets
    all_three = mcap_assets & price_assets & volume_assets
    
    # Missing from each
    mcap_no_price = mcap_assets - price_assets
    mcap_no_volume = mcap_assets - volume_assets
    price_no_mcap = price_assets - mcap_assets
    volume_no_mcap = volume_assets - mcap_assets
    
    # Perp coverage (if provided)
    perp_coverage = {}
    if perp_asset_ids:
        mcap_with_perp = mcap_assets & perp_asset_ids
        price_with_perp = price_assets & perp_asset_ids
        perp_coverage = {
            "mcap_assets_with_perp": len(mcap_with_perp),
            "mcap_assets_without_perp": len(mcap_assets - perp_asset_ids),
            "price_assets_with_perp": len(price_with_perp),
            "price_assets_without_perp": len(price_assets - perp_asset_ids),
        }
    
    # Get top examples of missing assets (by market cap)
    def get_top_missing_examples(asset_ids: Set[str], top_n: int = 50) -> List[Dict]:
        """Get top N missing assets by market cap."""
        if not asset_ids:
            return []
        
        missing_mcap = mcap_on_date[mcap_on_date["asset_id"].isin(asset_ids)]
        if len(missing_mcap) == 0:
            return []
        
        # Join with dim_asset to get symbol
        missing_with_symbol = missing_mcap.merge(
            dim_asset[["asset_id", "symbol"]],
            on="asset_id",
            how="left"
        )
        
        # Sort by marketcap descending
        missing_sorted = missing_with_symbol.sort_values("marketcap", ascending=False)
        
        # Return top N
        top_missing = missing_sorted.head(top_n)
        return [
            {
                "asset_id": row["asset_id"],
                "symbol": row.get("symbol", ""),
                "marketcap": float(row["marketcap"]),
            }
            for _, row in top_missing.iterrows()
        ]
    
    audit_result = {
        "snapshot_date": str(snapshot_date),
        "counts": {
            "mcap_assets": len(mcap_assets),
            "price_assets": len(price_assets),
            "volume_assets": len(volume_assets),
        },
        "intersections": {
            "mcap_and_price": len(mcap_and_price),
            "mcap_and_volume": len(mcap_and_volume),
            "all_three": len(all_three),
        },
        "coverage_pct": {
            "mcap_with_price": len(mcap_and_price) / len(mcap_assets) * 100 if len(mcap_assets) > 0 else 0,
            "mcap_with_volume": len(mcap_and_volume) / len(mcap_assets) * 100 if len(mcap_assets) > 0 else 0,
            "mcap_with_both": len(all_three) / len(mcap_assets) * 100 if len(mcap_assets) > 0 else 0,
        },
        "missing": {
            "mcap_no_price_count": len(mcap_no_price),
            "mcap_no_volume_count": len(mcap_no_volume),
            "price_no_mcap_count": len(price_no_mcap),
            "volume_no_mcap_count": len(volume_no_mcap),
        },
        "top_missing_examples": {
            "mcap_no_price": get_top_missing_examples(mcap_no_price, top_n=50),
            "mcap_no_volume": get_top_missing_examples(mcap_no_volume, top_n=50),
        },
    }
    
    if perp_coverage:
        audit_result["perp_coverage"] = perp_coverage
    
    return audit_result


def write_alignment_audit(
    audit_results: List[Dict],
    output_path: Path,
) -> None:
    """
    Write alignment audit results to JSON file.
    
    Args:
        audit_results: List of audit result dictionaries (one per rebalance date)
        output_path: Path to output JSON file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    audit_summary = {
        "audit_timestamp": pd.Timestamp.now().isoformat(),
        "total_rebalance_dates": len(audit_results),
        "audits": audit_results,
    }
    
    with open(output_path, "w") as f:
        json.dump(audit_summary, f, indent=2, default=str)
    
    print(f"  Alignment audit written to: {output_path}")
