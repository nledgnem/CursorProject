#!/usr/bin/env python3
"""
Fetch asset-to-category mappings from CoinGecko API.

This creates a mapping table showing which assets belong to which categories.
"""

import sys
from pathlib import Path
from datetime import date
from typing import Dict, List, Optional, Set
import polars as pl
import requests
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.providers.coingecko_analyst import COINGECKO_BASE, COINGECKO_API_KEY, check_api_usage

DATA_LAKE_DIR = Path("data/curated/data_lake")


def safe_print(text: str) -> None:
    """Print text safely, handling Unicode encoding errors on Windows."""
    try:
        print(text, end="", flush=True)
    except UnicodeEncodeError:
        safe_text = text.encode('ascii', 'replace').decode('ascii')
        print(safe_text, end="", flush=True)


def fetch_coin_categories(
    coingecko_id: str,
    sleep_seconds: float = 0.12,
) -> Optional[List[str]]:
    """
    Fetch categories for a specific coin.
    
    Returns list of category names (e.g., ["Proof of Work (PoW)", "Layer 1 (L1)"])
    """
    url = f"{COINGECKO_BASE}/coins/{coingecko_id}"
    
    params = {
        "x_cg_pro_api_key": COINGECKO_API_KEY,
        "localization": "false",
        "tickers": "false",
        "market_data": "false",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false",
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30, proxies={"http": None, "https": None})
        
        if resp.status_code == 200:
            data = resp.json()
            categories = data.get("categories", [])
            time.sleep(sleep_seconds)
            return categories if categories else None
        elif resp.status_code == 404:
            return None
        else:
            return None
    except Exception as e:
        return None


def normalize_category_name_to_id(category_name: str, dim_categories: pl.DataFrame) -> Optional[str]:
    """
    Map category name (from API) to category_id (from dim_categories).
    
    CoinGecko API returns category names like "Proof of Work (PoW)"
    but dim_categories has category_id like "proof-of-work"
    """
    # Try exact match first
    exact_match = dim_categories.filter(
        pl.col("category_name").str.to_lowercase() == category_name.lower()
    )
    if len(exact_match) > 0:
        return exact_match["category_id"][0]
    
    # Try fuzzy matching - remove special chars and compare
    normalized_name = category_name.lower().replace("(", "").replace(")", "").replace("-", " ").replace("_", " ")
    normalized_name = " ".join(normalized_name.split())  # Normalize whitespace
    
    for row in dim_categories.to_dicts():
        cat_name = row["category_name"].lower().replace("(", "").replace(")", "").replace("-", " ").replace("_", " ")
        cat_name = " ".join(cat_name.split())
        
        if normalized_name == cat_name or normalized_name in cat_name or cat_name in normalized_name:
            return row["category_id"]
    
    return None


def fetch_and_save_asset_categories(
    max_assets: Optional[int] = None,
    sample_only: bool = False,
) -> None:
    """Fetch category mappings for assets and save to data lake."""
    print("=" * 80)
    print("FETCHING ASSET-TO-CATEGORY MAPPINGS")
    print("=" * 80)
    print()
    
    # Load dim_asset to get coingecko_ids
    dim_asset_path = DATA_LAKE_DIR / "dim_asset.parquet"
    if not dim_asset_path.exists():
        print("[ERROR] dim_asset.parquet not found")
        return
    
    dim_asset = pl.read_parquet(str(dim_asset_path))
    
    # Filter to assets with coingecko_id
    assets_with_coingecko = dim_asset.filter(
        pl.col("coingecko_id").is_not_null()
    )
    
    if sample_only:
        # Just test with a small sample
        assets_with_coingecko = assets_with_coingecko.head(20)
        print(f"[INFO] Running in sample mode - testing with {len(assets_with_coingecko)} assets")
    elif max_assets:
        assets_with_coingecko = assets_with_coingecko.head(max_assets)
        print(f"[INFO] Processing {len(assets_with_coingecko)} assets (limited)")
    else:
        print(f"[INFO] Processing {len(assets_with_coingecko)} assets")
    
    print()
    
    # Load dim_categories for name-to-ID mapping
    dim_categories_path = DATA_LAKE_DIR / "dim_categories.parquet"
    if not dim_categories_path.exists():
        print("[ERROR] dim_categories.parquet not found")
        return
    
    dim_categories = pl.read_parquet(str(dim_categories_path))
    
    all_mappings = []
    processed = 0
    errors = 0
    
    for row in assets_with_coingecko.to_dicts():
        asset_id = row["asset_id"]
        coingecko_id = row["coingecko_id"]
        symbol = row.get("symbol", "")
        
        # Show progress every 50 assets
        if processed % 50 == 0:
            safe_print(f"\n[Progress: {processed}/{len(assets_with_coingecko)}] Processing...\n")
        
        safe_print(f"[{processed+1}/{len(assets_with_coingecko)}] {symbol} ({coingecko_id})... ")
        
        categories = fetch_coin_categories(coingecko_id)
        
        if categories:
            for cat_name in categories:
                # Map category name to category_id
                category_id = normalize_category_name_to_id(cat_name, dim_categories)
                
                if category_id:
                    all_mappings.append({
                        "asset_id": asset_id,
                        "category_id": category_id,
                        "category_name": cat_name,  # Keep original name for reference
                        "source": "coingecko",
                    })
                else:
                    # Category not found in dim_categories - might be a new category
                    safe_print(f"[WARN] Category '{cat_name}' not found in dim_categories\n")
            
            safe_print(f"[OK] {len(categories)} categories\n")
        else:
            safe_print("[SKIP] No categories\n")
            errors += 1
        
        processed += 1
    
    if not all_mappings:
        print("[ERROR] No category mappings created")
        return
    
    # Save to parquet
    df = pl.DataFrame(all_mappings)
    output_path = DATA_LAKE_DIR / "map_category_asset.parquet"
    
    # Merge with existing data (deduplicate by asset_id, category_id)
    if output_path.exists():
        existing = pl.read_parquet(str(output_path))
        # Remove mappings for assets we're updating (to avoid duplicates)
        updated_asset_ids = set(df["asset_id"].unique().to_list())
        existing = existing.filter(~pl.col("asset_id").is_in(updated_asset_ids))
        df = pl.concat([existing, df])
    
    # Remove duplicates (in case of re-runs)
    df = df.unique(subset=["asset_id", "category_id"])
    
    df.write_parquet(str(output_path))
    print()
    print(f"[SUCCESS] Saved {len(all_mappings)} asset-category mappings to {output_path}")
    print(f"  Unique assets: {df['asset_id'].n_unique()}")
    print(f"  Unique categories: {df['category_id'].n_unique()}")
    print()


def main():
    """Main function."""
    print("=" * 80)
    print("ASSET-TO-CATEGORY MAPPING FETCH")
    print("=" * 80)
    print()
    
    # Check API usage first
    usage = check_api_usage()
    if usage:
        print(f"API Usage: {usage.get('current_total_monthly_calls', 'N/A')} / {usage.get('monthly_call_credit', 'N/A')}")
        print()
    
    # Run full fetch for all assets
    print("[INFO] Starting full fetch for all assets...")
    print("[WARN] This will make ~2,700 API calls (one per asset)")
    print("[INFO] Estimated time: ~5-10 minutes (with rate limiting)")
    print()
    
    fetch_and_save_asset_categories(max_assets=None, sample_only=False)
    
    print("=" * 80)
    print("FULL FETCH COMPLETE!")
    print("=" * 80)
    print()
    
    # Final API usage check
    usage = check_api_usage()
    if usage:
        print(f"Final API Usage: {usage.get('current_total_monthly_calls', 'N/A')} / {usage.get('monthly_call_credit', 'N/A')}")
        remaining = usage.get('current_remaining_monthly_calls', 0)
        print(f"Remaining: {remaining:,} calls")


if __name__ == "__main__":
    main()
