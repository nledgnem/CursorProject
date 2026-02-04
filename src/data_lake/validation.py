"""Validation functions for data lake pipeline."""

from typing import Dict, List, Optional
from datetime import date
import pandas as pd
import sys


def validate_eligible_set_not_empty(
    eligible_count: int,
    rebalance_date: date,
    allow_empty: bool = False,
) -> tuple[bool, str]:
    """
    Validate that eligible set is not empty.
    
    Returns:
        (is_valid, error_message)
    """
    if eligible_count == 0 and not allow_empty:
        return False, f"Empty eligible set on {rebalance_date}. Pipeline must have at least one eligible asset."
    return True, ""


def validate_weights_sum_to_one(
    weights: pd.Series,
    rebalance_date: date,
    tolerance: float = 0.001,
) -> tuple[bool, str]:
    """
    Validate that weights sum to approximately 1.0.
    
    Args:
        weights: Series of weights (asset_id -> weight)
        rebalance_date: Rebalance date for error message
        tolerance: Allowed deviation from 1.0
    
    Returns:
        (is_valid, error_message)
    """
    total_weight = weights.sum()
    if abs(total_weight - 1.0) > tolerance:
        return False, f"Weights sum to {total_weight:.6f} (expected ~1.0) on {rebalance_date}"
    return True, ""


def validate_basket_coverage(
    coverage: float,
    threshold: float,
    rebalance_date: date,
    fail_on_below_threshold: bool = True,
) -> tuple[bool, str]:
    """
    Validate that basket coverage meets threshold.
    
    Args:
        coverage: Actual coverage (0.0-1.0)
        threshold: Required threshold (0.0-1.0)
        rebalance_date: Rebalance date for error message
        fail_on_below_threshold: If True, fail when below threshold
    
    Returns:
        (is_valid, error_message)
    """
    if coverage < threshold and fail_on_below_threshold:
        return False, f"Basket coverage {coverage:.2%} below threshold {threshold:.2%} on {rebalance_date}"
    return True, ""


def validate_no_forbidden_assets(
    basket_asset_ids: List[str],
    forbidden_asset_ids: set,
    rebalance_date: date,
) -> tuple[bool, str]:
    """
    Validate that no forbidden assets appear in basket.
    
    Args:
        basket_asset_ids: List of asset_ids in basket
        forbidden_asset_ids: Set of forbidden asset_ids (e.g., base asset, blacklist)
        rebalance_date: Rebalance date for error message
    
    Returns:
        (is_valid, error_message)
    """
    forbidden_in_basket = set(basket_asset_ids) & forbidden_asset_ids
    if forbidden_in_basket:
        return False, f"Forbidden assets in basket on {rebalance_date}: {forbidden_in_basket}"
    return True, ""


def validate_snapshot_invariants(
    snapshots_df: pd.DataFrame,
    config: Dict,
    allow_empty: bool = False,
) -> List[str]:
    """
    Validate all snapshot invariants.
    
    Returns:
        List of error messages (empty if all valid)
    """
    errors = []
    
    if len(snapshots_df) == 0:
        if not allow_empty:
            errors.append("No snapshots created - eligible set was empty for all rebalance dates")
        return errors
    
    # Group by rebalance_date
    for rebalance_date, group in snapshots_df.groupby("rebalance_date"):
        # Check weights sum to 1
        weights = group["weight"]
        is_valid, msg = validate_weights_sum_to_one(weights, rebalance_date)
        if not is_valid:
            errors.append(msg)
        
        # Check no forbidden assets (if base_asset specified)
        if "base_asset" in config:
            base_asset = config["base_asset"].upper()
            basket_asset_ids = group["asset_id"].tolist() if "asset_id" in group.columns else group["symbol"].tolist()
            is_valid, msg = validate_no_forbidden_assets(
                basket_asset_ids,
                {base_asset},
                rebalance_date,
            )
            if not is_valid:
                errors.append(msg)
    
    return errors


def fail_on_validation_errors(
    errors: List[str],
    context: str = "Validation",
) -> None:
    """
    Print errors and exit with non-zero code if any errors.
    
    Args:
        errors: List of error messages
        context: Context string for error message
    """
    if errors:
        print(f"\n{'=' * 70}")
        print(f"{context} FAILED")
        print(f"{'=' * 70}")
        for i, error in enumerate(errors, 1):
            print(f"  [{i}] {error}")
        print(f"\nPipeline failed due to validation errors.")
        sys.exit(1)
