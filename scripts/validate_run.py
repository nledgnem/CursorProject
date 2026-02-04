#!/usr/bin/env python3
"""Validate pipeline run with invariant checks and diagnostics."""

import sys
import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from datetime import date

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def get_eligible_mask(df: pd.DataFrame) -> pd.Series:
    """
    Get boolean mask for eligible rows.
    
    Prefers explicit 'eligible' column if present, otherwise falls back to
    inferring from 'exclusion_reason' being null.
    
    Args:
        df: DataFrame with universe eligibility data
    
    Returns:
        Boolean Series where True indicates eligible
    """
    if "eligible" in df.columns:
        return df["eligible"] == True
    elif "exclusion_reason" in df.columns:
        return df["exclusion_reason"].isna()
    else:
        # Return False mask if neither column exists
        return pd.Series([False] * len(df), index=df.index)


def check_universe_basket_invariants(
    universe_path: Path,
    basket_path: Path,
    top_n: int,
    base_asset: str,
    blacklist_path: Path = None,
    stablecoins_path: Path = None,
) -> Tuple[bool, List[str]]:
    """
    Check universe/basket invariants.
    
    Returns:
        (all_passed, list_of_violations)
    """
    violations = []
    
    if not universe_path.exists():
        violations.append(f"[WARN] Universe eligibility file not found: {universe_path} (validation will be limited)")
        # Return True with warnings - don't fail completely if file is missing
        # This allows validation to continue with other checks that don't require universe data
        # Note: We skip all universe-dependent checks when file is missing
        return True, violations
    
    if not basket_path.exists():
        violations.append(f"[CRITICAL] Basket snapshots file not found: {basket_path}")
        return False, violations
    
    try:
        universe_df = pd.read_parquet(universe_path)
        basket_df = pd.read_parquet(basket_path)
    except Exception as e:
        violations.append(f"[CRITICAL] Failed to load parquet files: {e}")
        return False, violations
    
    # Load blacklist and stablecoins
    blacklisted_symbols = set()
    stablecoin_symbols = set()
    
    if blacklist_path and blacklist_path.exists():
        blacklist_df = pd.read_csv(blacklist_path)
        blacklisted_symbols = set(blacklist_df["symbol"].values)
    
    if stablecoins_path and stablecoins_path.exists():
        stablecoins_df = pd.read_csv(stablecoins_path)
        stablecoin_symbols = set(stablecoins_df[stablecoins_df["is_stable"] == 1]["symbol"].values)
    
    # Invariant 1: Weights sum to 1 per rebalance (± tolerance)
    if len(basket_df) > 0:
        weight_sums = basket_df.groupby("rebalance_date")["weight"].sum()
        tolerance = 1e-6
        invalid_sums = weight_sums[(weight_sums < 1.0 - tolerance) | (weight_sums > 1.0 + tolerance)]
        if len(invalid_sums) > 0:
            for rebal_date, weight_sum in invalid_sums.items():
                violations.append(
                    f"[VIOLATION] Weights don't sum to 1.0 on {rebal_date}: sum = {weight_sum:.10f}"
                )
    
    # Invariant 2: Basket size == topN unless eligible < topN
    if len(basket_df) > 0:
        basket_counts = basket_df.groupby("rebalance_date").size()
        for rebal_date in basket_counts.index:
            basket_count = basket_counts[rebal_date]
            date_mask = universe_df["rebalance_date"] == rebal_date
            eligible_mask = get_eligible_mask(universe_df)
            eligible_count = (date_mask & eligible_mask).sum()
            
            if eligible_count >= top_n:
                if basket_count != top_n:
                    violations.append(
                        f"[VIOLATION] Basket size {basket_count} != top_n {top_n} on {rebal_date} "
                        f"(eligible: {eligible_count})"
                    )
            else:
                if basket_count != eligible_count:
                    violations.append(
                        f"[VIOLATION] Basket size {basket_count} != eligible count {eligible_count} "
                        f"on {rebal_date} (eligible < top_n)"
                    )
    
    # Invariant 3: No stablecoins/blacklist in basket
    if len(basket_df) > 0:
        basket_symbols = set(basket_df["symbol"].unique())
        blacklist_in_basket = basket_symbols & blacklisted_symbols
        stablecoins_in_basket = basket_symbols & stablecoin_symbols
        
        if blacklist_in_basket:
            violations.append(
                f"[VIOLATION] Blacklisted symbols in basket: {blacklist_in_basket}"
            )
        
        if stablecoins_in_basket:
            violations.append(
                f"[VIOLATION] Stablecoins in basket: {stablecoins_in_basket}"
            )
        
        # Base asset should not be in basket
        if base_asset in basket_symbols:
            violations.append(
                f"[VIOLATION] Base asset '{base_asset}' found in basket"
            )
    
    # Invariant 4: Every basket coin has eligible=True in universe_eligibility
    if len(basket_df) > 0:
        for rebal_date in basket_df["rebalance_date"].unique():
            basket_symbols = set(basket_df[basket_df["rebalance_date"] == rebal_date]["symbol"])
            date_mask = universe_df["rebalance_date"] == rebal_date
            eligible_mask = get_eligible_mask(universe_df)
            eligible_symbols = set(universe_df[date_mask & eligible_mask]["symbol"])
            
            ineligible_in_basket = basket_symbols - eligible_symbols
            if ineligible_in_basket:
                violations.append(
                    f"[VIOLATION] Ineligible symbols in basket on {rebal_date}: {ineligible_in_basket}"
                )
    
    # Invariant 5: If eligible=False, exclusion_reason must not be null
    # (Also check stablecoin/blacklist-specific case as extra validation)
    if "eligible" in universe_df.columns and "exclusion_reason" in universe_df.columns:
        # Check rows where eligible is False but exclusion_reason is null
        ineligible_mask = universe_df["eligible"] == False
        null_exclusion_mask = universe_df["exclusion_reason"].isna()
        invalid_rows = universe_df[ineligible_mask & null_exclusion_mask]
        if len(invalid_rows) > 0:
            violations.append(
                f"[VIOLATION] Found {len(invalid_rows)} rows with eligible=False but exclusion_reason is null"
            )
    
    # Extra check: stablecoins/blacklisted should have exclusion_reason
    excluded_rows = universe_df[universe_df["exclusion_reason"].isna() & (universe_df["is_stablecoin"] | universe_df["is_blacklisted"])]
    if len(excluded_rows) > 0:
        violations.append(
            f"[VIOLATION] Found {len(excluded_rows)} stablecoin/blacklisted rows with null exclusion_reason"
        )
    
    return len(violations) == 0, violations


def check_perp_listing_invariants(
    universe_path: Path,
    perp_listings_path: Path = None,
) -> Tuple[bool, List[str]]:
    """Check perp listing proxy invariants."""
    violations = []
    
    if not universe_path.exists():
        return True, []  # Skip if universe file doesn't exist
    
    try:
        universe_df = pd.read_parquet(universe_path)
    except Exception as e:
        violations.append(f"[ERROR] Failed to load universe file: {e}")
        return False, violations
    
    # Load perp listings if available
    symbol_to_onboard_date = {}
    if perp_listings_path and perp_listings_path.exists():
        try:
            perp_df = pd.read_parquet(perp_listings_path)
            for _, row in perp_df.iterrows():
                symbol = row["symbol"]
                # Normalize: remove USDT suffix
                if symbol.endswith("USDT"):
                    symbol = symbol[:-4]
                onboard_date = row["onboard_date"]
                if isinstance(onboard_date, str):
                    onboard_date = date.fromisoformat(onboard_date)
                elif isinstance(onboard_date, pd.Timestamp):
                    onboard_date = onboard_date.date()
                symbol_to_onboard_date[symbol] = onboard_date
        except Exception as e:
            violations.append(f"[WARN] Failed to load perp listings: {e}")
    
    if not symbol_to_onboard_date:
        return True, []  # Skip if no perp listings
    
    # Invariant 6: For any coin with onboard_date, if rebalance_date < onboard_date then eligible must be false
    for _, row in universe_df.iterrows():
        symbol = row["symbol"]
        rebal_date = row["rebalance_date"]
        if isinstance(rebal_date, str):
            rebal_date = date.fromisoformat(rebal_date)
        elif isinstance(rebal_date, pd.Timestamp):
            rebal_date = rebal_date.date()
        
        onboard_date = symbol_to_onboard_date.get(symbol)
        if onboard_date and rebal_date < onboard_date:
            # Coin wasn't listed yet, should be excluded
            # Check using eligible column if available, otherwise fall back to exclusion_reason
            is_eligible = False
            if "eligible" in row.index:
                is_eligible = row["eligible"] == True
            elif "exclusion_reason" in row.index:
                is_eligible = row["exclusion_reason"] is None or pd.isna(row["exclusion_reason"])
            
            if is_eligible:
                violations.append(
                    f"[VIOLATION] Symbol {symbol} on {rebal_date} has onboard_date {onboard_date} "
                    f"(future) but is eligible"
                )
            if row["perp_eligible_proxy"]:
                violations.append(
                    f"[VIOLATION] Symbol {symbol} on {rebal_date} has onboard_date {onboard_date} "
                    f"(future) but perp_eligible_proxy=True"
                )
    
    # Invariant 7: At least 80% of basket has perp_eligible_proxy true (warn if lower)
    basket_path = universe_path.parent / "universe_snapshots.parquet"
    if basket_path.exists():
        try:
            basket_df = pd.read_parquet(basket_path)
            if len(basket_df) > 0:
                # Join with universe to get perp_eligible_proxy
                merged = basket_df.merge(
                    universe_df[["rebalance_date", "symbol", "perp_eligible_proxy"]],
                    on=["rebalance_date", "symbol"],
                    how="left"
                )
                
                for rebal_date in merged["rebalance_date"].unique():
                    rebal_basket = merged[merged["rebalance_date"] == rebal_date]
                    perp_eligible_pct = rebal_basket["perp_eligible_proxy"].sum() / len(rebal_basket)
                    
                    if perp_eligible_pct < 0.80:
                        violations.append(
                            f"[WARN] Only {perp_eligible_pct:.1%} of basket has perp_eligible_proxy=True "
                            f"on {rebal_date} (threshold: 80%)"
                        )
        except Exception as e:
            violations.append(f"[WARN] Failed to check perp eligibility in basket: {e}")
    
    return len(violations) == 0, violations


def check_backtest_invariants(
    results_path: Path,
    turnover_path: Path = None,
) -> Tuple[bool, List[str]]:
    """Check backtest invariants."""
    violations = []
    
    if not results_path.exists():
        violations.append(f"[CRITICAL] Backtest results file not found: {results_path}")
        return False, violations
    
    try:
        results_df = pd.read_csv(results_path)
    except Exception as e:
        violations.append(f"[CRITICAL] Failed to load backtest results: {e}")
        return False, violations
    
    if len(results_df) == 0:
        violations.append("[CRITICAL] Backtest results are empty")
        return False, violations
    
    # Invariant 8: NaN basket return days must be ≤ 10% (DoD gate for research-grade backtests)
    # Exclude pre-basket days (NaN before first non-NaN basket return)
    if "r_basket" in results_df.columns:
        # Find first non-NaN basket return (first rebalance date)
        first_valid_idx = results_df["r_basket"].first_valid_index()
        if first_valid_idx is not None:
            # Only count NaN days after first basket is formed
            post_basket_df = results_df.loc[first_valid_idx:]
            nan_count = post_basket_df["r_basket"].isna().sum()
            total_post_basket = len(post_basket_df)
            nan_pct = (nan_count / total_post_basket * 100) if total_post_basket > 0 else 0
            max_nan_pct = 10.0  # Definition of Done: ≤ 10% NaN days after first basket
            
            if nan_pct > max_nan_pct:
                violations.append(
                    f"[VIOLATION] {nan_count} ({nan_pct:.1f}%) basket return days are NaN "
                    f"after first basket (exceeds DoD threshold of {max_nan_pct:.1f}%)"
                )
            elif nan_pct > 5.0:
                # Warn if between 5-10%
                violations.append(
                    f"[WARN] {nan_count} ({nan_pct:.1f}%) basket return days are NaN "
                    f"after first basket (approaching DoD threshold of {max_nan_pct:.1f}%)"
                )
        else:
            # All days are NaN (no basket formed at all)
            violations.append(
                "[VIOLATION] All basket return days are NaN (no basket was formed)"
            )
    
    # Invariant 9: Daily returns have no absurd spikes (|ret| > 50% triggers warning)
    if "r_basket" in results_df.columns:
        extreme_returns = results_df[results_df["r_basket"].abs() > 0.50]
        if len(extreme_returns) > 0:
            for _, row in extreme_returns.head(10).iterrows():
                violations.append(
                    f"[WARN] Extreme basket return on {row['date']}: {row['r_basket']:.2%}"
                )
    
    if "r_btc" in results_df.columns:
        extreme_returns = results_df[results_df["r_btc"].abs() > 0.50]
        if len(extreme_returns) > 0:
            for _, row in extreme_returns.head(10).iterrows():
                violations.append(
                    f"[WARN] Extreme BTC return on {row['date']}: {row['r_btc']:.2%}"
                )
    
    # Invariant 9: Coverage never exceeds 1, never negative (if tracked)
    # (Coverage is computed internally, but we can check if it's in results)
    
    # Invariant 10: Turnover is not always ~100% every rebalance
    if turnover_path and turnover_path.exists():
        try:
            turnover_df = pd.read_csv(turnover_path)
            if len(turnover_df) > 0:
                # Check if turnover is suspiciously high for all rebalances
                high_turnover = turnover_df[turnover_df["turnover"] > 0.95]
                if len(high_turnover) == len(turnover_df) and len(turnover_df) > 1:
                    violations.append(
                        f"[WARN] Turnover is >95% for all {len(turnover_df)} rebalances "
                        f"(possible bug: weights not persisting between rebalances)"
                    )
        except Exception as e:
            violations.append(f"[WARN] Failed to check turnover: {e}")
    
    # Invariant 11: Number of NaN return days is reported (covered by Invariant 8 above)
    # This is now a DoD gate, not just a warning
    
    return len(violations) == 0, violations


def generate_run_summary(
    universe_path: Path,
    basket_path: Path,
    results_path: Path,
    turnover_path: Path = None,
    output_path: Path = None,
) -> str:
    """Generate human-readable run summary."""
    lines = []
    lines.append("# Pipeline Run Summary")
    lines.append("")
    
    # Load data
    try:
        universe_df = pd.read_parquet(universe_path) if universe_path.exists() else None
        basket_df = pd.read_parquet(basket_path) if basket_path.exists() else None
        results_df = pd.read_csv(results_path) if results_path.exists() else None
        turnover_df = pd.read_csv(turnover_path) if turnover_path and turnover_path.exists() else None
    except Exception as e:
        lines.append(f"**Error loading data:** {e}")
        return "\n".join(lines)
    
    # Universe stats
    if universe_df is not None and len(universe_df) > 0:
        lines.append("## Universe Eligibility")
        lines.append("")
        rebalance_dates = sorted(universe_df["rebalance_date"].unique())
        lines.append(f"- **Number of rebalance dates:** {len(rebalance_dates)}")
        lines.append(f"- **Date range:** {rebalance_dates[0]} to {rebalance_dates[-1]}")
        lines.append("")
        
        # Eligible count per rebalance
        lines.append("### Eligible Count Per Rebalance")
        lines.append("")
        lines.append("| Rebalance Date | Eligible | Total Candidates | Eligible % |")
        lines.append("|----------------|----------|------------------|------------|")
        for rebal_date in rebalance_dates:
            rebal_data = universe_df[universe_df["rebalance_date"] == rebal_date]
            total = len(rebal_data)
            eligible_mask = get_eligible_mask(rebal_data)
            eligible = eligible_mask.sum()
            pct = (eligible / total * 100) if total > 0 else 0
            lines.append(f"| {rebal_date} | {eligible} | {total} | {pct:.1f}% |")
        lines.append("")
        
        # Exclusions by reason
        lines.append("### Exclusions by Reason (Total)")
        lines.append("")
        if "exclusion_reason" in universe_df.columns:
            exclusion_counts = universe_df["exclusion_reason"].value_counts()
            for reason, count in exclusion_counts.items():
                if pd.notna(reason):
                    lines.append(f"- **{reason}:** {count}")
        lines.append("")
    
    # Basket stats
    if basket_df is not None and len(basket_df) > 0:
        lines.append("## Basket Snapshots")
        lines.append("")
        lines.append(f"- **Total basket constituents:** {len(basket_df)}")
        lines.append(f"- **Number of rebalance dates:** {len(basket_df['rebalance_date'].unique())}")
        lines.append("")
        
        # Basket size per rebalance
        basket_counts = basket_df.groupby("rebalance_date").size()
        lines.append("### Basket Size Per Rebalance")
        lines.append("")
        lines.append("| Rebalance Date | Basket Size |")
        lines.append("|----------------|-------------|")
        for rebal_date, count in basket_counts.items():
            lines.append(f"| {rebal_date} | {count} |")
        lines.append("")
    
    # Turnover stats
    if turnover_df is not None and len(turnover_df) > 0:
        lines.append("## Turnover")
        lines.append("")
        lines.append(f"- **Average turnover:** {turnover_df['turnover'].mean():.2%}")
        lines.append(f"- **Median turnover:** {turnover_df['turnover'].median():.2%}")
        lines.append(f"- **Min turnover:** {turnover_df['turnover'].min():.2%}")
        lines.append(f"- **Max turnover:** {turnover_df['turnover'].max():.2%}")
        lines.append("")
        lines.append(f"- **Average entered per rebalance:** {turnover_df['entered_count'].mean():.1f}")
        lines.append(f"- **Average exited per rebalance:** {turnover_df['exited_count'].mean():.1f}")
        lines.append("")
    
    # Backtest results
    if results_df is not None and len(results_df) > 0:
        lines.append("## Backtest Results")
        lines.append("")
        lines.append(f"- **Total trading days:** {len(results_df)}")
        
        if "r_basket" in results_df.columns:
            nan_count = results_df["r_basket"].isna().sum()
            lines.append(f"- **Basket return NaN days:** {nan_count} ({nan_count/len(results_df):.1%})")
        
        if "r_ls_net" in results_df.columns:
            returns = results_df["r_ls_net"].dropna()
            if len(returns) > 0:
                total_return = (1.0 + returns).prod() - 1.0
                annualized = (1.0 + total_return) ** (252 / len(returns)) - 1.0
                sharpe = np.sqrt(252) * returns.mean() / returns.std() if returns.std() > 0 else 0.0
                
                lines.append(f"- **Total return:** {total_return:.2%}")
                lines.append(f"- **Annualized return:** {annualized:.2%}")
                lines.append(f"- **Annualized Sharpe:** {sharpe:.2f}")
        lines.append("")
    
    summary_text = "\n".join(lines)
    
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(summary_text)
    
    return summary_text


def main():
    parser = argparse.ArgumentParser(
        description="Validate pipeline run with invariant checks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--universe",
        type=Path,
        required=True,
        help="Path to universe_eligibility.parquet",
    )
    parser.add_argument(
        "--basket",
        type=Path,
        required=True,
        help="Path to universe_snapshots.parquet",
    )
    parser.add_argument(
        "--results",
        type=Path,
        required=True,
        help="Path to backtest_results.csv",
    )
    parser.add_argument(
        "--turnover",
        type=Path,
        default=None,
        help="Path to rebalance_turnover.csv (optional)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        required=True,
        help="Expected top-N size",
    )
    parser.add_argument(
        "--base-asset",
        type=str,
        default="BTC",
        help="Base asset symbol",
    )
    parser.add_argument(
        "--blacklist",
        type=Path,
        default=None,
        help="Path to blacklist.csv (optional)",
    )
    parser.add_argument(
        "--stablecoins",
        type=Path,
        default=None,
        help="Path to stablecoins.csv (optional)",
    )
    parser.add_argument(
        "--perp-listings",
        type=Path,
        default=None,
        help="Path to perp_listings_binance.parquet (optional)",
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        default=None,
        help="Path to write run summary markdown (optional)",
    )
    parser.add_argument(
        "--fail-on-violations",
        action="store_true",
        help="Exit with error code if any violations found",
    )
    parser.add_argument(
        "--validation-report",
        type=Path,
        default=None,
        help="Path to write validation report markdown (default: <outputs-dir>/validation_report.md)",
    )
    parser.add_argument(
        "--validation-failures",
        type=Path,
        default=None,
        help="Path to write validation failures JSON (default: <outputs-dir>/validation_failures.json)",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    
    # Resolve paths
    universe_path = args.universe if args.universe.is_absolute() else repo_root / args.universe
    basket_path = args.basket if args.basket.is_absolute() else repo_root / args.basket
    results_path = args.results if args.results.is_absolute() else repo_root / args.results
    turnover_path = args.turnover if args.turnover and args.turnover.is_absolute() else (repo_root / args.turnover if args.turnover else None)
    blacklist_path = args.blacklist if args.blacklist and args.blacklist.is_absolute() else (repo_root / args.blacklist if args.blacklist else None)
    stablecoins_path = args.stablecoins if args.stablecoins and args.stablecoins.is_absolute() else (repo_root / args.stablecoins if args.stablecoins else None)
    perp_listings_path = args.perp_listings if args.perp_listings and args.perp_listings.is_absolute() else (repo_root / args.perp_listings if args.perp_listings else None)
    
    print("=" * 60)
    print("Pipeline Run Validation")
    print("=" * 60)
    print()
    
    all_passed = True
    all_violations = []
    
    # Check universe/basket invariants
    print("[Check 1] Universe/Basket Invariants...")
    passed, violations = check_universe_basket_invariants(
        universe_path, basket_path, args.top_n, args.base_asset,
        blacklist_path, stablecoins_path
    )
    if violations:
        all_violations.extend(violations)
        for v in violations:
            print(f"  {v}")
    else:
        print("  [PASS] All universe/basket invariants passed")
    all_passed = all_passed and passed
    print()
    
    # Check perp listing invariants
    print("[Check 2] Perp Listing Invariants...")
    passed, violations = check_perp_listing_invariants(
        universe_path, perp_listings_path
    )
    if violations:
        all_violations.extend(violations)
        for v in violations:
            print(f"  {v}")
    else:
        print("  [PASS] All perp listing invariants passed")
    # Don't fail on warnings
    print()
    
    # Check backtest invariants
    print("[Check 3] Backtest Invariants...")
    passed, violations = check_backtest_invariants(results_path, turnover_path)
    if violations:
        all_violations.extend(violations)
        for v in violations:
            print(f"  {v}")
    else:
        print("  [PASS] All backtest invariants passed")
    # Don't fail on warnings
    print()
    
    # Generate summary
    print("[Summary] Generating run summary...")
    summary = generate_run_summary(
        universe_path, basket_path, results_path, turnover_path,
        args.summary_output
    )
    if args.summary_output:
        print(f"  Saved to {args.summary_output}")
    print()
    
    # Write validation report and failures JSON
    validation_report_path = args.validation_report
    validation_failures_path = args.validation_failures
    
    # Determine default paths if not provided
    if not validation_report_path:
        # Use outputs_dir from summary_output if provided, otherwise use results_path parent
        if args.summary_output:
            validation_report_path = args.summary_output.parent / "validation_report.md"
        else:
            validation_report_path = Path("outputs") / "validation_report.md"
    
    if not validation_failures_path:
        if args.summary_output:
            validation_failures_path = args.summary_output.parent / "validation_failures.json"
        else:
            validation_failures_path = Path("outputs") / "validation_failures.json"
    
    # Write validation report
    validation_report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(validation_report_path, "w") as f:
        f.write("# Validation Report\n\n")
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")
        
        critical_violations = [v for v in all_violations if "[CRITICAL]" in v]
        violations = [v for v in all_violations if "[VIOLATION]" in v]
        warnings = [v for v in all_violations if "[WARN]" in v]
        
        if len(critical_violations) == 0 and len(violations) == 0 and len(warnings) == 0:
            f.write("## Status: PASS\n\n")
            f.write("All invariants passed.\n")
        else:
            if len(critical_violations) > 0 or len(violations) > 0:
                f.write("## Status: FAIL\n\n")
            else:
                f.write("## Status: WARN\n\n")
            
            if critical_violations:
                f.write(f"### Critical Errors ({len(critical_violations)})\n\n")
                for v in critical_violations:
                    f.write(f"- {v}\n")
                f.write("\n")
            
            if violations:
                f.write(f"### Violations ({len(violations)})\n\n")
                for v in violations:
                    f.write(f"- {v}\n")
                f.write("\n")
            
            if warnings:
                f.write(f"### Warnings ({len(warnings)})\n\n")
                for v in warnings:
                    f.write(f"- {v}\n")
                f.write("\n")
    
    # Write validation failures JSON
    validation_failures_path.parent.mkdir(parents=True, exist_ok=True)
    failures_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "critical_errors": [v for v in all_violations if "[CRITICAL]" in v],
        "violations": [v for v in all_violations if "[VIOLATION]" in v],
        "warnings": [v for v in all_violations if "[WARN]" in v],
        "total_count": len(all_violations),
    }
    with open(validation_failures_path, "w") as f:
        json.dump(failures_data, f, indent=2)
    
    print(f"\n[Output] Validation report: {validation_report_path}")
    print(f"[Output] Validation failures: {validation_failures_path}")
    
    # Final result
    print("=" * 60)
    if all_passed and len([v for v in all_violations if "[CRITICAL]" in v or "[VIOLATION]" in v]) == 0:
        print("[SUCCESS] All critical invariants passed")
        print("=" * 60)
        return 0
    else:
        critical_count = len([v for v in all_violations if "[CRITICAL]" in v])
        violation_count = len([v for v in all_violations if "[VIOLATION]" in v])
        warn_count = len([v for v in all_violations if "[WARN]" in v])
        
        print(f"[RESULT] Found {critical_count} critical errors, {violation_count} violations, {warn_count} warnings")
        print("=" * 60)
        
        if args.fail_on_violations and (critical_count > 0 or violation_count > 0):
            return 1
        
        return 0


if __name__ == "__main__":
    sys.exit(main())



