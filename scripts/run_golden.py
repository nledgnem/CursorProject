#!/usr/bin/env python3
"""Run golden run: deterministic, repeatable test pipeline."""

import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import yaml

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def run_command(cmd, description):
    """Run a command and return success status."""
    print("\n" + "=" * 60)
    print(description)
    print("=" * 60)
    print(f"Command: {' '.join(cmd)}")
    print("-" * 60)
    
    result = subprocess.run(cmd, check=False)
    
    if result.returncode != 0:
        print(f"\n[ERROR] {description} failed with exit code {result.returncode}")
        return False
    
    print(f"\n[SUCCESS] {description} completed")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run golden run: deterministic, repeatable test pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to config (default: configs/golden.yaml)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for golden run (default: outputs/golden_YYYYMMDD_HHMMSS)",
    )
    parser.add_argument(
        "--skip-qc",
        action="store_true",
        help="Skip QC step (use existing curated data)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation step",
    )
    parser.add_argument(
        "--fetch-perp-listings",
        action="store_true",
        help="Fetch Binance perp listings before building snapshots",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    script_dir = Path(__file__).parent
    
    # Determine config
    if args.config:
        config_path = args.config if args.config.is_absolute() else repo_root / args.config
    else:
        config_path = repo_root / "configs" / "golden.yaml"
    
    if not config_path.exists():
        print(f"[ERROR] Config file not found: {config_path}")
        sys.exit(1)
    
    # Load config to get top_n and base_asset
    with open(config_path) as f:
        config = yaml.safe_load(f)
    top_n = config.get("top_n", 20)
    base_asset = config.get("base_asset", "BTC")
    
    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir if args.output_dir.is_absolute() else repo_root / args.output_dir
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = repo_root / "outputs" / f"golden_{timestamp}"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine data directories (use output_dir for this run)
    curated_dir = output_dir / "data" / "curated"
    curated_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("GOLDEN RUN")
    print("=" * 60)
    print(f"Config: {config_path}")
    print(f"Output directory: {output_dir}")
    print(f"Curated data: {curated_dir}")
    print("=" * 60)
    
    # Step 1: QC (unless skipped)
    if not args.skip_qc:
        qc_cmd = [sys.executable, str(script_dir / "qc_curate.py")]
        qc_cmd.extend(["--raw-dir", str(repo_root / "data" / "raw")])
        qc_cmd.extend(["--out-dir", str(curated_dir)])
        qc_cmd.extend(["--outputs-dir", str(output_dir)])
        
        if not run_command(qc_cmd, "Step 1: QC Curation"):
            print("\n[ERROR] Golden run failed at QC step")
            sys.exit(1)
    else:
        print("\n" + "=" * 60)
        print("Step 1: QC Curation (SKIPPED)")
        print("=" * 60)
    
    # Step 1.5: Fetch Binance Perp Listings (optional)
    if args.fetch_perp_listings:
        perp_cmd = [sys.executable, str(script_dir / "fetch_binance_perp_listings.py")]
        perp_output = curated_dir / "perp_listings_binance.parquet"
        perp_cmd.extend(["--output", str(perp_output)])
        
        if not run_command(perp_cmd, "Step 1.5: Fetch Binance Perp Listings"):
            print("\n[WARN] Binance perp listings fetch failed, continuing...")
    
    # Step 2: Build Snapshots
    snapshot_cmd = [sys.executable, str(script_dir / "build_universe_snapshots.py")]
    snapshot_cmd.extend(["--config", str(config_path)])
    snapshot_cmd.extend(["--data-dir", str(curated_dir)])
    
    if not run_command(snapshot_cmd, "Step 2: Build Universe Snapshots"):
        print("\n[ERROR] Golden run failed at snapshot building step")
        sys.exit(1)
    
    # Step 2b: Validate Snapshots
    universe_eligibility_path = curated_dir / "universe_eligibility.parquet"
    snapshots_path = curated_dir / "universe_snapshots.parquet"
    
    validate_cmd = [sys.executable, str(script_dir / "validate_snapshots.py")]
    validate_cmd.extend(["--snapshots", str(snapshots_path)])
    validate_cmd.extend(["--universe-eligibility", str(universe_eligibility_path)])
    validate_cmd.extend(["--top-n", str(top_n)])
    
    if not run_command(validate_cmd, "Step 2b: Validate Snapshots"):
        print("\n[ERROR] Golden run failed at snapshot validation step")
        sys.exit(1)
    
    # Step 3: Run Backtest
    backtest_cmd = [sys.executable, str(script_dir / "run_backtest.py")]
    backtest_cmd.extend(["--config", str(config_path)])
    backtest_cmd.extend(["--data-dir", str(curated_dir)])
    backtest_cmd.extend(["--snapshots-path", str(snapshots_path)])
    backtest_cmd.extend(["--output-dir", str(output_dir)])
    
    if not run_command(backtest_cmd, "Step 3: Run Backtest"):
        print("\n[ERROR] Golden run failed at backtest step")
        sys.exit(1)
    
    # Step 4: Validate Run (invariant checks)
    if not args.skip_validation:
        results_path = output_dir / "backtest_results.csv"
        turnover_path = output_dir / "rebalance_turnover.csv"
        summary_path = output_dir / "run_summary.md"
        
        # Find blacklist and stablecoins
        blacklist_path = repo_root / "data" / "blacklist.csv"
        stablecoins_path = repo_root / "data" / "stablecoins.csv"
        perp_listings_path = curated_dir / "perp_listings_binance.parquet"
        
        validate_run_cmd = [sys.executable, str(script_dir / "validate_run.py")]
        validate_run_cmd.extend(["--universe", str(universe_eligibility_path)])
        validate_run_cmd.extend(["--basket", str(snapshots_path)])
        validate_run_cmd.extend(["--results", str(results_path)])
        validate_run_cmd.extend(["--turnover", str(turnover_path)])
        validate_run_cmd.extend(["--top-n", str(top_n)])
        validate_run_cmd.extend(["--base-asset", base_asset])
        validate_run_cmd.extend(["--summary-output", str(summary_path)])
        
        if blacklist_path.exists():
            validate_run_cmd.extend(["--blacklist", str(blacklist_path)])
        if stablecoins_path.exists():
            validate_run_cmd.extend(["--stablecoins", str(stablecoins_path)])
        if perp_listings_path.exists():
            validate_run_cmd.extend(["--perp-listings", str(perp_listings_path)])
        
        if not run_command(validate_run_cmd, "Step 4: Validate Run (Invariant Checks)"):
            print("\n[WARN] Some validation checks failed (see output above)")
    
    # Success!
    print("\n" + "=" * 60)
    print("GOLDEN RUN COMPLETE")
    print("=" * 60)
    print(f"\nOutput directory: {output_dir}")
    print("\nGenerated files:")
    print(f"  - Universe eligibility: {universe_eligibility_path}")
    print(f"  - Basket snapshots: {snapshots_path}")
    print(f"  - Backtest results: {output_dir / 'backtest_results.csv'}")
    print(f"  - Run summary: {output_dir / 'run_summary.md'}")
    print("\nTo verify reproducibility, run again and compare file hashes.")


if __name__ == "__main__":
    main()



