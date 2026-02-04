#!/usr/bin/env python3
"""
One-command pipeline orchestrator: Data → QC → Snapshots → Backtest → Validation → Publish.

This is the single, repeatable "orchestrator" that:
1. Builds clean datasets
2. Builds point-in-time universe + baskets
3. Runs backtests/analyses
4. Emits auditable artifacts + sanity checks
"""

import sys
import argparse
import subprocess
import json
import hashlib
import platform
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import yaml
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.metadata import get_git_commit_hash, get_file_hash


def run_command(cmd: List[str], description: str) -> bool:
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


def resolve_path(path: Path, repo_root: Path) -> Path:
    """Resolve a path relative to repo_root if not absolute."""
    if path.is_absolute():
        return path
    return repo_root / path


def compute_config_hash(config: Dict[str, Any]) -> str:
    """Compute hash of config dict for run ID."""
    config_str = json.dumps(config, sort_keys=True)
    return hashlib.sha256(config_str.encode()).hexdigest()[:16]


def create_run_id(config: Dict[str, Any]) -> str:
    """Create unique run ID from timestamp + config hash."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    config_hash = compute_config_hash(config)
    return f"{timestamp}_{config_hash}"


def log_environment(run_dir: Path) -> Dict[str, Any]:
    """Log environment info (Python version, packages, OS, git commit)."""
    env_info = {
        "python_version": sys.version,
        "platform": platform.platform(),
        "python_executable": sys.executable,
    }
    
    # Try to get git commit hash
    repo_root = run_dir.parent.parent
    git_hash = get_git_commit_hash(repo_root)
    if git_hash:
        env_info["git_commit_hash"] = git_hash
    
    # Try to get package versions (if requirements.txt exists)
    try:
        import pkg_resources
        packages = {}
        for dist in pkg_resources.working_set:
            packages[dist.project_name] = dist.version
        env_info["installed_packages"] = packages
    except Exception:
        pass
    
    return env_info


def setup_duckdb_views(
    run_dir: Path,
    curated_dir: Path,
    snapshots_dir: Path,
    outputs_dir: Path,
    data_lake_dir: Optional[Path] = None,
) -> None:
    """Set up DuckDB views for querying outputs."""
    try:
        import duckdb
        
        db_path = run_dir / "research.duckdb"
        conn = duckdb.connect(str(db_path))
        
        print("\n[Setup] Creating DuckDB views...")
        
        # If data lake directory exists, use fact tables; otherwise use wide format
        if data_lake_dir and data_lake_dir.exists():
            # Use data lake fact tables and dimension tables
            from src.data_lake.build_duckdb import build_duckdb_views as build_data_lake_views
            build_data_lake_views(data_lake_dir, db_path, snapshots_dir, outputs_dir)
            # build_data_lake_views handles connection and closes it
        else:
            # Fall back to wide format (legacy)
            # Prices, market cap, volume
            for name, filename in [
                ("prices_daily", "prices_daily.parquet"),
                ("marketcap_daily", "marketcap_daily.parquet"),
                ("volume_daily", "volume_daily.parquet"),
            ]:
                file_path = curated_dir / filename
                if file_path.exists():
                    file_path_posix = file_path.resolve().as_posix()
                    conn.execute(f"""
                        CREATE OR REPLACE VIEW {name} AS
                        SELECT * FROM read_parquet('{file_path_posix}')
                    """)
                    print(f"  Created view: {name}")
            
            # Universe eligibility
            universe_path = snapshots_dir / "universe_eligibility.parquet"
            if universe_path.exists():
                universe_path_posix = universe_path.resolve().as_posix()
                conn.execute(f"""
                    CREATE OR REPLACE VIEW universe_eligibility AS
                    SELECT * FROM read_parquet('{universe_path_posix}')
                """)
                print(f"  Created view: universe_eligibility")
            
            # Basket snapshots
            basket_path = snapshots_dir / "universe_snapshots.parquet"
            if basket_path.exists():
                basket_path_posix = basket_path.resolve().as_posix()
                conn.execute(f"""
                    CREATE OR REPLACE VIEW basket_snapshots AS
                    SELECT * FROM read_parquet('{basket_path_posix}')
                """)
                conn.execute("""
                    CREATE OR REPLACE VIEW universe_snapshots AS
                    SELECT * FROM basket_snapshots
                """)
                print(f"  Created view: basket_snapshots (and universe_snapshots)")
            
            # Repair log
            repair_log_path = outputs_dir / "repair_log.parquet"
            if repair_log_path.exists():
                repair_log_path_posix = repair_log_path.resolve().as_posix()
                conn.execute(f"""
                    CREATE OR REPLACE VIEW repair_log AS
                    SELECT * FROM read_parquet('{repair_log_path_posix}')
                """)
                print(f"  Created view: repair_log")
            
            # Backtest results
            results_path = outputs_dir / "backtest_results.csv"
            if results_path.exists():
                results_path_posix = results_path.resolve().as_posix()
                conn.execute(f"""
                    CREATE OR REPLACE VIEW backtest_results AS
                    SELECT * FROM read_csv_auto('{results_path_posix}')
                """)
                print(f"  Created view: backtest_results")
            
            conn.close()
            print(f"  Database saved to: {db_path}")
        
    except ImportError:
        print("[WARN] DuckDB not available, skipping view creation")
    except Exception as e:
        print(f"[WARN] Failed to create DuckDB views: {e}")


def copy_manager_queries(run_dir: Path) -> None:
    """Copy manager queries to run directory."""
    repo_root = run_dir.parent.parent
    queries_source = repo_root / "docs" / "query_examples.md"
    
    if queries_source.exists():
        queries_dest = run_dir / "manager_queries.md"
        try:
            import shutil
            shutil.copy2(queries_source, queries_dest)
            print(f"  Copied manager queries to: {queries_dest}")
        except Exception as e:
            print(f"[WARN] Failed to copy manager queries: {e}")


def compute_manager_summary(
    config: Dict[str, Any],
    step_statuses: Dict[str, str],
    file_paths: Dict[str, Path],
) -> Dict[str, Any]:
    """Compute manager-facing summary metrics from artifacts."""
    summary = {
        "qc_run": step_statuses.get("qc_curation") == "SUCCESS",
        "validation_run": step_statuses.get("validation") not in (None, "SKIPPED"),
        "validation_status": step_statuses.get("validation"),  # SUCCESS, WARN, FAILED, or SKIPPED
        "mapping_validation_run": step_statuses.get("mapping_validation") == "SUCCESS",
    }
    
    # Extract time range from config
    if "start_date" in config and "end_date" in config:
        summary["time_range"] = {
            "start_date": config["start_date"],
            "end_date": config["end_date"],
        }
    
    # Extract metrics from universe_eligibility if available
    if file_paths.get("universe_eligibility") and file_paths["universe_eligibility"].exists():
        try:
            df = pd.read_parquet(file_paths["universe_eligibility"])
            date_col = "snapshot_date" if "snapshot_date" in df.columns else "rebalance_date"
            
            if date_col in df.columns:
                rebalance_dates = sorted(df[date_col].unique())
                summary["rebalance_dates_count"] = len(rebalance_dates)
                
                # Average eligible count per rebalance date
                eligible_counts = []
                coverage_values = []
                for rb_date in rebalance_dates:
                    date_df = df[df[date_col] == rb_date]
                    eligible_count = len(date_df[date_df.get("eligible", pd.Series([False] * len(date_df))) == True])
                    eligible_counts.append(eligible_count)
                    
                    # Coverage: eligible_with_price / eligible_assets
                    if "eligible" in date_df.columns and "has_price" in date_df.columns:
                        eligible_df = date_df[date_df["eligible"] == True]
                        if len(eligible_df) > 0:
                            eligible_with_price = len(eligible_df[eligible_df["has_price"] == True])
                            coverage = (eligible_with_price / len(eligible_df)) * 100
                            coverage_values.append(coverage)
                
                if eligible_counts:
                    summary["avg_eligible_count"] = sum(eligible_counts) / len(eligible_counts)
                
                if coverage_values:
                    summary["rebalance_coverage"] = {
                        "min_pct": min(coverage_values),
                        "median_pct": sorted(coverage_values)[len(coverage_values) // 2] if coverage_values else None,
                    }
        except Exception as e:
            summary["eligibility_metrics_error"] = str(e)
    
    # Extract metrics from snapshots if available
    if file_paths.get("basket_snapshots") and file_paths["basket_snapshots"].exists():
        try:
            snapshots_df = pd.read_parquet(file_paths["basket_snapshots"])
            top_n = config.get("top_n", 30)
            date_col = "rebalance_date" if "rebalance_date" in snapshots_df.columns else "snapshot_date"
            
            if date_col in snapshots_df.columns:
                # Get all rebalance dates from config (including skipped ones)
                all_rebalance_dates = []
                if "start_date" in config and "end_date" in config:
                    from src.universe.snapshot import get_rebalance_dates
                    from datetime import date as date_type
                    start_date = pd.to_datetime(config["start_date"]).date()
                    end_date = pd.to_datetime(config["end_date"]).date()
                    frequency = config.get("rebalance_frequency", "monthly")
                    rebalance_day = config.get("rebalance_day", 1)
                    all_rebalance_dates = get_rebalance_dates(start_date, end_date, frequency, rebalance_day)
                
                # Get dates that actually have snapshots
                snapshot_dates_raw = sorted(snapshots_df[date_col].unique())
                # Convert to date objects for comparison
                snapshot_dates = []
                for sd in snapshot_dates_raw:
                    if isinstance(sd, pd.Timestamp):
                        snapshot_dates.append(sd.date())
                    elif isinstance(sd, date_type):
                        snapshot_dates.append(sd)
                    else:
                        snapshot_dates.append(pd.to_datetime(sd).date())
                snapshot_date_set = set(snapshot_dates)
                
                # Metric 1: % of all rebalance dates that have any snapshot
                if all_rebalance_dates:
                    dates_with_snapshots = len([d for d in all_rebalance_dates if d in snapshot_date_set])
                    summary["pct_rebalance_dates_with_any_snapshot"] = (
                        (dates_with_snapshots / len(all_rebalance_dates) * 100) if all_rebalance_dates else 0
                    )
                else:
                    # Fallback: use snapshot dates if we can't determine all dates
                    summary["pct_rebalance_dates_with_any_snapshot"] = 100.0
                
                # Metric 2: % of dates with snapshots that achieved full top_n
                full_top_n_count = 0
                for rb_date in snapshot_dates_raw:
                    date_snapshots = snapshots_df[snapshots_df[date_col] == rb_date]
                    if len(date_snapshots) >= top_n:
                        full_top_n_count += 1
                
                summary["pct_rebalance_dates_with_full_top_n_given_snapshot"] = (
                    (full_top_n_count / len(snapshot_dates_raw) * 100) if snapshot_dates_raw else 0
                )
                # Keep old metric name for backwards compatibility
                summary["pct_rebalance_dates_with_full_top_n"] = summary["pct_rebalance_dates_with_full_top_n_given_snapshot"]
        except Exception as e:
            summary["snapshot_metrics_error"] = str(e)
    
    # Count assets dropped for missing data (from universe_eligibility)
    if file_paths.get("universe_eligibility") and file_paths["universe_eligibility"].exists():
        try:
            df = pd.read_parquet(file_paths["universe_eligibility"])
            if "exclusion_reason" in df.columns:
                missing_data_reasons = ["no_price_data", "no_volume_data", "no_marketcap_data"]
                dropped_for_missing_data = len(df[df["exclusion_reason"].isin(missing_data_reasons)])
                summary["assets_dropped_for_missing_data"] = dropped_for_missing_data
        except Exception:
            pass
    
    return summary


def compute_overall_status(
    mode: str,
    step_statuses: Dict[str, str],
    validation_status: str,
) -> tuple[str, List[str]]:
    """
    Compute overall_status based on mode and step statuses.
    
    Returns:
        (overall_status, skipped_steps)
    """
    # Required steps for research mode
    required_steps = [
        "qc_curation",
        "mapping_validation",
        "snapshots",
        "backtest",
        "validation",
    ]
    
    skipped_steps = [step for step, status in step_statuses.items() if status == "SKIPPED"]
    failed_steps = [step for step, status in step_statuses.items() if status == "FAILED"]
    
    if mode == "research":
        # In research mode, required steps must succeed
        required_skipped = [step for step in required_steps if step in skipped_steps]
        required_failed = [step for step in required_steps if step in failed_steps]
        
        if required_failed:
            return "FAIL", skipped_steps
        elif required_skipped:
            return "FAIL", skipped_steps  # Required steps cannot be skipped in research mode
        elif validation_status == "FAIL":
            return "FAIL", skipped_steps
        elif validation_status == "WARN" or skipped_steps:
            return "PASS_WITH_WARNINGS", skipped_steps
        else:
            return "PASS", skipped_steps
    else:  # smoke mode
        # In smoke mode, skips are allowed but downgrade status
        if failed_steps:
            return "FAIL", skipped_steps
        elif validation_status == "FAIL":
            return "FAIL", skipped_steps
        elif skipped_steps or validation_status == "WARN":
            return "PASS_WITH_WARNINGS", skipped_steps
        else:
            return "PASS", skipped_steps


def write_final_run_receipt(
    run_dir: Path,
    config: Dict[str, Any],
    config_path: Path,
    run_id: str,
    env_info: Dict[str, Any],
    step_statuses: Dict[str, str],
    file_paths: Dict[str, Path],
    validation_status: str,
    validation_failures: List[str],
    mode: str,
) -> None:
    """Write final run receipt JSON (the audit trail)."""
    # Compute overall status and skipped steps
    overall_status, skipped_steps = compute_overall_status(mode, step_statuses, validation_status)
    
    # Compute manager summary
    manager_summary = compute_manager_summary(config, step_statuses, file_paths)
    manager_summary["mode"] = mode
    
    # Determine run mode (FULL vs INCREMENTAL)
    # In research mode, this is always FULL. Incremental runs are handled separately.
    run_mode_detail = "FULL"
    appended_ranges = {}
    
    receipt = {
        "run_id": run_id,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "run_mode_detail": run_mode_detail,  # FULL or INCREMENTAL
        "overall_status": overall_status,
        "skipped_steps": skipped_steps,
        "config": {
            "path": str(config_path.relative_to(run_dir.parent.parent)) if config_path.is_relative_to(run_dir.parent.parent) else str(config_path),
            "hash": compute_config_hash(config),
            "content": config,  # Include full config for reproducibility
        },
        "environment": env_info,
        "step_statuses": step_statuses,
        "manager_summary": manager_summary,
        "output_files": {},
        "input_files": {},
        "validation": {
            "status": validation_status,  # PASS, WARN, FAIL
            "failures": validation_failures,
        },
        "incremental_metadata": {
            "run_mode": run_mode_detail,
            "appended_date_ranges": appended_ranges,
        },
        "known_limitations": [
            "Binance-only perp proxy (v0)",
            "CoinGecko data source",
            "Gap fill assumptions in backtest layer",
        ],
    }
    
    # Add file hashes and paths
    for name, path in file_paths.items():
        if path and path.exists():
            try:
                receipt["output_files"][name] = {
                    "path": str(path.relative_to(run_dir)) if path.is_relative_to(run_dir) else str(path),
                    "hash": get_file_hash(path),
                    "size_bytes": path.stat().st_size,
                }
            except Exception as e:
                receipt["output_files"][name] = {
                    "path": str(path),
                    "error": str(e),
                }
    
    # Write receipt
    receipt_path = run_dir / "run_receipt.json"
    with open(receipt_path, "w") as f:
        json.dump(receipt, f, indent=2)
    
    print(f"\n[Receipt] Final run receipt saved to: {receipt_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Run complete pipeline: Data → QC → Snapshots → Backtest → Validation → Publish",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline run (uses default: configs/golden.yaml)
  python scripts/run_pipeline.py

  # Full pipeline run with custom config
  python scripts/run_pipeline.py --config configs/strategy_benchmark.yaml

  # Skip data download (use existing raw data)
  python scripts/run_pipeline.py --skip-download

  # Skip QC (use existing curated data)
  python scripts/run_pipeline.py --skip-qc

  # Fast iteration (skip multiple steps)
  python scripts/run_pipeline.py --skip-download --skip-qc --skip-snapshots
        """,
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to strategy config YAML (default: configs/golden.yaml)",
    )
    
    # Data acquisition
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip data download step (use existing raw data)",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Force data download even if raw data exists",
    )
    
    # QC options
    parser.add_argument(
        "--skip-qc",
        action="store_true",
        help="Skip QC step (use existing curated data)",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Directory containing raw parquet files (default: data/raw)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Directory for curated parquet output files (default: data/curated)",
    )
    parser.add_argument(
        "--qc-config",
        type=Path,
        default=None,
        help="Path to QC config YAML file (optional)",
    )
    
    # Perp listings
    parser.add_argument(
        "--fetch-perp-listings",
        action="store_true",
        help="Fetch Binance perp listings before building snapshots",
    )
    parser.add_argument(
        "--skip-perp-listings",
        action="store_true",
        help="Skip perp listings fetch (use existing or allowlist-only)",
    )
    parser.add_argument(
        "--coinglass-api-key",
        type=str,
        default=None,
        help="Coinglass API key for fetching funding rates (optional)",
    )
    parser.add_argument(
        "--skip-funding",
        action="store_true",
        help="Skip fetching Coinglass funding rates",
    )
    
    # Snapshot options
    parser.add_argument(
        "--skip-snapshots",
        action="store_true",
        help="Skip snapshot building step",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Data directory for snapshot builder (default: data/curated)",
    )
    
    # Backtest options
    parser.add_argument(
        "--skip-backtest",
        action="store_true",
        help="Skip backtest step",
    )
    parser.add_argument(
        "--backtest-data-dir",
        type=Path,
        default=None,
        help="Data directory for backtest (default: data/curated)",
    )
    parser.add_argument(
        "--snapshots-path",
        type=Path,
        default=None,
        help="Path to universe snapshots file (default: data/curated/universe_snapshots.parquet)",
    )
    
    # Validation options
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run invariant checks after pipeline completes (recommended)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation step",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Exit with error if validation fails (default: warn only)",
    )
    
    # Output options
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Run directory (default: outputs/runs/<run_id>)",
    )
    parser.add_argument(
        "--skip-duckdb",
        action="store_true",
        help="Skip DuckDB view creation",
    )
    parser.add_argument(
        "--allow-mapping-errors",
        action="store_true",
        help="Allow pipeline to continue even if mapping validation fails (not recommended)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["smoke", "research"],
        default="research",
        help="Run mode: 'smoke' allows skipping steps, 'research' requires all steps (default: research)",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental mode: only fetch/convert new dates (auto-detect from existing data)",
    )
    
    args = parser.parse_args()
    
    repo_root = Path(__file__).parent.parent
    script_dir = Path(__file__).parent
    
    # Resolve config path (default to golden.yaml if not provided)
    if args.config:
        config_path = resolve_path(args.config, repo_root)
    else:
        config_path = repo_root / "configs" / "golden.yaml"
        print(f"[INFO] No config specified, using default: {config_path}")
    
    if not config_path.exists():
        print(f"[ERROR] Config file not found: {config_path}")
        sys.exit(1)
    
    # Load config
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Create run ID and run directory
    run_id = create_run_id(config)
    if args.run_dir:
        run_dir = resolve_path(args.run_dir, repo_root)
    else:
        run_dir = repo_root / "outputs" / "runs" / run_id
    
    run_dir.mkdir(parents=True, exist_ok=True)
    
    # Log environment
    env_info = log_environment(run_dir)
    env_path = run_dir / "environment.json"
    with open(env_path, "w") as f:
        json.dump(env_info, f, indent=2)
    
    # Save config copy to run directory
    config_copy_path = run_dir / "config.yaml"
    shutil.copy2(config_path, config_copy_path)
    
    print("=" * 60)
    print("PIPELINE RUN")
    print("=" * 60)
    print(f"Run ID: {run_id}")
    print(f"Mode: {args.mode.upper()}")
    print(f"Config: {config_path}")
    print(f"Run directory: {run_dir}")
    print(f"Python: {sys.version.split()[0]}")
    if env_info.get("git_commit_hash"):
        print(f"Git commit: {env_info['git_commit_hash']}")
    print("=" * 60)
    
    # In research mode, warn if skip flags are used
    if args.mode == "research":
        skip_flags = [
            ("--skip-download", args.skip_download),
            ("--skip-qc", args.skip_qc),
            ("--skip-snapshots", args.skip_snapshots),
            ("--skip-backtest", args.skip_backtest),
            ("--skip-validation", args.skip_validation),
        ]
        used_skip_flags = [flag for flag, used in skip_flags if used]
        if used_skip_flags:
            print(f"\n[WARN] Research mode with skip flags: {', '.join(used_skip_flags)}")
            print("[WARN] Required steps cannot be skipped in research mode - pipeline may fail")
    
    # Track step statuses
    step_statuses = {}
    file_paths = {}
    
    # Determine directories
    raw_dir = resolve_path(args.raw_dir, repo_root) if args.raw_dir else repo_root / "data" / "raw"
    curated_dir = resolve_path(args.out_dir, repo_root) if args.out_dir else repo_root / "data" / "curated"
    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    
    # Step 0: Data Acquisition (optional)
    if not args.skip_download:
        # Check if raw data exists
        required_raw_files = [
            raw_dir / "prices_daily.parquet",
            raw_dir / "marketcap_daily.parquet",
            raw_dir / "volume_daily.parquet",
        ]
        raw_data_exists = all(f.exists() for f in required_raw_files)
        
        if not raw_data_exists or args.force_download:
            download_cmd = [sys.executable, str(script_dir / "download_data.py")]
            if args.incremental and not args.force_download:
                download_cmd.append("--incremental")
            # Note: download_data.py currently saves to curated, but we want raw
            # For now, we'll let it save to curated and copy/move if needed
            # TODO: Update download_data.py to support --output-dir
            
            if run_command(download_cmd, "Step 0: Data Acquisition"):
                step_statuses["data_acquisition"] = "SUCCESS"
            else:
                step_statuses["data_acquisition"] = "FAILED"
                if not raw_data_exists:
                    print("[ERROR] Data download failed and no existing raw data found")
                    sys.exit(1)
        else:
            print("\n" + "=" * 60)
            print("Step 0: Data Acquisition (SKIPPED)")
            print("=" * 60)
            print("Raw data already exists (use --force-download to re-download)")
            step_statuses["data_acquisition"] = "SKIPPED"
    else:
        print("\n" + "=" * 60)
        print("Step 0: Data Acquisition (SKIPPED)")
        print("=" * 60)
        step_statuses["data_acquisition"] = "SKIPPED"
    
    # Step 1: QC Curation
    if args.mode == "research" and args.skip_qc:
        print("\n[ERROR] QC curation cannot be skipped in research mode")
        sys.exit(1)
    
    if not args.skip_qc:
        qc_cmd = [sys.executable, str(script_dir / "qc_curate.py")]
        qc_cmd.extend(["--raw-dir", str(raw_dir)])
        qc_cmd.extend(["--out-dir", str(curated_dir)])
        qc_cmd.extend(["--outputs-dir", str(outputs_dir)])
        if args.qc_config:
            qc_cmd.extend(["--config", str(resolve_path(args.qc_config, repo_root))])
        
        if run_command(qc_cmd, "Step 1: QC Curation"):
            step_statuses["qc_curation"] = "SUCCESS"
            file_paths["qc_report"] = outputs_dir / "qc_report.md"
            file_paths["repair_log"] = outputs_dir / "repair_log.parquet"
            file_paths["qc_metadata"] = outputs_dir / "run_metadata_qc.json"
        else:
            step_statuses["qc_curation"] = "FAILED"
            print("\n[ERROR] Pipeline failed at QC step")
            sys.exit(1)
    else:
        print("\n" + "=" * 60)
        print("Step 1: QC Curation (SKIPPED)")
        print("=" * 60)
        step_statuses["qc_curation"] = "SKIPPED"
        
        # Verify curated data exists
        required_curated = [
            curated_dir / "prices_daily.parquet",
            curated_dir / "marketcap_daily.parquet",
            curated_dir / "volume_daily.parquet",
        ]
        missing = [f for f in required_curated if not f.exists()]
        if missing:
            print(f"[ERROR] QC skipped but required curated files not found:")
            for f in missing:
                print(f"  - {f}")
            sys.exit(1)
    
    # Step 1.5: Convert to Data Lake Format (Fact Tables)
    data_lake_dir = curated_dir / "data_lake"
    if not args.skip_qc or all((curated_dir / f"{name}_daily.parquet").exists() for name in ["prices", "marketcap", "volume"]):
        convert_cmd = [sys.executable, str(script_dir / "convert_to_fact_tables.py")]
        convert_cmd.extend(["--curated-dir", str(curated_dir)])
        convert_cmd.extend(["--data-lake-dir", str(data_lake_dir)])
        convert_cmd.extend(["--stablecoins", str(repo_root / "data" / "stablecoins.csv")])
        if args.incremental:
            convert_cmd.append("--incremental")
        
        # Try to find perp listings (may exist from previous run or be fetched later)
        perp_listings_path = None
        for default_path in [
            raw_dir / "perp_listings_binance.parquet",
            curated_dir / "perp_listings_binance.parquet",
            repo_root / "outputs" / "perp_listings_binance.parquet",
        ]:
            if default_path.exists():
                perp_listings_path = default_path
                break
        
        if perp_listings_path:
            convert_cmd.extend(["--perp-listings", str(perp_listings_path)])
        
        if run_command(convert_cmd, "Step 1.5: Convert to Data Lake Format"):
            step_statuses["data_lake_conversion"] = "SUCCESS"
            file_paths["data_lake_dir"] = data_lake_dir
        else:
            step_statuses["data_lake_conversion"] = "FAILED"
            print("[WARN] Data lake conversion failed, continuing with wide format")
    else:
        print("\n" + "=" * 60)
        print("Step 1.5: Convert to Data Lake Format (SKIPPED)")
        print("=" * 60)
        step_statuses["data_lake_conversion"] = "SKIPPED"
    
    # Step 1.6: Validate Mappings (Data Lake)
    if args.mode == "research" and not data_lake_dir.exists():
        print("\n[ERROR] Data lake directory not found - mapping validation required in research mode")
        sys.exit(1)
    
    if data_lake_dir.exists():
        validate_mapping_cmd = [sys.executable, str(script_dir / "validate_mapping.py")]
        validate_mapping_cmd.extend(["--data-lake-dir", str(data_lake_dir)])
        validate_mapping_cmd.extend(["--min-coverage", "85.0"])
        validate_mapping_cmd.extend(["--fail-on-errors"])  # Fail pipeline if validation fails
        
        if run_command(validate_mapping_cmd, "Step 1.6: Validate Data Lake Mappings"):
            step_statuses["mapping_validation"] = "SUCCESS"
            file_paths["mapping_validation"] = data_lake_dir / "mapping_validation.json"
        else:
            step_statuses["mapping_validation"] = "FAILED"
            print("\n[ERROR] Mapping validation failed - check coverage and uniqueness")
            if args.mode == "research":
                print("[ERROR] Mapping validation is required in research mode")
                sys.exit(1)
            elif not args.allow_mapping_errors:
                sys.exit(1)
    else:
        if args.mode == "research":
            print("\n[ERROR] Data lake directory not found - mapping validation required in research mode")
            sys.exit(1)
        print("\n" + "=" * 60)
        print("Step 1.6: Validate Data Lake Mappings (SKIPPED)")
        print("=" * 60)
        step_statuses["mapping_validation"] = "SKIPPED"
    
    # Step 2: Derivatives Metadata (Perp Listings)
    if args.fetch_perp_listings and not args.skip_perp_listings:
        perp_cmd = [sys.executable, str(script_dir / "fetch_binance_perp_listings.py")]
        perp_output = curated_dir / "perp_listings_binance.parquet"
        perp_cmd.extend(["--output", str(perp_output)])
        if args.incremental:
            perp_cmd.append("--incremental")
        
        if run_command(perp_cmd, "Step 2: Fetch Binance Perp Listings"):
            step_statuses["perp_listings"] = "SUCCESS"
            file_paths["perp_listings"] = perp_output
        else:
            step_statuses["perp_listings"] = "FAILED"
            print("\n[WARN] Binance perp listings fetch failed, continuing with allowlist-only")
    else:
        print("\n" + "=" * 60)
        print("Step 2: Fetch Binance Perp Listings (SKIPPED)")
        print("=" * 60)
        step_statuses["perp_listings"] = "SKIPPED"
    
    # Step 2.5: Fetch Coinglass Funding Rates
    if not args.skip_funding and args.coinglass_api_key:
        funding_cmd = [sys.executable, str(script_dir / "fetch_coinglass_funding.py")]
        funding_output = data_lake_dir / "fact_funding.parquet"
        funding_cmd.extend(["--api-key", args.coinglass_api_key])
        funding_cmd.extend(["--output", str(funding_output)])
        if args.incremental:
            funding_cmd.append("--incremental")
        
        if run_command(funding_cmd, "Step 2.5: Fetch Coinglass Funding Rates"):
            step_statuses["funding_rates"] = "SUCCESS"
            file_paths["funding_rates"] = funding_output
        else:
            step_statuses["funding_rates"] = "FAILED"
            print("\n[WARN] Coinglass funding rates fetch failed, continuing without funding data")
    else:
        print("\n" + "=" * 60)
        print("Step 2.5: Fetch Coinglass Funding Rates (SKIPPED)")
        print("=" * 60)
        if not args.coinglass_api_key:
            print("  [INFO] No Coinglass API key provided (use --coinglass-api-key)")
        step_statuses["funding_rates"] = "SKIPPED"
    
    # Step 3: Universe + Basket Snapshots
    if args.mode == "research" and args.skip_snapshots:
        print("\n[ERROR] Snapshots cannot be skipped in research mode")
        sys.exit(1)
    
    if not args.skip_snapshots:
        snapshot_cmd = [sys.executable, str(script_dir / "build_universe_snapshots.py")]
        snapshot_cmd.extend(["--config", str(config_path)])
        
        data_dir = resolve_path(args.data_dir, repo_root) if args.data_dir else curated_dir
        snapshot_cmd.extend(["--data-dir", str(data_dir)])
        
        if run_command(snapshot_cmd, "Step 3: Build Universe + Basket Snapshots"):
            step_statuses["snapshots"] = "SUCCESS"
            
            # Determine snapshot paths
            snapshots_dir = data_dir
            file_paths["universe_eligibility"] = snapshots_dir / "universe_eligibility.parquet"
            file_paths["basket_snapshots"] = snapshots_dir / "universe_snapshots.parquet"
            file_paths["snapshots_metadata"] = snapshots_dir / "run_metadata_snapshots.json"
            
            # Validate snapshots
            top_n = config.get("top_n", 30)
            validate_cmd = [sys.executable, str(script_dir / "validate_snapshots.py")]
            validate_cmd.extend(["--snapshots", str(file_paths["basket_snapshots"])])
            validate_cmd.extend(["--universe-eligibility", str(file_paths["universe_eligibility"])])
            validate_cmd.extend(["--top-n", str(top_n)])
            
            if not run_command(validate_cmd, "Step 3b: Validate Snapshots"):
                print("\n[ERROR] Snapshot validation failed")
                sys.exit(1)
        else:
            step_statuses["snapshots"] = "FAILED"
            print("\n[ERROR] Pipeline failed at snapshot building step")
            sys.exit(1)
    else:
        print("\n" + "=" * 60)
        print("Step 3: Build Universe + Basket Snapshots (SKIPPED)")
        print("=" * 60)
        step_statuses["snapshots"] = "SKIPPED"
        
        # Determine snapshot paths from args or defaults
        if args.snapshots_path:
            snapshots_dir = args.snapshots_path.parent
            file_paths["basket_snapshots"] = args.snapshots_path
            file_paths["universe_eligibility"] = snapshots_dir / "universe_eligibility.parquet"
        else:
            snapshots_dir = resolve_path(args.data_dir, repo_root) if args.data_dir else curated_dir
            file_paths["basket_snapshots"] = snapshots_dir / "universe_snapshots.parquet"
            file_paths["universe_eligibility"] = snapshots_dir / "universe_eligibility.parquet"
    
    # Step 4: Backtest Run
    if args.mode == "research" and args.skip_backtest:
        print("\n[ERROR] Backtest cannot be skipped in research mode")
        sys.exit(1)
    
    if not args.skip_backtest:
        backtest_cmd = [sys.executable, str(script_dir / "run_backtest.py")]
        backtest_cmd.extend(["--config", str(config_path)])
        
        backtest_data_dir = resolve_path(args.backtest_data_dir, repo_root) if args.backtest_data_dir else curated_dir
        backtest_cmd.extend(["--data-dir", str(backtest_data_dir)])
        
        if args.snapshots_path:
            backtest_cmd.extend(["--snapshots-path", str(args.snapshots_path)])
        else:
            backtest_cmd.extend(["--snapshots-path", str(file_paths["basket_snapshots"])])
        
        backtest_cmd.extend(["--output-dir", str(outputs_dir)])
        
        if run_command(backtest_cmd, "Step 4: Run Backtest"):
            step_statuses["backtest"] = "SUCCESS"
            file_paths["backtest_results"] = outputs_dir / "backtest_results.csv"
            file_paths["backtest_report"] = outputs_dir / "report.md"
            file_paths["backtest_turnover"] = outputs_dir / "rebalance_turnover.csv"
            file_paths["backtest_metadata"] = outputs_dir / "run_metadata_backtest.json"
        else:
            step_statuses["backtest"] = "FAILED"
            print("\n[ERROR] Pipeline failed at backtest step")
            sys.exit(1)
    else:
        print("\n" + "=" * 60)
        print("Step 4: Run Backtest (SKIPPED)")
        print("=" * 60)
        step_statuses["backtest"] = "SKIPPED"
    
    # Step 5: Validation + Invariants
    validation_status = "SKIPPED"
    validation_failures = []
    
    # In research mode, validation is required (unless explicitly skipped, which will fail)
    if args.mode == "research" and args.skip_validation:
        print("\n[ERROR] Validation cannot be skipped in research mode")
        sys.exit(1)
    
    # Determine if validation should run:
    # - Research mode: run by default (unless skip_validation, which fails above)
    # - Smoke mode: only run if --validate is explicitly set
    should_validate = (args.mode == "research") or (args.validate and not args.skip_validation)
    
    if should_validate:
        # Find paths
        blacklist_path = repo_root / "data" / "blacklist.csv"
        stablecoins_path = repo_root / "data" / "stablecoins.csv"
        perp_listings_path = None
        for default_path in [
            curated_dir / "perp_listings_binance.parquet",
            raw_dir / "perp_listings_binance.parquet",
        ]:
            if default_path.exists():
                perp_listings_path = default_path
                break
        
        validate_run_cmd = [sys.executable, str(script_dir / "validate_run.py")]
        validate_run_cmd.extend(["--universe", str(file_paths["universe_eligibility"])])
        validate_run_cmd.extend(["--basket", str(file_paths["basket_snapshots"])])
        validate_run_cmd.extend(["--results", str(file_paths["backtest_results"])])
        validate_run_cmd.extend(["--turnover", str(file_paths["backtest_turnover"])])
        validate_run_cmd.extend(["--top-n", str(config.get("top_n", 30))])
        validate_run_cmd.extend(["--base-asset", config.get("base_asset", "BTC")])
        validate_run_cmd.extend(["--summary-output", str(outputs_dir / "run_summary.md")])
        
        if blacklist_path.exists():
            validate_run_cmd.extend(["--blacklist", str(blacklist_path)])
        if stablecoins_path.exists():
            validate_run_cmd.extend(["--stablecoins", str(stablecoins_path)])
        if perp_listings_path:
            validate_run_cmd.extend(["--perp-listings", str(perp_listings_path)])
        
        if args.fail_fast:
            validate_run_cmd.extend(["--fail-on-violations"])
        
        # Add validation report paths
        validate_run_cmd.extend(["--validation-report", str(outputs_dir / "validation_report.md")])
        validate_run_cmd.extend(["--validation-failures", str(outputs_dir / "validation_failures.json")])
        
        validation_failures_path = outputs_dir / "validation_failures.json"
        file_paths["validation_summary"] = outputs_dir / "run_summary.md"
        file_paths["validation_report"] = outputs_dir / "validation_report.md"
        file_paths["validation_failures"] = validation_failures_path
        
        if run_command(validate_run_cmd, "Step 5: Validation + Invariants"):
            # Parse validation failures JSON to determine actual status
            # validate_run.py returns 0 even with warnings, so we need to check the JSON
            validation_status = "PASS"
            step_statuses["validation"] = "SUCCESS"
            
            if validation_failures_path.exists():
                try:
                    with open(validation_failures_path) as f:
                        failures_data = json.load(f)
                    critical_count = len(failures_data.get("critical_errors", []))
                    violation_count = len(failures_data.get("violations", []))
                    warn_count = len(failures_data.get("warnings", []))
                    
                    if critical_count > 0 or violation_count > 0:
                        validation_status = "FAIL"
                        step_statuses["validation"] = "FAILED"
                        if args.fail_fast:
                            print("\n[ERROR] Validation found critical errors or violations and --fail-fast is set")
                            sys.exit(1)
                    elif warn_count > 0:
                        validation_status = "WARN"
                        step_statuses["validation"] = "WARN"
                except Exception as e:
                    print(f"[WARN] Failed to parse validation failures JSON: {e}")
        else:
            # Command failed (non-zero exit code)
            validation_status = "WARN"  # Default to warn unless critical failures
            step_statuses["validation"] = "WARN"
            
            # Try to parse failures JSON to get more details
            if validation_failures_path.exists():
                try:
                    with open(validation_failures_path) as f:
                        failures_data = json.load(f)
                    critical_count = len(failures_data.get("critical_errors", []))
                    violation_count = len(failures_data.get("violations", []))
                    
                    if critical_count > 0 or violation_count > 0:
                        validation_status = "FAIL"
                        step_statuses["validation"] = "FAILED"
                except Exception:
                    pass
            
            if args.fail_fast and step_statuses["validation"] == "FAILED":
                print("\n[ERROR] Validation failed and --fail-fast is set")
                sys.exit(1)
    else:
        print("\n" + "=" * 60)
        print("Step 5: Validation + Invariants (SKIPPED)")
        print("=" * 60)
        step_statuses["validation"] = "SKIPPED"
    
    # Step 6: Publish + Query Layer (DuckDB)
    if not args.skip_duckdb:
        print("\n" + "=" * 60)
        print("Step 6: Publish + Query Layer (DuckDB)")
        print("=" * 60)
        # Determine snapshots directory
        if args.snapshots_path:
            snapshots_dir_for_duckdb = args.snapshots_path.parent
        else:
            snapshots_dir_for_duckdb = resolve_path(args.data_dir, repo_root) if args.data_dir else curated_dir
        
        # Use data lake directory if it exists (from Step 1.5)
        data_lake_dir_for_duckdb = data_lake_dir if data_lake_dir.exists() else None
        
        setup_duckdb_views(run_dir, curated_dir, snapshots_dir_for_duckdb, outputs_dir, data_lake_dir_for_duckdb)
        copy_manager_queries(run_dir)
        step_statuses["publish"] = "SUCCESS"
        file_paths["duckdb_db"] = run_dir / "research.duckdb"
        file_paths["manager_queries"] = run_dir / "manager_queries.md"
    else:
        step_statuses["publish"] = "SKIPPED"
    
    # Step 7: Final Run Receipt
    print("\n" + "=" * 60)
    print("Step 7: Final Run Receipt")
    print("=" * 60)
    # Compute overall status
    overall_status, skipped_steps = compute_overall_status(args.mode, step_statuses, validation_status)
    
    write_final_run_receipt(
        run_dir,
        config,
        config_path,
        run_id,
        env_info,
        step_statuses,
        file_paths,
        validation_status,
        validation_failures,
        args.mode,
    )
    
    # Print final status
    print(f"\nOverall Status: {overall_status}")
    if skipped_steps:
        print(f"Skipped Steps: {', '.join(skipped_steps)}")
    if overall_status == "FAIL":
        print("\n[ERROR] Pipeline failed - check step_statuses in run_receipt.json")
        sys.exit(1)
    elif overall_status == "PASS_WITH_WARNINGS":
        print("\n[WARN] Pipeline completed with warnings - review skipped steps")
    
    # Success!
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"\nRun ID: {run_id}")
    print(f"Mode: {args.mode.upper()}")
    print(f"Overall Status: {overall_status}")
    print(f"Run directory: {run_dir}")
    print(f"Validation status: {validation_status}")
    print("\nGenerated artifacts:")
    print(f"  - Run receipt: {run_dir / 'run_receipt.json'}")
    print(f"  - Config copy: {run_dir / 'config.yaml'}")
    print(f"  - Environment: {run_dir / 'environment.json'}")
    if file_paths.get("universe_eligibility"):
        print(f"  - Universe eligibility: {file_paths['universe_eligibility']}")
    if file_paths.get("basket_snapshots"):
        print(f"  - Basket snapshots: {file_paths['basket_snapshots']}")
    if file_paths.get("backtest_results"):
        print(f"  - Backtest results: {file_paths['backtest_results']}")
    if file_paths.get("validation_summary"):
        print(f"  - Validation summary: {file_paths['validation_summary']}")
    if file_paths.get("duckdb_db"):
        print(f"  - DuckDB database: {file_paths['duckdb_db']}")
    print("\nTo query results:")
    print(f"  python scripts/query_duckdb.py --db {file_paths.get('duckdb_db', 'outputs/research.duckdb')}")


if __name__ == "__main__":
    main()
