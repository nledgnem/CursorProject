"""Batch run multiple experiments (sweep)."""

import argparse
import yaml
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import logging
import traceback
import sys

import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_experiment_files(glob_pattern: str) -> List[Path]:
    """Find experiment YAML files matching glob pattern."""
    from glob import glob
    
    # Expand glob pattern
    matches = glob(glob_pattern, recursive=True)
    
    # Filter to YAML files
    yaml_files = [Path(f) for f in matches if f.endswith(('.yaml', '.yml'))]
    
    return sorted(yaml_files)


def run_sweep(
    glob_pattern: str,
    start_date: str,
    end_date: str,
    base_config: str = "majors_alts_monitor/config.yaml",
    fail_fast: bool = False,
) -> Dict[str, Any]:
    """
    Run batch of experiments matching glob pattern.
    
    Args:
        glob_pattern: Glob pattern to match experiment YAML files
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        base_config: Base config file path
        fail_fast: If True, stop on first failure; if False, continue to next
    
    Returns:
        Dict with summary: {succeeded: [...], failed: [...], total: N}
    """
    # Find experiment files
    experiment_files = find_experiment_files(glob_pattern)
    
    if len(experiment_files) == 0:
        logger.warning(f"No experiment files found matching: {glob_pattern}")
        return {"succeeded": [], "failed": [], "total": 0}
    
    logger.info(f"Found {len(experiment_files)} experiment files")
    logger.info(f"Running sweep: {start_date} to {end_date}")
    logger.info("=" * 80)
    
    succeeded = []
    failed = []
    
    for idx, exp_file in enumerate(experiment_files, 1):
        logger.info(f"\n[{idx}/{len(experiment_files)}] Running: {exp_file}")
        logger.info("-" * 80)
        
        # Load experiment spec to get metadata
        try:
            with open(exp_file) as f:
                exp_spec = yaml.safe_load(f)
            
            exp_id = exp_spec.get("experiment_id", exp_file.stem)
            title = exp_spec.get("title", exp_file.stem)
            
            logger.info(f"Experiment ID: {exp_id}")
            logger.info(f"Title: {title}")
            logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            logger.error(f"Failed to load experiment spec: {e}")
            failed.append({
                "file": str(exp_file),
                "error": str(e),
                "run_id": None,
            })
            if fail_fast:
                raise
            continue
        
        # Run experiment
        start_time = datetime.now()
        try:
            # Run the experiment using subprocess to avoid sys.argv conflicts
            result = subprocess.run(
                [
                    sys.executable, "-m", "majors_alts_monitor.run",
                    "--start", start_date,
                    "--end", end_date,
                    "--config", base_config,
                    "--experiment", str(exp_file),
                ],
                capture_output=False,  # Let output go to console for progress visibility
                check=True,
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"✓ Completed in {duration:.1f}s")
            succeeded.append({
                "file": str(exp_file),
                "experiment_id": exp_id,
                "title": title,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            })
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            error_msg = str(e)
            error_traceback = traceback.format_exc()
            
            logger.error(f"✗ Failed after {duration:.1f}s: {error_msg}")
            logger.debug(error_traceback)
            
            # Try to get run_id from experiment manager if available
            run_id = None
            try:
                # Run ID would be generated, but we can't easily get it here
                # The error will be logged in the run directory if it was created
                pass
            except:
                pass
            
            failed.append({
                "file": str(exp_file),
                "experiment_id": exp_id,
                "title": title,
                "error": error_msg,
                "traceback": error_traceback,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "run_id": run_id,
            })
            
            if fail_fast:
                logger.error("Fail-fast enabled, stopping sweep")
                raise
        
        logger.info("-" * 80)
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("SWEEP SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total experiments: {len(experiment_files)}")
    logger.info(f"Succeeded: {len(succeeded)}")
    logger.info(f"Failed: {len(failed)}")
    
    if succeeded:
        logger.info("\nSucceeded experiments:")
        for item in succeeded:
            logger.info(f"  ✓ {item['experiment_id']}: {item['title']} ({item['duration_seconds']:.1f}s)")
    
    if failed:
        logger.info("\nFailed experiments:")
        for item in failed:
            logger.info(f"  ✗ {item['experiment_id']}: {item['title']} - {item['error']}")
    
    logger.info("=" * 80)
    
    return {
        "succeeded": succeeded,
        "failed": failed,
        "total": len(experiment_files),
    }


def main():
    parser = argparse.ArgumentParser(description="Run batch of experiments (sweep)")
    parser.add_argument("--glob", type=str, required=True, help="Glob pattern for experiment YAML files (e.g., 'experiments/msm/*.yaml')")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--config", type=str, default="majors_alts_monitor/config.yaml", help="Base config file path")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first failure")
    
    args = parser.parse_args()
    
    run_sweep(
        glob_pattern=args.glob,
        start_date=args.start,
        end_date=args.end,
        base_config=args.config,
        fail_fast=args.fail_fast,
    )


if __name__ == "__main__":
    main()
