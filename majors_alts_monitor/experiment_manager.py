"""Experiment management: run manifests, catalog, stability metrics."""

import json
import yaml
from pathlib import Path
from datetime import date, datetime
from typing import Dict, Optional, Any
import polars as pl
import subprocess
import hashlib
import logging

logger = logging.getLogger(__name__)


def get_git_commit_hash() -> Optional[str]:
    """Get current git commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def generate_run_id(experiment_id: str, timestamp: Optional[datetime] = None) -> str:
    """Generate unique run ID."""
    if timestamp is None:
        timestamp = datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    return f"{experiment_id}_{timestamp_str}"


class ExperimentManager:
    """Manages experiment runs, manifests, and catalog."""
    
    def __init__(self, runs_dir: Path = Path("runs"), catalog_path: Path = Path("catalog/catalog.parquet")):
        self.runs_dir = runs_dir
        self.catalog_path = catalog_path
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
    
    def create_run_directory(self, run_id: str) -> Path:
        """Create directory for a run."""
        run_dir = self.runs_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir
    
    def write_manifest(
        self,
        run_id: str,
        experiment_spec: Dict[str, Any],
        resolved_config: Dict[str, Any],
        data_snapshot_dates: Dict[str, Any],
    ) -> Path:
        """Write manifest.json with fully resolved config + git commit + lake snapshot dates."""
        run_dir = self.create_run_directory(run_id)
        manifest_path = run_dir / "manifest.json"
        
        git_commit = get_git_commit_hash()
        
        manifest = {
            "run_id": run_id,
            "experiment_id": experiment_spec.get("experiment_id"),
            "title": experiment_spec.get("title"),
            "category_path": experiment_spec.get("category_path"),
            "timestamp": datetime.now().isoformat(),
            "git_commit": git_commit,
            "experiment_spec": experiment_spec,
            "resolved_config": resolved_config,
            "data_snapshot_dates": data_snapshot_dates,
        }
        
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2, default=str)
        
        logger.info(f"Written manifest: {manifest_path}")
        return manifest_path
    
    def write_metrics(
        self,
        run_id: str,
        metrics: Dict[str, Any],
    ) -> Path:
        """Write metrics.json with headline stats."""
        run_dir = self.create_run_directory(run_id)
        metrics_path = run_dir / "metrics.json"
        
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=2, default=str)
        
        logger.info(f"Written metrics: {metrics_path}")
        return metrics_path
    
    def write_timeseries(
        self,
        run_id: str,
        regime_timeseries: pl.DataFrame,
        returns: pl.DataFrame,
    ) -> Dict[str, Path]:
        """Write timeseries parquet files."""
        run_dir = self.create_run_directory(run_id)
        
        regime_path = run_dir / "regime_timeseries.parquet"
        returns_path = run_dir / "returns.parquet"
        
        regime_timeseries.write_parquet(regime_path)
        returns.write_parquet(returns_path)
        
        logger.info(f"Written timeseries: {regime_path}, {returns_path}")
        return {
            "regime_timeseries": regime_path,
            "returns": returns_path,
        }
    
    def compute_stability_metrics(self, regime_series: pl.DataFrame) -> Dict[str, float]:
        """Compute stability metrics from regime series."""
        if len(regime_series) == 0:
            return {
                "switches_per_year": 0.0,
                "avg_regime_duration_days": 0.0,
                "regime_distribution": {},
            }
        
        # Count regime switches
        regime_col = regime_series["regime"]
        switches = (regime_col != regime_col.shift(1)).sum() - 1  # -1 because first row isn't a switch
        switches = max(0, switches)
        
        # Calculate total days
        dates = regime_series["date"].sort()
        if len(dates) > 1:
            total_days = (dates.max() - dates.min()).days + 1
            years = total_days / 365.25
            switches_per_year = switches / years if years > 0 else 0.0
        else:
            switches_per_year = 0.0
            total_days = 1
        
        # Calculate average regime duration
        regime_durations = []
        current_regime = None
        current_start_idx = None
        
        for idx, row in enumerate(regime_series.iter_rows(named=True)):
            regime = row["regime"]
            if regime != current_regime:
                if current_regime is not None and current_start_idx is not None:
                    duration = idx - current_start_idx
                    regime_durations.append(duration)
                current_regime = regime
                current_start_idx = idx
        
        # Add final regime duration
        if current_start_idx is not None:
            duration = len(regime_series) - current_start_idx
            regime_durations.append(duration)
        
        avg_regime_duration = sum(regime_durations) / len(regime_durations) if regime_durations else 0.0
        
        # Regime distribution (% time in each regime)
        regime_counts = regime_series["regime"].value_counts()
        regime_distribution = {
            regime: count / len(regime_series) * 100.0
            for regime, count in zip(regime_counts["regime"], regime_counts["count"])
        }
        
        return {
            "switches_per_year": switches_per_year,
            "avg_regime_duration_days": avg_regime_duration,
            "regime_distribution": regime_distribution,
            "total_switches": int(switches),
            "total_days": int(total_days),
        }
    
    def update_catalog(
        self,
        run_id: str,
        experiment_spec: Dict[str, Any],
        metrics: Dict[str, Any],
        stability_metrics: Dict[str, Any],
    ) -> Path:
        """Append row to catalog.parquet with run metadata."""
        # Create catalog entry
        catalog_entry = {
            "run_id": run_id,
            "title": experiment_spec.get("title"),
            "experiment_id": experiment_spec.get("experiment_id"),
            "category_path": experiment_spec.get("category_path"),
            "timestamp": datetime.now().isoformat(),
            # Feature IDs/versions
            "features": json.dumps([f["id"] for f in experiment_spec.get("features", [])]),
            # Key knobs
            "n_regimes": experiment_spec.get("state_mapping", {}).get("n_regimes", 3),
            "target_n": experiment_spec.get("target", {}).get("short_leg", {}).get("n", 20),
            "target_weighting": experiment_spec.get("target", {}).get("short_leg", {}).get("weighting", "equal"),
            # Key results
            "cagr": metrics.get("cagr", 0.0),
            "sharpe": metrics.get("sharpe", 0.0),
            "sortino": metrics.get("sortino", 0.0),
            "max_drawdown": metrics.get("max_drawdown", 0.0),
            "calmar": metrics.get("calmar", 0.0),
            "hit_rate": metrics.get("hit_rate", 0.0),
            # Stability stats
            "switches_per_year": stability_metrics.get("switches_per_year", 0.0),
            "avg_regime_duration_days": stability_metrics.get("avg_regime_duration_days", 0.0),
            "regime_distribution": json.dumps(stability_metrics.get("regime_distribution", {})),
        }
        
        # Load existing catalog or create new
        if self.catalog_path.exists():
            catalog_df = pl.read_parquet(self.catalog_path)
        else:
            # Create empty catalog with proper schema (use sample row then drop it)
            sample_entry = {
                "run_id": "dummy",
                "title": "dummy",
                "experiment_id": "dummy",
                "category_path": "dummy",
                "timestamp": datetime.now().isoformat(),
                "features": "[]",
                "n_regimes": 3,
                "target_n": 20,
                "target_weighting": "equal",
                "cagr": 0.0,
                "sharpe": 0.0,
                "sortino": 0.0,
                "max_drawdown": 0.0,
                "calmar": 0.0,
                "hit_rate": 0.0,
                "switches_per_year": 0.0,
                "avg_regime_duration_days": 0.0,
                "regime_distribution": "{}",
            }
            catalog_df = pl.DataFrame([sample_entry]).head(0)  # Create with schema, then take 0 rows
        
        # Append new entry
        try:
            new_row = pl.DataFrame([catalog_entry])
            catalog_df = pl.concat([catalog_df, new_row])
            
            # Write catalog
            catalog_df.write_parquet(self.catalog_path)
            logger.info(f"Updated catalog: {self.catalog_path}")
        except Exception as e:
            logger.error(f"Failed to update catalog: {e}", exc_info=True)
            raise
        
        return self.catalog_path
