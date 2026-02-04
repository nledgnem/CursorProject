"""Utilities for generating run metadata."""

import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import subprocess


def get_git_commit_hash(repo_root: Path) -> Optional[str]:
    """Get the current git commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    return None


def get_file_hash(file_path: Path) -> Optional[str]:
    """Get SHA256 hash of a file."""
    if not file_path.exists():
        return None
    try:
        with open(file_path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()[:16]  # First 16 chars
    except Exception:
        return None


def create_run_metadata(
    script_name: str,
    config_path: Optional[Path] = None,
    data_paths: Optional[Dict[str, Path]] = None,
    row_counts: Optional[Dict[str, int]] = None,
    filter_thresholds: Optional[Dict[str, Any]] = None,
    date_range: Optional[Dict[str, str]] = None,
    repo_root: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Create run metadata dictionary.
    
    Args:
        script_name: Name of the script that generated this run
        config_path: Path to config file (if applicable)
        data_paths: Dict of data file names -> paths
        row_counts: Dict of dataset names -> row counts
        filter_thresholds: Dict of filter parameters used
        date_range: Dict with 'start_date' and 'end_date' keys
        repo_root: Repository root path (for git hash)
    
    Returns:
        Dictionary with metadata
    """
    if repo_root is None:
        repo_root = Path(__file__).parent.parent.parent
    
    metadata = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "script_name": script_name,
        "git_commit_hash": get_git_commit_hash(repo_root),
    }
    
    if config_path:
        # Convert Path to string for JSON serialization
        if isinstance(config_path, Path):
            rel_config = config_path.relative_to(repo_root) if config_path.is_relative_to(repo_root) else str(config_path)
            metadata["config_file"] = str(rel_config)
        else:
            metadata["config_file"] = str(config_path)
        metadata["config_hash"] = get_file_hash(config_path)
    
    if data_paths:
        metadata["data_files"] = {}
        for name, path in data_paths.items():
            # Convert Path to string for JSON serialization
            if isinstance(path, Path):
                rel_path = path.relative_to(repo_root) if path.is_relative_to(repo_root) else str(path)
                # Convert Path to string
                rel_path = str(rel_path)
            else:
                rel_path = str(path)
            metadata["data_files"][name] = {
                "path": rel_path,
                "hash": get_file_hash(path),
            }
    
    if row_counts:
        metadata["row_counts"] = row_counts
    
    if filter_thresholds:
        metadata["filter_thresholds"] = filter_thresholds
    
    if date_range:
        metadata["date_range"] = date_range
    
    return metadata


def save_run_metadata(metadata: Dict[str, Any], output_path: Path) -> None:
    """Save run metadata to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved run metadata to {output_path}")

