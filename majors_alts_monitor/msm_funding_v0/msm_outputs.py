"""Output generation for MSM v0: CSV files and JSON manifest."""

import polars as pl
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)


def write_timeseries_csv(
    data: List[Dict[str, Any]],
    output_path: Path,
) -> None:
    """
    Write msm_timeseries.csv with decision_date, next_date, basket info, features, labels, returns.
    
    Args:
        data: List of dicts with keys:
            - decision_date, next_date
            - basket_hash, basket_members, n_valid, coverage
            - F_tk (feature)
            - label_v0_0, label_v0_1
            - r_alts, r_btc, r_eth, r_maj_70_30, y
        output_path: Path to output CSV file
    """
    if len(data) == 0:
        logger.warning("No data to write to timeseries CSV")
        return
    
    df = pd.DataFrame(data)
    
    # Ensure columns are in desired order (dynamic majors columns will be added)
    base_columns = [
        "decision_date", "next_date",
        "basket_hash", "basket_members", "n_valid", "coverage",
        "F_tk",
        "label_v0_0", "label_v0_1",
        "p_v0_0", "p_v0_1",  # Percentile ranks
        "r_alts", "r_maj_weighted", "y",
    ]
    
    # Add dynamic major return columns (r_btc, r_eth, etc.) if they exist
    all_columns = list(df.columns)
    major_return_cols = [col for col in all_columns if col.startswith("r_") and col not in ["r_alts", "r_maj_weighted"]]
    
    # Build final column order: base columns + major return cols (sorted) + any remaining
    columns = base_columns + sorted(major_return_cols)
    
    # Add any remaining columns not in our list
    remaining = [col for col in all_columns if col not in columns]
    columns = columns + remaining
    
    # Only include columns that exist in data
    columns = [col for col in columns if col in df.columns]
    
    df = df[columns]
    
    # Write CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Wrote timeseries CSV: {output_path} ({len(df)} rows)")


def write_summary_by_label(
    data: List[Dict[str, Any]],
    label_col: str,
    output_path: Path,
) -> None:
    """
    Write summary_by_label CSV with count, mean/median/std of y, hit_rate(y<0).
    
    Args:
        data: List of dicts with label and y values
        label_col: Column name for label (e.g., "label_v0_0" or "label_v0_1")
        output_path: Path to output CSV file
    """
    df = pd.DataFrame(data)
    
    if label_col not in df.columns or "y" not in df.columns:
        logger.warning(f"Missing required columns: {label_col} or y")
        return
    
    # Filter out rows with missing labels or y
    df_valid = df[[label_col, "y"]].dropna()
    
    if len(df_valid) == 0:
        logger.warning(f"No valid data for summary by {label_col}")
        return
    
    # Group by label
    summary = []
    for label in df_valid[label_col].unique():
        label_data = df_valid[df_valid[label_col] == label]
        y_values = label_data["y"].dropna()
        
        if len(y_values) == 0:
            continue
        
        summary.append({
            "label": label,
            "count": len(y_values),
            "mean_y": y_values.mean(),
            "median_y": y_values.median(),
            "std_y": y_values.std(),
            "hit_rate": (y_values < 0).sum() / len(y_values),  # Fraction where y < 0
        })
    
    summary_df = pd.DataFrame(summary)
    
    # Sort by label order (if applicable)
    label_order = ["Red", "Orange", "Yellow", "YellowGreen", "Green"]
    if all(l in label_order for l in summary_df["label"]):
        summary_df["label"] = pd.Categorical(
            summary_df["label"], categories=label_order, ordered=True
        )
        summary_df = summary_df.sort_values("label")
    
    # Write CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(output_path, index=False)
    logger.info(f"Wrote summary CSV: {output_path} ({len(summary_df)} labels)")


def write_run_manifest(
    config: Dict[str, Any],
    metadata: Dict[str, Any],
    output_path: Path,
) -> None:
    """
    Write run_manifest.json with full config + metadata.
    
    Args:
        config: Full configuration dict
        metadata: Metadata dict (run_id, start_date, end_date, n_weeks, etc.)
        output_path: Path to output JSON file
    """
    manifest = {
        "run_id": metadata.get("run_id", "unknown"),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "config": config,
        "metadata": metadata,
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    
    logger.info(f"Wrote run manifest: {output_path}")


def generate_outputs(
    timeseries_data: List[Dict[str, Any]],
    config: Dict[str, Any],
    metadata: Dict[str, Any],
    output_dir: Path,
) -> None:
    """
    Generate all output files for a run.
    
    Args:
        timeseries_data: List of timeseries records
        config: Full configuration dict
        metadata: Metadata dict
        output_dir: Output directory
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write timeseries CSV
    write_timeseries_csv(
        timeseries_data,
        output_dir / "msm_timeseries.csv"
    )
    
    # Write summary by label v0.0
    write_summary_by_label(
        timeseries_data,
        "label_v0_0",
        output_dir / "summary_by_label_v0_0.csv"
    )
    
    # Write summary by label v0.1
    write_summary_by_label(
        timeseries_data,
        "label_v0_1",
        output_dir / "summary_by_label_v0_1.csv"
    )
    
    # Write run manifest
    write_run_manifest(
        config,
        metadata,
        output_dir / "run_manifest.json"
    )
    
    logger.info(f"Generated all outputs in: {output_dir}")
