"""Output generation for MSM v0: CSV files, JSON manifest, and returns chart."""

import polars as pl
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    _MATPLOTLIB_AVAILABLE = True
except ImportError:
    _MATPLOTLIB_AVAILABLE = False


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
            - r_alts, r_btc, r_eth, r_maj_weighted, y (y = r_maj - r_alts, long majors / short alts)
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
    y = r_maj - r_alts (long majors / short alts); hit_rate = fraction of weeks where y < 0 (majors underperformed alts).
    
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


def write_returns_chart(
    data: List[Dict[str, Any]],
    output_path: Path,
    run_id: Optional[str] = None,
) -> None:
    """
    Write a returns chart (PNG): weekly spread return (y) and cumulative return.
    y = r_maj - r_alts (long majors / short alts).

    Args:
        data: List of timeseries dicts with decision_date, y, r_alts, r_maj_weighted
        output_path: Path for the PNG file (e.g. output_dir / "returns_chart.png")
        run_id: Optional run ID for the chart title
    """
    if not _MATPLOTLIB_AVAILABLE:
        logger.warning("matplotlib not available, skipping returns chart")
        return
    if len(data) == 0:
        logger.warning("No data for returns chart")
        return

    df = pd.DataFrame(data)
    if "y" not in df.columns or "decision_date" not in df.columns:
        logger.warning("Missing 'y' or 'decision_date' for returns chart")
        return

    df = df.dropna(subset=["y", "decision_date"]).copy()
    if len(df) == 0:
        logger.warning("No valid y/decision_date rows for returns chart")
        return

    df["decision_date"] = pd.to_datetime(df["decision_date"])
    df = df.sort_values("decision_date").reset_index(drop=True)
    df["cumulative_return"] = (1 + df["y"]).cumprod() - 1

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), height_ratios=[1, 1], sharex=True)
    fig.suptitle(
        f"MSM v0 returns (long majors / short alts)" + (f" — {run_id}" if run_id else ""),
        fontsize=11,
    )

    # Top: weekly spread return (y) as bars
    ax1.bar(df["decision_date"], df["y"] * 100, width=5, color="steelblue", alpha=0.8, edgecolor="none")
    ax1.axhline(0, color="gray", linewidth=0.8, linestyle="-")
    ax1.set_ylabel("Weekly return (%)")
    ax1.set_title("Weekly spread return (y = r_maj − r_alts)")
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    ax1.grid(True, alpha=0.3)

    # Bottom: cumulative return
    ax2.fill_between(df["decision_date"], 0, df["cumulative_return"] * 100, alpha=0.4, color="green")
    ax2.plot(df["decision_date"], df["cumulative_return"] * 100, color="darkgreen", linewidth=1.5, label="Cumulative return")
    ax2.axhline(0, color="gray", linewidth=0.8, linestyle="-")
    ax2.set_ylabel("Cumulative return (%)")
    ax2.set_xlabel("Decision date")
    ax2.set_title("Cumulative return (long majors / short alts)")
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax2.legend(loc="upper left", fontsize=8)
    ax2.grid(True, alpha=0.3)

    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.xticks(rotation=45)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Wrote returns chart: {output_path}")


def write_returns_chart_from_csv(
    timeseries_csv_path: Path,
    output_path: Optional[Path] = None,
    run_id: Optional[str] = None,
) -> None:
    """
    Read msm_timeseries.csv and write returns_chart.png in the same directory.
    Used to add charts to existing run folders.

    Args:
        timeseries_csv_path: Path to msm_timeseries.csv
        output_path: Path for PNG (default: same dir as CSV, file returns_chart.png)
        run_id: Optional run ID for title (default: parent directory name)
    """
    if not _MATPLOTLIB_AVAILABLE:
        logger.warning("matplotlib not available, skipping returns chart")
        return
    path = Path(timeseries_csv_path)
    if not path.exists():
        logger.warning(f"CSV not found: {path}")
        return
    df = pd.read_csv(path)
    data = df.to_dict("records")
    out = Path(output_path) if output_path else path.parent / "returns_chart.png"
    rid = run_id or path.parent.name
    write_returns_chart(data, out, run_id=rid)


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
    
    # Write returns chart (weekly y and cumulative return)
    write_returns_chart(
        timeseries_data,
        output_dir / "returns_chart.png",
        run_id=metadata.get("run_id"),
    )
    
    logger.info(f"Generated all outputs in: {output_dir}")
