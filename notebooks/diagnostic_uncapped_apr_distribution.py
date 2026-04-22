#!/usr/bin/env python3
"""
Uncapped F_tk APR distribution diagnostic.

Loads msm_timeseries.csv (from uncapped Silver → Gold run), drops NaNs in F_tk_apr,
plots a high-resolution histogram with log Y-axis and vertical lines for mean/median/95th,
and prints the physics summary (mean, median, abs max, abs min, 95th percentile) to terminal.
"""

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CSV = REPO_ROOT / "reports" / "msm_funding_v0" / "uncapped_ftk" / "msm_timeseries.csv"


def load_and_prepare(csv_path: Path) -> pd.DataFrame:
    """Load timeseries CSV, ensure F_tk_apr exists, drop NaNs in F_tk_apr."""
    df = pd.read_csv(csv_path)
    if "F_tk_apr" not in df.columns and "F_tk" in df.columns:
        df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0  # APR in %
    df = df.dropna(subset=["F_tk_apr"])
    return df


def plot_distribution(df: pd.DataFrame, out_path: Path) -> None:
    """Plot histogram of F_tk_apr with log Y-axis and mean/median/95th vlines."""
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(df["F_tk_apr"], bins=100, color="steelblue", alpha=0.8, edgecolor="white", linewidth=0.3)

    mean_val = df["F_tk_apr"].mean()
    median_val = df["F_tk_apr"].median()
    p95_val = df["F_tk_apr"].quantile(0.95)

    ax.axvline(mean_val, color="blue", linestyle="--", linewidth=2, label=f"Global Mean: {mean_val:.2f}%")
    ax.axvline(median_val, color="green", linestyle="--", linewidth=2, label=f"Median: {median_val:.2f}%")
    ax.axvline(p95_val, color="red", linestyle="--", linewidth=2, label=f"95th Percentile: {p95_val:.2f}%")

    ax.set_yscale("log")
    ax.set_xlabel("Strategy Market Temperature (F_tk APR %)")
    ax.set_ylabel("Frequency (Log Scale)")
    ax.set_title("Uncapped Global Distribution of F_tk (APR)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()


def print_physics_summary(df: pd.DataFrame) -> None:
    """Print the five physics summary lines to terminal."""
    mean_val = df["F_tk_apr"].mean()
    median_val = df["F_tk_apr"].median()
    abs_max = df["F_tk_apr"].max()
    abs_min = df["F_tk_apr"].min()
    p95 = df["F_tk_apr"].quantile(0.95)

    print("Uncapped F_tk APR - Global Mean: {:.2f}%".format(mean_val))
    print("Uncapped F_tk APR - Global Median: {:.2f}%".format(median_val))
    print("Uncapped F_tk APR - Absolute Maximum: {:.2f}%".format(abs_max))
    print("Uncapped F_tk APR - Absolute Minimum: {:.2f}%".format(abs_min))
    print("Uncapped F_tk APR - 95th Percentile (The True Danger Zone): {:.2f}%".format(p95))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Uncapped F_tk APR distribution: histogram + physics summary."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help=f"Path to msm_timeseries.csv (default: {DEFAULT_CSV})",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "scripts" / "chart_uncapped_ftk_apr_distribution.png",
        help="Output path for histogram PNG",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}")

    df = load_and_prepare(args.csv)
    if df.empty:
        raise SystemExit("No rows left after dropping NaNs in F_tk_apr.")

    plot_distribution(df, args.out)
    print(f"Saved histogram to: {args.out}")

    print_physics_summary(df)


if __name__ == "__main__":
    main()
