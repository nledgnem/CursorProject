"""Funding Lake Audit - Macro EDA for Silver Layer funding data.

This script performs a one-shot exploratory data analysis over the
Silver funding fact table, printing a concise terminal summary and
producing a global funding-rate distribution chart.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple, Dict

import polars as pl
import numpy as np
import matplotlib.pyplot as plt

from repo_paths import data_lake_root


DEF_RELATIVE_PARQUET = data_lake_root() / "silver_fact_funding.parquet"


def resolve_project_root() -> Path:
    """Resolve project root assuming this file lives in `<root>/scripts/`."""
    return Path(__file__).resolve().parents[1]


def resolve_parquet_path(cli_path: Optional[str]) -> Path:
    """Resolve the funding parquet path, with a sensible default."""
    if cli_path:
        path = Path(cli_path).expanduser().resolve()
    else:
        path = DEF_RELATIVE_PARQUET

    if not path.exists():
        raise FileNotFoundError(
            f"Funding parquet not found at {path}. "
            "Pass --parquet-path if the location differs."
        )
    return path


def load_funding_table(path: Path) -> Tuple[pl.DataFrame, Dict[str, str]]:
    """Load the silver funding fact table and standardize key column names.

    Returns:
        df: Polars DataFrame with at least date, asset, funding_rate, exchange (if present)
        colmap: Mapping of logical names -> physical column names
    """
    df = pl.read_parquet(path)

    cols = set(df.columns)

    # Date column candidates
    date_candidates = ["date", "trading_date", "ds"]
    asset_candidates = ["asset_id", "symbol", "base_asset_id"]
    funding_candidates = ["funding_rate_raw_pct", "funding_rate", "funding_rate_daily", "funding_rate_1d"]
    exchange_candidates = ["exchange", "venue", "exchange_name"]

    def pick(name: str, candidates: list[str], required: bool = True) -> Optional[str]:
        for c in candidates:
            if c in cols:
                return c
        if required:
            raise KeyError(
                f"Required logical column '{name}' not found. "
                f"Tried candidates {candidates}, available columns: {sorted(cols)}"
            )
        return None

    date_col = pick("date", date_candidates, required=True)
    asset_col = pick("asset", asset_candidates, required=True)
    funding_col = pick("funding_rate", funding_candidates, required=True)
    exchange_col = pick("exchange", exchange_candidates, required=False)

    # Ensure date-like column is of date type (not string)
    dtype = df[date_col].dtype
    is_date = dtype == pl.Date
    is_datetime = dtype == pl.Datetime

    if not is_date and not is_datetime:
        df = df.with_columns(pl.col(date_col).str.strptime(pl.Date, strict=False))

    # If datetime, cast down to date for daily aggregation
    if df[date_col].dtype == pl.Datetime:
        df = df.with_columns(pl.col(date_col).dt.date().alias(date_col))

    colmap = {
        "date": date_col,
        "asset": asset_col,
        "funding_rate": funding_col,
    }
    if exchange_col is not None:
        colmap["exchange"] = exchange_col

    return df, colmap


def compute_macro_eda(df: pl.DataFrame, colmap: Dict[str, str]) -> Dict[str, object]:
    """Compute the requested macro EDA metrics."""
    date_col = colmap["date"]
    asset_col = colmap["asset"]
    fr_col = colmap["funding_rate"]
    exch_col = colmap.get("exchange")

    # Global timeline
    timeline = df.select(
        pl.col(date_col).min().alias("min_date"),
        pl.col(date_col).max().alias("max_date"),
    ).row(0)
    min_date, max_date = timeline

    # Asset coverage
    n_assets = df.select(pl.col(asset_col).n_unique().alias("n_assets")).item()

    # Exchange breakdown
    if exch_col is not None:
        exchange_breakdown = (
            df.group_by(exch_col)
            .len()
            .sort("len", descending=True)
            .rename({exch_col: "exchange", "len": "row_count"})
        )
    else:
        exchange_breakdown = None

    # Cap audit
    fr = pl.col(fr_col)
    cap_mask = (fr >= 0.05) | (fr <= -0.05)
    cap_counts = df.select(
        pl.len().alias("n_total"),
        cap_mask.cast(pl.Int64).sum().alias("n_capped"),
    ).row(0)
    n_total, n_capped = cap_counts
    pct_capped = float(n_capped) / n_total * 100.0 if n_total > 0 else 0.0

    # Global distribution stats
    stats = df.select(
        pl.col(fr_col).mean().alias("mean"),
        pl.col(fr_col).median().alias("median"),
        pl.col(fr_col).quantile(0.01, "nearest").alias("p01"),
        pl.col(fr_col).quantile(0.99, "nearest").alias("p99"),
    ).row(0)
    mean_val, median_val, p01, p99 = stats

    # Sparsity: missing days per asset between their min and max date
    per_asset = df.group_by(asset_col).agg(
        pl.col(date_col).min().alias("min_date"),
        pl.col(date_col).max().alias("max_date"),
        pl.col(date_col).n_unique().alias("n_days_observed"),
    )

    per_asset = per_asset.with_columns(
        (
            (pl.col("max_date") - pl.col("min_date"))
            .dt.total_days()
            .cast(pl.Int64)
            + 1
        ).alias("n_days_expected")
    )

    per_asset = per_asset.with_columns(
        (pl.col("n_days_expected") - pl.col("n_days_observed")).alias("missing_days")
    )

    sparsity_top5 = (
        per_asset.sort("missing_days", descending=True)
        .head(5)
        .select([asset_col, "min_date", "max_date", "n_days_observed", "n_days_expected", "missing_days"])
    )

    return {
        "min_date": min_date,
        "max_date": max_date,
        "n_assets": int(n_assets),
        "exchange_breakdown": exchange_breakdown,
        "n_total": int(n_total),
        "n_capped": int(n_capped),
        "pct_capped": pct_capped,
        "mean": float(mean_val) if mean_val is not None else float("nan"),
        "median": float(median_val) if median_val is not None else float("nan"),
        "p01": float(p01) if p01 is not None else float("nan"),
        "p99": float(p99) if p99 is not None else float("nan"),
        "sparsity_top5": sparsity_top5,
    }


def print_macro_summary(metrics: Dict[str, object]) -> None:
    """Pretty-print the macro EDA summary to the terminal."""
    print("\n" + "=" * 72)
    print("FUNDING LAKE MACRO EDA SUMMARY")
    print("=" * 72)

    # Global timeline
    print("\n[Global Timeline]")
    print(f"  Start Date : {metrics['min_date']}")
    print(f"  End Date   : {metrics['max_date']}")

    # Asset coverage
    print("\n[Asset Coverage]")
    print(f"  Unique Assets : {metrics['n_assets']:,}")

    # Exchange breakdown
    print("\n[Exchange Breakdown]")
    exchange_breakdown = metrics["exchange_breakdown"]
    if exchange_breakdown is None:
        print("  Exchange column not present in table.")
    else:
        print("  Exchange           Rows")
        print("  ---------  -------------")
        for row in exchange_breakdown.iter_rows(named=True):
            print(f"  {row['exchange']:<9}  {row['row_count']:>13,}")

    # Cap audit
    print("\n[Silver Cap Audit | |funding_rate| >= 0.05]")
    print(f"  Total Rows      : {metrics['n_total']:,}")
    print(f"  Capped Rows     : {metrics['n_capped']:,}")
    print(f"  Capped % of Rows: {metrics['pct_capped']:.4f}%")

    # Global distribution
    print("\n[Global Funding Rate Distribution]")
    print(f"  Mean            : {metrics['mean']:.6f}")
    print(f"  Median          : {metrics['median']:.6f}")
    print(f"  1st Percentile  : {metrics['p01']:.6f}")
    print(f"  99th Percentile : {metrics['p99']:.6f}")

    # Sparsity
    print("\n[Sparsity Check | Top 5 Assets by Missing Days]")
    sparsity_top5 = metrics["sparsity_top5"]
    if sparsity_top5.height == 0:
        print("  No assets found.")
    else:
        asset_col = sparsity_top5.columns[0]
        header = (
            f"  {asset_col:<15}  {'min_date':<10}  {'max_date':<10}  "
            f"{'obs_days':>9}  {'exp_days':>9}  {'missing':>9}"
        )
        print(header)
        print("  " + "-" * (len(header) - 2))
        for row in sparsity_top5.iter_rows(named=True):
            print(
                f"  {str(row[asset_col]):<15}  "
                f"{row['min_date']}  {row['max_date']}  "
                f"{row['n_days_observed']:>9}  "
                f"{row['n_days_expected']:>9}  "
                f"{row['missing_days']:>9}"
            )

    print("\n" + "=" * 72 + "\n")


def plot_global_distribution(
    df: pl.DataFrame, colmap: Dict[str, str], mean_value: float
) -> Path:
    """Plot the global funding-rate distribution with log-scale Y-axis."""
    fr_col = colmap["funding_rate"]
    values = (
        df.select(pl.col(fr_col))
        .drop_nulls()
        .to_series()
        .to_numpy()
        .astype(float)
    )

    if values.size == 0:
        raise ValueError("No non-null funding_rate values available for plotting.")

    out_path = Path(__file__).with_name("chart_funding_lake_distribution.png")

    plt.style.use("seaborn-v0_8-darkgrid")
    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)

    ax.hist(values, bins=100, color="#1f77b4", alpha=0.8, edgecolor="none", log=True)

    # Vertical lines: mean (blue) and ±0.05 caps (red)
    ax.axvline(mean_value, color="blue", linestyle="--", linewidth=1.5, label="Mean")
    ax.axvline(0.05, color="red", linestyle="--", linewidth=1.5, label="+0.05 Cap")
    ax.axvline(-0.05, color="red", linestyle="--", linewidth=1.5, label="-0.05 Cap")

    ax.set_xlabel("Daily Funding Rate")
    ax.set_ylabel("Count (log scale)")
    ax.set_title(
        "Funding Lake Physics: Global Daily Funding Rate Distribution (Log Scale)"
    )
    ax.legend()

    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)

    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Macro EDA audit of the Silver Layer funding lake."
    )
    parser.add_argument(
        "--parquet-path",
        type=str,
        default=None,
        help="Optional override path to silver_fact_funding.parquet",
    )

    args = parser.parse_args()

    parquet_path = resolve_parquet_path(args.parquet_path)
    df, colmap = load_funding_table(parquet_path)

    metrics = compute_macro_eda(df, colmap)
    print_macro_summary(metrics)

    chart_path = plot_global_distribution(df, colmap, mean_value=metrics["mean"])
    print(f"Saved global funding distribution chart to: {chart_path}")


if __name__ == "__main__":
    main()

