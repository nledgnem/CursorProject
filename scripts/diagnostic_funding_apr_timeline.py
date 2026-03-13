#!/usr/bin/env python3
"""
Timeline chart: Average funding APR (all coins) vs F_tk_apr (strategy basket), Jan 2024–Dec 2025.

- Avg funding APR: from silver_fact_funding, daily mean across all coins, then annualized (rate * 365 * 100).
- F_tk_apr: from msm_timeseries.csv (weekly decision dates).
"""

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_LAKE = REPO_ROOT / "data" / "curated" / "data_lake"
DEFAULT_FUNDING = DATA_LAKE / "silver_fact_funding.parquet"
DEFAULT_MSM_CSV = REPO_ROOT / "reports" / "msm_funding_v0" / "uncapped_ftk" / "msm_timeseries.csv"
START_DATE = "2024-01-01"
END_DATE = "2025-12-31"


def load_daily_avg_funding_apr(
    funding_path: Path,
    start: str,
    end: str,
) -> pd.DataFrame:
    """Daily average funding APR across all coins (one row per date)."""
    df = pd.read_parquet(funding_path)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df[(df["date"] >= start) & (df["date"] <= end)]

    # Per (asset_id, date) take mean rate if multiple instruments
    daily_per_asset = (
        df.groupby(["asset_id", "date"], as_index=False)["funding_rate"]
        .mean()
    )
    # Daily average across all coins → APR in %
    daily_avg = (
        daily_per_asset.groupby("date")["funding_rate"]
        .mean()
        .reset_index()
    )
    daily_avg["funding_apr_pct"] = daily_avg["funding_rate"] * 365.0 * 100.0
    return daily_avg[["date", "funding_apr_pct"]]


def load_ftk_apr_timeline(csv_path: Path, start: str, end: str) -> pd.DataFrame:
    """Weekly F_tk_apr from msm_timeseries (decision_date)."""
    df = pd.read_csv(csv_path)
    df["decision_date"] = pd.to_datetime(df["decision_date"]).dt.normalize()
    df = df[(df["decision_date"] >= start) & (df["decision_date"] <= end)]
    if "F_tk_apr" not in df.columns and "F_tk" in df.columns:
        df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0
    return df[["decision_date", "F_tk_apr"]].dropna(subset=["F_tk_apr"])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Timeline: avg funding APR (all coins) and F_tk_apr (basket), Jan 2024–Dec 2025",
    )
    parser.add_argument(
        "--funding",
        type=Path,
        default=DEFAULT_FUNDING,
        help="Path to silver_fact_funding.parquet",
    )
    parser.add_argument(
        "--msm-csv",
        type=Path,
        default=DEFAULT_MSM_CSV,
        help="Path to msm_timeseries.csv",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=START_DATE,
        help="Start date YYYY-MM-DD",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=END_DATE,
        help="End date YYYY-MM-DD",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "scripts" / "chart_funding_apr_timeline.png",
        help="Output PNG path",
    )
    args = parser.parse_args()

    if not args.funding.exists():
        raise SystemExit(f"Funding parquet not found: {args.funding}")
    if not args.msm_csv.exists():
        raise SystemExit(f"MSM CSV not found: {args.msm_csv}")

    daily_apr = load_daily_avg_funding_apr(args.funding, args.start, args.end)
    weekly_ftk = load_ftk_apr_timeline(args.msm_csv, args.start, args.end)

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(
        daily_apr["date"],
        daily_apr["funding_apr_pct"],
        color="steelblue",
        linewidth=1.0,
        alpha=0.9,
        label="Avg funding APR (all coins, daily)",
    )
    ax.plot(
        weekly_ftk["decision_date"],
        weekly_ftk["F_tk_apr"],
        color="darkorange",
        linewidth=2.0,
        marker="o",
        markersize=3,
        alpha=0.9,
        label="F_tk APR (strategy basket, weekly)",
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("APR (%)")
    ax.set_title("Funding APR timeline: All-coins daily average vs strategy F_tk (Jan 2024 – Dec 2025)")
    ax.legend(loc="upper right")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.xticks(rotation=45)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(args.out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {args.out}")


if __name__ == "__main__":
    main()
