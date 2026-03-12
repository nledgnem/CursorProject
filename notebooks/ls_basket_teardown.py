import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def main() -> None:
    sns.set_theme(style="darkgrid")
    plt.rcParams["figure.figsize"] = (14, 7)

    reports_dir = Path("../reports/msm_funding_v0")
    if not reports_dir.exists():
        print(f"Reports directory not found: {reports_dir}")
        return

    run_dirs = [d for d in reports_dir.iterdir() if d.is_dir()]
    if not run_dirs:
        print("No backtest runs found. Please run the MSM v0 backtest first.")
        return

    latest_run = max(run_dirs, key=os.path.getmtime)
    data_path = latest_run / "msm_timeseries.csv"

    if not data_path.exists():
        print(f"msm_timeseries.csv not found in latest run: {latest_run}")
        return

    print(f"Loading backtest data from: {data_path.parent.name}")

    df = pd.read_csv(data_path)
    if not {"decision_date", "y", "r_maj_weighted", "r_alts"} <= set(df.columns):
        print("Required columns not found in msm_timeseries.csv")
        print("Expected at least: decision_date, y, r_maj_weighted, r_alts")
        return

    df["decision_date"] = pd.to_datetime(df["decision_date"])
    df = df.sort_values("decision_date").reset_index(drop=True)
    df = df.dropna(subset=["y", "r_maj_weighted", "r_alts"])

    if df.empty:
        print("No valid rows after dropping NaNs in y, r_maj_weighted, r_alts.")
        return

    df["cum_strategy"] = (1 + df["y"]).cumprod()
    df["cum_long_majors"] = (1 + df["r_maj_weighted"]).cumprod()
    df["cum_short_alts"] = (1 + (-df["r_alts"])).cumprod()

    fig, ax = plt.subplots()

    ax.plot(
        df["decision_date"],
        df["cum_strategy"],
        label="Total L/S Strategy (y)",
        color="purple",
        linewidth=2.5,
    )
    ax.plot(
        df["decision_date"],
        df["cum_long_majors"],
        label="Long Leg: BTC/ETH",
        color="blue",
        alpha=0.6,
        linestyle="--",
    )
    ax.plot(
        df["decision_date"],
        df["cum_short_alts"],
        label="Short Leg: 30-Alt Basket",
        color="red",
        alpha=0.6,
        linestyle="--",
    )

    ax.axhline(1.0, color="black", linewidth=1, linestyle="-")

    ax.set_title(
        "MSM v0: Long Majors vs. Short Alts Equity Curve",
        fontsize=16,
        fontweight="bold",
    )
    ax.set_ylabel("Cumulative Return (1.0 = Initial Capital)", fontsize=12)
    ax.set_xlabel("Date", fontsize=12)
    ax.legend(loc="upper left", fontsize=11)

    total_return = (df["cum_strategy"].iloc[-1] - 1) * 100
    win_rate = (df["y"] > 0).mean() * 100

    print("--- Strategy Quick Stats ---")
    print(f"Total Cumulative Return: {total_return:.2f}%")
    print(f"Weekly Win Rate:         {win_rate:.1f}%")
    print(f"Total Weeks Traded:      {len(df)}")

    plt.tight_layout()
    output_png = latest_run / "ls_basket_teardown_equity.png"
    plt.savefig(output_png, dpi=120, bbox_inches="tight")
    print(f"Saved equity curve plot to: {output_png}")


if __name__ == "__main__":
    main()

