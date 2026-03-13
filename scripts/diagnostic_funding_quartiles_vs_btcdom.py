import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


def run_funding_quartiles_vs_btcdom(csv_path: str):
    # Load data
    df = pd.read_csv(csv_path)

    # Keep only needed columns
    # We derive a 7-day BTCDOM log return from the btcd_index_decision level.
    df = df[["F_tk_apr", "y", "btcd_index_decision"]].copy()

    # Compute BTCDOM 7-day log return from index levels
    df["btcdom_7d_ret"] = np.log(df["btcd_index_decision"].shift(-1) / df["btcd_index_decision"])

    # Drop rows with NaNs in any required fields
    df = df.dropna(subset=["F_tk_apr", "y", "btcdom_7d_ret"])

    if df.empty:
        raise ValueError(
            "No valid rows after filtering for 'F_tk_apr', 'y', and 'btcdom_7d_ret'."
        )

    # Convert weekly log returns to simple percentage returns
    df["y_pct"] = (np.exp(df["y"]) - 1.0) * 100.0
    df["btcdom_pct"] = (np.exp(df["btcdom_7d_ret"]) - 1.0) * 100.0

    # Quartile binning on F_tk_apr
    df["funding_quartile"] = pd.qcut(df["F_tk_apr"], q=4)

    # Build explicit labels with physical min/max APR per quartile
    quartile_labels = {}
    for idx, q in enumerate(
        sorted(df["funding_quartile"].unique(), key=lambda x: x.left), start=1
    ):
        sub = df[df["funding_quartile"] == q]
        q_min = sub["F_tk_apr"].min()
        q_max = sub["F_tk_apr"].max()
        label = f"Q{idx}: {q_min:.2f}% to {q_max:.2f}% APR"
        quartile_labels[q] = label

    # Stable regime ordering
    ordered_quartiles = sorted(quartile_labels.keys(), key=lambda x: x.left)
    ordered_labels = [quartile_labels[q] for q in ordered_quartiles]

    # Map labels back to dataframe
    df["Regime"] = df["funding_quartile"].map(quartile_labels)
    df["Regime"] = pd.Categorical(df["Regime"], categories=ordered_labels, ordered=True)

    # Reshape for seaborn: melt LS Strategy and BTCDOM returns
    melt_df = pd.melt(
        df[["Regime", "y_pct", "btcdom_pct"]],
        id_vars="Regime",
        value_vars=["y_pct", "btcdom_pct"],
        var_name="Return_Type",
        value_name="Value_Pct",
    )
    melt_df["Return_Type"] = melt_df["Return_Type"].map(
        {"y_pct": "LS Strategy", "btcdom_pct": "BTCDOM"}
    )

    # Regime statistics for terminal output
    stats = (
        melt_df.groupby(["Regime", "Return_Type"])["Value_Pct"]
        .agg(
            Mean_Pct="mean",
            StdDev_Pct="std",
            Count="count",
        )
        .reset_index()
    )

    # Pivot for a clear grouped table (Regime x Return_Type with metrics)
    stats_pivot = stats.pivot_table(
        index="Regime",
        columns="Return_Type",
        values=["Mean_Pct", "StdDev_Pct", "Count"],
    )

    print("=== Regime Statistics: LS Strategy vs. BTCDOM by Explicit F_tk_apr Quartiles ===")
    with pd.option_context("display.float_format", lambda x: f"{x:0.2f}"):
        print(stats_pivot.round(2).to_string())
    print()

    # Visualization: two-panel grouped chart
    sns.set(style="whitegrid")
    fig, (ax_top, ax_bottom) = plt.subplots(
        2,
        1,
        figsize=(12, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [2, 3]},
    )

    # Top panel: grouped bar chart of average weekly return
    sns.barplot(
        data=melt_df,
        x="Regime",
        y="Value_Pct",
        hue="Return_Type",
        estimator=np.mean,
        errorbar="sd",
        order=ordered_labels,
        ax=ax_top,
    )
    ax_top.axhline(0.0, color="black", linewidth=1.0)
    ax_top.set_ylabel("Average Weekly Return (%)", fontsize=12, fontweight="bold")
    ax_top.set_title(
        "Regime Analysis: LS Strategy vs. BTCDOM across F_tk_apr Quartiles",
        fontsize=14,
        fontweight="bold",
    )
    ax_top.legend(title="Return Type")
    ax_top.grid(True, axis="y", alpha=0.3)

    # Bottom panel: grouped boxplot of weekly return distributions
    sns.boxplot(
        data=melt_df,
        x="Regime",
        y="Value_Pct",
        hue="Return_Type",
        order=ordered_labels,
        ax=ax_bottom,
        showfliers=True,
    )
    ax_bottom.axhline(0.0, color="red", linestyle="--", linewidth=1.0, alpha=0.7)
    ax_bottom.set_ylabel(
        "Weekly Return Distribution (%)", fontsize=12, fontweight="bold"
    )
    ax_bottom.set_xlabel("Funding APR Regime (Explicit Quartiles)", fontsize=11, fontweight="bold")

    # Handle legends: keep a single combined legend if desired
    handles, labels = ax_bottom.get_legend_handles_labels()
    ax_bottom.legend(handles, labels, title="Return Type")

    # X-axis labels formatting
    plt.setp(ax_bottom.get_xticklabels(), rotation=20, ha="right")

    fig.tight_layout()

    # Save chart
    output_path = Path("scripts/chart_funding_quartiles_vs_btcdom.png")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path, dpi=300)
    print(f"Chart saved to {output_path}")


if __name__ == "__main__":
    run_funding_quartiles_vs_btcdom(
        "reports/msm_funding_v0/silver_router_variance_shield/msm_timeseries.csv"
    )

