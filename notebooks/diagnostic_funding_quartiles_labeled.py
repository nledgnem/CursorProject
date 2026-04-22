import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


def run_funding_quartiles_labeled(csv_path: str):
    # Load data
    df = pd.read_csv(csv_path, parse_dates=["decision_date"])

    # Keep only needed columns and drop NaNs
    df = df[["decision_date", "F_tk_apr", "y"]].dropna(subset=["F_tk_apr", "y"]).copy()

    if df.empty:
        raise ValueError("No valid rows after filtering for 'F_tk_apr' and 'y'.")

    # Convert weekly log return y to simple percentage return
    df["y_pct"] = (np.exp(df["y"]) - 1.0) * 100.0

    # Quartile binning on F_tk_apr
    df["funding_quartile"] = pd.qcut(df["F_tk_apr"], q=4)

    # Build explicit labels with physical min/max APR per quartile
    quartile_labels = {}
    for idx, q in enumerate(sorted(df["funding_quartile"].unique(), key=lambda x: x.left), start=1):
        sub = df[df["funding_quartile"] == q]
        q_min = sub["F_tk_apr"].min()
        q_max = sub["F_tk_apr"].max()
        label = f"Q{idx}: {q_min:.2f}% to {q_max:.2f}% APR"
        quartile_labels[q] = label

    # Map labels into a stable ordinal index 1..4
    ordered_quartiles = sorted(quartile_labels.keys(), key=lambda x: x.left)
    quartile_to_idx = {q: i for i, q in enumerate(ordered_quartiles)}

    df["quartile_idx"] = df["funding_quartile"].map(quartile_to_idx)
    df["quartile_label"] = df["funding_quartile"].map(quartile_labels)

    # Regime statistics
    grp = df.groupby("quartile_idx")

    stats = pd.DataFrame(
        {
            "Quartile": grp["quartile_label"].first(),
            "Mean_Return_%": grp["y_pct"].mean(),
            "StdDev_Return_%": grp["y_pct"].std(ddof=1),
            "Win_Rate_%": grp.apply(lambda g: (g["y_pct"] > 0).mean() * 100.0),
            "Count_Weeks": grp.size(),
        }
    ).reset_index(drop=True)

    # Print stats table for PM
    print("=== Regime Statistics by Explicit F_tk_apr Quartiles ===")
    print(
        stats.rename(
            columns={
                "Quartile": "Regime",
                "Mean_Return_%": "Mean_%", 
                "StdDev_Return_%": "StdDev_%",
                "Win_Rate_%": "Win_Rate_%",
                "Count_Weeks": "Count",
            }
        ).to_string(index=False, float_format=lambda x: f"{x:0.2f}")
    )
    print()

    # Visualization: strictly aligned two-panel figure
    sns.set(style="whitegrid")
    fig, (ax_top, ax_bottom) = plt.subplots(
        2, 1, figsize=(12, 8), sharex=True, gridspec_kw={"height_ratios": [2, 3]}
    )

    # Ensure quartiles are ordered consistently
    stats = stats.sort_values("Quartile").reset_index(drop=True)
    ordered_labels = stats["Quartile"].tolist()

    # Top panel: Average return bar chart
    mean_returns = stats["Mean_Return_%"].values
    bar_colors = ["green" if m > 0 else "red" for m in mean_returns]

    ax_top.bar(ordered_labels, mean_returns, color=bar_colors)
    ax_top.axhline(0.0, color="black", linewidth=1.0)
    ax_top.set_ylabel("Average Weekly Return (%)", fontsize=12, fontweight="bold")
    ax_top.set_title(
        "Regime Analysis: Strategy Alpha vs. Explicit Funding APR Quartiles",
        fontsize=14,
        fontweight="bold",
    )
    ax_top.grid(True, axis="y", alpha=0.3)

    # Bottom panel: Boxplot of raw weekly returns for each quartile
    sns.boxplot(
        x="quartile_label",
        y="y_pct",
        data=df,
        order=ordered_labels,
        ax=ax_bottom,
        showfliers=True,
    )
    ax_bottom.axhline(0.0, color="red", linestyle="--", linewidth=1.0, alpha=0.7)
    ax_bottom.set_ylabel("Weekly Return Distribution (%)", fontsize=12, fontweight="bold")

    # X-axis labels formatting
    ax_bottom.set_xlabel("Funding APR Regime (Explicit Quartiles)", fontsize=11, fontweight="bold")
    plt.setp(ax_bottom.get_xticklabels(), rotation=20, ha="right")

    fig.tight_layout()

    # Save chart
    output_path = Path("scripts/chart_funding_quartiles_labeled.png")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path, dpi=300)
    print(f"Chart saved to {output_path}")


if __name__ == "__main__":
    run_funding_quartiles_labeled(
        "reports/msm_funding_v0/silver_router_variance_shield/msm_timeseries.csv"
    )

