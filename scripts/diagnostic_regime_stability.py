import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def _count_streaks(mask: pd.Series) -> int:
    """Count continuous True streaks in a boolean Series."""
    # Convert to int to detect rising edges (0 -> 1)
    arr = mask.astype(int).values
    if arr.size == 0:
        return 0
    starts = (arr[1:] == 1) & (arr[:-1] == 0)
    # If the very first element is True, that's also a start
    return int(mask.iloc[0]) + int(starts.sum())


def run_regime_stability_diagnostic(csv_path: str):
    # Load data
    df = pd.read_csv(csv_path, parse_dates=["decision_date"])

    # Keep only needed columns and sort chronologically
    df = df[["decision_date", "F_tk_apr", "y"]].dropna(subset=["F_tk_apr", "y"]).copy()
    df = df.sort_values("decision_date").reset_index(drop=True)

    if df.empty:
        raise ValueError("No valid rows after filtering for 'F_tk_apr' and 'y'.")

    # Convert weekly log return y to simple percentage return
    df["y_pct"] = (np.exp(df["y"]) - 1.0) * 100.0

    # --- State Machine Parameters ---
    lower = 1.0
    upper = 4.0
    buffer = 0.5

    lower_exit = lower - buffer  # 0.5
    upper_exit = upper + buffer  # 4.5

    # Comparison flags (raw)
    df["regime_raw"] = (df["F_tk_apr"] >= lower) & (df["F_tk_apr"] <= upper)
    df["regime_below"] = df["F_tk_apr"] < lower
    df["regime_above"] = df["F_tk_apr"] > upper

    # --- Buffered state machine (hysteresis) ---
    in_regime = False
    in_regime_buffered = []

    for val in df["F_tk_apr"].values:
        if not in_regime:
            # Enter regime when within [lower, upper]
            if lower <= val <= upper:
                in_regime = True
        else:
            # Stay in regime until we leave buffered band
            if val < lower_exit or val > upper_exit:
                in_regime = False
        in_regime_buffered.append(in_regime)

    df["in_regime_buffered"] = in_regime_buffered

    # --- Step 2: Physical Counts & Streaks ---
    total_raw_weeks = int(df["regime_raw"].sum())

    raw_streaks = _count_streaks(df["regime_raw"])
    buffered_streaks = _count_streaks(df["in_regime_buffered"])

    total_buffered_weeks = int(df["in_regime_buffered"].sum())
    avg_buffered_streak = (
        total_buffered_weeks / buffered_streaks if buffered_streaks > 0 else 0.0
    )

    # --- Step 3: 4-Panel Visualization ---
    dates = df["decision_date"]
    y_all = df["y_pct"]

    fig, axes = plt.subplots(
        4, 1, figsize=(14, 10), sharex=True, gridspec_kw={"hspace": 0.1}
    )

    # Graph 1: Raw LS with coloring by 1-4% band membership (raw definition)
    colors_raw = ["green" if in_band else "pink" for in_band in df["regime_raw"]]
    axes[0].bar(dates, y_all, color=colors_raw, width=5)
    axes[0].axhline(0.0, color="grey", linestyle="--", linewidth=1.0, alpha=0.7)
    axes[0].set_ylabel("All Returns (%)", fontsize=10, fontweight="bold")
    axes[0].set_title(
        "Sensor Regime Audit: 1-4% APR Performance & Stability",
        fontsize=14,
        fontweight="bold",
    )

    # Graph 2: Alpha Zone (1.0 <= F_tk_apr <= 4.0)
    alpha_returns = np.where(df["regime_raw"], y_all, 0.0)
    axes[1].bar(dates, alpha_returns, color="green", width=5)
    axes[1].axhline(0.0, color="grey", linestyle="--", linewidth=1.0, alpha=0.7)
    axes[1].set_ylabel("Alpha Zone (%)", fontsize=10, fontweight="bold")

    # Graph 3: Cold Zone (F_tk_apr < 1.0)
    cold_returns = np.where(df["regime_below"], y_all, 0.0)
    axes[2].bar(dates, cold_returns, color="pink", width=5)
    axes[2].axhline(0.0, color="grey", linestyle="--", linewidth=1.0, alpha=0.7)
    axes[2].set_ylabel("Cold Zone (%)", fontsize=10, fontweight="bold")

    # Graph 4: Toxic Zone (F_tk_apr > 4.0)
    toxic_returns = np.where(df["regime_above"], y_all, 0.0)
    axes[3].bar(dates, toxic_returns, color="darkred", width=5)
    axes[3].axhline(0.0, color="grey", linestyle="--", linewidth=1.0, alpha=0.7)
    axes[3].set_ylabel("Toxic Zone (%)", fontsize=10, fontweight="bold")
    axes[3].set_xlabel("Decision Date", fontsize=10, fontweight="bold")

    # Rotate x-axis labels for readability
    for ax in axes:
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_ha("right")

    fig.tight_layout()

    output_path = Path("scripts/chart_regime_stability_audit.png")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path, dpi=300)

    # --- Step 4: Terminal Output ---
    print("=== Regime Stability Audit (1-4% APR with 0.5% Hysteresis) ===")
    print(f"Total Weeks in Raw 1-4% Range: {total_raw_weeks}")
    print(f"Continuous Periods (Raw): {raw_streaks}")
    print(f"Continuous Periods (Buffered 0.5%): {buffered_streaks}")
    print(f"Average Streak Length (Buffered): {avg_buffered_streak:.2f} weeks")
    print(f"Chart saved to {output_path}")


if __name__ == "__main__":
    run_regime_stability_diagnostic(
        "reports/msm_funding_v0/silver_router_variance_shield/msm_timeseries.csv"
    )

