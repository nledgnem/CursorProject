import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def _max_drawdown_active(equity: pd.Series, active: pd.Series) -> float:
    """Compute max drawdown (in %) restricted to periods where strategy is active."""
    active_equity = equity[active]
    if active_equity.empty:
        return 0.0
    running_max = active_equity.cummax()
    drawdown = (active_equity - running_max) / running_max
    max_dd = drawdown.min()  # negative number or 0
    return abs(max_dd) * 100.0


def run_sticky_hysteresis_simulation(csv_path: str):
    # Load data
    df = pd.read_csv(csv_path, parse_dates=["decision_date"])

    # Keep only needed columns and sort chronologically
    df = df[["decision_date", "F_tk_apr", "y"]].dropna(subset=["F_tk_apr", "y"]).copy()
    df = df.sort_values("decision_date").reset_index(drop=True)

    if df.empty:
        raise ValueError("No valid rows after filtering for 'F_tk_apr' and 'y'.")

    # Convert weekly log return y to simple percentage return
    df["y_pct"] = (np.exp(df["y"]) - 1.0) * 100.0

    # --- Step 1: Sticky State Machine ---
    entry_lower = 1.0
    entry_upper = 4.0
    exit_lower = 0.0
    exit_upper = 5.0

    is_on = False
    strategy_on = []
    exit_event = []

    for f_val in df["F_tk_apr"].values:
        prev_on = is_on
        if not is_on:
            if entry_lower <= f_val <= entry_upper:
                is_on = True
        else:
            if f_val < exit_lower or f_val > exit_upper:
                is_on = False
        strategy_on.append(is_on)
        exit_event.append(prev_on and not is_on)

    df["strategy_on"] = strategy_on
    df["exit_event"] = exit_event

    # Exit classifications
    df["exit_cold"] = df["exit_event"] & (df["F_tk_apr"] < 0.0)
    df["exit_hot"] = df["exit_event"] & (df["F_tk_apr"] > 5.0)

    # --- Step 2: Returns & Cumulative Equity ---
    df["strategy_return"] = np.where(df["strategy_on"], df["y_pct"], 0.0)
    df["equity_curve"] = (1.0 + df["strategy_return"] / 100.0).cumprod()
    df["cumulative_return"] = df["equity_curve"] - 1.0

    final_cum_return_pct = df["cumulative_return"].iloc[-1] * 100.0
    total_on_weeks = int(df["strategy_on"].sum())

    # Exit statistics
    total_exits = int(df["exit_event"].sum())
    exits_cold = int(df["exit_cold"].sum())
    exits_hot = int(df["exit_hot"].sum())

    exit_week_avg_return = (
        df.loc[df["exit_event"], "y_pct"].mean() if total_exits > 0 else 0.0
    )

    max_dd_active_pct = _max_drawdown_active(df["equity_curve"], df["strategy_on"])

    # --- Step 3: Visualization (3-Panel Chart) ---
    dates = df["decision_date"]

    fig, axes = plt.subplots(
        3, 1, figsize=(14, 9), sharex=True, gridspec_kw={"hspace": 0.1}
    )

    # Top Panel: Funding Temperature with background zones
    ax_top = axes[0]
    f_vals = df["F_tk_apr"].values

    # Background regime coloring per week via axvspan
    # Blue: < 0, Green/Yellow: 0-4, Orange/Red: >4
    for d, f in zip(dates, f_vals):
        if f < 0.0:
            color = "lightblue"
        elif f <= 4.0:
            color = "palegreen"
        else:
            color = "mistyrose"
        ax_top.axvspan(d, d + pd.Timedelta(days=7), color=color, alpha=0.5)

    ax_top.plot(dates, f_vals, color="black", linewidth=1.5)
    ax_top.set_ylabel("F_tk_apr (%)", fontsize=10, fontweight="bold")
    # Add legend / text box summarizing exits
    summary_text = (
        f"Exits: {total_exits} Total | {exits_cold} Cold (<0%) | {exits_hot} Hot (>5%)"
    )
    ax_top.set_title(
        "Sticky Hysteresis Audit: Tracking Exit Events (0-5% Boundary)",
        fontsize=14,
        fontweight="bold",
    )
    ax_top.text(
        0.01,
        0.02,
        summary_text,
        transform=ax_top.transAxes,
        fontsize=10,
        verticalalignment="bottom",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
    )

    # Middle Panel: Strategy Activity (raw y_pct bars colored by strategy_on)
    ax_mid = axes[1]
    colors_mid = ["green" if on else "grey" for on in df["strategy_on"]]
    ax_mid.bar(dates, df["y_pct"], color=colors_mid, width=5)
    ax_mid.axhline(0.0, color="grey", linestyle="--", linewidth=1.0, alpha=0.7)
    ax_mid.set_ylabel("Weekly Return (%)", fontsize=10, fontweight="bold")

    # Middle panel: mark exit events
    for d, flag in zip(dates, df["exit_event"]):
        if flag:
            ax_mid.axvline(d, color="red", linestyle="--", linewidth=1.0, alpha=0.9)

    # Bottom Panel: Cumulative Equity
    ax_bot = axes[2]
    ax_bot.plot(dates, df["cumulative_return"] * 100.0, color="blue", linewidth=2)
    ax_bot.axhline(0.0, color="grey", linestyle="--", linewidth=1.0, alpha=0.7)
    ax_bot.set_ylabel("Cumulative Return (%)", fontsize=10, fontweight="bold")
    ax_bot.set_xlabel("Decision Date", fontsize=10, fontweight="bold")

    # Bottom panel: mark exit events
    for d, flag in zip(dates, df["exit_event"]):
        if flag:
            ax_bot.axvline(d, color="red", linestyle="--", linewidth=1.0, alpha=0.9)

    # Rotate x-axis labels for readability
    for ax in axes:
        for label in ax.get_xticklabels():
            label.set_rotation(30)
            label.set_ha("right")

    fig.tight_layout()

    output_path = Path("scripts/chart_sticky_hysteresis_exits.png")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path, dpi=300)

    # --- Step 4: Terminal Stats ---
    print("=== Sticky Hysteresis Exit Audit ===")
    print(f"Total Weeks Strategy was ON: {total_on_weeks}")
    print(f"Total Exit Events: {total_exits}")
    print(f"Exits due to Cold Market (<0%): {exits_cold}")
    print(f"Exits due to Toxic Market (>5%): {exits_hot}")
    print(f"Final Cumulative Return: {final_cum_return_pct:.2f}%")
    print(f"Max Drawdown during Active Periods: {max_dd_active_pct:.2f}%")
    print(f"Average Return of the 'Exit Week': {exit_week_avg_return:.2f}%")
    print(f"Chart saved to {output_path}")


if __name__ == "__main__":
    run_sticky_hysteresis_simulation(
        "reports/msm_funding_v0/silver_router_variance_shield/msm_timeseries.csv"
    )

