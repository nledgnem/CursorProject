import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def run_threshold_sweep(csv_path: str):
    # 1. Load data and filter to last 2 years (approx 104 weeks)
    df = pd.read_csv(csv_path, parse_dates=["decision_date"])
    df = df.sort_values("decision_date").dropna(subset=["F_tk", "y"])
    df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0  # Unit: APR % (DATA_DICTIONARY.md)
    df_2yr = df.tail(104).copy()

    print(
        f"Running sweep on {len(df_2yr)} weeks from {df_2yr['decision_date'].min().date()} to {df_2yr['decision_date'].max().date()}"
    )

    # 2. Define the Funding Rate grid (R)
    # Using percentiles ensures a smooth curve across where the data actually lives
    R_grid = np.quantile(df_2yr["F_tk"], np.linspace(0, 1, 51))

    results = []

    # 3. Sweep the thresholds
    for R in R_grid:
        # Gate is ON when Funding > R
        pos_k = df_2yr["F_tk"] > R

        # Apply gate to our log returns (y is already Long Maj / Short Alts)
        gated_log_returns = np.where(pos_k, df_2yr["y"], 0.0)

        # Sum log returns and convert back to arithmetic cumulative return
        cum_log_return = np.sum(gated_log_returns)
        cum_arith_return = np.exp(cum_log_return) - 1.0

        results.append(
            {
                "R": R,
                "cum_return": cum_arith_return * 100,  # Convert to percentage
            }
        )

    res_df = pd.DataFrame(results)

    # 4. Plotting the Clean Chart
    fig, ax = plt.subplots(figsize=(10, 6))

    # Single Axis: Cumulative Return vs Funding Rate
    ax.plot(
        res_df["R"],
        res_df["cum_return"],
        color="tab:blue",
        linewidth=2.5,
        marker="o",
        markersize=4,
    )

    ax.set_xlabel(
        "Average Alt Funding Rate Threshold (R)", fontsize=12, fontweight="bold"
    )
    ax.set_ylabel("2-Year Cumulative Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(
        "Strategy Returns vs. Funding Rate Threshold\n(Gate ON when Funding > R)",
        fontsize=14,
    )
    ax.grid(True, alpha=0.3)

    # Add a vertical line at 0 to show the neutral funding baseline
    ax.axvline(
        x=0.0, color="red", linestyle="--", alpha=0.6, label="Neutral Funding (R=0)"
    )
    ax.legend()

    fig.tight_layout()

    # Save the plot
    output_path = Path("reports/funding_threshold_sweep.png")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path, dpi=300)
    print(f"Chart saved to {output_path}")


if __name__ == "__main__":
    # Point this to your latest timeseries CSV that contains the log-returns
    run_threshold_sweep("reports/msm_funding_v0/20260310_103356/msm_timeseries.csv")


