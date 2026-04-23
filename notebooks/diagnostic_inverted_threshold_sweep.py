import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def run_inverted_threshold_sweep(csv_path: str):
    df = pd.read_csv(csv_path, parse_dates=["decision_date"])
    df = df.sort_values("decision_date")

    df = df[["decision_date", "F_tk_apr", "y"]].dropna(subset=["F_tk_apr", "y"])

    if df.empty:
        raise ValueError("No valid rows after filtering for 'F_tk_apr' and 'y'.")

    F_min = df["F_tk_apr"].min()
    F_p95 = df["F_tk_apr"].quantile(0.95)

    T_grid = np.linspace(F_min, F_p95, 100)

    results = []

    total_weeks = len(df)

    for T in T_grid:
        mask_on = df["F_tk_apr"] >= T

        gated_log_returns = np.where(mask_on, df["y"], 0.0)

        cum_log_return = np.sum(gated_log_returns)
        cum_arith_return = np.exp(cum_log_return) - 1.0

        weeks_on = int(mask_on.sum())

        results.append(
            {
                "T_apr": T,
                "cum_return_pct": cum_arith_return * 100.0,
                "weeks_on": weeks_on,
            }
        )

    res_df = pd.DataFrame(results)

    idx_opt = res_df["cum_return_pct"].idxmax()
    opt_row = res_df.loc[idx_opt]

    T_opt = float(opt_row["T_apr"])
    max_return_pct = float(opt_row["cum_return_pct"])
    weeks_on_opt = int(opt_row["weeks_on"])

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(
        res_df["T_apr"],
        res_df["cum_return_pct"],
        color="tab:blue",
        linewidth=2.5,
    )

    ax.scatter(
        [T_opt],
        [max_return_pct],
        color="red",
        s=80,
        zorder=5,
    )

    label_text = (
        f"Optimal Entry: T = {T_opt:.2f}% APR\n"
        f"Max Return: {max_return_pct:.2f}%"
    )
    ax.text(
        T_opt,
        max_return_pct,
        label_text,
        fontsize=10,
        fontweight="bold",
        color="red",
        ha="left",
        va="bottom",
    )

    ax.set_xlabel(
        "Threshold T (F_tk_apr ≥ T) [APR %]",
        fontsize=12,
        fontweight="bold",
    )
    ax.set_ylabel("Cumulative Return (%)", fontsize=12, fontweight="bold")
    ax.set_title(
        "Yield Hunter Sweep: Cumulative Return vs Minimum APR Entry Threshold",
        fontsize=14,
        fontweight="bold",
    )
    ax.grid(True, alpha=0.3)

    fig.tight_layout()

    output_path = Path("scripts/chart_inverted_threshold_sweep.png")
    output_path.parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(output_path, dpi=300)

    print(f"Optimal Minimum Entry Threshold (T): {T_opt:.2f}% APR")
    print(f"Maximum Cumulative Return at Optimal T: {max_return_pct:.2f}%")
    print(
        "Total Weeks Strategy was ON (Deployed) at this T: "
        f"{weeks_on_opt} / {total_weeks}"
    )


if __name__ == "__main__":
    run_inverted_threshold_sweep(
        "reports/msm_funding_v0/silver_router_variance_shield/msm_timeseries.csv"
    )

