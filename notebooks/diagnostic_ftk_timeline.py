"""FT_k Timeline Diagnostic

Market temperature visualization for the 7‑day average daily funding rate
F_tk of the Top 30 Altcoin basket.

Loads the MSM v0 timeseries, plots F_tk over time with the global mean and
the PM's kill‑switch threshold, and prints a concise physics summary.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# Threshold in **percent per day** units (0.0148%).
KILL_SWITCH_THRESHOLD_PCT = 0.0148


def resolve_project_root() -> Path:
    """Assume this file lives in `<root>/scripts/` and return `<root>`."""
    return Path(__file__).resolve().parents[1]


def load_timeseries(csv_path: Path) -> pd.DataFrame:
    """Load MSM timeseries and return a cleaned F_tk track."""
    if not csv_path.exists():
        raise FileNotFoundError(
            f"MSM timeseries CSV not found at {csv_path}. "
            "Ensure MSM v0 run has produced msm_timeseries.csv."
        )

    df = pd.read_csv(csv_path)

    if "decision_date" not in df.columns or "F_tk" not in df.columns:
        raise KeyError(
            "Expected columns 'decision_date' and 'F_tk' not found in "
            f"{csv_path}. Available columns: {sorted(df.columns)}"
        )

    df["decision_date"] = pd.to_datetime(df["decision_date"], errors="coerce")
    if "F_tk_apr" not in df.columns:
        df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0
        df.to_csv(csv_path, index=False)
    df = df.dropna(subset=["decision_date", "F_tk"]).copy()

    # Sort by time to ensure a clean, left‑to‑right sensor track
    df = df.sort_values("decision_date").reset_index(drop=True)
    return df


def load_optimal_threshold_apr(report_dir: Path) -> float | None:
    """Load optimal T_apr from optimal_threshold_apr.json if present."""
    path = report_dir / "optimal_threshold_apr.json"
    if not path.exists():
        return None
    with open(path) as f:
        data = json.load(f)
    return data.get("optimal_threshold_apr")


def plot_ftk_timeline(df: pd.DataFrame) -> Path:
    """Generate the F_tk timeline chart and save to scripts/.

    Plots F_tk on a %/day scale for visual interpretation.
    """
    mean_ftk = df["F_tk"].mean()

    out_path = Path(__file__).with_name("chart_ftk_timeline.png")

    plt.style.use("seaborn-v0_8-darkgrid")
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)

    x = df["decision_date"]
    # Convert to percent per day for plotting
    y = df["F_tk"] * 100.0

    # Core timeline
    ax.plot(x, y, color="blue", linewidth=1.5, label="F_tk (7d Avg Daily Funding)")

    # Shaded area under the curve
    ax.fill_between(x, y, 0.0, color="steelblue", alpha=0.2)

    # Global mean (green dashed)
    ax.axhline(
        mean_ftk * 100.0,
        color="green",
        linestyle="--",
        linewidth=1.5,
        label=f"Global Mean F_tk ({mean_ftk * 100.0:.4f}% / day)",
    )

    # Kill-switch threshold (red bold dashed), in %/day
    ax.axhline(
        KILL_SWITCH_THRESHOLD_PCT,
        color="red",
        linestyle="--",
        linewidth=2.0,
        label="Threshold: 0.0148% / day",
    )

    ax.set_title("Market Temperature: F_tk (Daily Funding Rate) Over Time")
    ax.set_xlabel("Decision Date")
    ax.set_ylabel("Average Daily Funding Rate (% per day)")

    # Tight x-axis margins to keep the sensor visually centered
    fig.autofmt_xdate()
    ax.legend()
    fig.tight_layout()

    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)

    return out_path


def plot_ftk_timeline_apr(
    df: pd.DataFrame,
    optimal_t_apr: float | None,
    out_path: Path,
) -> Path:
    """Plot F_tk_apr over time with global mean and optional optimal threshold."""
    if "F_tk_apr" not in df.columns:
        df = df.copy()
        df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0
    df_apr = df.dropna(subset=["F_tk_apr", "decision_date"]).copy()
    if df_apr.empty:
        return out_path
    mean_apr = df_apr["F_tk_apr"].mean()
    plt.style.use("seaborn-v0_8-darkgrid")
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
    x = df_apr["decision_date"]
    y = df_apr["F_tk_apr"]
    ax.plot(x, y, color="blue", linewidth=1.5, label="F_tk (APR %)")
    ax.fill_between(x, y, 0.0, color="steelblue", alpha=0.2)
    ax.axhline(
        mean_apr,
        color="green",
        linestyle="--",
        linewidth=1.5,
        label=f"Global Mean F_tk (APR): {mean_apr:.2f}%",
    )
    if optimal_t_apr is not None:
        ax.axhline(
            optimal_t_apr,
            color="red",
            linestyle="--",
            linewidth=2.0,
            label=f"Optimal Kill-Switch: {optimal_t_apr:.2f}% APR",
        )
    ax.set_title("Market Temperature: F_tk (Annualized APR) Over Time")
    ax.set_xlabel("Decision Date")
    ax.set_ylabel("Average Funding Rate (APR %)")
    fig.autofmt_xdate()
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


def print_physics_summary(df: pd.DataFrame) -> None:
    """Print the requested terminal summary."""
    start_date = df["decision_date"].min().date()
    end_date = df["decision_date"].max().date()

    mean_ftk = df["F_tk"].mean()
    max_ftk = df["F_tk"].max()

    # Weeks where the sensor breached the kill-switch threshold
    # Threshold is defined in %/day; convert F_tk to %/day for comparison.
    n_breached = int(((df["F_tk"] * 100.0) >= KILL_SWITCH_THRESHOLD_PCT).sum())

    print("\n" + "=" * 72)
    print("F_TK MARKET TEMPERATURE SUMMARY")
    print("=" * 72)
    print(f"Start Date            : {start_date}")
    print(f"End Date              : {end_date}")
    print(f"Global Mean F_tk      : {mean_ftk:.6f}")
    print(f"Global Maximum F_tk   : {max_ftk:.6f}")
    print(
        f"Weeks breaching T=0.0148%/day : {n_breached} "
        "(F_tk >= 0.0148%/day threshold)"
    )
    print("=" * 72 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Diagnostic timeline of F_tk (7‑day average daily funding rate) "
            "with global mean and kill-switch threshold."
        )
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default=None,
        help=(
            "Optional override path to msm_timeseries.csv. "
            "Defaults to reports/msm_funding_v0/silver_router_variance_shield/msm_timeseries.csv"
        ),
    )

    args = parser.parse_args()

    if args.csv_path:
        csv_path = Path(args.csv_path).expanduser().resolve()
    else:
        csv_path = (
            resolve_project_root()
            / "reports"
            / "msm_funding_v0"
            / "silver_router_variance_shield"
            / "msm_timeseries.csv"
        )

    df = load_timeseries(csv_path)

    chart_path = plot_ftk_timeline(df)
    print_physics_summary(df)
    print(f"Saved F_tk timeline chart to: {chart_path}")

    # APR timeline chart and terminal report
    report_dir = csv_path.parent
    optimal_t_apr = load_optimal_threshold_apr(report_dir)
    apr_path = Path(__file__).with_name("chart_ftk_timeline_apr.png")
    plot_ftk_timeline_apr(df, optimal_t_apr, apr_path)
    print(f"Saved F_tk APR timeline to: {apr_path}")

    if "F_tk_apr" not in df.columns:
        df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0
    mean_apr = df["F_tk_apr"].mean()
    max_apr = df["F_tk_apr"].max()
    print("\n" + "=" * 72)
    print("APR METRICS (Simple Annualized)")
    print("=" * 72)
    print(f"Global Mean F_tk (APR): {mean_apr:.2f}%")
    if optimal_t_apr is not None:
        print(f"Optimal Kill-Switch Threshold (APR): {optimal_t_apr:.2f}%")
    else:
        print("Optimal Kill-Switch Threshold (APR): (run diagnostic_optimal_threshold_sweep.py first)")
    print(f"Max Historical F_tk (APR): {max_apr:.2f}%")
    print("=" * 72 + "\n")


if __name__ == "__main__":
    main()

