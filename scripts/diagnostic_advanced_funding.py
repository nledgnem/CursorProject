"""
Momentum & Regime Duration diagnostics for advanced funding features.

- First derivative (momentum): week-over-week change in funding vs log return spread.
- Regime duration: average return by consecutive weeks above high-funding threshold.
"""

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats

# Paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
TS_PATH = REPO_ROOT / "reports" / "msm_funding_v0" / "20260310_103356" / "msm_timeseries.csv"
OUT_MOMENTUM = REPO_ROOT / "reports" / "funding_momentum_scatter.png"
OUT_DURATION = REPO_ROOT / "reports" / "funding_duration_bar.png"
WEEKS_LOOKBACK = 104


def load_and_prepare() -> pd.DataFrame:
    """Load timeseries, last 104 weeks, drop NaNs in F_tk and y."""
    df = pd.read_csv(TS_PATH)
    df["decision_date"] = pd.to_datetime(df["decision_date"])
    df = df.sort_values("decision_date").tail(WEEKS_LOOKBACK).copy()
    df = df.dropna(subset=["F_tk", "y"])
    return df


def momentum_diagnostics(df: pd.DataFrame) -> None:
    """Delta F vs y scatter, OLS line, R² and p-value; save plot."""
    df = df.copy()
    df["delta_F"] = df["F_tk"].diff()
    # Drop first row (NaN from diff)
    plot_df = df.dropna(subset=["delta_F", "y"])

    x = plot_df["delta_F"].values
    y_vals = plot_df["y"].values
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y_vals)
    r_squared = r_value**2

    print("=== First Derivative (Momentum) ===")
    print(f"R-squared: {r_squared:.4f}")
    print(f"P-value:  {p_value:.4e}")

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(plot_df["delta_F"], plot_df["y"], alpha=0.6, s=24, edgecolors="none")
    x_line = np.linspace(x.min(), x.max(), 100)
    ax.plot(x_line, slope * x_line + intercept, "r-", lw=2, label="OLS")
    ax.set_xlabel("delta_F (week-over-week change in funding)")
    ax.set_ylabel("y (Log Return Spread)")
    ax.set_title("Funding momentum vs log return spread")
    ax.legend()
    ax.axhline(0, color="gray", ls="--", alpha=0.5)
    ax.axvline(0, color="gray", ls="--", alpha=0.5)
    plt.tight_layout()
    OUT_MOMENTUM.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_MOMENTUM, dpi=150)
    plt.close()
    print(f"Saved: {OUT_MOMENTUM}")


def duration_diagnostics(df: pd.DataFrame) -> None:
    """T_high (consecutive weeks above 75th %ile) vs average return; bar chart."""
    threshold = df["F_tk"].quantile(0.75)
    print(f"\n=== Regime Duration (Coiled Spring) ===")
    print(f"High Funding Threshold (75th pct): {threshold:.6f}")

    # Rolling counter: if F_tk > threshold increment, else reset to 0
    t_high = []
    counter = 0
    for v in df["F_tk"].values:
        if v > threshold:
            counter += 1
        else:
            counter = 0
        t_high.append(counter)
    df = df.copy()
    df["T_high"] = t_high

    # Bin: 0, 1, 2, 3+ weeks
    df["T_high_bin"] = df["T_high"].clip(upper=3)
    df.loc[df["T_high"] > 3, "T_high_bin"] = 3  # 3+ weeks

    # Group by T_high bin: 0, 1, 2, 3 (meaning 3+)
    group_avg_log = df.groupby("T_high_bin")["y"].mean()
    # Arithmetic return in %: (exp(avg_log) - 1) * 100
    avg_return_pct = (np.exp(group_avg_log) - 1) * 100

    labels = ["0 weeks", "1 week", "2 weeks", "3+ weeks"]
    x_pos = np.arange(len(labels))
    vals = [avg_return_pct.get(i, np.nan) for i in range(4)]
    counts = df.groupby("T_high_bin").size()
    n_obs = [counts.get(i, 0) for i in range(4)]

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(x_pos, vals, color="steelblue", edgecolor="black", linewidth=0.8)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels)
    ax.set_xlabel("T_high (consecutive weeks above threshold)")
    ax.set_ylabel("Average return (%)")
    ax.set_title("Regime duration vs average return (Coiled Spring)")
    ax.axhline(0, color="gray", ls="--", alpha=0.5)
    for i, (v, n) in enumerate(zip(vals, n_obs)):
        ax.text(i, v + (0.3 if v >= 0 else -0.3), f"n={n}", ha="center", fontsize=8)
    plt.tight_layout()
    OUT_DURATION.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_DURATION, dpi=150)
    plt.close()
    print("Average return by T_high:", {l: round(v, 3) for l, v in zip(labels, vals)})
    print(f"Saved: {OUT_DURATION}")


def main() -> None:
    df = load_and_prepare()
    print(f"Loaded {len(df)} rows (last {WEEKS_LOOKBACK} weeks, NaNs dropped).")
    momentum_diagnostics(df)
    duration_diagnostics(df)


if __name__ == "__main__":
    main()
