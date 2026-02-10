"""
Generate presentation charts for 15+15 baskets.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT_DIR = Path(__file__).resolve().parents[2] / "outputs" / "ls_basket_low_vol" / "baskets_15x15"
RUNS_DIR = OUT_DIR / "runs"


def load_basket_data():
    """Load summary and PnL for all baskets."""
    summaries = []
    for f in sorted(RUNS_DIR.glob("*_summary.csv")):
        stem = f.stem.replace("_summary", "")
        parts = stem.split("_")
        if len(parts) < 3:
            continue
        rank = int(parts[0].replace("rank", ""))
        btype = parts[1]
        strategy = "_".join(parts[2:])
        df = pd.read_csv(f)
        df["rank"] = rank
        df["basket_type"] = btype
        df["strategy"] = strategy
        df["label"] = f"{btype}\n{strategy}"
        df["file_key"] = stem
        summaries.append(df)
    if not summaries:
        return pd.DataFrame(), {}
    sum_df = pd.concat(summaries, ignore_index=True).sort_values("realized_vol_ann")
    sum_df["rank"] = range(1, len(sum_df) + 1)

    pnl_files = list(RUNS_DIR.glob("*_daily_pnl.csv"))
    pnl_data = {}
    for f in pnl_files:
        key = f.stem.replace("_daily_pnl", "")
        df = pd.read_csv(f)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        pnl_data[key] = df

    return sum_df, pnl_data


def chart_equity_curves(sum_df, pnl_data, top_n=5):
    """Equity curves for top N baskets."""
    fig, ax = plt.subplots(figsize=(10, 5))
    colors = plt.cm.viridis(np.linspace(0.2, 0.9, top_n))

    for i, (_, row) in enumerate(sum_df.head(top_n).iterrows()):
        label = f"{row['basket_type']} / {row['strategy']}"
        key = row["file_key"]
        if key in pnl_data:
            eq = pnl_data[key]["equity"]
            ax.plot(eq.index, eq.values, label=label, color=colors[i], linewidth=2)

    ax.axhline(1.0, color="gray", linestyle="--", alpha=0.5)
    ax.set_title("Portfolio Value Over Time (Top 5 Baskets)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Portfolio Value ($1 start)")
    ax.legend(loc="upper left", fontsize=9)
    ax.set_ylim(bottom=0.5)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "chart_01_equity_curves.png", dpi=150, bbox_inches="tight")
    plt.close()


def chart_volatility_comparison(sum_df, top_n=10):
    """Bar chart of realized volatility by basket."""
    fig, ax = plt.subplots(figsize=(10, 6))
    top = sum_df.head(top_n)
    labels = [f"{r.basket_type}\n{r.strategy}" for _, r in top.iterrows()]
    vols = top["realized_vol_ann"].values * 100
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.9, len(vols)))

    bars = ax.barh(range(len(labels)), vols, color=colors)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("Realized Volatility (% per year)")
    ax.set_title("Basket Volatility Comparison (Lower is Better)", fontsize=14, fontweight="bold")
    ax.invert_yaxis()
    for i, v in enumerate(vols):
        ax.text(v + 0.5, i, f"{v:.1f}%", va="center", fontsize=9)
    ax.grid(True, axis="x", alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "chart_02_volatility_comparison.png", dpi=150, bbox_inches="tight")
    plt.close()


def chart_risk_metrics(sum_df, top_n=5):
    """Stacked/grouped view: vol, max DD, turnover for top baskets."""
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    top = sum_df.head(top_n)
    labels = [f"{r.basket_type}\n{r.strategy}" for _, r in top.iterrows()]
    x = np.arange(len(labels))

    ax1 = axes[0]
    ax1.bar(x, top["realized_vol_ann"].values * 100, color="steelblue", alpha=0.8)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=8, rotation=15, ha="right")
    ax1.set_ylabel("(%)")
    ax1.set_title("Volatility")
    ax1.grid(True, axis="y", alpha=0.3)

    ax2 = axes[1]
    ax2.bar(x, top["max_drawdown"].values * 100, color="coral", alpha=0.8)
    ax2.set_xticks(x)
    ax2.set_xticklabels(labels, fontsize=8, rotation=15, ha="right")
    ax2.set_ylabel("(%)")
    ax2.set_title("Max Drawdown")
    ax2.grid(True, axis="y", alpha=0.3)

    ax3 = axes[2]
    ax3.bar(x, top["avg_turnover"].values * 100, color="seagreen", alpha=0.8)
    ax3.set_xticks(x)
    ax3.set_xticklabels(labels, fontsize=8, rotation=15, ha="right")
    ax3.set_ylabel("(%)")
    ax3.set_title("Avg Turnover")
    ax3.grid(True, axis="y", alpha=0.3)

    fig.suptitle("Key Metrics — Top 5 Baskets", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "chart_03_risk_metrics.png", dpi=150, bbox_inches="tight")
    plt.close()


def chart_best_basket_detail(pnl_data, weights_path):
    """Rolling volatility + drawdown for best basket."""
    key = "rank1_optimized_momentum_rank"
    if key not in pnl_data:
        return

    df = pnl_data[key]
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)

    ax1 = axes[0]
    ax1.plot(df.index, df["equity"], color="steelblue", linewidth=2)
    ax1.fill_between(df.index, 1.0, df["equity"], where=(df["equity"] < 1.0), alpha=0.3, color="red")
    ax1.axhline(1.0, color="gray", linestyle="--", alpha=0.5)
    ax1.set_ylabel("Portfolio Value")
    ax1.set_title("Best Basket: Optimized Momentum Rank — Equity Curve", fontsize=12, fontweight="bold")
    ax1.grid(True, alpha=0.3)

    ax2 = axes[1]
    ret = df["pnl"].dropna()
    roll_vol = ret.rolling(30).std() * np.sqrt(252) * 100
    ax2.plot(roll_vol.index, roll_vol.values, color="darkgreen", alpha=0.8)
    ax2.set_ylabel("Rolling 30d Vol (%)")
    ax2.set_xlabel("Date")
    ax2.set_title("Rolling Volatility (30-day)")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(OUT_DIR / "chart_04_best_basket_detail.png", dpi=150, bbox_inches="tight")
    plt.close()


def chart_best_basket_weights(weights_path):
    """Top longs and shorts for best basket."""
    path = RUNS_DIR / "rank1_optimized_momentum_rank_weights.csv"
    if not path.exists():
        return

    df = pd.read_csv(path)
    df = df.sort_values("weight", key=abs, ascending=False)
    longs = df[df["weight"] > 0].head(10)
    shorts = df[df["weight"] < 0].head(10)

    fig, axes = plt.subplots(1, 2, figsize=(10, 5))

    ax1 = axes[0]
    ax1.barh(range(len(longs)), longs["weight"].values * 100, color="green", alpha=0.7)
    ax1.set_yticks(range(len(longs)))
    ax1.set_yticklabels(longs["symbol"].values)
    ax1.set_xlabel("Weight (%)")
    ax1.set_title("Top 10 Long Positions")
    ax1.invert_yaxis()

    ax2 = axes[1]
    ax2.barh(range(len(shorts)), shorts["weight"].values * 100, color="red", alpha=0.7)
    ax2.set_yticks(range(len(shorts)))
    ax2.set_yticklabels(shorts["symbol"].values)
    ax2.set_xlabel("Weight (%)")
    ax2.set_title("Top 10 Short Positions")

    fig.suptitle("Best Basket — Position Breakdown", fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "chart_05_best_basket_weights.png", dpi=150, bbox_inches="tight")
    plt.close()


def chart_overview_dashboard(sum_df, pnl_data):
    """One-page overview: equity curves + vol bar + key takeaways."""
    fig = plt.figure(figsize=(14, 8))
    gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.25)

    ax1 = fig.add_subplot(gs[0, :])
    for i, (_, row) in enumerate(sum_df.head(3).iterrows()):
        key = row["file_key"]
        if key in pnl_data:
            eq = pnl_data[key]["equity"]
            ax1.plot(eq.index, eq.values, label=f"#{row['rank']} {row['basket_type']} / {row['strategy']}", linewidth=2)
    ax1.axhline(1.0, color="gray", linestyle="--", alpha=0.5)
    ax1.set_title("Top 3 Baskets — Portfolio Value", fontsize=12, fontweight="bold")
    ax1.set_ylabel("Value ($1 start)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2 = fig.add_subplot(gs[1, 0])
    top = sum_df.head(5)
    ax2.barh(range(5), top["realized_vol_ann"].values * 100, color=plt.cm.RdYlGn_r(np.linspace(0.3, 0.8, 5)))
    ax2.set_yticks(range(5))
    ax2.set_yticklabels([f"{r.basket_type} / {r.strategy}" for _, r in top.iterrows()], fontsize=9)
    ax2.set_xlabel("Volatility (%)")
    ax2.set_title("Volatility Ranking")
    ax2.invert_yaxis()

    ax3 = fig.add_subplot(gs[1, 1])
    metrics = ["Volatility", "Max Drawdown", "Turnover"]
    vals = [
        sum_df.iloc[0]["realized_vol_ann"] * 100,
        sum_df.iloc[0]["max_drawdown"] * 100,
        sum_df.iloc[0]["avg_turnover"] * 100,
    ]
    colors = ["#2ecc71", "#e74c3c", "#3498db"]
    bars = ax3.bar(metrics, vals, color=colors)
    ax3.set_ylabel("(%)")
    ax3.set_title("Best Basket — Key Metrics")
    for b, v in zip(bars, vals):
        ax3.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.5, f"{v:.1f}%", ha="center", fontsize=10)

    fig.suptitle("15+15 Long/Short Baskets — Executive Summary", fontsize=16, fontweight="bold", y=1.02)
    plt.savefig(OUT_DIR / "chart_00_executive_summary.png", dpi=150, bbox_inches="tight")
    plt.close()


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sum_df, pnl_data = load_basket_data()
    chart_overview_dashboard(sum_df, pnl_data)
    chart_equity_curves(sum_df, pnl_data)
    chart_volatility_comparison(sum_df)
    chart_risk_metrics(sum_df)
    chart_best_basket_detail(pnl_data, None)
    chart_best_basket_weights(None)
    print(f"Charts saved to {OUT_DIR}")


if __name__ == "__main__":
    main()
