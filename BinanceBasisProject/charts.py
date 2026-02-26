"""
Optional charts: APR distribution per window, per-symbol time series.
"""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def plot_apr_distribution(df: pd.DataFrame, out_dir: Path, window_days: int) -> None:
    """Plot distribution of apr_simple across symbols for a window."""
    apr = df["apr_simple"].dropna()
    if apr.empty:
        return
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(apr, bins=50, edgecolor="black", alpha=0.7)
    ax.axvline(0, color="red", linestyle="--")
    ax.set_xlabel("APR (simple, %)")
    ax.set_ylabel("Count")
    ax.set_title(f"Funding APR distribution across symbols (window={window_days}d, n={len(apr)})")
    fig.tight_layout()
    path = out_dir / f"apr_dist_{window_days}d.png"
    fig.savefig(path, dpi=120)
    plt.close()


def plot_symbol_series(rows: list[dict], symbol: str, out_dir: Path) -> None:
    """Plot per-symbol cumulative funding time series."""
    df = pd.DataFrame(rows)
    if df.empty or "fundingRate" not in df.columns:
        return
    df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
    df = df.dropna(subset=["fundingRate"])
    if df.empty:
        return
    df["ts"] = pd.to_datetime(df["fundingTime"], unit="ms")
    df = df.sort_values("ts")
    df["cum_funding"] = df["fundingRate"].cumsum()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    ax1.plot(df["ts"], df["fundingRate"] * 100, alpha=0.8)
    ax1.axhline(0, color="gray", linestyle="--")
    ax1.set_ylabel("Funding rate (%)")
    ax1.set_title(f"{symbol} - Funding rate per interval")
    ax2.plot(df["ts"], df["cum_funding"] * 100)
    ax2.axhline(0, color="gray", linestyle="--")
    ax2.set_ylabel("Cumulative funding (%)")
    ax2.set_title("Cumulative funding return (short-perp)")
    ax2.set_xlabel("Date")
    fig.suptitle(f"{symbol} funding history")
    fig.tight_layout()
    path = out_dir / f"series_{symbol}.png"
    fig.savefig(path, dpi=120)
    plt.close()
