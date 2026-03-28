#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
visualize_top_30.py
===================

Phase 5: Visual Cross-Sectional Audit (Top 30 Assets)

Generates a trellis (small-multiples) grid of daily close prices for the
top 30 assets by:
  - median 30-day rolling USD volume over the most recent month of data.

This script intentionally avoids plotting 30 assets on one shared axis.
Each subplot gets an independent y-axis scale (`sharey=False`).

Backfill highlight:
  Shade the UTC daily window 2026-01-30 through 2026-03-03 (inclusive).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parent.parent

BACKFILL_START = pd.Timestamp("2026-01-30")
BACKFILL_END_INCLUSIVE = pd.Timestamp("2026-03-03")


def _is_bad_symbol(col) -> bool:
    """
    Filter out pathological column labels seen in parquet exports.
    """
    if col is None:
        return True
    if isinstance(col, float) and np.isnan(col):
        return True
    s = str(col).strip().lower()
    return s == "nan"


def select_top_30_by_volume(
    volume_df: pd.DataFrame,
    window_days: int = 30,
    last_month_days: int = 30,
    top_k: int = 30,
) -> List[str]:
    """
    Top-K selection score:
      score(asset) = median( rolling_median(volume(asset), window_days)
                              over last_month_days slice )
    """
    if volume_df.empty:
        return []

    if not isinstance(volume_df.index, pd.DatetimeIndex):
        volume_df = volume_df.copy()
        volume_df.index = pd.to_datetime(volume_df.index)

    idx = volume_df.index
    end_ts = idx.max()
    start_ts = end_ts - pd.Timedelta(days=last_month_days)
    last_month_mask = (idx >= start_ts) & (idx <= end_ts)

    if not bool(last_month_mask.any()):
        raise ValueError(
            "No data found in the last-month slice. "
            f"idx min/max={idx.min()}..{idx.max()}, computed slice={start_ts}..{end_ts}"
        )

    # Rolling median using `window_days` *periods*.
    # Given our daily panel definition, this corresponds to 30 calendar days.
    rolling_med = volume_df.rolling(
        window=window_days,
        min_periods=max(5, window_days // 2),
    ).median()

    scores: dict[str, float] = {}
    for col in rolling_med.columns:
        if _is_bad_symbol(col):
            continue
        series = rolling_med[col]
        score = series.loc[last_month_mask].median(skipna=True)
        if pd.notna(score):
            scores[str(col)] = float(score)

    if not scores:
        raise RuntimeError("No assets produced non-NaN volume scores.")

    ranked = (
        pd.Series(scores, dtype="float64")
        .sort_values(ascending=False)
        .head(top_k)
    )
    return ranked.index.tolist()


def plot_price_grid(
    prices_df: pd.DataFrame,
    tickers: List[str],
    out_path: Path,
    shade_color: str = "orange",
    shade_alpha: float = 0.15,
) -> None:
    """
    Trellis grid of daily close prices with independent y-scales.
    """
    n = len(tickers)
    if n == 0:
        raise ValueError("No tickers provided.")

    ncols = 6
    nrows = int(np.ceil(n / ncols))

    fig, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(ncols * 3.0, nrows * 2.0),
        sharex=True,
        sharey=False,  # critical: each subplot independent y-axis
        constrained_layout=True,
    )
    axes_arr = np.array(axes).reshape(-1)

    # Inclusive shading: add one day so the last tick is visibly covered.
    shade_start = BACKFILL_START
    shade_end = BACKFILL_END_INCLUSIVE + pd.Timedelta(days=1)

    for i, ticker in enumerate(tickers):
        ax = axes_arr[i]
        if ticker not in prices_df.columns:
            ax.set_title(f"{ticker} (missing)")
            ax.grid(True, alpha=0.25)
            continue

        s = prices_df[ticker].astype(float)
        ax.plot(prices_df.index, s.values, linewidth=0.7, color="steelblue")
        ax.axvspan(shade_start, shade_end, color=shade_color, alpha=shade_alpha)
        ax.set_title(ticker, fontsize=10)
        ax.grid(True, alpha=0.25)

    # Hide unused subplots.
    for j in range(n, len(axes_arr)):
        axes_arr[j].set_visible(False)

    fig.suptitle(
        "Top 30 assets by median 30-day rolling USD volume\n"
        "Small multiples of daily close prices (backfill window shaded)",
        fontsize=12,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220)
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser(description="Top-30 small-multiples price audit plotter.")
    p.add_argument(
        "--prices-parquet",
        type=Path,
        default=REPO_ROOT / "data/curated/prices_daily.parquet",
    )
    p.add_argument(
        "--volume-parquet",
        type=Path,
        default=REPO_ROOT / "data/curated/volume_daily.parquet",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "validation_output/top_30_price_grid.png",
    )
    args = p.parse_args()

    prices_df = pd.read_parquet(args.prices_parquet)
    volume_df = pd.read_parquet(args.volume_parquet)

    # Align shared x-axis dates so the grid is consistent.
    common_idx = prices_df.index.intersection(volume_df.index)
    prices_df = prices_df.loc[common_idx]
    volume_df = volume_df.loc[common_idx]

    tickers = select_top_30_by_volume(volume_df=volume_df)
    plot_price_grid(prices_df=prices_df, tickers=tickers, out_path=args.out)

    print("Top 30 tickers selected by median 30-day rolling USD volume score:")
    for t in tickers:
        print(t)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

