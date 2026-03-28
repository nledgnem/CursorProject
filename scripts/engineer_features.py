#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
engineer_features.py
====================

Phase 6 + 7: Feature engineering for L1 universe (BTC, ETH, SOL, BNB).

FEATURE 1 — Cross-Sectional Momentum Rank (Phase 6)
----------------------------------------------------
For each calendar day *t* with a valid 00:00 UTC close:

  R14(asset, t) = close(t) / close(t - 14) - 1

Cross-sectional rank on day *t*:
  - Rank all assets by R14 ascending (weakest = rank 1).
  - Normalize to [0.0, 1.0]:  cs_rank = (ordinal - 1) / (n_valid - 1)

FEATURE 2 — Rank-Based Moving Average Trend Filter (Phase 7)
-------------------------------------------------------------
Non-linear absolute trend gate using rolling *medians* (not means).

  med_7   = close.rolling(7,  min_periods=4).median()
  med_30  = close.rolling(30, min_periods=15).median()
  rbma_spread = (med_7 / med_30) - 1
  trend_gate  = 1 if rbma_spread > 0 else 0

Why medians: a single flash-crash or API glitch does not distort the
centre estimate the way a mean does, providing a rank-stable filter.

Temporal alignment contract
---------------------------
All features computed on day *t* (using data up to the 00:00 UTC close of *t*)
are signals for t+1. This script stamps values at *t* (observation date).
The downstream strategy layer must shift +1 day before using as a position
signal to avoid look-ahead bias.

Output
------
  data/features/cross_sectional_rank.parquet
    columns: date, ticker, r14_return, cs_rank, rbma_spread, trend_gate

  validation_output/cs_rank_audit.png
  validation_output/rbma_audit_sol.png
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
TICKERS = ["BTC", "ETH", "SOL", "BNB"]
R14_WINDOW = 14
RBMA_FAST = 7
RBMA_SLOW = 30


def load_wide_prices(tickers: list[str]) -> pd.DataFrame:
    path = REPO_ROOT / "data" / "curated" / "prices_daily.parquet"
    wide = pd.read_parquet(path).sort_index()
    missing = [t for t in tickers if t not in wide.columns]
    if missing:
        raise KeyError(f"Tickers missing from prices_daily: {missing}")
    return wide[tickers].copy()


def compute_r14(wide: pd.DataFrame) -> pd.DataFrame:
    """
    Rolling 14-day cumulative return per asset.

    Formula (compound, not linear average):
        R14(t) = close(t) / close(t - 14) - 1

    We use .pct_change(periods=14) which computes exactly:
        (close(t) - close(t-14)) / close(t-14)  ==  close(t)/close(t-14) - 1
    """
    return wide.pct_change(periods=R14_WINDOW, fill_method=None)


def cross_sectional_rank(r14_wide: pd.DataFrame) -> pd.DataFrame:
    """
    For each row (day), rank assets by R14 ascending (weakest = rank 1),
    then normalize to [0, 1]:

        cs_rank = (ordinal_rank - 1) / (n_valid - 1)

    Uses pandas rank(method='average', ascending=True) which handles ties
    symmetrically and preserves NaN propagation.

    ── exact pandas logic (copy-pasteable for quant review) ──

        ordinal = r14_wide.rank(axis=1, method='average', ascending=True, na_option='keep')
        n_valid = r14_wide.notna().sum(axis=1)
        cs_rank = ordinal.sub(1).div(n_valid.sub(1), axis=0)

    With 4 assets and no NaN: ranks [1,2,3,4] → cs_rank [0.0, 0.333, 0.667, 1.0].
    """
    ordinal = r14_wide.rank(axis=1, method="average", ascending=True, na_option="keep")
    n_valid = r14_wide.notna().sum(axis=1)
    cs_rank = ordinal.sub(1).div(n_valid.sub(1), axis=0)
    return cs_rank


def compute_rbma(wide: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Rank-Based Moving Average (RBMA) trend filter.

    Uses rolling *medians* — not means — so a single flash-crash or API
    glitch does not distort the centre estimate.

    ── exact pandas logic (copy-pasteable for quant review) ──

        med_7       = wide.rolling(7,  min_periods=4).median()
        med_30      = wide.rolling(30, min_periods=15).median()
        rbma_spread = (med_7 / med_30) - 1
        trend_gate  = (rbma_spread > 0).astype(int)

    No look-ahead: rolling windows are strictly causal (use data ≤ t).
    min_periods set to ~half the window to avoid excessive warm-up NaN.
    """
    med_fast = wide.rolling(RBMA_FAST, min_periods=max(4, RBMA_FAST // 2)).median()
    med_slow = wide.rolling(RBMA_SLOW, min_periods=max(15, RBMA_SLOW // 2)).median()
    rbma_spread = (med_fast / med_slow) - 1
    trend_gate = (rbma_spread > 0).astype("Int8")
    # Preserve NaN propagation: where spread is NaN, gate should be pd.NA
    trend_gate = trend_gate.where(rbma_spread.notna())
    return rbma_spread, trend_gate


def build_long_output(
    r14_wide: pd.DataFrame,
    cs_rank_wide: pd.DataFrame,
    rbma_spread_wide: pd.DataFrame,
    trend_gate_wide: pd.DataFrame,
) -> pd.DataFrame:
    """Melt wide feature frames into a single long DataFrame."""
    r14_long = (
        r14_wide.reset_index()
        .melt(id_vars=["date"], var_name="ticker", value_name="r14_return")
    )
    rank_long = (
        cs_rank_wide.reset_index()
        .melt(id_vars=["date"], var_name="ticker", value_name="cs_rank")
    )
    spread_long = (
        rbma_spread_wide.reset_index()
        .melt(id_vars=["date"], var_name="ticker", value_name="rbma_spread")
    )
    gate_long = (
        trend_gate_wide.reset_index()
        .melt(id_vars=["date"], var_name="ticker", value_name="trend_gate")
    )
    merged = (
        r14_long
        .merge(rank_long, on=["date", "ticker"], how="inner")
        .merge(spread_long, on=["date", "ticker"], how="inner")
        .merge(gate_long, on=["date", "ticker"], how="inner")
    )
    merged = merged.sort_values(["date", "ticker"]).reset_index(drop=True)
    return merged


def plot_cs_rank_audit(
    cs_rank_wide: pd.DataFrame,
    out_path: Path,
    months: int = 6,
) -> None:
    """Multi-line chart of cs_rank over last *months* months."""
    end = cs_rank_wide.index.max()
    start = end - pd.DateOffset(months=months)
    sub = cs_rank_wide.loc[cs_rank_wide.index >= start]

    fig, ax = plt.subplots(figsize=(12, 4))
    colors = {"BTC": "#f7931a", "ETH": "#627eea", "SOL": "#00ffa3", "BNB": "#f3ba2f"}
    for ticker in sub.columns:
        ax.plot(
            sub.index,
            sub[ticker].values,
            linewidth=1.2,
            label=ticker,
            color=colors.get(ticker, None),
        )

    ax.set_ylim(-0.05, 1.05)
    ax.set_ylabel("Cross-Sectional Rank (0 = weakest, 1 = strongest)")
    ax.set_title(
        f"L1 Cross-Sectional Momentum Rank (R14) — last {months} months\n"
        "Signal at t uses close(t); downstream must shift +1 for position entry"
    )
    ax.legend(loc="upper left", ncol=4)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def plot_rbma_audit_sol(
    wide: pd.DataFrame,
    rbma_spread_wide: pd.DataFrame,
    trend_gate_wide: pd.DataFrame,
    out_path: Path,
) -> None:
    """
    2-panel chart for SOL:
      Top:    close + 7-day median + 30-day median
      Bottom: rbma_spread oscillator with trend_gate coloring
    """
    sym = "SOL"
    close = wide[sym].astype(float)
    med7 = close.rolling(RBMA_FAST, min_periods=max(4, RBMA_FAST // 2)).median()
    med30 = close.rolling(RBMA_SLOW, min_periods=max(15, RBMA_SLOW // 2)).median()
    spread = rbma_spread_wide[sym].astype(float)
    gate = trend_gate_wide[sym]

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 6), sharex=True, gridspec_kw={"height_ratios": [2, 1]}
    )

    # --- Top panel: price + median overlays ---
    ax1.plot(close.index, close.values, linewidth=0.8, color="steelblue", label="Close")
    ax1.plot(med7.index, med7.values, linewidth=1.0, color="#e85d04", label=f"Med {RBMA_FAST}d")
    ax1.plot(med30.index, med30.values, linewidth=1.0, color="#6a040f", label=f"Med {RBMA_SLOW}d")
    ax1.set_ylabel("SOL Close (USD)")
    ax1.set_title("SOL — RBMA Trend Filter Audit")
    ax1.legend(loc="upper left", ncol=3)
    ax1.grid(True, alpha=0.25)

    # --- Bottom panel: spread oscillator with regime shading ---
    ax2.axhline(0, color="black", linewidth=0.6)
    pos = spread.where(gate == 1)
    neg = spread.where(gate != 1)
    ax2.fill_between(spread.index, 0, pos.values, color="green", alpha=0.4, label="trend_gate=1")
    ax2.fill_between(spread.index, 0, neg.values, color="red", alpha=0.4, label="trend_gate=0")
    ax2.set_ylabel("RBMA Spread")
    ax2.set_xlabel("Date (UTC)")
    ax2.legend(loc="upper left", ncol=2)
    ax2.grid(True, alpha=0.25)

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def main() -> int:
    wide = load_wide_prices(TICKERS)
    wide.index.name = "date"

    # Phase 6: cross-sectional rank
    r14_wide = compute_r14(wide)
    cs_rank_wide = cross_sectional_rank(r14_wide)

    # Phase 7: RBMA trend filter
    rbma_spread_wide, trend_gate_wide = compute_rbma(wide)

    # Merge all features into one long table
    long_df = build_long_output(r14_wide, cs_rank_wide, rbma_spread_wide, trend_gate_wide)

    feat_dir = REPO_ROOT / "data" / "features"
    feat_dir.mkdir(parents=True, exist_ok=True)
    out_parquet = feat_dir / "cross_sectional_rank.parquet"
    long_df.to_parquet(out_parquet, index=False)
    print(f"Saved feature: {out_parquet} ({len(long_df)} rows)")

    # Charts
    chart_path = REPO_ROOT / "validation_output" / "cs_rank_audit.png"
    plot_cs_rank_audit(cs_rank_wide, chart_path)
    print(f"Saved chart:   {chart_path}")

    rbma_chart = REPO_ROOT / "validation_output" / "rbma_audit_sol.png"
    plot_rbma_audit_sol(wide, rbma_spread_wide, trend_gate_wide, rbma_chart)
    print(f"Saved chart:   {rbma_chart}")

    print()
    print("=== Cross-Sectional Rank: exact pandas logic ===")
    print("  r14        = wide.pct_change(periods=14, fill_method=None)")
    print("  ordinal    = r14.rank(axis=1, method='average', ascending=True, na_option='keep')")
    print("  n_valid    = r14.notna().sum(axis=1)")
    print("  cs_rank    = ordinal.sub(1).div(n_valid.sub(1), axis=0)")
    print()
    print("=== RBMA Trend Filter: exact pandas logic ===")
    print(f"  med_{RBMA_FAST}       = wide.rolling({RBMA_FAST},  min_periods={max(4, RBMA_FAST // 2)}).median()")
    print(f"  med_{RBMA_SLOW}      = wide.rolling({RBMA_SLOW}, min_periods={max(15, RBMA_SLOW // 2)}).median()")
    print(f"  rbma_spread = (med_{RBMA_FAST} / med_{RBMA_SLOW}) - 1")
    print("  trend_gate  = (rbma_spread > 0).astype(int)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
