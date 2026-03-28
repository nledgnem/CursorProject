#!/usr/bin/env python3
"""
Phase 1: Visual audit suite for Environment_APR vs forward altcoin basket returns.
Phase 3: Spearman/Pearson correlations vs y; export master_macro_features.csv.

Reads msm_timeseries.csv (expects Environment_APR, Delta_APR, r_alts, y from gold pipeline).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import linregress, spearmanr, pearsonr
from statsmodels.nonparametric.smoothers_lowess import lowess

REPO_ROOT = Path(__file__).resolve().parent.parent


def _find_latest_msm_csv(reports_root: Path) -> Optional[Path]:
    run_dirs = [p for p in reports_root.iterdir() if p.is_dir()]
    if not run_dirs:
        return None
    latest = max(run_dirs, key=lambda p: p.stat().st_mtime)
    p = latest / "msm_timeseries.csv"
    return p if p.exists() else None


def log_modulus(x: np.ndarray) -> np.ndarray:
    return np.sign(x) * np.log1p(np.abs(x))


def plot_histogram(env_pct: pd.Series, out_path: Path) -> None:
    x = env_pct.dropna().astype(float)
    if len(x) == 0:
        return
    lo, hi = float(np.floor(x.min())), float(np.ceil(x.max()))
    bins = np.arange(lo, hi + 1.01, 1.0)
    fig, ax = plt.subplots(figsize=(11, 6), dpi=150)
    ax.hist(x, bins=bins, color="steelblue", edgecolor="white", linewidth=0.3)
    ax.axvline(0.0, color="red", linestyle="--", linewidth=1.5, label="Cold boundary (0%)")
    ax.set_xlabel("Weekly Environment_APR (% points)")
    ax.set_ylabel("Count")
    ax.set_title("Environment_APR — univariate histogram (1% bins)")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(True, alpha=0.25)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_raw_lowess(x: pd.Series, y_ret: pd.Series, out_path: Path) -> None:
    df = pd.DataFrame({"x": x, "y": y_ret}).dropna()
    if len(df) < 10:
        return
    xs = df["x"].values.astype(float)
    ys = df["y"].values.astype(float)
    sort_idx = np.argsort(xs)
    xs_s, ys_s = xs[sort_idx], ys[sort_idx]
    smoothed = lowess(ys_s, xs_s, frac=0.35, return_sorted=True)
    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    ax.scatter(xs, ys, s=14, alpha=0.45, color="navy")
    ax.plot(smoothed[:, 0], smoothed[:, 1], color="darkorange", linewidth=2.2, label="LOWESS")
    ax.set_xlabel("Weekly Environment_APR (% points)")
    ax.set_ylabel("Forward 7-day altcoin basket return (%)")
    ax.set_title("Raw vs LOWESS — leverage exhaustion in the right tail")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.25)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_log_modulus_linear(x: pd.Series, y_ret: pd.Series, out_path: Path) -> None:
    df = pd.DataFrame({"x": x, "y": y_ret}).dropna()
    if len(df) < 10:
        return
    xm = log_modulus(df["x"].values.astype(float))
    ym = df["y"].values.astype(float)
    mask = np.isfinite(xm) & np.isfinite(ym)
    xm, ym = xm[mask], ym[mask]
    lr = linregress(xm, ym)
    xx = np.linspace(np.nanmin(xm), np.nanmax(xm), 200)
    yy = lr.slope * xx + lr.intercept
    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    ax.scatter(xm, ym, s=14, alpha=0.45, color="teal")
    ax.plot(xx, yy, color="crimson", linewidth=2.0, label=f"OLS (R²={lr.rvalue**2:.3f})")
    ax.set_xlabel("Log-Modulus(Environment_APR)")
    ax.set_ylabel("Forward 7-day altcoin basket return (%)")
    ax.set_title("Log-modulus transform — linear fit restored")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.25)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_delta_lowess(delta: pd.Series, y_ret: pd.Series, out_path: Path) -> None:
    df = pd.DataFrame({"x": delta, "y": y_ret}).dropna()
    if len(df) < 10:
        return
    xs = df["x"].values.astype(float)
    ys = df["y"].values.astype(float)
    sort_idx = np.argsort(xs)
    xs_s, ys_s = xs[sort_idx], ys[sort_idx]
    smoothed = lowess(ys_s, xs_s, frac=0.4, return_sorted=True)
    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    ax.scatter(xs, ys, s=14, alpha=0.45, color="purple")
    ax.plot(smoothed[:, 0], smoothed[:, 1], color="darkgreen", linewidth=2.2, label="LOWESS")
    ax.set_xlabel("Week-over-week Δ Environment_APR (% points)")
    ax.set_ylabel("Forward 7-day altcoin basket return (%)")
    ax.set_title("Momentum trap (Δ) — inverted U via LOWESS")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.25)
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def correlation_tables(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    cols = ["Environment_APR", "Z_Score_90d", "Fragmentation_Spread", "Conditioned_Momentum", "y"]
    sub = df[[c for c in cols if c in df.columns]].copy()
    sub = sub.dropna()
    if len(sub) < 5:
        empty = pd.DataFrame()
        return empty, empty
    pear = sub.corr(method="pearson")
    spear = sub.corr(method="spearman")
    return pear, spear


def run_audit(
    msm_csv: Path,
    out_dir: Path,
):
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(msm_csv, parse_dates=["decision_date", "next_date"])
    need = ["Environment_APR", "r_alts", "y", "Delta_APR"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"msm_timeseries missing columns: {missing}")

    r_alt_pct = df["r_alts"].astype(float) * 100.0

    plot_histogram(df["Environment_APR"], out_dir / "audit_histogram_environment_apr.png")
    plot_raw_lowess(df["Environment_APR"], r_alt_pct, out_dir / "audit_scatter_raw_vs_lowess.png")
    plot_log_modulus_linear(df["Environment_APR"], r_alt_pct, out_dir / "audit_scatter_log_modulus_linear.png")
    plot_delta_lowess(df["Delta_APR"], r_alt_pct, out_dir / "audit_scatter_delta_momentum_trap.png")

    master_path = out_dir / "master_macro_features.csv"
    df.to_csv(master_path, index=False)

    pear, spear = correlation_tables(df)
    if not pear.empty:
        pear.to_csv(out_dir / "macro_correlation_pearson.csv")
        spear.to_csv(out_dir / "macro_correlation_spearman.csv")

    return pear, spear, master_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Macro Environment visual audit + master export")
    parser.add_argument(
        "--msm-csv",
        type=Path,
        default=None,
        help="Path to msm_timeseries.csv (default: latest under reports/msm_funding_v0)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory for PNGs and CSVs (default: same folder as msm-csv)",
    )
    args = parser.parse_args()

    if args.msm_csv is not None:
        msm_csv = args.msm_csv.resolve()
    else:
        cand = _find_latest_msm_csv(REPO_ROOT / "reports" / "msm_funding_v0")
        if cand is None:
            raise SystemExit("No msm_timeseries.csv found; pass --msm-csv")
        msm_csv = cand

    out_dir = args.out_dir.resolve() if args.out_dir else msm_csv.parent / "macro_audit"
    pear, spear, master_path = run_audit(msm_csv, out_dir)

    print(f"Wrote audit charts and master export under: {out_dir}")
    print(f"master_macro_features.csv -> {master_path}")
    if not pear.empty:
        print("\n--- Pearson (Environment_APR, Z_Score_90d, Fragmentation_Spread, Conditioned_Momentum, y) ---")
        print(pear.round(4).to_string())
        print("\n--- Spearman ---")
        print(spear.round(4).to_string())


if __name__ == "__main__":
    main()
