"""
Validation: align reconstructed BTCDOM to Binance index klines and compute metrics.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def align_series(
    recon: pd.DataFrame,
    binance: pd.DataFrame,
    *,
    time_col_recon: str = "timestamp",
    value_col_recon: str = "btcdom_recon",
    time_col_binance: str = "timestamp",
    value_col_binance: str = "close",
) -> pd.DataFrame:
    """
    Align recon and Binance to common timestamps (inner join).
    Returns DataFrame with timestamp, recon, binance, error, pct_error.
    """
    r = recon[[time_col_recon, value_col_recon]].copy()
    r = r.rename(columns={time_col_recon: "timestamp", value_col_recon: "recon"})
    b = binance[[time_col_binance, value_col_binance]].copy()
    b = b.rename(columns={time_col_binance: "timestamp", value_col_binance: "binance"})
    r["timestamp"] = pd.to_datetime(r["timestamp"]).dt.tz_localize(None)
    b["timestamp"] = pd.to_datetime(b["timestamp"]).dt.tz_localize(None)
    merged = pd.merge(r, b, on="timestamp", how="inner")
    merged["error"] = merged["recon"] - merged["binance"]
    merged["pct_error"] = np.where(
        merged["binance"].abs() > 1e-12,
        merged["error"] / merged["binance"] * 100.0,
        np.nan,
    )
    return merged


def compute_metrics(merged: pd.DataFrame) -> dict[str, Any]:
    """Compute MAE, MAPE, max_abs_error, correlation."""
    if merged.empty or "error" not in merged.columns:
        return {
            "mae": None,
            "mape_pct": None,
            "max_abs_error": None,
            "correlation": None,
            "n_aligned": 0,
        }
    err = merged["error"].dropna()
    pct_err = merged["pct_error"].replace([np.inf, -np.inf], np.nan).dropna()
    mae = float(err.abs().mean()) if len(err) else None
    mape = float(pct_err.abs().mean()) if len(pct_err) else None
    max_abs = float(err.abs().max()) if len(err) else None
    corr = (
        float(merged["recon"].corr(merged["binance"]))
        if len(merged) > 1 and merged["binance"].std() > 0
        else None
    )
    return {
        "mae": mae,
        "mape_pct": mape,
        "max_abs_error": max_abs,
        "correlation": corr,
        "n_aligned": int(len(merged)),
    }


def save_validation_outputs(
    recon: pd.DataFrame,
    binance: pd.DataFrame,
    metrics: dict[str, Any],
    out_dir: str | Path,
    *,
    plot: bool = True,
) -> None:
    """Save recon.csv, binance.csv, metrics.json, and optional overlay.png."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    recon_out = recon.copy()
    if "timestamp" in recon_out.columns:
        recon_out["timestamp"] = pd.to_datetime(recon_out["timestamp"])
    recon_out.to_csv(out / "recon.csv", index=False)
    binance.to_csv(out / "binance.csv", index=False)
    with open(out / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    if plot:
        # Recon-only chart (reconstructed BTCDOM index)
        recon_ts = pd.to_datetime(recon["timestamp"])
        plt.figure(figsize=(12, 5))
        plt.plot(recon_ts, recon["btcdom_recon"], color="#f0b90b", linewidth=1.2)
        plt.xlabel("Timestamp")
        plt.ylabel("BTCDOM (reconstructed)")
        plt.title("Reconstructed BTCDOM index from data lake")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(out / "btcdom_recon_chart.png", dpi=150)
        plt.close()
        logger.info("Saved btcdom_recon_chart.png to %s", out)

        merged = align_series(recon, binance)
        if merged.empty:
            logger.warning("No aligned points for overlay plot")
            return
        fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
        ax1, ax2 = axes
        ax1.plot(merged["timestamp"], merged["recon"], label="Recon", color="C0", alpha=0.9)
        ax1.plot(merged["timestamp"], merged["binance"], label="Binance", color="C1", alpha=0.9)
        ax1.set_ylabel("BTCDOM index")
        ax1.legend(loc="upper right")
        ax1.set_title("BTCDOM: Reconstructed vs Binance index")
        ax1.grid(True, alpha=0.3)
        ax2.plot(merged["timestamp"], merged["error"], color="C2", alpha=0.9)
        ax2.axhline(0, color="gray", ls="--")
        ax2.set_ylabel("Error (recon - binance)")
        ax2.set_xlabel("Timestamp")
        ax2.set_title("Reconstruction error")
        ax2.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(out / "overlay.png", dpi=150)
        plt.close()
        logger.info("Saved overlay.png to %s", out)


def rolling_error(merged: pd.DataFrame, window: int = 24) -> pd.Series:
    """Rolling MAE of error (for optional analysis)."""
    if merged.empty or "error" not in merged.columns:
        return pd.Series(dtype=float)
    return merged.set_index("timestamp")["error"].rolling(window, min_periods=1).apply(
        lambda x: np.abs(x).mean(), raw=True
    )
