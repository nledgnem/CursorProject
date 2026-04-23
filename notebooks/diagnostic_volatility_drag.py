"""
Generate a PNG visualizing Volatility Drag for the MSM v0 L/S basket:
- Compounded equity curve (cum_raw_ls) vs
- Additive PnL (sum of weekly spread returns y).

Output:
  notebooks/volatility_drag_diagnostic.png
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
MSM_PATH = ROOT / "reports" / "msm_funding_v0" / "msm_v0_full_2023_2026" / "msm_timeseries.csv"
OUT_PNG = ROOT / "notebooks" / "volatility_drag_diagnostic.png"


def main() -> None:
    if not MSM_PATH.exists():
        raise SystemExit(f"msm_timeseries.csv not found at {MSM_PATH}")

    df = pd.read_csv(MSM_PATH, parse_dates=["decision_date", "next_date"])
    if not {"decision_date", "y"} <= set(df.columns):
        raise SystemExit("msm_timeseries.csv missing required columns: decision_date, y")

    df = df.sort_values("decision_date").reset_index(drop=True)
    df = df[["decision_date", "y"]].dropna()

    df["cum_raw_ls"] = (1 + df["y"]).cumprod() - 1
    df["additive_raw_ls"] = df["y"].cumsum()

    fig, ax = plt.subplots(figsize=(12, 7))
    ax.step(
        df["decision_date"],
        df["cum_raw_ls"] * 100,
        where="post",
        label="Compounded Equity (cum_raw_ls)",
        linewidth=2.0,
        color="darkred",
    )
    ax.step(
        df["decision_date"],
        df["additive_raw_ls"] * 100,
        where="post",
        label="Additive PnL (sum y)",
        linewidth=2.0,
        color="navy",
    )
    ax.set_title("Diagnostic: Volatility Drag (Compounded vs Additive L/S Return)", fontsize=20, pad=18)
    ax.set_xlabel("Decision Date", fontsize=16)
    ax.set_ylabel("Return / PnL (%)", fontsize=16)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x:.0f}%"))
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=12, loc="best")
    fig.autofmt_xdate()
    plt.tight_layout()

    OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PNG, dpi=200, bbox_inches="tight")
    plt.close()

    final_comp = float(df["cum_raw_ls"].iloc[-1]) * 100.0
    final_add = float(df["additive_raw_ls"].iloc[-1]) * 100.0
    print(f"Saved: {OUT_PNG}")
    print(f"Final compounded equity (cum_raw_ls): {final_comp:.2f}%")
    print(f"Final additive PnL (sum y): {final_add:.2f}%")


if __name__ == "__main__":
    main()

