"""
Diagnostic: Absolute funding threshold sweep for MSM v0.

For a grid of absolute funding thresholds, simulate a simple gate:
  - If F_tk < threshold: weekly return = y (gate ON)
  - If F_tk >= threshold: weekly return = 0 (gate OFF)

Then compute for each threshold:
  - 2-year and 3-year cumulative return
  - Annualized Sharpe (RF=0, 52 periods/year)
  - Max drawdown (%)

Results are printed as a markdown-style table.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
MSM_PATH = ROOT / "reports" / "msm_funding_v0" / "msm_v0_full_2023_2026" / "msm_timeseries.csv"


def annualized_sharpe(r: pd.Series, periods_per_year: int = 52) -> float:
    m, s = r.mean(), r.std()
    if s is None or s == 0 or np.isnan(s):
        return float("nan")
    return float((m / s) * np.sqrt(periods_per_year))


def max_drawdown_pct(r: pd.Series) -> float:
    wealth = (1 + r).cumprod()
    peaks = wealth.cummax()
    dd = (wealth - peaks) / peaks
    return float(dd.min() * 100.0)


def cumulative_return(r: pd.Series) -> float:
    if r.empty:
        return float("nan")
    return float((1 + r).prod() - 1.0)


def sweep_thresholds(df: pd.DataFrame, thresholds: List[float]) -> pd.DataFrame:
    df = df.copy().sort_values("decision_date").reset_index(drop=True)
    max_date = df["decision_date"].max()
    two_year_start = max_date - timedelta(days=2 * 365)
    three_year_start = max_date - timedelta(days=3 * 365)

    rows = []
    for thr in thresholds:
        gate_on = df["F_tk"] < thr
        y_thr = np.where(gate_on, df["y"], 0.0)
        df_thr = df.copy()
        df_thr["y_thr"] = y_thr

        # 2Y window
        df_2y = df_thr[df_thr["decision_date"] >= two_year_start]
        # 3Y window
        df_3y = df_thr[df_thr["decision_date"] >= three_year_start]

        r_2y = df_2y["y_thr"]
        r_3y = df_3y["y_thr"]

        rows.append(
            {
                "Threshold": thr,
                "2Y_CumRet": cumulative_return(r_2y),
                "3Y_CumRet": cumulative_return(r_3y),
                "Sharpe": annualized_sharpe(df_thr["y_thr"]),
                "MaxDD": max_drawdown_pct(df_thr["y_thr"]),
            }
        )

    return pd.DataFrame(rows)


def format_markdown_table(df: pd.DataFrame) -> str:
    # Round for display
    out = df.copy()
    out["2Y_CumRet"] = out["2Y_CumRet"].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "NA")
    out["3Y_CumRet"] = out["3Y_CumRet"].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "NA")
    out["Sharpe"] = out["Sharpe"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "NA")
    out["MaxDD"] = out["MaxDD"].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "NA")

    headers = ["Threshold", "2Y_CumRet", "3Y_CumRet", "Sharpe", "MaxDD"]
    lines = []
    # Header
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    # Rows
    for _, row in out.iterrows():
        vals = [row[h] for h in headers]
        lines.append("| " + " | ".join(str(v) for v in vals) + " |")
    return "\n".join(lines)


def main() -> None:
    if not MSM_PATH.exists():
        raise SystemExit(f"msm_timeseries.csv not found at {MSM_PATH}")

    msm = pd.read_csv(MSM_PATH, parse_dates=["decision_date", "next_date"])
    if not {"decision_date", "F_tk", "y"} <= set(msm.columns):
        raise SystemExit("msm_timeseries.csv missing required columns: decision_date, F_tk, y")

    # Absolute funding thresholds (F_tk stored as decimal funding rate)
    thresholds = [0.0, 0.01, 0.02, 0.05, 0.10, 0.15, 0.20, 0.25, 0.50, 1.0]

    result = sweep_thresholds(msm[["decision_date", "F_tk", "y"]].dropna(), thresholds)
    print("\n## Absolute Funding Threshold Sweep (Gate OFF when F_tk >= threshold)\n")
    print(format_markdown_table(result))


if __name__ == "__main__":
    main()

