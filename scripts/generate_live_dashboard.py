"""Generate a live, read-only MSM Funding Macro Sensor dashboard in the terminal.

This script is a pure presentation layer:
- It NEVER hits external APIs.
- It ONLY reads the latest Gold Layer `msm_timeseries.csv` produced by the pipeline.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


def find_latest_timeseries_csv(base_reports_dir: Path) -> Optional[Path]:
    """Locate the most recently modified `msm_timeseries.csv` under the reports tree."""
    pattern = "msm_timeseries.csv"
    candidates = list(base_reports_dir.rglob(pattern))

    if not candidates:
        print(f"No `{pattern}` files found under {base_reports_dir.resolve()}.")
        return None

    # Sort by modification time (newest first)
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    latest = candidates[0]

    print(f"Using latest Gold Layer timeseries: {latest.resolve()}")
    return latest


def load_sorted_timeseries(csv_path: Path) -> pd.DataFrame:
    """Load `msm_timeseries.csv`, parse dates, and sort chronologically."""
    df = pd.read_csv(csv_path)
    if "decision_date" not in df.columns:
        raise ValueError("Expected 'decision_date' column in msm_timeseries.csv.")

    df["decision_date"] = pd.to_datetime(df["decision_date"])
    df = df.sort_values("decision_date").reset_index(drop=True)
    if df.empty:
        raise ValueError("Timeseries CSV is empty after loading.")
    return df


def evaluate_asymmetric_safe_sticky(df: pd.DataFrame) -> Tuple[bool, int]:
    """Apply the Asymmetric Safe-Sticky state machine over history and return (is_on, duration_weeks).

    Rules (applied sequentially over decision_date) calibrated to the Top-64 Liquid Universe.
    F_tk_apr is decimal annualized APR (data_dictionary: F_tk_apr_dec).
    - Entry (Goldilocks): 1.0%–4.0% APR -> 0.01 <= f_apr_dec <= 0.04
    - Exit (Cold): f_apr_dec < 0.0
    - Exit (Toxic): f_apr_dec > 0.04
    """
    if "F_tk_apr" not in df.columns:
        raise ValueError("Expected 'F_tk_apr' column in msm_timeseries.csv.")

    is_on_history = []
    is_on = False

    for _, row in df.iterrows():
        f_apr_dec = float(row["F_tk_apr"])

        # ZERO-TRUST PATCH: Thresholds mapped to pure decimals
        # Entry gate: 1.0% to 4.0% APR
        if not is_on and 0.01 <= f_apr_dec <= 0.04:
            is_on = True
        # Exit gates
        elif is_on:
            if f_apr_dec < 0.0:
                is_on = False  # Cold exit (Negative funding)
            elif f_apr_dec > 0.04:
                is_on = False  # Toxic exit (Outlier squeeze)

        is_on_history.append(is_on)

    if not is_on_history:
        return False, 0

    current_state = is_on_history[-1]

    # Count consecutive weeks in current state, walking backwards from T-0
    duration_weeks = 0
    for state in reversed(is_on_history):
        if state == current_state:
            duration_weeks += 1
        else:
            break

    return current_state, duration_weeks


def print_terminal_tearsheet(df: pd.DataFrame, csv_path: Path, is_on: bool, duration_weeks: int) -> None:
    """Render the live terminal tear sheet for the MSM Funding Macro Sensor."""
    last_row = df.iloc[-1]
    decision_date = last_row["decision_date"]
    f_apr_dec = float(last_row["F_tk_apr"])

    source_path = csv_path.resolve()
    state_label = "[ON] DEPLOYED" if is_on else "[OFF] STAY CASH"

    print("=========================================================")
    print("MSM FUNDING MACRO SENSOR - LIVE STATUS")
    print("=========================================================")
    print(f"Data Source: {source_path}")
    print(f"Date of Last Snapshot: {decision_date.date()}")
    print(f"Current Funding Rate (APR): {f_apr_dec * 100:.2f}%")
    print()
    print("---------------------------------------------------------")
    print(f"TARGET PORTFOLIO STATE: {state_label}")
    print("---------------------------------------------------------")
    print(f"Time in Current State: {duration_weeks} Weeks")
    print("=========================================================")


def main() -> None:
    base_reports_dir = Path("reports")
    latest_csv = find_latest_timeseries_csv(base_reports_dir)
    if latest_csv is None:
        return

    df = load_sorted_timeseries(latest_csv)
    is_on, duration_weeks = evaluate_asymmetric_safe_sticky(df)
    print_terminal_tearsheet(df, latest_csv, is_on, duration_weeks)


if __name__ == "__main__":
    main()

