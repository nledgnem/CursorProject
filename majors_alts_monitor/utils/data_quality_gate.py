from __future__ import annotations

from datetime import timedelta
from typing import Iterable

import numpy as np
import pandas as pd


def _assert_no_nans(df: pd.DataFrame, cols: Iterable[str]) -> None:
    """Raise ValueError if any of the given columns contain NaNs."""
    for col in cols:
        if col not in df.columns:
            raise ValueError(f"DATA QUALITY GATE: Missing required column '{col}' in Gold Layer dataframe.")
        n_missing = int(df[col].isna().sum())
        if n_missing > 0:
            raise ValueError(
                f"DATA QUALITY GATE: NaNs detected in '{col}' for aligned dates "
                f"({n_missing} rows). Gold Layer must be fully dense."
            )


def run_gold_layer_audit(df: pd.DataFrame) -> None:
    """
    Run Zero-Trust data quality tripwires on the final Gold Layer timeseries.

    Expected columns (see data_dictionary.yaml):
    - F_tk_apr: annualized funding rate as decimal APR (F_tk_apr_dec; e.g. 0.04 = 4%)
    - y: 7-day log return of Long Majors / Short Alts spread
    - btcdom_7d_ret: strict 7-day log return of BTCDOM index (decision_date -> next_date)
    - decision_date, next_date: weekly decision and next decision dates
    """
    if df.empty:
        raise ValueError("DATA QUALITY GATE: Gold Layer dataframe is empty.")

    # -------------------------------
    # 1) Unit Bound Tripwire (F_tk_apr)
    # -------------------------------
    if "F_tk_apr" not in df.columns:
        raise ValueError("DATA QUALITY GATE: Missing 'F_tk_apr' column in Gold Layer.")

    max_apr = float(df["F_tk_apr"].max())
    if not np.isfinite(max_apr):
        raise ValueError("DATA QUALITY GATE: F_tk_apr max is not finite.")

    # Assert: decimal APR — for the Top-30 universe the expected scale is typically a few percent APR.
    # We only flag "unit drift" on catastrophic under-scaling (e.g., another accidental /100).
    # Lower bound (0.001) == 0.1% APR in decimal terms.
    if max_apr <= 0.001:
        raise ValueError(
            "UNIT DRIFT DETECTED: F_tk_apr max is suspiciously low for decimal APR. "
            "Check Silver/Gold scaling vs data_dictionary.yaml."
        )
    if max_apr >= 100.0:
        raise ValueError(
            "UNIT DRIFT DETECTED: F_tk_apr max exceeds 10,000% APR as decimal. "
            "Check for runaway values or unit scaling errors."
        )

    # -------------------------------
    # 2) NaNs Tripwire (aligned dates subset)
    # -------------------------------
    # "Aligned dates" = rows where we have a 7-day BTCDOM return
    if "btcdom_7d_ret" not in df.columns:
        raise ValueError("DATA QUALITY GATE: Missing 'btcdom_7d_ret' column in Gold Layer.")

    aligned = df.dropna(subset=["btcdom_7d_ret"]).copy()
    if aligned.empty:
        raise ValueError(
            "DATA QUALITY GATE: No rows with valid btcdom_7d_ret. "
            "BTCDOM alignment or index ingestion may have failed."
        )

    _assert_no_nans(aligned, ["F_tk_apr", "y", "btcdom_7d_ret"])

    # -------------------------------
    # 3) Temporal Desync Tripwire
    # -------------------------------
    for col in ("decision_date", "next_date"):
        if col not in df.columns:
            raise ValueError(f"DATA QUALITY GATE: Missing '{col}' column in Gold Layer.")

    decision_dates = pd.to_datetime(df["decision_date"])
    next_dates = pd.to_datetime(df["next_date"])
    deltas = next_dates - decision_dates

    # All rows must be exactly 7 days apart
    expected_delta = timedelta(days=7)
    bad_mask = deltas != expected_delta
    if bool(bad_mask.any()):
        n_bad = int(bad_mask.sum())
        sample = df.loc[bad_mask, ["decision_date", "next_date"]].head(3)
        raise ValueError(
            "TEMPORAL DESYNC: decision_date and next_date are not exactly 7 days apart "
            f"for {n_bad} rows. Sample:\n{sample.to_string(index=False)}"
        )

    # Optional sanity: enforce consistent weekday for decision_date (typically Monday)
    # Enforce consistent weekday for all HISTORICAL rows, but allow the LIVE (last) row to differ.
    historical_dates = decision_dates.iloc[:-1]
    historical_weekday_counts = historical_dates.dt.weekday.value_counts()

    if len(historical_weekday_counts) > 1:
        raise ValueError(
            "TEMPORAL DESYNC: Historical decision_date weekdays are not consistent. "
            f"Observed counts: {historical_weekday_counts.to_dict()}"
        )

    # Log the presence of the Live intra-week row
    last_date = decision_dates.iloc[-1]
    historical_weekday = historical_weekday_counts.index[0]
    if last_date.weekday() != historical_weekday:
        print(
            f"DATA QUALITY GATE: Validated Live T-0 Row present for {last_date.date()} "
            f"(Weekday {last_date.weekday()} vs Historical {historical_weekday})."
        )

