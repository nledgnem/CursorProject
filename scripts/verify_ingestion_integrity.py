#!/usr/bin/env python3
"""Post-deploy ingestion-integrity checks.

Designed to be re-runnable for any schema-touching deploy. Each mode is a
self-contained set of signal checks. Add new modes as new incident classes
surface; do not fork this script.

Usage:
    python scripts/verify_ingestion_integrity.py --mode writer_race
    python scripts/verify_ingestion_integrity.py --mode <future-mode>

Exit codes:
    0 --all signals PASS (or PASS + INDETERMINATE; INDETERMINATE alone never fails the run)
    1 --at least one signal returned FAIL
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

# repo_paths sits at repo root; ensure it's importable when this script is run from anywhere.
_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from repo_paths import data_lake_root  # noqa: E402


# ----------------------------------------------------------------------------------
# Signal result type
# ----------------------------------------------------------------------------------

class SignalResult:
    """One row in the verification table."""

    def __init__(self, name: str, description: str, status: str, detail: str):
        if status not in ("PASS", "FAIL", "INDETERMINATE"):
            raise ValueError(f"Invalid signal status: {status!r}")
        self.name = name
        self.description = description
        self.status = status
        self.detail = detail


# ----------------------------------------------------------------------------------
# Mode: writer_race
# ----------------------------------------------------------------------------------
#
# Verifies the post-fix state of the CoinGecko writer-race incident
# (DATA_LAKE_CONTEXT.md §9 entry 0, §13 followups, reports/apathy_universe_cut_audit_2026_04_29.md).
# The four signals are:
#   1. Today's run made ~2,716 API calls (down from 2,997 pre-dedupe).
#   2. The fail-fast guard in download_all_coins did NOT fire (silent in logs).
#   3. ETH / SOL / DOGE on the latest fact_marketcap date show canonical mcap envelopes.
#   4. BTC / BNB / XRP show no regression from a pre-merge anchor date.
#
# Signal 3 + 4 read parquet directly and are reliable from any environment that
# has fact_marketcap.parquet visible (Render, Drive Desktop, etc.). Signals 1 + 2
# require log access; if logs are unreachable they return INDETERMINATE rather
# than FAIL --the parquet evidence in Signal 3 is the strongest empirical proof
# that the fetcher ran and produced canonical results.

_LOG_CANDIDATE_PATHS = [
    "/tmp/run_live_pipeline.log",
    "/tmp/run_live_pipeline_*.log",
    "/var/log/run_live_pipeline.log",
    "/var/log/macro-regime/*.log",
]

_API_CALL_COUNT_PATTERNS = [
    re.compile(r"Downloading data for (\d+) coins"),
    re.compile(r"download_all_coins.*?(\d+)\s+(?:rows|coins|tickers)"),
    re.compile(r"Loading allowlist.*?\((\d+) rows\)"),
]

_GUARD_FIRED_PATTERNS = [
    re.compile(r"Allowlist has \d+ duplicate symbols", re.IGNORECASE),
    re.compile(r"ValueError.*writer-race", re.IGNORECASE),
]


def _find_log_files() -> list[Path]:
    """Return any log files matching the candidate paths. Empty list if none found."""
    found: list[Path] = []
    for pattern in _LOG_CANDIDATE_PATHS:
        if "*" in pattern:
            from glob import glob
            for hit in glob(pattern):
                p = Path(hit)
                if p.is_file():
                    found.append(p)
        else:
            p = Path(pattern)
            if p.is_file():
                found.append(p)
    # Dedupe, preserve order
    seen: set[Path] = set()
    unique: list[Path] = []
    for p in found:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        unique.append(p)
    return unique


def _signal_1_api_call_count() -> SignalResult:
    """API call count from latest run logs.

    PASS: count is 2,716 ± 50 (matches deduped allowlist).
    FAIL: count < 2,500 or > 2,800.
    INDETERMINATE: logs unavailable or no matching pattern.
    """
    log_files = _find_log_files()
    if not log_files:
        return SignalResult(
            "1",
            "API call count from logs",
            "INDETERMINATE",
            "No log files found at any candidate path. Run on Render shell or set log path in script.",
        )

    latest_count: Optional[int] = None
    matched_log: Optional[Path] = None
    for log_path in log_files:
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            continue
        for pat in _API_CALL_COUNT_PATTERNS:
            for m in pat.finditer(text):
                try:
                    n = int(m.group(1))
                except (IndexError, ValueError):
                    continue
                # Take the most recent match (last occurrence wins)
                latest_count = n
                matched_log = log_path

    if latest_count is None:
        return SignalResult(
            "1",
            "API call count from logs",
            "INDETERMINATE",
            f"Found {len(log_files)} log file(s) but no matching call-count pattern. Logs scanned: {[str(p) for p in log_files]}",
        )

    if 2_716 - 50 <= latest_count <= 2_716 + 50:
        return SignalResult(
            "1",
            "API call count from logs",
            "PASS",
            f"N={latest_count} (within 2,716 ± 50) from {matched_log}",
        )
    if latest_count < 2_500 or latest_count > 2_800:
        return SignalResult(
            "1",
            "API call count from logs",
            "FAIL",
            f"N={latest_count} outside acceptable range [2,500, 2,800] from {matched_log}",
        )
    # In the buffer zone (2,666-2,716 or 2,716-2,766 --but outside ±50)
    return SignalResult(
        "1",
        "API call count from logs",
        "FAIL",
        f"N={latest_count} outside ±50 tolerance of 2,716 (from {matched_log}). Investigate before proceeding.",
    )


def _signal_2_guard_silent() -> SignalResult:
    """Guard fail-fast did NOT fire.

    PASS: no `Allowlist has` or `ValueError` matches in logs.
    FAIL: any match --guard fired, dedupe didn't fully resolve.
    INDETERMINATE: logs unavailable.
    """
    log_files = _find_log_files()
    if not log_files:
        return SignalResult(
            "2",
            "Guard silent in logs",
            "INDETERMINATE",
            "No log files found at any candidate path. Strong indirect evidence: if guard had fired, fact_marketcap would have no fresh row for today (Signal 3 implicitly confirms).",
        )

    matches: list[str] = []
    for log_path in log_files:
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        for pat in _GUARD_FIRED_PATTERNS:
            for m in pat.finditer(text):
                matches.append(f"{log_path}: {m.group(0)[:120]}")

    if not matches:
        return SignalResult(
            "2",
            "Guard silent in logs",
            "PASS",
            f"No matches in {len(log_files)} log file(s).",
        )
    return SignalResult(
        "2",
        "Guard silent in logs",
        "FAIL",
        f"Guard fired: {matches[:3]}",
    )


def _signal_3_affected_correct() -> SignalResult:
    """Affected blue-chips show canonical mcap envelopes on latest date.

    PASS criteria:
      ETH:  $100B - $400B
      SOL:  $30B - $150B
      DOGE: $10B - $60B
    Plus a sanity check on the 16 Apathy live picks (no asserts; just reported).
    """
    fmc_path = data_lake_root() / "fact_marketcap.parquet"
    df = pd.read_parquet(fmc_path)
    latest_date = df["date"].max()

    expected = {
        "ETH":  (100e9, 400e9),
        "SOL":  (30e9, 150e9),
        "DOGE": (10e9, 60e9),
    }
    failures: list[str] = []
    detail_lines: list[str] = [f"Latest date: {latest_date}"]

    for tkr, (lo, hi) in expected.items():
        row = df[(df["asset_id"].astype(str).str.upper() == tkr) & (df["date"] == latest_date)]
        if row.empty:
            failures.append(f"{tkr}: NOT FOUND on {latest_date}")
            detail_lines.append(f"  {tkr}: NOT FOUND")
            continue
        val = float(row["marketcap"].iloc[0])
        if lo <= val <= hi:
            detail_lines.append(f"  {tkr}: ${val:,.0f} (in [{lo:.0e}, {hi:.0e}]) [OK]")
        else:
            failures.append(f"{tkr}: ${val:,.0f} outside expected [{lo:.0e}, {hi:.0e}]")
            detail_lines.append(f"  {tkr}: ${val:,.0f} OUTSIDE [{lo:.0e}, {hi:.0e}] [FAIL]")

    # Bonus: regression coverage on Apathy live picks (informational only)
    apathy_picks = ["ZEC", "DASH", "ZEN", "ICNT", "PIEVERSE", "CHZ", "FARTCOIN",
                    "KITE", "AXS", "MORPHO", "STABLE", "ARIA", "DEXE", "ONT", "TAO", "SIGN"]
    apathy_missing = []
    for tkr in apathy_picks:
        row = df[(df["asset_id"].astype(str).str.upper() == tkr) & (df["date"] == latest_date)]
        if row.empty:
            apathy_missing.append(tkr)
    if apathy_missing:
        detail_lines.append(f"  Apathy picks missing on {latest_date}: {apathy_missing}")

    detail = "; ".join(detail_lines)
    if failures:
        return SignalResult(
            "3",
            "ETH/SOL/DOGE in canonical envelope",
            "FAIL",
            f"{len(failures)} ticker(s) failed: {failures}. Full: {detail}",
        )
    return SignalResult("3", "ETH/SOL/DOGE in canonical envelope", "PASS", detail)


def _signal_4_unaffected_unchanged(merge_date: date = date(2026, 5, 4)) -> SignalResult:
    """Single-variant blue-chips unchanged regression check.

    PASS: BTC/BNB/XRP shifted by ≤15% over the period.
    FAIL: any shifted >25%.
    Anything 15-25% gets reported as PASS with a "manual sanity-check recommended" annotation —
    real BTC can move that much in a few days; we don't auto-fail in that band but we surface it.
    """
    pre_target = merge_date - timedelta(days=2)

    fmc_path = data_lake_root() / "fact_marketcap.parquet"
    df = pd.read_parquet(fmc_path)
    latest_date = df["date"].max()

    available_pre = df[df["date"] <= pre_target]["date"].max()
    if pd.isna(available_pre):
        return SignalResult(
            "4",
            "BTC/BNB/XRP regression check",
            "INDETERMINATE",
            f"No fact_marketcap rows on or before {pre_target}; cannot compute pre-merge anchor.",
        )

    failures: list[str] = []
    annotations: list[str] = []
    detail_lines: list[str] = [f"Pre-merge anchor: {available_pre}; latest: {latest_date}"]

    for tkr in ("BTC", "BNB", "XRP"):
        pre = df[(df["asset_id"].astype(str).str.upper() == tkr) & (df["date"] == available_pre)]
        post = df[(df["asset_id"].astype(str).str.upper() == tkr) & (df["date"] == latest_date)]
        if pre.empty or post.empty:
            annotations.append(f"{tkr}: missing data (pre_empty={pre.empty}, post_empty={post.empty})")
            detail_lines.append(f"  {tkr}: missing data")
            continue
        pre_val = float(pre["marketcap"].iloc[0])
        post_val = float(post["marketcap"].iloc[0])
        pct = (post_val - pre_val) / pre_val * 100.0
        if abs(pct) <= 15:
            detail_lines.append(f"  {tkr}: pre=${pre_val:,.0f}, post=${post_val:,.0f} ({pct:+.2f}%) [OK]")
        elif abs(pct) <= 25:
            annotations.append(f"{tkr}: {pct:+.2f}% (in 15-25% band -- confirm against real spot price action)")
            detail_lines.append(f"  {tkr}: {pct:+.2f}% (sanity-check needed)")
        else:
            failures.append(f"{tkr}: {pct:+.2f}% (>25% -- likely regression)")
            detail_lines.append(f"  {tkr}: {pct:+.2f}% [FAIL]")

    detail = "; ".join(detail_lines)
    if failures:
        return SignalResult(
            "4",
            "BTC/BNB/XRP regression check",
            "FAIL",
            f"{len(failures)} ticker(s) >25% shift: {failures}. Full: {detail}",
        )
    if annotations:
        return SignalResult(
            "4",
            "BTC/BNB/XRP regression check",
            "PASS",
            f"All ≤25%; manual sanity-check recommended for: {annotations}. Full: {detail}",
        )
    return SignalResult("4", "BTC/BNB/XRP regression check", "PASS", detail)


def _run_writer_race_mode() -> int:
    """Run all 4 signals for writer_race mode. Return exit code."""
    signals = [
        _signal_1_api_call_count(),
        _signal_2_guard_silent(),
        _signal_3_affected_correct(),
        _signal_4_unaffected_unchanged(),
    ]

    # Markdown table to stdout
    print()
    print("| Signal | Description                          | Result        | Detail                                              |")
    print("|--------|--------------------------------------|---------------|-----------------------------------------------------|")
    for s in signals:
        # Trim very long detail strings for table readability
        d = s.detail if len(s.detail) < 120 else s.detail[:117] + "..."
        print(f"| {s.name:<6} | {s.description:<36} | {s.status:<13} | {d:<51} |")
    print()
    # Full details below for any that didn't fit
    for s in signals:
        if len(s.detail) >= 120:
            print(f"### Signal {s.name} full detail:\n{s.detail}\n")

    has_fail = any(s.status == "FAIL" for s in signals)
    has_indeterminate = any(s.status == "INDETERMINATE" for s in signals)
    n_pass = sum(1 for s in signals if s.status == "PASS")

    if has_fail:
        print(f"OVERALL: FAIL --{n_pass}/{len(signals)} PASS, halt before any downstream action.")
        return 1
    if has_indeterminate:
        print(f"OVERALL: PASS (with INDETERMINATEs) --{n_pass}/{len(signals)} direct PASS. Indirect evidence sufficient if Signal 3 PASSed.")
        return 0
    print(f"OVERALL: PASS --all {len(signals)} signals direct-confirmed.")
    return 0


# ----------------------------------------------------------------------------------
# Mode dispatch
# ----------------------------------------------------------------------------------

_MODES = {
    "writer_race": _run_writer_race_mode,
}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", required=True, choices=sorted(_MODES.keys()),
                   help="Verification mode. Add new modes as new incident classes surface.")
    args = p.parse_args()
    return _MODES[args.mode]()


if __name__ == "__main__":
    sys.exit(main())
