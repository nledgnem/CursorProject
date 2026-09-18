"""Nightly lake-integrity checks (non-blocking): content freshness + asset identity.

Runs once per UTC day from system_heartbeat after the Drive export, so what it reports matches
what just landed on Drive. Each check is scripts/verify_ingestion_integrity.py in a subprocess with
a timeout; results come back as JSON. On any FAIL it sends one Telegram message listing the
failing signals, marking those that were not failing on the previous run as NEW. It never raises
and never blocks ingestion.

[DECISION 2026-09-18] Non-blocking until the asset-identity repair lands and the lake passes
cleanly; then invalid/duplicate mappings and mass coin-switch events become blocking
(reports/incidents/2026-09-18_asset_identity/README.md).
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from src.notifications.telegram_client import send_telegram_text

logger = logging.getLogger(__name__)

BASE_DIR = Path("/data")
DEDUP_MARKER = BASE_DIR / ".last_lake_integrity_utc_day"
STATE_PATH = BASE_DIR / ".lake_integrity_last_failures.json"
TIMEOUT_SECONDS = 900
IDENTITY_LOOKBACK_DAYS = 60   # A3/A4 nightly: catch new splices; full history is a manual run


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _checks(repo_root: Path, workdir: Path) -> list[tuple[str, list[str], Path]]:
    script = str(repo_root / "scripts" / "verify_ingestion_integrity.py")
    since = (_utc_today() - timedelta(days=IDENTITY_LOOKBACK_DAYS)).isoformat()
    return [
        ("freshness", [sys.executable, script, "--mode", "freshness"], workdir / "freshness.json"),
        ("asset_identity", [sys.executable, script, "--mode", "asset_identity", "--since", since],
         workdir / "asset_identity.json"),
    ]


def run_checks(repo_root: Path, workdir: Path) -> tuple[list[dict], list[str]]:
    """Run every check; return (FAIL signals, problems running a check). Never raises."""
    fails, problems = [], []
    for mode, cmd, out in _checks(repo_root, workdir):
        out.unlink(missing_ok=True)
        try:
            proc = subprocess.run(cmd + ["--json-out", str(out)], cwd=str(repo_root), capture_output=True,
                                  text=True, timeout=TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            problems.append(f"{mode}: timed out after {TIMEOUT_SECONDS}s")
            continue
        if not out.exists():
            tail = "\n".join((proc.stderr or proc.stdout or "").strip().splitlines()[-5:])
            problems.append(f"{mode}: no result (exit {proc.returncode})\n{tail}")
            continue
        for s in json.loads(out.read_text(encoding="utf-8"))["signals"]:
            if s["status"] == "FAIL":
                fails.append({"mode": mode, **s})
    return fails, problems


def format_message(fails: list[dict], problems: list[str], previous: set[str]) -> str:
    lines = [f"[LAKE INTEGRITY] {_utc_today().isoformat()} UTC -- {len(fails)} failing check(s) (non-blocking)"]
    for f in fails:
        key = f"{f['mode']}:{f['name']}"
        lines.append(f"{'NEW ' if key not in previous else ''}{f['mode']} {f['name']} {f['description']}: "
                     f"{f['detail'][:300]}")
    for p in problems:
        lines.append(f"CHECK ERROR {p[:300]}")
    lines.append("Runbook: reports/incidents/2026-09-18_asset_identity/README.md")
    return "\n".join(lines)


def run(*, repo_root: Optional[Path] = None) -> bool:
    """Once per UTC day. Returns True if a check ran. Never raises."""
    try:
        root = repo_root or Path(__file__).resolve().parents[2]
        today = _utc_today().isoformat()
        try:
            if DEDUP_MARKER.read_text(encoding="utf-8").strip() == today:
                return False
        except FileNotFoundError:
            pass
        fails, problems = run_checks(root, BASE_DIR)
        try:
            previous = set(json.loads(STATE_PATH.read_text(encoding="utf-8")))
        except Exception:
            previous = set()
        if fails or problems:
            send_telegram_text(format_message(fails, problems, previous))
        STATE_PATH.write_text(json.dumps(sorted(f"{f['mode']}:{f['name']}" for f in fails)), encoding="utf-8")
        # Marked even on failure: catch-up ticks must not resend the same alert today.
        DEDUP_MARKER.write_text(today + "\n", encoding="utf-8")
        logger.info("[LAKE INTEGRITY] %d failing signal(s), %d check problem(s)", len(fails), len(problems))
        return True
    except Exception:
        logger.exception("[LAKE INTEGRITY] run failed (non-fatal).")
        return False
