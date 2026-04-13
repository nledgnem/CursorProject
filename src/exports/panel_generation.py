from __future__ import annotations

import logging
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.notifications.telegram_client import send_telegram_text

logger = logging.getLogger(__name__)


BASE_DIR = Path("/data")
OUTPUT_FINAL = BASE_DIR / "single_coin_panel.csv"
OUTPUT_TMP = BASE_DIR / "single_coin_panel.csv.tmp"
DEDUP_MARKER = BASE_DIR / ".last_single_coin_panel_utc_day"

# Keep aligned with export job’s safety threshold.
MIN_FREE_BYTES = 500 * 1024 * 1024  # 500MB


@dataclass(frozen=True)
class PanelGenerationResult:
    ran: bool
    reason: str


def _utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _free_space_bytes(path: Path) -> int:
    usage = shutil.disk_usage(str(path))
    return int(usage.free)


def _should_run_today(marker_path: Path) -> bool:
    today = _utc_today_iso()
    try:
        last = marker_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        last = ""
    except Exception:
        logger.warning("Could not read panel dedup marker; treating as not run.", exc_info=True)
        last = ""
    return last != today


def _mark_done(marker_path: Path) -> None:
    marker_path.write_text(_utc_today_iso() + "\n", encoding="utf-8")


def _atomic_rename(tmp_path: Path, final_path: Path) -> None:
    # Path.replace is atomic on POSIX when on same filesystem.
    tmp_path.replace(final_path)


def _panel_cmd(repo_root: Path) -> list[str]:
    script = repo_root / "scripts" / "generate_single_coin_panel.py"
    return [
        sys.executable,
        str(script),
        "--output",
        str(OUTPUT_TMP),
    ]


def run(*, repo_root: Optional[Path] = None) -> PanelGenerationResult:
    """
    Generate /data/single_coin_panel.csv once per UTC day.

    - Uses a dedup marker on /data so heartbeat restarts don't rerun mid-day.
    - Writes to .tmp first and atomically renames on success.
    - Never raises: callers can still choose to catch, but we contain failures here.
    """
    root = repo_root or Path(__file__).resolve().parents[2]

    if not BASE_DIR.exists() or not BASE_DIR.is_dir():
        msg = f"[PANEL] /data not mounted or not a directory: {BASE_DIR}"
        logger.error(msg)
        send_telegram_text(msg)
        return PanelGenerationResult(ran=False, reason="data_dir_missing")

    if not _should_run_today(DEDUP_MARKER):
        logger.info("[PANEL] already ran for UTC day=%s; skipping.", _utc_today_iso())
        return PanelGenerationResult(ran=False, reason="already_ran_today")

    free = _free_space_bytes(BASE_DIR)
    if free < MIN_FREE_BYTES:
        msg = f"[PANEL] Low disk free space under {BASE_DIR}: free={free} bytes (< {MIN_FREE_BYTES}). Skipping panel generation."
        logger.warning(msg)
        send_telegram_text(msg)
        return PanelGenerationResult(ran=False, reason="low_disk")

    # Ensure no stale tmp is accidentally promoted later.
    try:
        OUTPUT_TMP.unlink(missing_ok=True)
    except Exception:
        logger.warning("[PANEL] could not remove stale tmp: %s", OUTPUT_TMP, exc_info=True)

    cmd = _panel_cmd(root)
    logger.info("[PANEL] starting panel generation: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, cwd=str(root), check=True)
        if not OUTPUT_TMP.exists():
            raise FileNotFoundError(f"Panel generation completed but tmp output missing: {OUTPUT_TMP}")

        # Promote atomically.
        _atomic_rename(OUTPUT_TMP, OUTPUT_FINAL)
        _mark_done(DEDUP_MARKER)
        logger.info("[PANEL] success: wrote %s (UTC day=%s)", OUTPUT_FINAL, _utc_today_iso())
        return PanelGenerationResult(ran=True, reason="success")
    except Exception as e:
        logger.exception("[PANEL] generation failed.")
        send_telegram_text(f"[PANEL] generation failed (non-fatal): {type(e).__name__}: {e}")
        return PanelGenerationResult(ran=False, reason="failed")

