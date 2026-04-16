from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from src.exports.gdrive_uploader import (
    DriveIdCache,
    RetryConfig,
    build_drive_service,
    resolve_target_folder_id,
    upload_or_update_file,
)
from src.notifications.telegram_client import send_telegram_text

logger = logging.getLogger(__name__)


LAST_EXPORT_MARKER = Path("/data/.last_export_utc_day")
DRIVE_ID_CACHE_PATH = Path("/data/exports/.drive_file_ids.json")
DRIVE_FOLDER_STATE_PATH = Path("/data/exports/.drive_target_folder_id.txt")


@dataclass(frozen=True)
class ExportConfig:
    enabled: bool
    base_dir: Path
    sources: dict[str, Path]
    msm_reports_root: Path
    service_account_json_path: Path
    target_folder_id: str
    filenames: dict[str, str]
    retry: RetryConfig


def _utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _read_yaml(path: Path) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid YAML (expected dict at top-level): {path}")
    return raw


def _parse_config(cfg_path: Path) -> ExportConfig:
    raw = _read_yaml(cfg_path)
    root = raw.get("export_gdrive", {})
    if not isinstance(root, dict):
        raise ValueError(f"Missing export_gdrive block in {cfg_path}")

    enabled = bool(root.get("enabled", True))
    base_dir = Path(str(root.get("base_dir", "/data")))

    sources_raw = root.get("sources", {})
    if not isinstance(sources_raw, dict):
        raise ValueError("export_gdrive.sources must be a mapping")
    sources: dict[str, Path] = {str(k): Path(str(v)) for k, v in sources_raw.items()}

    msm_reports_root = Path(str(root.get("msm_reports_root", "reports/msm_funding_v0")))

    gdrive = root.get("gdrive", {})
    if not isinstance(gdrive, dict):
        raise ValueError("export_gdrive.gdrive must be a mapping")
    service_account_json_path = Path(str(gdrive.get("service_account_json_path", "/data/secrets/gdrive_service_account.json")))
    target_folder_id = str(gdrive.get("target_folder_id", "")).strip()
    filenames_raw = gdrive.get("filenames", {})
    if not isinstance(filenames_raw, dict):
        raise ValueError("export_gdrive.gdrive.filenames must be a mapping")
    filenames: dict[str, str] = {str(k): str(v) for k, v in filenames_raw.items()}

    retry_raw = root.get("retry", {})
    if not isinstance(retry_raw, dict):
        raise ValueError("export_gdrive.retry must be a mapping")
    retry = RetryConfig(
        max_attempts=int(retry_raw.get("max_attempts", 5)),
        initial_backoff_seconds=float(retry_raw.get("initial_backoff_seconds", 5)),
        max_backoff_seconds=float(retry_raw.get("max_backoff_seconds", 120)),
        backoff_multiplier=float(retry_raw.get("backoff_multiplier", 2.0)),
        jitter=bool(retry_raw.get("jitter", True)),
    )

    return ExportConfig(
        enabled=enabled,
        base_dir=base_dir,
        sources=sources,
        msm_reports_root=msm_reports_root,
        service_account_json_path=service_account_json_path,
        target_folder_id=target_folder_id,
        filenames=filenames,
        retry=retry,
    )


def _assert_dir_writable(p: Path) -> None:
    if not p.exists():
        raise RuntimeError(f"Required directory missing: {p}")
    if not p.is_dir():
        raise RuntimeError(f"Expected directory but got file: {p}")
    test = p / ".write_test.tmp"
    try:
        test.write_text("ok\n", encoding="utf-8")
        test.unlink(missing_ok=True)
    except Exception as e:
        raise RuntimeError(f"Directory not writable: {p} ({e})") from e


def _free_space_bytes(path: Path) -> int:
    usage = shutil.disk_usage(str(path))
    return int(usage.free)


def _find_latest_msm_timeseries(reports_root: Path) -> Path:
    if not reports_root.exists():
        raise FileNotFoundError(f"MSM reports root not found: {reports_root}")
    candidates = list(reports_root.rglob("msm_timeseries.csv"))
    if not candidates:
        raise FileNotFoundError(f"No msm_timeseries.csv found under: {reports_root}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _atomic_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    shutil.copyfile(src, tmp)
    tmp.replace(dst)


def _maybe_skip_due_to_low_disk(*, base_dir: Path, min_free_bytes: int = 500 * 1024 * 1024) -> bool:
    free = _free_space_bytes(base_dir)
    if free >= min_free_bytes:
        return False
    msg = f"[EXPORT] Low disk free space under {base_dir}: free={free} bytes (< {min_free_bytes}). Skipping export."
    logger.warning(msg)
    send_telegram_text(msg)
    return True


def _should_export_today(marker_path: Path) -> bool:
    today = _utc_today_iso()
    try:
        last = marker_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        last = ""
    except Exception:
        logger.warning("Could not read export marker; treating as not exported.", exc_info=True)
        last = ""
    return last != today


def _mark_export_done(marker_path: Path) -> None:
    marker_path.write_text(_utc_today_iso() + "\n", encoding="utf-8")


def run(*, config_path: Path | None = None) -> None:
    """
    Nightly export entrypoint.

    Raises on unrecoverable errors. Caller (heartbeat) is responsible for catching.
    """
    cfg_path = config_path or (Path(__file__).resolve().parents[2] / "configs" / "gdrive_export.yaml")
    cfg = _parse_config(cfg_path)
    if not cfg.enabled:
        logger.info("Nightly export disabled in config.")
        return

    # Assert /data is mounted and writable.
    _assert_dir_writable(cfg.base_dir)

    if not _should_export_today(LAST_EXPORT_MARKER):
        logger.info("Nightly export already completed for UTC day=%s", _utc_today_iso())
        return

    if _maybe_skip_due_to_low_disk(base_dir=cfg.base_dir):
        return

    # Materialize stable MSM snapshot first (non-destructive: overwrite stable path only).
    repo_root = Path(__file__).resolve().parents[2]
    reports_root = cfg.msm_reports_root
    if not reports_root.is_absolute():
        reports_root = (repo_root / reports_root).resolve()
    latest_msm = _find_latest_msm_timeseries(reports_root)
    msm_stable = cfg.sources.get("msm_timeseries_csv")
    if msm_stable is None:
        raise ValueError("sources.msm_timeseries_csv missing from config")
    logger.info("Materializing MSM stable snapshot: %s -> %s", latest_msm, msm_stable)
    _atomic_copy(latest_msm, msm_stable)

    # Validate all sources exist (after MSM snapshot is materialized).
    missing = [k for k, p in cfg.sources.items() if not Path(p).exists()]
    if missing:
        for k in missing:
            logger.error("Missing export source: key=%s path=%s", k, cfg.sources[k])
        raise FileNotFoundError(f"Missing export sources: {missing}")

    # Auth + uploader
    service = build_drive_service(cfg.service_account_json_path)
    folder_id = resolve_target_folder_id(
        service,
        configured_folder_id=cfg.target_folder_id,
        state_path=DRIVE_FOLDER_STATE_PATH,
    )
    cache = DriveIdCache(DRIVE_ID_CACHE_PATH, folder_id=folder_id)

    # Upload all 5 files.
    for key, local_path in cfg.sources.items():
        drive_name = cfg.filenames.get(key)
        if not drive_name:
            raise ValueError(f"Missing gdrive.filenames entry for key={key}")
        upload_or_update_file(
            service=service,
            folder_id=folder_id,
            local_path=Path(local_path),
            drive_name=drive_name,
            cache=cache,
            retry=cfg.retry,
        )

    send_telegram_text(f"✅ Nightly Drive export complete — 5 files uploaded [{_utc_today_iso()} UTC]")
    _mark_export_done(LAST_EXPORT_MARKER)
    logger.info("Nightly export completed successfully for UTC day=%s", _utc_today_iso())

