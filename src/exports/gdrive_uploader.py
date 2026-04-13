from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger(__name__)


DRIVE_SCOPES = ("https://www.googleapis.com/auth/drive",)


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int
    initial_backoff_seconds: float
    max_backoff_seconds: float
    backoff_multiplier: float
    jitter: bool = True


def _is_retriable_http_error(err: Exception) -> bool:
    if not isinstance(err, HttpError):
        return False
    try:
        status = int(getattr(err.resp, "status", 0) or 0)
    except Exception:
        status = 0
    return status in (408, 429, 500, 502, 503, 504)


def _sleep_backoff(attempt_idx: int, retry: RetryConfig) -> None:
    # attempt_idx is 1-based (1..max_attempts)
    base = float(retry.initial_backoff_seconds) * (float(retry.backoff_multiplier) ** max(0, attempt_idx - 1))
    delay = min(float(retry.max_backoff_seconds), base)
    if retry.jitter:
        delay = delay * (0.5 + random.random())  # [0.5x, 1.5x)
    time.sleep(max(0.0, delay))


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def build_drive_service(service_account_json_path: Path):
    if not service_account_json_path.exists():
        raise FileNotFoundError(f"Google service account JSON not found: {service_account_json_path}")
    creds = service_account.Credentials.from_service_account_file(
        str(service_account_json_path),
        scopes=list(DRIVE_SCOPES),
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_files_by_name_in_folder(service, *, folder_id: str) -> dict[str, str]:
    """
    Return mapping {name -> file_id} for files currently visible to this principal.
    If duplicates exist, keep the most recently modified one.
    """
    name_to_best: dict[str, tuple[str, str]] = {}  # name -> (file_id, modifiedTime)
    page_token: Optional[str] = None

    while True:
        resp = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces="drive",
                fields="nextPageToken, files(id, name, modifiedTime)",
                pageToken=page_token,
                pageSize=1000,
            )
            .execute()
        )
        for f in resp.get("files", []) or []:
            name = str(f.get("name", "") or "")
            fid = str(f.get("id", "") or "")
            mtime = str(f.get("modifiedTime", "") or "")
            if not name or not fid:
                continue
            prev = name_to_best.get(name)
            if prev is None or mtime > prev[1]:
                name_to_best[name] = (fid, mtime)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return {k: v[0] for k, v in name_to_best.items()}


class DriveIdCache:
    def __init__(self, path: Path, *, folder_id: str):
        self.path = path
        self.folder_id = folder_id

    def load(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        try:
            raw = _load_json(self.path)
            if raw.get("target_folder_id") != self.folder_id:
                return {}
            m = raw.get("name_to_id", {})
            if not isinstance(m, dict):
                return {}
            out: dict[str, str] = {}
            for k, v in m.items():
                if isinstance(k, str) and isinstance(v, str) and k and v:
                    out[k] = v
            return out
        except Exception:
            logger.warning("Could not read drive file id cache; rebuilding. path=%s", self.path, exc_info=True)
            return {}

    def save(self, mapping: dict[str, str]) -> None:
        _atomic_write_json(
            self.path,
            {
                "target_folder_id": self.folder_id,
                "name_to_id": dict(mapping),
            },
        )


def ensure_id_cache_populated(
    service,
    *,
    folder_id: str,
    cache: DriveIdCache,
) -> dict[str, str]:
    cached = cache.load()
    if cached:
        return cached
    logger.info("Drive ID cache missing/empty; listing folder contents once.")
    mapping = _list_files_by_name_in_folder(service, folder_id=folder_id)
    cache.save(mapping)
    return mapping


def upload_or_update_file(
    *,
    service,
    folder_id: str,
    local_path: Path,
    drive_name: str,
    cache: DriveIdCache,
    retry: RetryConfig,
) -> str:
    if not local_path.exists():
        raise FileNotFoundError(f"Export source missing: {local_path}")

    name_to_id = ensure_id_cache_populated(service, folder_id=folder_id, cache=cache)
    file_id = name_to_id.get(drive_name)

    media = MediaFileUpload(str(local_path), resumable=True)

    def _do() -> str:
        nonlocal file_id
        if file_id:
            logger.info("Updating Drive file: name=%s id=%s local=%s", drive_name, file_id, local_path)
            resp = service.files().update(fileId=file_id, media_body=media, fields="id").execute()
            return str(resp.get("id"))

        logger.info("Creating Drive file: name=%s local=%s", drive_name, local_path)
        body = {"name": drive_name, "parents": [folder_id]}
        resp = service.files().create(body=body, media_body=media, fields="id").execute()
        new_id = str(resp.get("id"))
        if new_id:
            file_id = new_id
        return new_id

    last_err: Optional[Exception] = None
    for attempt in range(1, max(1, retry.max_attempts) + 1):
        try:
            out_id = _do()
            if not out_id:
                raise RuntimeError(f"Drive upload returned empty id for {drive_name}")
            # Update cache with the final id.
            name_to_id[drive_name] = out_id
            cache.save(name_to_id)
            logger.info("Drive upload ok: name=%s id=%s", drive_name, out_id)
            return out_id
        except Exception as e:
            last_err = e
            retriable = _is_retriable_http_error(e)
            if attempt >= retry.max_attempts or not retriable:
                logger.exception(
                    "Drive upload failed (final). name=%s attempt=%s/%s retriable=%s",
                    drive_name,
                    attempt,
                    retry.max_attempts,
                    retriable,
                )
                raise
            logger.warning(
                "Drive upload failed; retrying. name=%s attempt=%s/%s err=%s",
                drive_name,
                attempt,
                retry.max_attempts,
                str(e),
            )
            _sleep_backoff(attempt, retry)

    raise RuntimeError(f"Drive upload failed unexpectedly: {drive_name} ({last_err})")

