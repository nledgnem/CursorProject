"""Shared paths for local and cloud (e.g. Render) deployments."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent


def macro_state_db_path() -> Path:
    """
    SQLite DB for the macro dashboard and live ingest.

    Set MACRO_STATE_DB_PATH=/data/macro_state.db on Render (persistent disk).
    Default: data/state/macro_state.db under the repo.
    """
    raw = os.environ.get("MACRO_STATE_DB_PATH", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (REPO_ROOT / "data" / "state" / "macro_state.db").resolve()


def heartbeat_last_success_path() -> Path:
    """
    Marker file written by system_heartbeat after a successful live pipeline run.

    Kept next to macro_state.db so on Render it lives on the persistent disk
    (e.g. /data/heartbeat_last_pipeline_success.txt) and is not reset on deploy.
    """
    return (macro_state_db_path().parent / "heartbeat_last_pipeline_success.txt").resolve()


def data_lake_root() -> Path:
    """
    Curated data lake root directory.

    Resolution order (first match wins):
      1. RENDER_DATA_LAKE_PATH env var (for Render production, or explicit override)
      2. DATA_LAKE_ROOT env var (generic override, e.g. tests)
      3. LOCAL_DATA_LAKE_PATH env var (for local analysis reads from a
         Drive-for-Desktop mounted folder, e.g. G:/My Drive/Render Exports on
         Windows, /Users/<you>/Library/CloudStorage/GoogleDrive-.../Render Exports
         on macOS). Read-only in intent; Render remains the canonical writer.
      4. /data/curated/data_lake (Render persistent disk, Linux only — guarded
         so Windows with a spurious top-level C:/data doesn't false-positive)
      5. <repo_root>/data/curated/data_lake (repo seed, last resort)
    """
    # 1 + 2: explicit overrides
    override = (os.environ.get("RENDER_DATA_LAKE_PATH", "") or os.environ.get("DATA_LAKE_ROOT", "")).strip()
    if override:
        p = Path(override).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p

    # 3: local Drive-mounted analysis path
    local_lake = os.environ.get("LOCAL_DATA_LAKE_PATH", "").strip()
    if local_lake:
        p = Path(local_lake).expanduser().resolve()
        # Do not mkdir: if the Drive mount is down, we want to fail loudly
        # rather than silently create an empty local directory.
        if not p.exists():
            raise FileNotFoundError(
                f"LOCAL_DATA_LAKE_PATH points to a non-existent directory: {p}. "
                f"Check that Google Drive for Desktop is running and the folder is synced."
            )
        return p

    # 4: Render persistent disk (Linux-only guard)
    if os.name == "posix":
        render_base = Path("/data")
        if render_base.exists() and render_base.is_dir():
            p = (render_base / "curated" / "data_lake").resolve()
            p.mkdir(parents=True, exist_ok=True)
            return p

    # 5: repo seed (last resort)
    p = (REPO_ROOT / "data" / "curated" / "data_lake").resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


# Convenience constant (evaluated at import time).
DATA_LAKE_ROOT = data_lake_root()
