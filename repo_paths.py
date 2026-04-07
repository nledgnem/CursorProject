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
