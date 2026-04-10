from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import yaml

from repo_paths import REPO_ROOT


@dataclass(frozen=True)
class ApathyAlertsConfig:
    hold_days: int
    warning_threshold_pct: float
    critical_threshold_pct: float
    stop_threshold_pct: float
    exit_reminder_days: list[int]
    scan_reminder_milestone_days: list[int]
    formation_window_days: int
    price_check_interval_minutes: int
    daily_bundle_utc_hour: int
    portfolio_snapshot_utc_hour: int
    variational_base_url: str
    variational_stats_path: str
    variational_timeout_seconds: float
    book_csv: Path
    alert_log_csv: Path
    stop_proximity_state_json: Path
    exit_reminder_state_json: Path
    scanner_reminder_state_json: Path
    daily_snapshot_state_json: Path
    daily_bundle_state_json: Path


def load_apathy_alerts_config(repo_root: Path | None = None) -> ApathyAlertsConfig:
    root = repo_root or REPO_ROOT
    path = root / "configs" / "apathy_alerts.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid apathy_alerts.yaml: expected mapping, got {type(raw)}")

    var = raw.get("variational") or {}
    paths = raw.get("paths") or {}

    def _p(key: str, default: str) -> Path:
        rel = paths.get(key, default)
        p = Path(rel)
        return (root / p).resolve() if not p.is_absolute() else p

    return ApathyAlertsConfig(
        hold_days=int(raw.get("hold_days", 150)),
        warning_threshold_pct=float(raw["warning_threshold_pct"]),
        critical_threshold_pct=float(raw["critical_threshold_pct"]),
        stop_threshold_pct=float(raw["stop_threshold_pct"]),
        exit_reminder_days=list(raw.get("exit_reminder_days") or [7, 3, 1]),
        scan_reminder_milestone_days=list(raw.get("scan_reminder_milestone_days") or [40, 43, 45]),
        formation_window_days=int(raw.get("formation_window_days", 45)),
        price_check_interval_minutes=int(raw.get("price_check_interval_minutes", 60)),
        daily_bundle_utc_hour=int(
            raw.get("daily_bundle_utc_hour", raw.get("daily_snapshot_utc_hour", 8))
        ),
        portfolio_snapshot_utc_hour=int(
            raw.get("portfolio_snapshot_utc_hour", raw.get("daily_bundle_utc_hour", 8))
        ),
        variational_base_url=str(var.get("base_url", "")).rstrip("/"),
        variational_stats_path=str(var.get("stats_path", "/metadata/stats")),
        variational_timeout_seconds=float(var.get("timeout_seconds", 20)),
        book_csv=_p("book_csv", "data/curated/data_lake/apathy_bleed_book.csv"),
        alert_log_csv=_p("alert_log_csv", "data/curated/data_lake/apathy_alert_log.csv"),
        stop_proximity_state_json=_p(
            "stop_proximity_state_json", "data/curated/data_lake/apathy_stop_proximity_state.json"
        ),
        exit_reminder_state_json=_p(
            "exit_reminder_state_json", "data/curated/data_lake/apathy_exit_reminder_state.json"
        ),
        scanner_reminder_state_json=_p(
            "scanner_reminder_state_json", "data/curated/data_lake/apathy_scanner_reminder_state.json"
        ),
        daily_snapshot_state_json=_p(
            "daily_snapshot_state_json", "data/curated/data_lake/apathy_daily_snapshot_state.json"
        ),
        daily_bundle_state_json=_p(
            "daily_bundle_state_json", "data/curated/data_lake/apathy_daily_bundle_state.json"
        ),
    )
