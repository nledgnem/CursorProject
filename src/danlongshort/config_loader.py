from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from repo_paths import REPO_ROOT


@dataclass(frozen=True)
class DanLongShortAlertsConfig:
    snapshot_interval_hours: float
    positions_csv: Path
    price_cache_parquet: Path
    alert_log_csv: Path
    snapshot_state_json: Path
    allowlist_csv: Path
    symbol_map_yaml: Path


def load_danlongshort_alerts_config(repo_root: Path | None = None) -> DanLongShortAlertsConfig:
    root = repo_root or REPO_ROOT
    path = root / "configs" / "danlongshort_alerts.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid danlongshort_alerts.yaml: expected mapping, got {type(raw)}")

    paths = raw.get("paths") or {}

    def _p(key: str, default: str) -> Path:
        rel = paths.get(key, default)
        p = Path(str(rel))
        return (root / p).resolve() if not p.is_absolute() else p

    return DanLongShortAlertsConfig(
        snapshot_interval_hours=float(raw.get("snapshot_interval_hours", 12.0)),
        positions_csv=_p("positions_csv", "/data/curated/data_lake/danlongshort_positions.csv"),
        price_cache_parquet=_p("price_cache_parquet", "/data/curated/data_lake/danlongshort_price_cache.parquet"),
        alert_log_csv=_p("alert_log_csv", "/data/curated/data_lake/danlongshort_alert_log.csv"),
        snapshot_state_json=_p("snapshot_state_json", "/data/curated/data_lake/danlongshort_snapshot_state.json"),
        allowlist_csv=_p("allowlist_csv", "data/perp_allowlist.csv"),
        symbol_map_yaml=_p("symbol_map_yaml", "configs/danlongshort_symbol_map.yaml"),
    )

