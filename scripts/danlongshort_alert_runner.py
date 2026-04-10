from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.danlongshort.alert_ops import run_periodic_snapshot
from src.danlongshort.config_loader import load_danlongshort_alerts_config

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ | %(levelname)s | %(message)s",
    )
    cfg = load_danlongshort_alerts_config(REPO_ROOT)
    logger.info(
        "danlongshort alert runner started. positions=%s interval_hours=%s",
        cfg.positions_csv,
        cfg.snapshot_interval_hours,
    )

    while True:
        try:
            run_periodic_snapshot(cfg)
        except Exception as e:
            logger.warning("danlongshort runner tick failed (non-fatal): %s", e, exc_info=True)
        time.sleep(30.0)


if __name__ == "__main__":
    main()

