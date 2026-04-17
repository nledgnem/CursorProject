#!/usr/bin/env python3
from __future__ import annotations

import logging
from pathlib import Path

from src.data_lake.perp_listings import run_daily_perp_ingestion

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    logger.info("Running daily perp listings ingestion. repo_root=%s", repo_root)
    run_daily_perp_ingestion(repo_root=repo_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

