#!/usr/bin/env python3
"""
=============================================================================
PRODUCTION LIVE PIPELINE — SINGLE ENTRY POINT FOR CRON / SCHEDULER
=============================================================================

This is the only script that should be triggered by the production cron job.

It runs the full live pipeline DAG in order (halt-on-failure):
  Step 0.5: Ingest Perp Listings (Hyperliquid + Variational)
  Step 1: Ingest Raw Funding (fact_funding.parquet) via CoinGlass API
  Step 2: Ingest Raw Prices/Marketcap (fact_price.parquet / fact_marketcap.parquet)
  Step 3: Build Macro Indices (btcdom_reconstructed.csv)
  Step 4: Execute Strategy Engine (MSM v0)

Do not schedule msm_run.py or any ingestion scripts directly. Schedule this
orchestrator so that all upstream data is refreshed before the strategy runs.

Usage (from project root):
  python run_live_pipeline.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]

If --start-date / --end-date are omitted, defaults are used for the strategy
run (see below).
=============================================================================
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import date, timedelta, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent
SCRIPT_MARKETS_SNAPSHOT = PROJECT_ROOT / "scripts" / "fetch_high_priority_data.py"
SCRIPT_PERP_LISTINGS = PROJECT_ROOT / "scripts" / "run_perp_listings_ingestion.py"
SCRIPT_FUNDING = PROJECT_ROOT / "scripts" / "fetch_coinglass_data.py"
SCRIPT_PRICES_MCAP = PROJECT_ROOT / "scripts" / "incremental_update.py"
SCRIPT_MACRO = PROJECT_ROOT / "scripts" / "data_ingestion" / "btcdom_backfill.py"
SCRIPT_BUILD_SILVER = PROJECT_ROOT / "scripts" / "data_ingestion" / "build_silver_layer.py"
MSM_RUN = PROJECT_ROOT / "majors_alts_monitor" / "msm_funding_v0" / "msm_run.py"


def run_step(cwd: Path, cmd: list[str], step_name: str) -> bool:
    logger.info("=========================================")
    logger.info("%s", step_name)
    logger.info("=========================================")
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            check=False,
        )
        if result.returncode != 0:
            logger.error("Step failed with return code %s: %s", result.returncode, step_name)
            return False
        return True
    except Exception as e:
        logger.exception("Step failed: %s — %s", step_name, e)
        return False


def main() -> int:
    # Load environment variables from .env at project root (if present)
    load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=False)

    parser = argparse.ArgumentParser(
        description="Run the production live pipeline: data update then strategy.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Strategy start date YYYY-MM-DD (default: ~2 years before end-date)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Strategy end date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip Steps 1-3 (data updates); only run strategy.",
    )
    args = parser.parse_args()

    # Default to *UTC* "today" to avoid local timezone leakage into strategy date alignment.
    end_date = datetime.now(timezone.utc).date() if args.end_date is None else date.fromisoformat(args.end_date)
    if args.start_date is None:
        start_date = end_date - timedelta(days=730)
    else:
        start_date = date.fromisoformat(args.start_date)

    # ------------------------------------------------------------------
    # Steps 1-3: Updating Data Lake (halt on failure)
    # ------------------------------------------------------------------
    if not args.skip_ingestion:
        # ZERO-TRUST PATCH: Enforce dynamic UTC boundary to override any static configs
        today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # ------------------------------------------------------------------
        # Step 0 — Market Snapshot (circulating/total supply for exclusion gates)
        # ------------------------------------------------------------------
        # Non-fatal: if this fails, continue with the rest of the pipeline.
        # The 401 errors on exchange volume history (Analyst tier) are expected
        # and do not affect the markets snapshot which is the critical output.
        if not SCRIPT_MARKETS_SNAPSHOT.exists():
            logger.warning("Market snapshot script not found: %s — continuing pipeline.", SCRIPT_MARKETS_SNAPSHOT)
        else:
            ok = run_step(
                PROJECT_ROOT,
                [sys.executable, str(SCRIPT_MARKETS_SNAPSHOT)],
                "Step 0 — Market Snapshot (supply data)",
            )
            if not ok:
                logger.warning("Step 0 (market snapshot) failed — continuing pipeline.")

        # ------------------------------------------------------------------
        # Step 0.5 — Perp Listings Snapshot (Hyperliquid + Variational)
        # ------------------------------------------------------------------
        # Non-fatal: if upstream APIs are down, continue with the rest of the pipeline.
        if not SCRIPT_PERP_LISTINGS.exists():
            logger.warning("Perp listings script not found: %s — continuing pipeline.", SCRIPT_PERP_LISTINGS)
        else:
            ok = run_step(
                PROJECT_ROOT,
                [sys.executable, str(SCRIPT_PERP_LISTINGS)],
                "Step 0.5 — Perp Listings Snapshot (Hyperliquid + Variational)",
            )
            if not ok:
                logger.warning("Step 0.5 (perp listings) failed — continuing pipeline.")

        # Step 1: Funding (CoinGlass -> fact_funding.parquet)
        if not SCRIPT_FUNDING.exists():
            logger.error("Funding ingestion script not found: %s", SCRIPT_FUNDING)
            return 1
        ok = run_step(
            cwd=PROJECT_ROOT,
            # Use incremental + merge so we only backfill missing dates
            # and keep extending the existing fact_funding / fact_open_interest /
            # fact_liquidations. Pass the three fetch flags explicitly so the
            # pipeline's scope is visible in the invocation (rather than relying
            # on the "no flag -> fetch all" fallback inside the script).
            cmd=[
                sys.executable,
                str(SCRIPT_FUNDING),
                "--fetch-funding",
                "--fetch-oi",
                "--fetch-liquidations",
                "--incremental",
                "--merge-existing",
                "--end-date",
                today_utc,
            ],
            step_name="Step 1: Ingest Raw Funding + OI + Liquidations (scripts/fetch_coinglass_data.py)",
        )
        if not ok:
            logger.error("Halting: funding ingestion failed. Strategy will not run on stale funding.")
            return 1

        # Step 2: Prices/marketcap (incremental update -> fact tables)
        if not SCRIPT_PRICES_MCAP.exists():
            logger.error("Prices/marketcap ingestion script not found: %s", SCRIPT_PRICES_MCAP)
            return 1
        ok = run_step(
            cwd=PROJECT_ROOT,
            cmd=[sys.executable, str(SCRIPT_PRICES_MCAP)],
            step_name="Step 2: Ingest Raw Prices/Marketcap (scripts/incremental_update.py)",
        )
        if not ok:
            logger.error("Halting: price/marketcap ingestion failed. Strategy will not run on stale prices.")
            return 1

        # Step 3: Macro indices (BTCDOM reconstruction)
        if not SCRIPT_MACRO.exists():
            logger.error("Macro index builder not found: %s", SCRIPT_MACRO)
            return 1
        ok = run_step(
            cwd=PROJECT_ROOT,
            cmd=[sys.executable, str(SCRIPT_MACRO)],
            step_name="Step 3: Build Macro Indices (scripts/data_ingestion/btcdom_backfill.py)",
        )
        if not ok:
            logger.error("Halting: macro index build failed. Strategy will not run on stale macro.")
            return 1

        # Step 3.5: Build Silver Layer (prices, funding, marketcap) from updated Bronze
        if not SCRIPT_BUILD_SILVER.exists():
            logger.error("Silver layer builder not found: %s", SCRIPT_BUILD_SILVER)
            return 1

        data_lake_override = os.environ.get("RENDER_DATA_LAKE_PATH", "").strip()
        data_lake_path = Path(data_lake_override).expanduser() if data_lake_override else (PROJECT_ROOT / "data" / "curated" / "data_lake")
        if not data_lake_path.is_absolute():
            data_lake_path = (PROJECT_ROOT / data_lake_path).resolve()
        ok = run_step(
            cwd=PROJECT_ROOT,
            cmd=[
                sys.executable,
                str(SCRIPT_BUILD_SILVER),
                "--data-lake",
                str(data_lake_path),
            ],
            step_name="Step 3.5: Build Silver Layer (scripts/data_ingestion/build_silver_layer.py)",
        )
        if not ok:
            logger.error("Halting: Silver layer build failed. Strategy will not run on stale Silver.")
            return 1

        logger.info("Data update steps complete (funding, prices/marketcap, macro indices).")
    else:
        logger.info("Data update steps skipped (--skip-ingestion).")

    # ------------------------------------------------------------------
    # Step 4: Executing Strategy Engine
    # ------------------------------------------------------------------
    if not MSM_RUN.exists():
        logger.error("Strategy script not found: %s", MSM_RUN)
        return 1
    ok = run_step(
        cwd=PROJECT_ROOT,
        cmd=[
            sys.executable,
            "-m",
            "majors_alts_monitor.msm_funding_v0.msm_run",
            "--start-date", start_date.isoformat(),
            "--end-date", end_date.isoformat(),
        ],
        step_name="Step 4: Execute Strategy Engine (majors_alts_monitor/msm_funding_v0/msm_run.py)",
    )
    if not ok:
        return 1
    logger.info("Step 4 complete: strategy run finished.")

    logger.info("Live pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
