from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.apathy_bleed.alert_ops import (
    run_exit_reminders,
    run_portfolio_snapshot,
    run_scanner_reminders,
    run_stop_proximity,
)
from src.apathy_bleed.book import read_book_rows
from src.apathy_bleed.config_loader import load_apathy_alerts_config
from src.apathy_bleed.variational_prices import fetch_variational_mark_prices

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _load_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return dict(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(default)


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ | %(levelname)s | %(message)s",
    )
    cfg = load_apathy_alerts_config(REPO_ROOT)
    book_path = cfg.book_csv

    last_stop_mono = 0.0
    interval_s = max(60, cfg.price_check_interval_minutes * 60)

    logger.info(
        "Apathy alert runner started. Book=%s stop_interval=%s min portfolio_snapshot_utc_hour=%s daily_bundle_utc_hour=%s",
        book_path,
        cfg.price_check_interval_minutes,
        cfg.portfolio_snapshot_utc_hour,
        cfg.daily_bundle_utc_hour,
    )

    while True:
        now = _utc_now()
        try:
            rows = read_book_rows(book_path)
        except Exception as e:
            logger.warning("Could not read book (non-fatal): %s", e)
            rows = []

        try:
            prices = fetch_variational_mark_prices(
                cfg.variational_base_url,
                cfg.variational_stats_path,
                timeout_seconds=cfg.variational_timeout_seconds,
            )
        except Exception as e:
            logger.warning("Mark price fetch failed (non-fatal): %s", e)
            prices = {}

        # Stop proximity on interval
        elapsed = time.monotonic() - last_stop_mono
        if last_stop_mono == 0.0 or elapsed >= interval_s:
            try:
                if rows:
                    run_stop_proximity(cfg, rows, prices)
                else:
                    logger.info("Stop proximity skip: book empty.")
            except Exception as e:
                logger.warning("Stop proximity failed (non-fatal): %s", e, exc_info=True)
            last_stop_mono = time.monotonic()

        # Variational portfolio snapshot (Telegram): once per UTC day in configured hour (first five minutes).
        if now.hour == cfg.portfolio_snapshot_utc_hour and 0 <= now.minute < 5:
            try:
                run_portfolio_snapshot(cfg, rows, prices)
            except Exception as e:
                logger.warning("Portfolio snapshot failed (non-fatal): %s", e, exc_info=True)

        # Daily bundle: exit reminders + scanner at configured UTC hour.
        if now.hour == cfg.daily_bundle_utc_hour and 0 <= now.minute < 5:
            bundle_path = cfg.daily_bundle_state_json
            bundle = _load_json(bundle_path, {"last_bundle_utc_day": ""})
            today_s = now.date().isoformat()
            if bundle.get("last_bundle_utc_day") != today_s:
                try:
                    run_exit_reminders(cfg, rows, prices)
                except Exception as e:
                    logger.warning("Exit reminders failed (non-fatal): %s", e, exc_info=True)
                try:
                    run_scanner_reminders(cfg, rows)
                except Exception as e:
                    logger.warning("Scanner reminders failed (non-fatal): %s", e, exc_info=True)
                bundle["last_bundle_utc_day"] = today_s
                _save_json(bundle_path, bundle)

        time.sleep(30.0)


if __name__ == "__main__":
    main()
