from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import schedule

from repo_paths import heartbeat_last_success_path


REPO_ROOT = Path(__file__).resolve().parent
LOG_DIR = REPO_ROOT / "logs"
LOG_PATH = LOG_DIR / "system_heartbeat.log"
LAST_PIPELINE_SUCCESS_PATH = heartbeat_last_success_path()

# Marker for the once-per-day allowlist refresh-due Telegram alert. Lives on the
# Render persistent disk alongside the pipeline-success marker so it survives
# daemon restarts. Format: "<UTC date>|<last_refresh date>" — the second field
# tracks the data_dictionary.yaml::ingestion_universe.last_refresh value at the
# time the alert was sent, so when the operator advances last_refresh after a
# refresh, we treat the marker as stale and the next overdue period fires fresh.
ALLOWLIST_REMINDER_MARKER = LAST_PIPELINE_SUCCESS_PATH.parent / ".last_allowlist_refresh_reminder_utc"


UTC_RUN_TIMES = ("00:05",)  # UTC once per day (early UTC morning)
RUN_WINDOW_SECONDS = 55  # trigger if within this window after HH:MM

# If the PC was asleep and missed all UTC slots, catch up once per UTC day after wake.
# Throttle retries after failures so we do not hammer the live pipeline every 5 seconds.
CATCHUP_RETRY_AFTER_FAIL_SECONDS = 900.0  # 15 minutes


def _setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _load_last_pipeline_success_date() -> Optional[date]:
    try:
        raw = LAST_PIPELINE_SUCCESS_PATH.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        return date.fromisoformat(raw)
    except FileNotFoundError:
        return None
    except Exception:
        logging.exception("Could not read %s; treating as no prior success.", LAST_PIPELINE_SUCCESS_PATH)
        return None


def _save_last_pipeline_success_date(d: date) -> None:
    LAST_PIPELINE_SUCCESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    LAST_PIPELINE_SUCCESS_PATH.write_text(d.isoformat() + "\n", encoding="utf-8")


def _needs_catchup_pipeline(now_utc: datetime, last_success: Optional[date]) -> bool:
    today = now_utc.date()
    if last_success is None:
        return True
    return last_success < today


def _check_and_alert_allowlist_refresh() -> None:
    """Send a once-per-day Telegram if the CoinGecko allowlist is overdue for quarterly refresh.

    Reads next_refresh_due + last_refresh from
        data_dictionary.yaml::data_sources.coingecko.ingestion_universe

    Idempotent per UTC day via ALLOWLIST_REMINDER_MARKER. The marker also tracks
    the last_refresh value at alert-send time, so when the operator updates
    last_refresh after running the refresh runbook, the marker is treated as
    stale and the next overdue period fires fresh alerts.

    Non-fatal: any error is logged and swallowed; the reminder is best-effort,
    same pattern as the nightly_export Telegram alerts.
    """
    try:
        import yaml  # local import; not needed elsewhere in this module
    except Exception:
        logging.exception("PyYAML unavailable; skipping allowlist refresh-due check.")
        return

    try:
        with open(REPO_ROOT / "data_dictionary.yaml", "rb") as f:
            cfg = yaml.safe_load(f)
        iu = (
            cfg.get("data_sources", {})
            .get("coingecko", {})
            .get("ingestion_universe")
        )
        if iu is None:
            # Block doesn't exist (yet) — nothing to remind about. Silent.
            return
        next_due = iu.get("next_refresh_due")
        last_refresh = iu.get("last_refresh")
        if next_due is None or last_refresh is None:
            return

        # YAML may deserialize as datetime.date or as plain string; coerce to date.
        if not isinstance(next_due, date):
            try:
                next_due = datetime.strptime(str(next_due), "%Y-%m-%d").date()
            except Exception:
                logging.warning("Could not parse next_refresh_due=%r; skipping reminder.", next_due)
                return
        if not isinstance(last_refresh, date):
            try:
                last_refresh = datetime.strptime(str(last_refresh), "%Y-%m-%d").date()
            except Exception:
                logging.warning("Could not parse last_refresh=%r; skipping reminder.", last_refresh)
                return

        today = _utc_now().date()
        if today < next_due:
            return  # Not yet due

        # Idempotent dedupe via marker file.
        ALLOWLIST_REMINDER_MARKER.parent.mkdir(parents=True, exist_ok=True)
        try:
            marker_raw = ALLOWLIST_REMINDER_MARKER.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            marker_raw = ""
        except Exception:
            logging.exception("Could not read %s; treating as no prior reminder.", ALLOWLIST_REMINDER_MARKER)
            marker_raw = ""

        marker_date = ""
        marker_last_refresh = ""
        if marker_raw:
            parts = marker_raw.split("|", 1)
            marker_date = parts[0] if len(parts) >= 1 else ""
            marker_last_refresh = parts[1] if len(parts) >= 2 else ""

        # If the operator advanced last_refresh since our last alert, treat marker as stale.
        if marker_last_refresh and marker_last_refresh != last_refresh.isoformat():
            marker_date = ""

        if marker_date == today.isoformat():
            return  # already alerted today

        # Fire the alert.
        try:
            from src.notifications.telegram_client import send_telegram_text
        except Exception:
            logging.exception("Telegram client import failed; cannot send refresh-due alert.")
            return

        days_since_refresh = (today - last_refresh).days
        days_overdue = (today - next_due).days
        msg = (
            f"⏰ Allowlist refresh due. Last refreshed {last_refresh.isoformat()}; "
            f"{days_since_refresh} days have elapsed (overdue by {days_overdue} days). "
            f"Run `python scripts/archive/expand_allowlist.py --n 1000 --min-mcap 1000000 "
            f"--output data/perp_allowlist.csv` per `docs/runbooks/allowlist_refresh.md`. "
            f"Until refreshed, the ingestion universe drifts further from current top-1000 each day."
        )
        try:
            send_telegram_text(msg)
        except Exception:
            logging.exception("Telegram send failed; will retry tomorrow.")
            # Don't write marker — that way tomorrow's tick will retry.
            return

        try:
            ALLOWLIST_REMINDER_MARKER.write_text(
                f"{today.isoformat()}|{last_refresh.isoformat()}\n",
                encoding="utf-8",
            )
        except Exception:
            logging.exception("Could not write %s; alert may re-fire today.", ALLOWLIST_REMINDER_MARKER)

        logging.info(
            "Allowlist refresh-due Telegram sent (last_refresh=%s, next_due=%s, today=%s, overdue=%s days).",
            last_refresh, next_due, today, days_overdue,
        )
    except Exception:
        # Outer guard: refresh reminders are best-effort; never crash the heartbeat.
        logging.exception("Allowlist refresh-due check failed (non-fatal).")


def _ensure_pythonpath_repo_root() -> None:
    """So Streamlit and pipeline children can `import repo_paths` on PaaS (e.g. Render)."""
    root = str(REPO_ROOT)
    cur = os.environ.get("PYTHONPATH", "")
    parts = [p for p in cur.split(os.pathsep) if p]
    if root not in parts:
        os.environ["PYTHONPATH"] = root + (os.pathsep + cur if cur else "")


def _streamlit_cmd() -> list[str]:
    # Prefer module invocation so PATH does not need streamlit.exe.
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "dashboards/app_regime_monitor.py",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]
    # Render (and similar) set PORT; Streamlit must bind it and listen on all interfaces.
    port = os.environ.get("PORT", "").strip()
    if port:
        try:
            int(port)
            cmd.extend(["--server.port", port, "--server.address", "0.0.0.0"])
        except ValueError:
            logging.warning("Ignoring invalid PORT environment value: %r", port)
    return cmd


def _pipeline_cmd() -> list[str]:
    # Production pulse: refresh data + ingest latest features into SQLite for the dashboard.
    return [sys.executable, str(REPO_ROOT / "scripts" / "live" / "live_data_fetcher.py")]


def _popen_dashboard() -> subprocess.Popen:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("STREAMLIT_SERVER_HEADLESS", "true")
    env.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")
    return subprocess.Popen(
        _streamlit_cmd(),
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _terminate_process(proc: subprocess.Popen, name: str, timeout_s: float = 10.0) -> None:
    if proc.poll() is not None:
        return
    logging.info("Stopping %s (pid=%s)", name, proc.pid)
    try:
        proc.terminate()
        t0 = time.time()
        while proc.poll() is None and (time.time() - t0) < timeout_s:
            time.sleep(0.1)
        if proc.poll() is None:
            logging.warning("%s did not terminate; killing (pid=%s)", name, proc.pid)
            proc.kill()
    except Exception:
        logging.exception("Failed to stop %s cleanly (pid=%s)", name, proc.pid)


def _should_trigger_run(now_utc: datetime, hhmm_utc: str) -> bool:
    try:
        hh, mm = (int(x) for x in hhmm_utc.split(":"))
    except Exception:
        return False

    target = now_utc.replace(hour=hh, minute=mm, second=0, microsecond=0)
    delta = (now_utc - target).total_seconds()
    return 0 <= delta <= RUN_WINDOW_SECONDS


def main() -> None:
    _setup_logging()
    _ensure_pythonpath_repo_root()
    logging.info("System heartbeat starting. Repo root: %s", REPO_ROOT)
    logging.info("UTC schedule: %s", ", ".join(UTC_RUN_TIMES))

    stop_flag = {"stop": False}

    def _handle_stop(signum: int, _frame) -> None:
        logging.info("Received signal %s; stopping heartbeat loop.", signum)
        stop_flag["stop"] = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_stop)
        except Exception:
            pass

    dashboard_proc: Optional[subprocess.Popen] = None
    last_trigger_key: Optional[str] = None  # YYYY-MM-DDTHH:MM in UTC
    last_pipeline_attempt_wall_s: float = 0.0
    last_allowlist_check_date: Optional[str] = None  # YYYY-MM-DD; in-memory dedupe so we read YAML only once per UTC day per daemon-run

    try:
        dashboard_proc = _popen_dashboard()
        logging.info("Dashboard started (pid=%s).", dashboard_proc.pid)

        def _run_pipeline_with_persistence(reason: str) -> None:
            nonlocal last_pipeline_attempt_wall_s
            last_pipeline_attempt_wall_s = time.time()
            logging.info("Live pipeline (%s): %s", reason, " ".join(_pipeline_cmd()))
            try:
                subprocess.run(_pipeline_cmd(), cwd=str(REPO_ROOT), check=True)
            except subprocess.CalledProcessError as e:
                logging.error(
                    "Live pipeline failed (%s). Dashboard remains online using last known state.",
                    reason,
                )
                # Telegram alert: a CalledProcessError here means a fatal step in
                # run_live_pipeline.py exited non-zero (Step 1/2/3/3.5/4). Without
                # this, silent staleness goes unnoticed -- which is exactly what
                # happened during the 2026-04-26 .. 2026-04-28 CoinGecko credit-cap
                # incident (Step 2 silently 'succeeded' with zero new rows for 2
                # days before anyone noticed). We do NOT capture stdout/stderr
                # here -- the pipeline is long-running and Render's log stream
                # is the right place for the full traceback. The alert just
                # surfaces that something failed and points to the logs.
                try:
                    from src.notifications.telegram_client import send_telegram_text

                    today_utc = _utc_now().date().isoformat()
                    send_telegram_text(
                        f"\u26a0\ufe0f Live pipeline FAILED [{today_utc} UTC] ({reason})\n"
                        f"exit={e.returncode}\n"
                        f"Strategy will not advance until this is resolved. "
                        f"Check Render logs for the failing step's traceback. "
                        f"Common causes: CoinGecko credit cap (Step 2), "
                        f"CoinGlass outage (Step 1), silver build failure (Step 3.5)."
                    )
                except Exception:
                    # Telegram itself failed; we've already logged the pipeline error.
                    logging.exception("Failed to send Telegram alert about pipeline failure.")
                raise
            except Exception:
                logging.exception(
                    "Unexpected error during live pipeline (%s). Dashboard remains online using last known state.",
                    reason,
                )
                raise
            logging.info("Live pipeline completed successfully (%s).", reason)
            _save_last_pipeline_success_date(_utc_now().date())

            # Panel generation (non-fatal): once per UTC day on /data, before export.
            try:
                from src.exports.panel_generation import run as run_panel_generation

                run_panel_generation(repo_root=REPO_ROOT)
            except Exception:
                logging.exception("Panel generation failed (non-fatal).")

            # Nightly export hook (non-fatal): runs once per UTC day after successful pipeline.
            try:
                from src.exports.nightly_export import run as run_nightly_export

                run_nightly_export()
            except Exception as exc:
                # Drive export failures are non-fatal to the pipeline (we don't want a
                # broken Drive sync to crash the heartbeat / strategy), but they MUST
                # surface to operators. Without alerting, staleness can go unnoticed
                # for days (which happened 2026-04-23 -> 2026-04-27 with an OAuth
                # refresh-token expiry that no one saw until Mads spotted it).
                #
                # Send a Telegram alert with the exception class + message so the
                # operator can act immediately. Best-effort: send_telegram_text is
                # itself non-fatal, so a Telegram outage won't propagate.
                logging.exception("Nightly export failed (non-fatal).")
                try:
                    from src.notifications.telegram_client import send_telegram_text

                    err_class = type(exc).__name__
                    err_msg = str(exc)[:500]  # truncate to keep telegram message reasonable
                    today_utc = _utc_now().date().isoformat()
                    send_telegram_text(
                        f"\u26a0\ufe0f Nightly Drive export FAILED [{today_utc} UTC]\n"
                        f"{err_class}: {err_msg}\n\n"
                        f"Pipeline data is current on Render but Drive is stale. "
                        f"Check Render logs and `nightly_export.run()` output. "
                        f"Common causes: OAuth refresh token expired/revoked, "
                        f"Drive API quota, network."
                    )
                except Exception:
                    # Telegram itself failed; we've already logged the original export error.
                    logging.exception("Failed to send Telegram alert about Drive export failure.")

        def heartbeat_tick() -> None:
            nonlocal dashboard_proc, last_trigger_key, last_allowlist_check_date

            now = _utc_now()

            # Keep dashboard alive.
            if dashboard_proc is None or dashboard_proc.poll() is not None:
                if dashboard_proc is not None:
                    logging.warning("Dashboard process exited (code=%s). Restarting.", dashboard_proc.returncode)
                dashboard_proc = _popen_dashboard()
                logging.info("Dashboard restarted (pid=%s).", dashboard_proc.pid)

            # Allowlist refresh-due reminder: once per UTC day, regardless of pipeline state.
            # In-memory dedupe avoids re-reading data_dictionary.yaml every 5s tick; the
            # function itself uses ALLOWLIST_REMINDER_MARKER for cross-restart dedupe.
            today_iso = now.date().isoformat()
            if last_allowlist_check_date != today_iso:
                last_allowlist_check_date = today_iso
                _check_and_alert_allowlist_refresh()

            last_success = _load_last_pipeline_success_date()

            # Trigger pipeline on UTC schedule without double-firing.
            scheduled_fired = False
            for hhmm in UTC_RUN_TIMES:
                if not _should_trigger_run(now, hhmm):
                    continue
                key = f"{now.date().isoformat()}T{hhmm}"
                if last_trigger_key == key:
                    continue
                last_trigger_key = key
                scheduled_fired = True

                try:
                    _run_pipeline_with_persistence(reason=f"scheduled UTC {hhmm}")
                except (subprocess.CalledProcessError, Exception):
                    pass
                return

            # Catch-up: missed the daily slot while asleep — run once per UTC day.
            if not scheduled_fired and _needs_catchup_pipeline(now, last_success):
                elapsed = time.time() - last_pipeline_attempt_wall_s
                if last_pipeline_attempt_wall_s > 0 and elapsed < CATCHUP_RETRY_AFTER_FAIL_SECONDS:
                    return
                logging.info(
                    "Catch-up pipeline: last success date=%s, today UTC=%s",
                    last_success.isoformat() if last_success else "never",
                    now.date().isoformat(),
                )
                try:
                    _run_pipeline_with_persistence(reason="catch-up (missed UTC day)")
                except (subprocess.CalledProcessError, Exception):
                    pass

        schedule.every(5).seconds.do(heartbeat_tick)

        while not stop_flag["stop"]:
            schedule.run_pending()
            time.sleep(0.5)

    finally:
        if dashboard_proc is not None:
            _terminate_process(dashboard_proc, name="dashboard")
        logging.info("System heartbeat stopped.")


if __name__ == "__main__":
    main()

