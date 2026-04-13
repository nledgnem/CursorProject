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

    try:
        dashboard_proc = _popen_dashboard()
        logging.info("Dashboard started (pid=%s).", dashboard_proc.pid)

        def _run_pipeline_with_persistence(reason: str) -> None:
            nonlocal last_pipeline_attempt_wall_s
            last_pipeline_attempt_wall_s = time.time()
            logging.info("Live pipeline (%s): %s", reason, " ".join(_pipeline_cmd()))
            try:
                subprocess.run(_pipeline_cmd(), cwd=str(REPO_ROOT), check=True)
            except subprocess.CalledProcessError:
                logging.error(
                    "Live pipeline failed (%s). Dashboard remains online using last known state.",
                    reason,
                )
                raise
            except Exception:
                logging.exception(
                    "Unexpected error during live pipeline (%s). Dashboard remains online using last known state.",
                    reason,
                )
                raise
            logging.info("Live pipeline completed successfully (%s).", reason)
            _save_last_pipeline_success_date(_utc_now().date())

            # Nightly export hook (non-fatal): runs once per UTC day after successful pipeline.
            try:
                from src.exports.nightly_export import run as run_nightly_export

                run_nightly_export()
            except Exception:
                logging.exception("Nightly export failed (non-fatal).")

        def heartbeat_tick() -> None:
            nonlocal dashboard_proc, last_trigger_key

            now = _utc_now()

            # Keep dashboard alive.
            if dashboard_proc is None or dashboard_proc.poll() is not None:
                if dashboard_proc is not None:
                    logging.warning("Dashboard process exited (code=%s). Restarting.", dashboard_proc.returncode)
                dashboard_proc = _popen_dashboard()
                logging.info("Dashboard restarted (pid=%s).", dashboard_proc.pid)

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

