from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import schedule


REPO_ROOT = Path(__file__).resolve().parent
LOG_DIR = REPO_ROOT / "logs"
LOG_PATH = LOG_DIR / "system_heartbeat.log"


UTC_RUN_TIMES = ("00:05", "08:05", "16:05")  # UTC daily
RUN_WINDOW_SECONDS = 55  # trigger if within this window after HH:MM


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


def _streamlit_cmd() -> list[str]:
    # Prefer module invocation so PATH does not need streamlit.exe.
    return [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "dashboards/app_regime_monitor.py",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]


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


def _run_pipeline_once() -> None:
    logging.info("Running live pipeline: %s", " ".join(_pipeline_cmd()))
    subprocess.run(_pipeline_cmd(), cwd=str(REPO_ROOT), check=True)
    logging.info("Live pipeline completed successfully.")


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

    try:
        dashboard_proc = _popen_dashboard()
        logging.info("Dashboard started (pid=%s).", dashboard_proc.pid)

        def heartbeat_tick() -> None:
            nonlocal dashboard_proc, last_trigger_key

            now = _utc_now()

            # Keep dashboard alive.
            if dashboard_proc is None or dashboard_proc.poll() is not None:
                if dashboard_proc is not None:
                    logging.warning("Dashboard process exited (code=%s). Restarting.", dashboard_proc.returncode)
                dashboard_proc = _popen_dashboard()
                logging.info("Dashboard restarted (pid=%s).", dashboard_proc.pid)

            # Trigger pipeline on UTC schedule without double-firing.
            for hhmm in UTC_RUN_TIMES:
                if not _should_trigger_run(now, hhmm):
                    continue
                key = f"{now.date().isoformat()}T{hhmm}"
                if last_trigger_key == key:
                    continue
                last_trigger_key = key

                try:
                    _run_pipeline_once()
                except subprocess.CalledProcessError as e:
                    logging.error(
                        "Live pipeline failed (exit=%s). Dashboard remains online using last known state.",
                        e.returncode,
                    )
                except Exception:
                    logging.exception(
                        "Unexpected error during live pipeline run. Dashboard remains online using last known state."
                    )

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

