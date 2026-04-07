from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from repo_paths import macro_state_db_path

DEFAULT_DB_PATH = macro_state_db_path()
REPORTS_ROOT = REPO_ROOT / "reports" / "msm_funding_v0"

logger = logging.getLogger(__name__)


def _find_latest_file(path: Path, name: str) -> Optional[Path]:
    if not path.exists():
        return None
    candidates = list(path.rglob(name))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _ensure_db_has_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='macro_features';"
        )
        if cur.fetchone() is None:
            raise SystemExit(
                f"DB missing macro_features table. Initialize first. db={db_path}"
            )


def _safe_float(v) -> float:
    try:
        if v is None or pd.isna(v):
            return float("nan")
        return float(v)
    except Exception:
        return float("nan")


def _regime_label(row: dict) -> str:
    # Stable, human-readable label for comparisons + alerts.
    funding = str(row.get("funding_regime", "Unknown"))
    btcd = str(row.get("BTCDOM_Trend", "Unknown"))
    gate = row.get("is_mrf_active", None)
    try:
        gate_on = bool(int(gate)) if isinstance(gate, (int, str)) and str(gate).isdigit() else bool(gate)
    except Exception:
        gate_on = False
    gate_label = "GATE:ON" if gate_on else "GATE:OFF"
    return f"{funding} | {btcd} | {gate_label}"


def _load_latest_row(conn: sqlite3.Connection) -> Optional[dict]:
    cur = conn.cursor()
    cur.execute(
        """
SELECT decision_date, funding_regime, BTCDOM_Trend, is_mrf_active, Environment_APR, Fragmentation_Spread
FROM macro_features
ORDER BY decision_date DESC
LIMIT 1;
        """.strip()
    )
    r = cur.fetchone()
    if r is None:
        return None
    cols = ["decision_date", "funding_regime", "BTCDOM_Trend", "is_mrf_active", "Environment_APR", "Fragmentation_Spread"]
    return dict(zip(cols, r))


def _ensure_meta_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
        """.strip()
    )
    conn.commit()


def _meta_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key = ? LIMIT 1;", (key,))
    r = cur.fetchone()
    return None if r is None else str(r[0])


def _meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """
INSERT INTO meta(key, value) VALUES(?, ?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value;
        """.strip(),
        (key, value),
    )
    conn.commit()


def _utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def send_telegram_alert(old_regime: str, new_regime: str, apr: float, spread: float) -> None:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token or not chat_id:
        logger.warning("Missing Telegram credentials (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Skipping alert.")
        return

    text_payload = (
        "MACRO REGIME CHANGE DETECTED\n\n"
        f"Shift: {old_regime} -> {new_regime}\n"
        f"Environment APR: {apr:.2f}%\n"
        f"Fragmentation Spread: {spread:.6f}\n\n"
        "Check the Streamlit dashboard for full details."
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text_payload}

    try:
        resp = requests.post(url, json=payload, timeout=5)
        resp.raise_for_status()
        logger.info("Telegram alert dispatched successfully.")
    except Exception as e:
        logger.error("Telegram delivery failed (non-fatal): %s", e)


def send_telegram_daily_status(regime: str, decision_date: str, apr: float, spread: float) -> None:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token or not chat_id:
        logger.warning("Missing Telegram credentials (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Skipping alert.")
        return

    today_utc = _utc_today_iso()
    text_payload = (
        "DAILY MACRO REGIME STATUS\n\n"
        f"UTC Day: {today_utc}\n"
        f"Latest decision_date: {decision_date}\n"
        f"Regime: {regime}\n"
        f"Environment APR: {apr:.2f}%\n"
        f"Fragmentation Spread: {spread:.6f}\n\n"
        "Check the Streamlit dashboard for full details."
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text_payload}

    try:
        resp = requests.post(url, json=payload, timeout=5)
        resp.raise_for_status()
        logger.info("Telegram daily status dispatched successfully.")
    except Exception as e:
        logger.error("Telegram delivery failed (non-fatal): %s", e)


def _ensure_unique_index_on_decision_date(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_features_decision_date
ON macro_features(decision_date);
        """.strip()
    )
    conn.commit()


def _upsert_dataframe(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    if "decision_date" not in df.columns:
        raise SystemExit("Cannot upsert without decision_date column.")

    cols = [str(c) for c in df.columns]
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(f'"{c}"' for c in cols)
    update_cols = [c for c in cols if c != "decision_date"]
    update_set = ", ".join([f'"{c}"=excluded."{c}"' for c in update_cols])

    sql = f"""
INSERT INTO macro_features ({col_list})
VALUES ({placeholders})
ON CONFLICT(decision_date) DO UPDATE SET
  {update_set};
    """.strip()

    def _sqlite_bindable(v):
        if pd.isna(v) or v is pd.NaT:
            return None
        # Normalize pandas/numpy datetime-like types for sqlite3 binder.
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        if hasattr(v, "isoformat") and callable(getattr(v, "isoformat")):
            # Covers datetime/date objects.
            try:
                return v.isoformat()
            except Exception:
                pass
        return v

    values = []
    for row in df.itertuples(index=False, name=None):
        values.append(tuple(_sqlite_bindable(v) for v in row))

    cur = conn.cursor()
    cur.executemany(sql, values)
    conn.commit()


def _infer_sqlite_type_from_series(s: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(s):
        return "INTEGER"
    if pd.api.types.is_float_dtype(s):
        return "REAL"
    if pd.api.types.is_bool_dtype(s):
        return "INTEGER"
    return "TEXT"


def _init_in_memory_schema(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    cols = [(str(c), _infer_sqlite_type_from_series(df[c])) for c in df.columns]
    col_defs = ", ".join([f'"{c}" {t}' for c, t in cols])
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=MEMORY;")
    cur.execute(f"CREATE TABLE IF NOT EXISTS macro_features ({col_defs});")
    cur.execute(
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_features_decision_date ON macro_features("decision_date");'
    )
    conn.commit()


def _prepare_macro_history_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize master_macro_features.csv for SQLite: sort, dedupe by day, string decision_date."""
    if df is None or df.empty:
        return df
    if "decision_date" not in df.columns:
        raise SystemExit("master csv missing decision_date")
    d = df.copy()
    d["decision_date"] = pd.to_datetime(d["decision_date"], errors="coerce")
    d = d.dropna(subset=["decision_date"]).sort_values("decision_date")
    if d.empty:
        raise SystemExit("No valid decision_date rows found to ingest.")
    d = d.drop_duplicates(subset=["decision_date"], keep="last").reset_index(drop=True)
    d["decision_date"] = d["decision_date"].dt.strftime("%Y-%m-%d")
    return d


def smoke_test_upsert(master_csv: Path) -> None:
    df = _prepare_macro_history_df(pd.read_csv(master_csv))
    if df.empty:
        raise SystemExit("SMOKE TEST FAILED: empty dataframe after prepare.")
    n_expected = len(df)
    last_date = str(df.iloc[-1]["decision_date"])

    with sqlite3.connect(":memory:") as conn:
        _init_in_memory_schema(conn, df)
        _ensure_unique_index_on_decision_date(conn)
        _upsert_dataframe(conn, df)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM macro_features;")
        total = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM macro_features WHERE decision_date = ?;", (last_date,))
        by_key = int(cur.fetchone()[0])

    if total != n_expected or by_key != 1:
        raise SystemExit(
            f"SMOKE TEST FAILED: expected {n_expected} rows total and 1 for last decision_date={last_date}. "
            f"got total={total}, by_key={by_key}"
        )


def run_live_pipeline(repo_root: Path) -> None:
    # Uses the existing production boundary: always through present date.
    subprocess.run(
        [sys.executable, str(repo_root / "run_live_pipeline.py")],
        check=True,
        cwd=str(repo_root),
    )


def ingest_latest_master_csv(db_path: Path, master_csv: Path) -> None:
    df = _prepare_macro_history_df(pd.read_csv(master_csv))
    if "decision_date" not in df.columns:
        raise SystemExit(f"master csv missing decision_date: {master_csv}")
    with sqlite3.connect(db_path) as conn:
        prev = _load_latest_row(conn)
        _ensure_meta_table(conn)
        _ensure_unique_index_on_decision_date(conn)
        _upsert_dataframe(conn, df)

        # Telegram alerts are strictly non-fatal to the pipeline.
        try:
            new_row = df.iloc[-1].to_dict()
            new_regime = _regime_label(new_row)
            decision_date = str(new_row.get("decision_date", ""))
            apr = _safe_float(new_row.get("Environment_APR"))
            spread = _safe_float(new_row.get("Fragmentation_Spread"))

            # Fire every day (once per UTC day), regardless of regime change.
            today_utc = _utc_today_iso()
            last_sent = _meta_get(conn, "telegram_daily_status_last_sent_utc_day")
            if last_sent != today_utc:
                send_telegram_daily_status(
                    regime=new_regime,
                    decision_date=decision_date,
                    apr=apr,
                    spread=spread,
                )
                _meta_set(conn, "telegram_daily_status_last_sent_utc_day", today_utc)

            # Keep the regime-change ping as an extra high-signal alert.
            if prev is not None:
                old_regime = _regime_label(prev)
                if old_regime != new_regime:
                    send_telegram_alert(old_regime=old_regime, new_regime=new_regime, apr=apr, spread=spread)
        except Exception as e:
            logger.warning("Regime change alert evaluation failed (non-fatal): %s", e, exc_info=True)


def main() -> None:
    p = argparse.ArgumentParser(
        description="8-hour pulse: run pipeline then upsert full macro feature history into SQLite"
    )
    p.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="SQLite db path (default: data/state/macro_state.db)",
    )
    p.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Skip running run_live_pipeline.py (for debugging).",
    )
    p.add_argument(
        "--smoke-test",
        action="store_true",
        help="Run an in-memory UPSERT smoke test and exit without touching production DB.",
    )
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if not args.skip_pipeline:
        run_live_pipeline(REPO_ROOT)

    master_csv = _find_latest_file(REPORTS_ROOT, "master_macro_features.csv")
    if master_csv is None:
        raise SystemExit(
            "Could not find master_macro_features.csv under reports/msm_funding_v0."
        )

    if args.smoke_test:
        smoke_test_upsert(master_csv)
        print(f"Smoke test passed (in-memory): {master_csv}")
        return

    db_path = args.db.resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}. Initialize it first.")

    _ensure_db_has_table(db_path)

    ingest_latest_master_csv(db_path, master_csv)
    print(f"Ingested -> {db_path} from {master_csv}")


if __name__ == "__main__":
    main()

