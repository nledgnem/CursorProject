from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from repo_paths import macro_state_db_path

DEFAULT_DB_PATH = macro_state_db_path()
REPORTS_ROOT = REPO_ROOT / "reports" / "msm_funding_v0"


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


def _slice_terminal_row(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "decision_date" not in df.columns:
        raise SystemExit("master csv missing decision_date")
    d = df.copy()
    d["decision_date"] = pd.to_datetime(d["decision_date"], errors="coerce")
    d = d.dropna(subset=["decision_date"]).sort_values("decision_date")
    if d.empty:
        raise SystemExit("No valid decision_date rows found to ingest.")
    # Persist decision_date as canonical YYYY-MM-DD string for SQLite key stability.
    out = d.tail(1).reset_index(drop=True)
    out["decision_date"] = out["decision_date"].dt.strftime("%Y-%m-%d")
    return out


def smoke_test_upsert(master_csv: Path) -> None:
    df_full = pd.read_csv(master_csv)
    df = _slice_terminal_row(df_full)
    decision_date = str(df.loc[0, "decision_date"])

    with sqlite3.connect(":memory:") as conn:
        _init_in_memory_schema(conn, df)
        _ensure_unique_index_on_decision_date(conn)
        _upsert_dataframe(conn, df)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM macro_features;")
        total = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM macro_features WHERE decision_date = ?;", (decision_date,))
        by_key = int(cur.fetchone()[0])

    if total != 1 or by_key != 1:
        raise SystemExit(
            f"SMOKE TEST FAILED: expected 1 row total and 1 row for decision_date={decision_date}. "
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
    df = _slice_terminal_row(pd.read_csv(master_csv))
    if "decision_date" not in df.columns:
        raise SystemExit(f"master csv missing decision_date: {master_csv}")
    with sqlite3.connect(db_path) as conn:
        _ensure_unique_index_on_decision_date(conn)
        _upsert_dataframe(conn, df)


def main() -> None:
    p = argparse.ArgumentParser(
        description="8-hour pulse: run pipeline then append latest macro features into SQLite"
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

