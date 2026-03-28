from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = REPO_ROOT / "data" / "state" / "macro_state.db"
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
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='macro_features';")
        if cur.fetchone() is None:
            raise SystemExit(
                f"DB missing macro_features table. Initialize first. db={db_path}"
            )


def _load_last_n_rows(master_csv: Path, n: int) -> pd.DataFrame:
    df = pd.read_csv(master_csv)
    if "decision_date" not in df.columns:
        raise SystemExit(f"master csv missing decision_date: {master_csv}")

    d = df.copy()
    d["decision_date"] = pd.to_datetime(d["decision_date"], errors="coerce")
    d = d.dropna(subset=["decision_date"]).sort_values("decision_date")
    if d.empty:
        raise SystemExit("No valid decision_date rows found in master csv.")

    out = d.tail(int(n)).reset_index(drop=True)
    # Canonicalize to YYYY-MM-DD strings for SQLite key stability.
    out["decision_date"] = out["decision_date"].dt.strftime("%Y-%m-%d")
    return out


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
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        if hasattr(v, "isoformat") and callable(getattr(v, "isoformat")):
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


def main() -> None:
    p = argparse.ArgumentParser(
        description="One-time seed: UPSERT last N master_macro_features rows into macro_state.db"
    )
    p.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="SQLite db path (default: data/state/macro_state.db)",
    )
    p.add_argument(
        "--rows",
        type=int,
        default=90,
        help="How many most-recent decision_date rows to seed (default: 90)",
    )
    p.add_argument(
        "--master-csv",
        type=Path,
        default=None,
        help="Optional explicit path to master_macro_features.csv (default: newest under reports/msm_funding_v0)",
    )
    args = p.parse_args()

    db_path = args.db.resolve()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}. Initialize it first.")
    _ensure_db_has_table(db_path)

    master_csv = args.master_csv.resolve() if args.master_csv else _find_latest_file(REPORTS_ROOT, "master_macro_features.csv")
    if master_csv is None or not master_csv.exists():
        raise SystemExit("Could not find master_macro_features.csv under reports/msm_funding_v0.")

    df_seed = _load_last_n_rows(master_csv, args.rows)
    with sqlite3.connect(db_path) as conn:
        _ensure_unique_index_on_decision_date(conn)
        _upsert_dataframe(conn, df_seed)

    print(f"Seeded {len(df_seed)} rows into {db_path} from {master_csv}")


if __name__ == "__main__":
    main()

