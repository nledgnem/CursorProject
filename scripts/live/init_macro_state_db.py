from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = REPO_ROOT / "data" / "state" / "macro_state.db"


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    sqlite_type: str


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _infer_sqlite_type_from_series(s: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(s):
        return "INTEGER"
    if pd.api.types.is_float_dtype(s):
        return "REAL"
    if pd.api.types.is_bool_dtype(s):
        return "INTEGER"
    return "TEXT"


def _build_column_specs_from_csv(csv_path: Path, sample_rows: int = 500) -> list[ColumnSpec]:
    df = pd.read_csv(csv_path, nrows=sample_rows)
    specs: list[ColumnSpec] = []
    for c in df.columns:
        specs.append(ColumnSpec(name=str(c), sqlite_type=_infer_sqlite_type_from_series(df[c])))
    return specs


def _exec_many(conn: sqlite3.Connection, statements: Iterable[str]) -> None:
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
    conn.commit()


def init_db(db_path: Path, schema_from_csv: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    cols = _build_column_specs_from_csv(schema_from_csv)
    col_defs = ",\n  ".join(f"{_quote_ident(c.name)} {c.sqlite_type}" for c in cols)

    statements = [
        "PRAGMA journal_mode=WAL;",
        "PRAGMA synchronous=NORMAL;",
        "PRAGMA foreign_keys=ON;",
        f"""
CREATE TABLE IF NOT EXISTS macro_features (
  {col_defs}
);
        """.strip(),
        # Enforce one row per decision_date in production.
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_features_decision_date ON macro_features("decision_date");',
        'CREATE INDEX IF NOT EXISTS idx_macro_features_decision_date ON macro_features("decision_date");',
        "CREATE INDEX IF NOT EXISTS idx_macro_features_next_date ON macro_features(next_date);",
    ]

    with sqlite3.connect(db_path) as conn:
        _exec_many(conn, statements)


def main() -> None:
    p = argparse.ArgumentParser(description="Initialize macro_state.db schema from master_macro_features.csv")
    p.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="SQLite db path (default: data/state/macro_state.db)",
    )
    p.add_argument(
        "--schema-from-csv",
        type=Path,
        required=True,
        help="CSV whose header defines the macro_features schema (e.g. master_macro_features.csv)",
    )
    args = p.parse_args()

    init_db(db_path=args.db.resolve(), schema_from_csv=args.schema_from_csv.resolve())
    print(f"Initialized SQLite DB: {args.db.resolve()}")


if __name__ == "__main__":
    main()

