from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Iterable

import pandas as pd


@dataclass
class RebalanceSnapshot:
    rebalance_date: date
    divisor: str
    symbols: List[str]
    weights: List[str]
    rebalance_prices: List[str]


class StateStorage:
    """
    Lightweight persistence layer using sqlite3 for:
    - Rebalance snapshots
    - Optional index timeseries
    """

    def __init__(self, db_path: Path | str = "btcdom_state.db") -> None:
        self.db_path = Path(db_path)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS rebalance_snapshots (
                    rebalance_date TEXT PRIMARY KEY,
                    divisor TEXT NOT NULL,
                    symbols_json TEXT NOT NULL,
                    weights_json TEXT NOT NULL,
                    rebalance_prices_json TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS btcdom_index_timeseries (
                    date TEXT PRIMARY KEY,
                    index_value TEXT NOT NULL,
                    divisor TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def upsert_snapshot(self, snapshot: RebalanceSnapshot) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO rebalance_snapshots
                (rebalance_date, divisor, symbols_json, weights_json, rebalance_prices_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(rebalance_date) DO UPDATE SET
                    divisor = excluded.divisor,
                    symbols_json = excluded.symbols_json,
                    weights_json = excluded.weights_json,
                    rebalance_prices_json = excluded.rebalance_prices_json
                """,
                (
                    snapshot.rebalance_date.isoformat(),
                    snapshot.divisor,
                    json.dumps(snapshot.symbols),
                    json.dumps(snapshot.weights),
                    json.dumps(snapshot.rebalance_prices),
                ),
            )
            conn.commit()

    def get_latest_snapshot(self) -> RebalanceSnapshot | None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT rebalance_date, divisor, symbols_json, weights_json, rebalance_prices_json
                FROM rebalance_snapshots
                ORDER BY rebalance_date DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row is None:
                return None
            d_str, divisor, symbols_json, weights_json, prices_json = row
            return RebalanceSnapshot(
                rebalance_date=date.fromisoformat(d_str),
                divisor=divisor,
                symbols=json.loads(symbols_json),
                weights=json.loads(weights_json),
                rebalance_prices=json.loads(prices_json),
            )

    def write_index_timeseries(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        tmp = df.copy()
        tmp["date"] = pd.to_datetime(tmp["date"]).dt.date
        with self._connect() as conn:
            cur = conn.cursor()
            for _, row in tmp.iterrows():
                d = row["date"].isoformat()
                idx = str(row["reconstructed_index_value"])
                div = str(row["daily_divisor"])
                cur.execute(
                    """
                    INSERT INTO btcdom_index_timeseries (date, index_value, divisor)
                    VALUES (?, ?, ?)
                    ON CONFLICT(date) DO UPDATE SET
                        index_value = excluded.index_value,
                        divisor = excluded.divisor
                    """,
                    (d, idx, div),
                )
            conn.commit()

    def load_index_timeseries(self) -> pd.DataFrame:
        with self._connect() as conn:
            df = pd.read_sql_query(
                "SELECT date, index_value, divisor FROM btcdom_index_timeseries ORDER BY date ASC",
                conn,
            )
        if df.empty:
            return df
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df.rename(
            columns={
                "index_value": "reconstructed_index_value",
                "divisor": "daily_divisor",
            }
        )
