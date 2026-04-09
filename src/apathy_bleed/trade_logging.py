from __future__ import annotations

from datetime import date
from pathlib import Path

from src.apathy_bleed.config_loader import load_apathy_alerts_config
from src.apathy_bleed.book import (
    append_book_row,
    book_summary,
    build_long_btc_row,
    format_book_summary_line,
    has_open_long_btc_for_cohort,
    open_short_notional_by_cohort,
    read_book_rows,
)


def log_btc_hedge(
    *,
    repo_root: Path,
    cohort: str,
    btc_price: float,
    entry_date: date,
    hold_days: int,
    notes: str = "",
) -> dict[str, str]:
    cfg = load_apathy_alerts_config(repo_root)
    book_path = cfg.book_csv
    rows = read_book_rows(book_path)
    co = cohort.strip().upper()
    notion = open_short_notional_by_cohort(rows, co)
    if notion <= 0:
        raise ValueError(f"No OPEN SHORT notional found for cohort {co}.")

    if has_open_long_btc_for_cohort(rows, co):
        raise ValueError(f"OPEN LONG_BTC already exists for cohort {co}.")

    if btc_price <= 0:
        raise ValueError("btc_price must be > 0")

    row = build_long_btc_row(
        cohort=co,
        btc_price=btc_price,
        notional=notion,
        entry_date=entry_date,
        hold_days=hold_days,
        notes=notes or None,
    )
    append_book_row(book_path, row)
    return row
