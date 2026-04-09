from __future__ import annotations

import csv
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
BOOK_FIELDNAMES = [
    "trade_id",
    "cohort",
    "ticker",
    "side",
    "entry_date_utc",
    "entry_price_usd",
    "notional_usd",
    "quantity",
    "stop_price_usd",
    "exit_date_target_utc",
    "status",
    "exit_date_utc",
    "exit_price_usd",
    "pnl_usd",
    "pnl_pct",
    "notes",
]


def read_book_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return []
        rows: list[dict[str, str]] = []
        for row in reader:
            if not any((v or "").strip() for v in row.values()):
                continue
            rows.append({k: (row.get(k) or "") for k in BOOK_FIELDNAMES})
        return rows


def atomic_write_book(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=BOOK_FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in BOOK_FIELDNAMES})
    tmp.replace(path)


def append_book_row(path: Path, row: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=BOOK_FIELDNAMES, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in BOOK_FIELDNAMES})


def parse_iso_date(s: str) -> date:
    return date.fromisoformat((s or "").strip())


def open_short_notional_by_cohort(rows: list[dict[str, str]], cohort: str) -> float:
    c = cohort.strip().upper()
    total = 0.0
    for r in rows:
        if (r.get("status") or "").strip().upper() != "OPEN":
            continue
        if (r.get("side") or "").strip().upper() != "SHORT":
            continue
        if (r.get("cohort") or "").strip().upper() != c:
            continue
        total += float(r.get("notional_usd") or 0)
    return total


def has_open_long_btc_for_cohort(rows: list[dict[str, str]], cohort: str) -> bool:
    c = cohort.strip().upper()
    for r in rows:
        if (r.get("status") or "").strip().upper() != "OPEN":
            continue
        if (r.get("side") or "").strip().upper() != "LONG_BTC":
            continue
        if (r.get("cohort") or "").strip().upper() == c:
            return True
    return False


def has_open_short_duplicate(rows: list[dict[str, str]], cohort: str, ticker: str) -> bool:
    co = cohort.strip().upper()
    tk = ticker.strip().upper()
    for r in rows:
        if (r.get("status") or "").strip().upper() != "OPEN":
            continue
        if (r.get("side") or "").strip().upper() != "SHORT":
            continue
        if (r.get("cohort") or "").strip().upper() == co and (r.get("ticker") or "").strip().upper() == tk:
            return True
    return False


@dataclass(frozen=True)
class BookSummary:
    open_short_count: int
    open_long_btc_count: int
    total_short_notional_usd: float
    total_open_count: int


def book_summary(rows: list[dict[str, str]]) -> BookSummary:
    n_short = 0
    n_btc = 0
    notion = 0.0
    n_open = 0
    for r in rows:
        if (r.get("status") or "").strip().upper() != "OPEN":
            continue
        n_open += 1
        side = (r.get("side") or "").strip().upper()
        if side == "SHORT":
            n_short += 1
            notion += float(r.get("notional_usd") or 0)
        elif side == "LONG_BTC":
            n_btc += 1
    return BookSummary(
        open_short_count=n_short,
        open_long_btc_count=n_btc,
        total_short_notional_usd=notion,
        total_open_count=n_open,
    )


def format_book_summary_line(s: BookSummary) -> str:
    k = s.total_short_notional_usd / 1000.0
    if k >= 10:
        notion_s = f"${k:.0f}K"
    elif k >= 1:
        notion_s = f"${k:.1f}K"
    else:
        notion_s = f"${s.total_short_notional_usd:,.0f}"
    return f"Book: {s.open_short_count} OPEN short legs, {notion_s} short notional ({s.total_open_count} OPEN rows incl. hedges)."


def max_open_entry_date(rows: list[dict[str, str]]) -> date | None:
    best: date | None = None
    for r in rows:
        if (r.get("status") or "").strip().upper() != "OPEN":
            continue
        raw = (r.get("entry_date_utc") or "").strip()
        if not raw:
            continue
        try:
            d = parse_iso_date(raw)
        except ValueError:
            continue
        if best is None or d > best:
            best = d
    return best


def new_trade_id() -> str:
    return str(uuid.uuid4())


def build_short_entry_row(
    *,
    cohort: str,
    ticker: str,
    entry_price: float,
    notional: float,
    entry_date: date,
    hold_days: int,
    notes: str | None = None,
) -> dict[str, str]:
    qty = notional / entry_price if entry_price else 0.0
    stop = entry_price * 1.60
    target = entry_date + timedelta(days=hold_days)
    return {
        "trade_id": new_trade_id(),
        "cohort": cohort.strip().upper(),
        "ticker": ticker.strip().upper(),
        "side": "SHORT",
        "entry_date_utc": entry_date.isoformat(),
        "entry_price_usd": f"{entry_price:.8f}".rstrip("0").rstrip("."),
        "notional_usd": f"{notional:.2f}",
        "quantity": f"{qty:.8f}".rstrip("0").rstrip("."),
        "stop_price_usd": f"{stop:.8f}".rstrip("0").rstrip("."),
        "exit_date_target_utc": target.isoformat(),
        "status": "OPEN",
        "exit_date_utc": "",
        "exit_price_usd": "",
        "pnl_usd": "",
        "pnl_pct": "",
        "notes": notes or "",
    }


def build_long_btc_row(
    *,
    cohort: str,
    btc_price: float,
    notional: float,
    entry_date: date,
    hold_days: int,
    notes: str | None = None,
) -> dict[str, str]:
    qty = notional / btc_price if btc_price else 0.0
    target = entry_date + timedelta(days=hold_days)
    return {
        "trade_id": new_trade_id(),
        "cohort": cohort.strip().upper(),
        "ticker": "BTC",
        "side": "LONG_BTC",
        "entry_date_utc": entry_date.isoformat(),
        "entry_price_usd": f"{btc_price:.8f}".rstrip("0").rstrip("."),
        "notional_usd": f"{notional:.2f}",
        "quantity": f"{qty:.8f}".rstrip("0").rstrip("."),
        "stop_price_usd": "",
        "exit_date_target_utc": target.isoformat(),
        "status": "OPEN",
        "exit_date_utc": "",
        "exit_price_usd": "",
        "pnl_usd": "",
        "pnl_pct": "",
        "notes": notes or "",
    }


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def short_adverse_move(entry: float, mark: float) -> float:
    """SHORT: fractional move against you when mark rises (price - entry) / entry."""
    if entry <= 0:
        return 0.0
    return (mark - entry) / entry


def unrealized_short_pct(entry: float, mark: float) -> float:
    if entry <= 0:
        return 0.0
    return (entry - mark) / entry


def unrealized_long_pct(entry: float, mark: float) -> float:
    if entry <= 0:
        return 0.0
    return (mark - entry) / entry
