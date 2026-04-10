from __future__ import annotations

import csv
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

from repo_paths import macro_state_db_path

from src.apathy_bleed.book import (
    book_summary,
    max_open_entry_date,
    parse_iso_date,
    read_book_rows,
    short_adverse_move,
    unrealized_long_pct,
    unrealized_short_pct,
)
from src.apathy_bleed.config_loader import ApathyAlertsConfig
from src.apathy_bleed.macro_snapshot import format_regime_apathy_line
from src.apathy_bleed.variational_prices import fetch_variational_mark_prices
from src.notifications.telegram_client import send_telegram_text

logger = logging.getLogger(__name__)

TIER_RANK = {"NONE": 0, "WARNING": 1, "CRITICAL": 2, "STOP_HIT": 3}

ALERT_LOG_FIELDS = ["timestamp_utc", "alert_type", "trade_id", "cohort", "ticker", "dedup_bucket", "message"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def append_alert_log(path: Path, row: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ALERT_LOG_FIELDS, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in ALERT_LOG_FIELDS})


def _load_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return dict(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Could not read JSON state %s; using defaults.", path)
        return dict(default)


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def classify_short_tier(adverse: float, cfg: ApathyAlertsConfig) -> str:
    if adverse >= cfg.stop_threshold_pct:
        return "STOP_HIT"
    if adverse >= cfg.critical_threshold_pct:
        return "CRITICAL"
    if adverse >= cfg.warning_threshold_pct:
        return "WARNING"
    return "NONE"


def run_stop_proximity(cfg: ApathyAlertsConfig, rows: list[dict[str, str]], prices: dict[str, float]) -> None:
    state_path = cfg.stop_proximity_state_json
    state = _load_json(
        state_path,
        {"last_tier_by_trade": {}, "hour_tier_sent": {}},
    )
    last_tier: dict[str, str] = {str(k): str(v) for k, v in (state.get("last_tier_by_trade") or {}).items()}
    hour_tier_sent: dict[str, dict] = dict(state.get("hour_tier_sent") or {})

    open_short_ids = {
        (r.get("trade_id") or "").strip()
        for r in rows
        if (r.get("status") or "").upper() == "OPEN" and (r.get("side") or "").upper() == "SHORT" and (r.get("trade_id") or "").strip()
    }
    for k in list(last_tier.keys()):
        if k not in open_short_ids:
            last_tier.pop(k, None)
            hour_tier_sent.pop(k, None)

    now = _utc_now()
    hour_key = now.strftime("%Y-%m-%dT%H")

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        if (r.get("side") or "").upper() != "SHORT":
            continue
        tid = (r.get("trade_id") or "").strip()
        if not tid:
            continue
        tkr = (r.get("ticker") or "").strip().upper()
        cohort = (r.get("cohort") or "").strip().upper()
        try:
            entry = float(r.get("entry_price_usd") or 0)
        except ValueError:
            continue
        mark = prices.get(tkr)
        if mark is None:
            continue
        adverse = short_adverse_move(entry, mark)
        stop_px = float(r.get("stop_price_usd") or entry * 1.60)
        curr = classify_short_tier(adverse, cfg)
        prev = last_tier.get(tid, "NONE")

        r_curr = TIER_RANK.get(curr, 0)
        r_prev = TIER_RANK.get(prev, 0)

        should_send = False
        if r_curr > r_prev and r_curr >= TIER_RANK["WARNING"]:
            should_send = True
        elif r_curr == r_prev and r_curr >= TIER_RANK["WARNING"]:
            hs = hour_tier_sent.get(tid) or {}
            if not (hs.get("hour") == hour_key and hs.get("tier") == curr):
                should_send = True
        elif r_curr < r_prev:
            hour_tier_sent.pop(tid, None)

        if should_send:
            pct_display = adverse * 100.0
            if curr == "STOP_HIT":
                msg = (
                    f"🔴 STOP HIT: {tkr} ({cohort}) breached {cfg.stop_threshold_pct * 100:.0f}% stop. "
                    f"Close position immediately.\n"
                    f"Entry: ${entry:.2f} | Current: ${mark:.2f} | Stop: ${stop_px:.2f}"
                )
            elif curr == "CRITICAL":
                msg = (
                    f"🚨 STOP CRITICAL: {tkr} ({cohort}) at +{pct_display:.1f}% from entry "
                    f"(${entry:.2f} → ${mark:.2f}).\n"
                    f"Stop at ${stop_px:.2f} (+{cfg.stop_threshold_pct * 100:.0f}%). Action needed if continues."
                )
            else:
                msg = (
                    f"⚠️ STOP WARNING: {tkr} ({cohort}) at +{pct_display:.1f}% from entry "
                    f"(${entry:.2f} → ${mark:.2f}).\n"
                    f"Stop at ${stop_px:.2f} (+{cfg.stop_threshold_pct * 100:.0f}%). Action needed if continues."
                )
            send_telegram_text(msg)
            append_alert_log(
                cfg.alert_log_csv,
                {
                    "timestamp_utc": now.isoformat(),
                    "alert_type": f"STOP_{curr}",
                    "trade_id": tid,
                    "cohort": cohort,
                    "ticker": tkr,
                    "dedup_bucket": f"{hour_key}|{curr}",
                    "message": msg.replace("\n", " / "),
                },
            )
            logger.info("Stop proximity alert: %s %s %s", tkr, cohort, curr)
            hour_tier_sent[tid] = {"hour": hour_key, "tier": curr}

        last_tier[tid] = curr

    state["last_tier_by_trade"] = last_tier
    state["hour_tier_sent"] = hour_tier_sent
    _save_json(state_path, state)


def run_exit_reminders(cfg: ApathyAlertsConfig, rows: list[dict[str, str]], prices: dict[str, float]) -> None:
    state_path = cfg.exit_reminder_state_json
    sent_keys: dict[str, str] = _load_json(state_path, {})

    today = _utc_now().date()
    now = _utc_now()

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        tid = (r.get("trade_id") or "").strip()
        if not tid:
            continue
        tkr = (r.get("ticker") or "").strip().upper()
        cohort = (r.get("cohort") or "").strip().upper()
        try:
            target = parse_iso_date(r.get("exit_date_target_utc") or "")
            entry = float(r.get("entry_price_usd") or 0)
        except ValueError:
            continue
        days_left = (target - today).days
        if days_left not in cfg.exit_reminder_days:
            continue
        dedup = f"{tid}|D{days_left}|{target.isoformat()}"
        if sent_keys.get(dedup) == today.isoformat():
            continue

        side = (r.get("side") or "").upper()
        mark = prices.get(tkr)
        if mark is None:
            msg = (
                f"📅 EXIT REMINDER: {tkr} ({cohort}) expires in {days_left} days ({target.strftime('%b %d')}).\n"
                f"Entry: ${entry:.2f} | Current: (no mark) | Unrealized: N/A"
            )
        else:
            if side == "SHORT":
                ur = unrealized_short_pct(entry, mark)
            elif side == "LONG_BTC":
                ur = unrealized_long_pct(entry, mark)
            else:
                ur = float("nan")
            ur_s = f"{ur * 100:+.1f}%" if ur == ur else "N/A"
            msg = (
                f"📅 EXIT REMINDER: {tkr} ({cohort}) expires in {days_left} days ({target.strftime('%b %d')}).\n"
                f"Entry: ${entry:.2f} | Current: ${mark:.2f} | Unrealized: {ur_s}"
            )

        send_telegram_text(msg)
        append_alert_log(
            cfg.alert_log_csv,
            {
                "timestamp_utc": now.isoformat(),
                "alert_type": "EXIT_REMINDER",
                "trade_id": tid,
                "cohort": cohort,
                "ticker": tkr,
                "dedup_bucket": dedup,
                "message": msg.replace("\n", " / "),
            },
        )
        sent_keys[dedup] = today.isoformat()
        logger.info("Exit reminder: %s %s days_left=%s", tkr, cohort, days_left)

    _save_json(state_path, sent_keys)


def run_scanner_reminders(cfg: ApathyAlertsConfig, rows: list[dict[str, str]]) -> None:
    state_path = cfg.scanner_reminder_state_json
    sent_today: dict[str, str] = _load_json(state_path, {})

    last_entry = max_open_entry_date(rows)
    today = _utc_now().date()
    now = _utc_now()
    if last_entry is None:
        return
    days_since = (today - last_entry).days

    for milestone in cfg.scan_reminder_milestone_days:
        if days_since != milestone:
            continue
        key = f"milestone_{milestone}"
        if sent_today.get(key) == today.isoformat():
            continue
        closes_in = max(0, cfg.formation_window_days - days_since)
        msg = (
            f"🔍 NEW COHORT SCAN DUE: {days_since} days since last OPEN entry ({last_entry.isoformat()}).\n"
            f"{cfg.formation_window_days}-day formation window closes in ~{closes_in} days.\n"
            f"Run: python scripts/xref_perp_listings.py to generate candidates."
        )
        send_telegram_text(msg)
        append_alert_log(
            cfg.alert_log_csv,
            {
                "timestamp_utc": now.isoformat(),
                "alert_type": "SCANNER_REMINDER",
                "trade_id": "",
                "cohort": "",
                "ticker": "",
                "dedup_bucket": f"{key}|{today.isoformat()}",
                "message": msg.replace("\n", " / "),
            },
        )
        sent_today[key] = today.isoformat()
        logger.info("Scanner reminder: milestone=%s days_since=%s", milestone, days_since)

    _save_json(state_path, sent_today)


def _cohort_stats(
    rows: list[dict[str, str]], prices: dict[str, float]
) -> dict[str, dict[str, float | str | int]]:
    """Per cohort: short count, avg unrealized fraction, min exit target among open."""
    from collections import defaultdict

    by_c: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        if (r.get("side") or "").upper() != "SHORT":
            continue
        c = (r.get("cohort") or "").strip().upper()
        by_c[c].append(r)

    out: dict[str, dict[str, float | str | int]] = {}
    for c, lst in by_c.items():
        pnl_fracs: list[float] = []
        targets: list[date] = []
        for r in lst:
            try:
                entry = float(r.get("entry_price_usd") or 0)
                tkr = (r.get("ticker") or "").strip().upper()
                mark = prices.get(tkr)
                if mark is not None and entry > 0:
                    pnl_fracs.append(unrealized_short_pct(entry, mark))
                targets.append(parse_iso_date(r.get("exit_date_target_utc") or ""))
            except ValueError:
                continue
        avg = sum(pnl_fracs) / len(pnl_fracs) if pnl_fracs else float("nan")
        min_tgt = min(targets) if targets else None
        out[c] = {
            "n": len(lst),
            "avg_ur": avg,
            "exit_min": min_tgt.isoformat() if min_tgt else "",
        }
    return out


def run_portfolio_snapshot(cfg: ApathyAlertsConfig, rows: list[dict[str, str]], prices: dict[str, float]) -> None:
    state_path = cfg.daily_snapshot_state_json
    state = _load_json(state_path, {"last_portfolio_snapshot_utc_hour": ""})
    now = _utc_now()
    hour_key = now.strftime("%Y-%m-%dT%H")
    if state.get("last_portfolio_snapshot_utc_hour") == hour_key:
        logger.info("Portfolio snapshot skip: already sent for UTC hour %s.", hour_key)
        return

    summ = book_summary(rows)
    if summ.total_open_count == 0:
        state["last_portfolio_snapshot_utc_hour"] = hour_key
        _save_json(state_path, state)
        logger.info("Portfolio snapshot skip: no OPEN rows (UTC hour %s).", hour_key)
        return
    open_shorts = [r for r in rows if (r.get("status") or "").upper() == "OPEN" and (r.get("side") or "").upper() == "SHORT"]

    nearest_exp: tuple[str, str, int] | None = None
    today_d = _utc_now().date()
    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        try:
            tgt = parse_iso_date(r.get("exit_date_target_utc") or "")
            dleft = (tgt - today_d).days
        except ValueError:
            continue
        tkr = (r.get("ticker") or "").strip().upper()
        co = (r.get("cohort") or "").strip().upper()
        if nearest_exp is None or dleft < nearest_exp[2]:
            nearest_exp = (tkr, co, dleft)

    nearest_stop: tuple[str, str, float] | None = None
    for r in open_shorts:
        tkr = (r.get("ticker") or "").strip().upper()
        co = (r.get("cohort") or "").strip().upper()
        try:
            entry = float(r.get("entry_price_usd") or 0)
        except ValueError:
            continue
        mark = prices.get(tkr)
        if mark is None or entry <= 0:
            continue
        adv = short_adverse_move(entry, mark) * 100.0
        if nearest_stop is None or adv > nearest_stop[2]:
            nearest_stop = (tkr, co, adv)

    db_path = macro_state_db_path()
    regime_line = format_regime_apathy_line(db_path)

    lines = [
        f"📊 APATHY BLEED PORTFOLIO SNAPSHOT (UTC {now:%Y-%m-%d %H:00})",
        f"Open positions: {summ.total_open_count} | Short legs: {summ.open_short_count} | Total short notional: ${summ.total_short_notional_usd:,.0f}",
    ]
    if nearest_exp:
        lines.append(f"Nearest expiry: {nearest_exp[0]} ({nearest_exp[1]}) in {nearest_exp[2]} days")
    else:
        lines.append("Nearest expiry: n/a")
    if nearest_stop:
        lines.append(f"Nearest stop stress: {nearest_stop[0]} ({nearest_stop[1]}) at +{nearest_stop[2]:.1f}% from entry")
    else:
        lines.append("Nearest stop stress: n/a")

    lines.append("")
    lines.append(regime_line)
    lines.append("")
    lines.append("Cohort breakdown:")

    cstats = _cohort_stats(rows, prices)
    for c in sorted(cstats.keys()):
        s = cstats[c]
        avg = s["avg_ur"]
        avg_s = f"{float(avg) * 100:+.1f}%" if avg == avg else "N/A"
        ex = s.get("exit_min") or "n/a"
        lines.append(f"{c}: {int(s['n'])} positions, avg {avg_s} unrealized (short), exits {ex}")

    msg = "\n".join(lines)
    send_telegram_text(msg)
    append_alert_log(
        cfg.alert_log_csv,
        {
            "timestamp_utc": _utc_now().isoformat(),
            "alert_type": "PORTFOLIO_SNAPSHOT",
            "trade_id": "",
            "cohort": "",
            "ticker": "",
            "dedup_bucket": f"snapshot|{hour_key}",
            "message": msg.replace("\n", " / "),
        },
    )
    state["last_portfolio_snapshot_utc_hour"] = hour_key
    _save_json(state_path, state)
    logger.info("Portfolio snapshot sent (UTC hour %s).", hour_key)


def run_daily_snapshot(cfg: ApathyAlertsConfig, rows: list[dict[str, str]], prices: dict[str, float]) -> None:
    """Backward-compatible alias for :func:`run_portfolio_snapshot`."""
    run_portfolio_snapshot(cfg, rows, prices)
