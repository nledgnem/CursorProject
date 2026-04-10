from __future__ import annotations

import csv
import json
import logging
import html
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from src.danlongshort.config_loader import DanLongShortAlertsConfig
from src.danlongshort.funding import estimate_daily_funding_pnl_usd, fetch_binance_usdm_funding_rates
from src.danlongshort.portfolio import (
    btc_rebalance_target_notional,
    compute_30d_betas,
    compute_portfolio_snapshot,
    fetch_30d_closes_usd,
    latest_prices_from_closes,
    load_positions_csv,
    load_symbol_to_coingecko_id,
)
from src.notifications.telegram_client import send_telegram_message

logger = logging.getLogger(__name__)

ALERT_LOG_FIELDS = ["timestamp_utc", "alert_type", "dedup_bucket", "message"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _load_json(path: Path, default: dict) -> dict:
    if not path.exists():
        return dict(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(default)


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _append_alert_log(path: Path, row: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ALERT_LOG_FIELDS, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in ALERT_LOG_FIELDS})


def _fmt_money(x: float) -> str:
    if x != x or x is None:  # nan
        return "n/a"
    return f"${x:,.0f}"


def _fmt_float(x: float, *, ndp: int = 2) -> str:
    if x != x or x is None:
        return "n/a"
    return f"{x:.{ndp}f}"


def _fmt_pct(x: float, *, ndp: int = 1) -> str:
    if x != x or x is None:
        return "n/a"
    return f"{x * 100:.{ndp}f}%"


def build_portfolio_snapshot_text(cfg: DanLongShortAlertsConfig) -> str | None:
    positions = load_positions_csv(cfg.positions_csv)
    if not positions:
        return None

    tickers = [p.ticker for p in positions]
    symbol_to_cg = load_symbol_to_coingecko_id(cfg.allowlist_csv, override_yaml=cfg.symbol_map_yaml)

    closes = fetch_30d_closes_usd(
        tickers=tickers,
        symbol_to_cg=symbol_to_cg,
        enable_cache=True,
        cache_max_age_hours=12.0,
    )
    betas = compute_30d_betas(closes, btc_ticker="BTC")
    latest_prices = latest_prices_from_closes(closes)

    tbl, summ = compute_portfolio_snapshot(positions, betas, latest_prices)
    reb = btc_rebalance_target_notional(positions, betas)

    # Funding rates (binance perps)
    funding_rates = fetch_binance_usdm_funding_rates([p.ticker for p in positions])
    funding_daily_total = 0.0
    funding_daily_known = False
    per_funding: dict[str, float | None] = {}
    for p in positions:
        r = funding_rates.get(p.ticker)
        per_funding[p.ticker] = r if r is not None else None
        pnl = estimate_daily_funding_pnl_usd(
            notional_usd=p.notional_usd, direction=p.direction, funding_rate_per_8h=r
        )
        if pnl is not None:
            funding_daily_total += float(pnl)
            funding_daily_known = True

    net_beta = float(summ.get("net_beta_exposure_usd") or 0.0)
    gross = float(summ.get("gross_notional_usd") or 0.0)
    net_notional = float(summ.get("net_notional_usd") or 0.0)
    lsr = float(summ.get("net_long_short_ratio") or np.nan)
    pnl_total = float(summ.get("unrealized_pnl_total_usd") or 0.0)

    # Per-position lines (monospace for Telegram HTML <pre>).
    pos_lines: list[str] = []
    show = tbl.sort_values("notional_usd", key=lambda s: s.abs(), ascending=False)
    for _, r in show.iterrows():
        tkr = str(r["ticker"]).upper()
        side = str(r["side"]).upper()
        notional = float(r["notional_usd"])
        px = float(r["current_price"]) if r["current_price"] == r["current_price"] else np.nan
        beta = float(r["beta_30d"]) if r["beta_30d"] == r["beta_30d"] else np.nan
        exp = float(r["beta_weighted_exposure_usd"])
        pnl = float(r["unrealized_pnl_usd"]) if r["unrealized_pnl_usd"] == r["unrealized_pnl_usd"] else np.nan
        fr = per_funding.get(tkr)
        fr_s = _fmt_float(float(fr), ndp=6) if fr is not None else "n/a"
        pos_lines.append(
            f"{tkr:>10} {side:<5} {_fmt_money(notional):>12}  beta={_fmt_float(beta, ndp=2):>6}  exp={_fmt_money(exp):>12}  pnl={_fmt_money(pnl):>10}  fund8h={fr_s:>10}"
        )

    long_total = float(summ.get("long_total_notional_usd") or 0.0)
    short_total = float(summ.get("short_total_notional_usd") or 0.0)
    conc = float(summ.get("largest_position_concentration_pct") or 0.0)

    funding_line = (
        f"Net funding est (daily): {_fmt_money(funding_daily_total)}"
        if funding_daily_known
        else "Net funding est (daily): n/a (missing funding rates)"
    )

    pre = "\n".join(
        [
            "TICKER      SIDE     NOTIONAL_USD    BETA      BETA_EXP_USD      UPNL_USD      FUND8H",
            "-------------------------------------------------------------------------------------",
            *pos_lines,
        ]
    )
    pre_escaped = html.escape(pre)

    # Telegram HTML: keep everything readable; <pre> provides alignment.
    lines = [
        f"[danlongshort] Portfolio snapshot (UTC {_utc_now():%Y-%m-%d %H:%M})",
        "",
        "<b>Core metrics</b>",
        f"- Net portfolio beta exposure (vs BTC): {_fmt_money(net_beta)}",
        f"- Gross notional: {_fmt_money(gross)}",
        f"- Net notional (longs - shorts): {_fmt_money(net_notional)}",
        f"- Net long/short ratio: {_fmt_pct(lsr)}",
        "",
        "<b>Risk metrics</b>",
        f"- Largest position concentration: {_fmt_float(conc, ndp=1)}%",
        f"- Long total vs short total: {_fmt_money(long_total)} vs {_fmt_money(short_total)}",
        f"- Beta imbalance (BTC adj to neutral): {_fmt_money(float(summ.get('btc_adjustment_usd_to_neutral') or 0.0))}",
        f"- Unrealized PnL total: {_fmt_money(pnl_total)}",
        "",
        "<b>Funding</b>",
        f"- {funding_line}",
        "",
        "<b>Per-position</b>",
        f"<pre>{pre_escaped}</pre>",
        "",
        "<b>BTC rebalance (adjust BTC only)</b>",
        f"- Required BTC leg: {reb['required_btc_side']} {_fmt_money(float(reb['required_btc_notional_usd']))}",
    ]
    return "\n".join(lines)


def run_periodic_snapshot(cfg: DanLongShortAlertsConfig) -> None:
    state_path = cfg.snapshot_state_json
    state = _load_json(state_path, {"last_sent_ts_utc": ""})
    now = _utc_now()

    # de-dupe by interval; runner loop checks time, but state protects retries/restarts.
    last_raw = str(state.get("last_sent_ts_utc") or "").strip()
    if last_raw:
        try:
            last = datetime.fromisoformat(last_raw.replace("Z", "+00:00"))
            age_s = (now - last).total_seconds()
            if age_s < float(cfg.snapshot_interval_hours) * 3600.0:
                return
        except Exception:
            pass

    try:
        msg = build_portfolio_snapshot_text(cfg)
    except Exception as e:
        logger.warning("danlongshort snapshot build failed (non-fatal): %s", e, exc_info=True)
        return

    if not msg:
        state["last_sent_ts_utc"] = now.isoformat().replace("+00:00", "Z")
        _save_json(state_path, state)
        logger.info("danlongshort snapshot skip: no positions.")
        return

    ok = send_telegram_message(msg, parse_mode="HTML")
    if ok:
        _append_alert_log(
            cfg.alert_log_csv,
            {
                "timestamp_utc": now.isoformat().replace("+00:00", "Z"),
                "alert_type": "PORTFOLIO_SNAPSHOT",
                "dedup_bucket": f"interval_{cfg.snapshot_interval_hours}h",
                "message": msg.replace("\n", " / "),
            },
        )
        state["last_sent_ts_utc"] = now.isoformat().replace("+00:00", "Z")
        _save_json(state_path, state)
        logger.info("danlongshort snapshot sent. cfg=%s", {k: str(v) for k, v in asdict(cfg).items()})

