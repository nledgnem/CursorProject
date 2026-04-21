from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.danlongshort.alert_ops import build_portfolio_snapshot_text
from src.danlongshort.config_loader import load_danlongshort_alerts_config
from src.danlongshort.portfolio import (
    Position,
    btc_rebalance_target_notional,
    compute_30d_betas,
    compute_portfolio_snapshot,
    fetch_30d_closes_usd,
    latest_prices_from_closes,
    load_positions_csv,
    load_symbol_to_coingecko_id,
)
from src.danlongshort.storage import append_position_row, ensure_positions_csv, remove_positions_by_ticker
from src.notifications.telegram_client import send_telegram_message

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BotEnv:
    bot_token: str
    chat_id: str
    state_json: Path


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fingerprint_footer() -> str:
    ts = _utc_now().isoformat().replace("+00:00", "Z")
    return f"\n\n<code>fp: danlongshort_bot|{ts}</code>"


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


def _telegram_get_updates(env: BotEnv, *, offset: int | None, timeout_s: int = 30) -> list[dict[str, Any]]:
    url = f"https://api.telegram.org/bot{env.bot_token}/getUpdates"
    payload: dict[str, Any] = {"timeout": int(timeout_s), "allowed_updates": ["message"]}
    if offset is not None:
        payload["offset"] = int(offset)
    resp = requests.post(url, json=payload, timeout=timeout_s + 5)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict) or not data.get("ok"):
        return []
    out = data.get("result") or []
    return out if isinstance(out, list) else []


def _parse_cmd(text: str) -> tuple[str, list[str]]:
    raw = (text or "").strip()
    if not raw.startswith("/"):
        return "", []
    parts = raw.split()
    cmd = parts[0].split("@")[0].lower()
    args = parts[1:]
    return cmd, args


def _side_norm(s: str) -> str:
    x = (s or "").strip().lower()
    if x in {"long", "l"}:
        return "LONG"
    if x in {"short", "s"}:
        return "SHORT"
    raise ValueError("side must be long/short")


def _fmt_money(x: float) -> str:
    return f"${x:,.0f}"


def _beta_check_reply(
    *,
    ticker: str,
    side: str,
    notional_usd: float,
    symbol_to_cg: dict[str, str],
) -> str:
    t = str(ticker).strip().upper()
    direction = 1.0 if side == "LONG" else -1.0
    closes = fetch_30d_closes_usd([t, "BTC"], symbol_to_cg=symbol_to_cg, enable_cache=True, cache_max_age_hours=12.0)
    betas = compute_30d_betas(closes, btc_ticker="BTC")
    beta = float(betas.get(t, float("nan")))
    exposure = notional_usd * direction * (beta if beta == beta else 0.0)
    btc_leg = -exposure  # BTC beta=1
    btc_side = "LONG" if btc_leg >= 0 else "SHORT"

    return (
        f"[danlongshort] Beta check: {t}\n"
        f"30d beta vs BTC: {beta:.2f}\n"
        f"To neutralize {_fmt_money(notional_usd)} {side} {t} \u2192 {btc_side} {_fmt_money(abs(btc_leg))} BTC"
        + _fingerprint_footer()
    )


def _portfolio_state_reply(positions_path: Path, allowlist: Path, symbol_map: Path) -> tuple[str, float]:
    positions = load_positions_csv(positions_path)
    symbol_to_cg = load_symbol_to_coingecko_id(allowlist, override_yaml=symbol_map)
    tickers = [p.ticker for p in positions]
    closes = fetch_30d_closes_usd(tickers, symbol_to_cg=symbol_to_cg, enable_cache=True, cache_max_age_hours=12.0)
    betas = compute_30d_betas(closes, btc_ticker="BTC")
    latest = latest_prices_from_closes(closes)
    _tbl, summ = compute_portfolio_snapshot(positions, betas, latest)
    net_beta = float(summ.get("net_beta_exposure_usd") or 0.0)
    adj = float(summ.get("btc_adjustment_usd_to_neutral") or 0.0)
    side = "LONG" if adj >= 0 else "SHORT"
    msg = (
        f"[danlongshort] Portfolio\n"
        f"Net portfolio beta exposure (vs BTC): {_fmt_money(net_beta)}\n"
        f"BTC adjustment needed: {side} {_fmt_money(abs(adj))}"
        + _fingerprint_footer()
    )
    return msg, net_beta


def _rebalance_reply(positions_path: Path, allowlist: Path, symbol_map: Path) -> str:
    positions = load_positions_csv(positions_path)
    if not positions:
        return "[danlongshort] Rebalance\nNo positions." + _fingerprint_footer()
    symbol_to_cg = load_symbol_to_coingecko_id(allowlist, override_yaml=symbol_map)
    tickers = [p.ticker for p in positions]
    closes = fetch_30d_closes_usd(tickers, symbol_to_cg=symbol_to_cg, enable_cache=True, cache_max_age_hours=12.0)
    betas = compute_30d_betas(closes, btc_ticker="BTC")
    reb = btc_rebalance_target_notional(positions, betas)
    return (
        "[danlongshort] Rebalance (adjust BTC only)\n"
        f"Required BTC leg: {reb['required_btc_side']} {_fmt_money(float(reb['required_btc_notional_usd']))}"
        + _fingerprint_footer()
    )


def _handle_message(env: BotEnv, msg: dict[str, Any]) -> int | None:
    update_id = msg.get("update_id")
    message = msg.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = str(chat.get("id") or "").strip()
    if chat_id != env.chat_id:
        return update_id if isinstance(update_id, int) else None

    text = str(message.get("text") or "")
    cmd, args = _parse_cmd(text)
    if not cmd:
        return update_id if isinstance(update_id, int) else None

    cfg = load_danlongshort_alerts_config(REPO_ROOT)
    ensure_positions_csv(cfg.positions_csv)
    symbol_to_cg = load_symbol_to_coingecko_id(cfg.allowlist_csv, override_yaml=cfg.symbol_map_yaml)

    try:
        if cmd == "/beta":
            if len(args) != 3:
                out = "[danlongshort] Usage: /beta <TICKER> <SIDE> <NOTIONAL>" + _fingerprint_footer()
            else:
                tkr = args[0].upper()
                side = _side_norm(args[1])
                notional = float(args[2])
                out = _beta_check_reply(ticker=tkr, side=side, notional_usd=notional, symbol_to_cg=symbol_to_cg)
            send_telegram_message(out, parse_mode="HTML")

        elif cmd == "/add":
            if len(args) < 3:
                out = "[danlongshort] Usage: /add <TICKER> <SIDE> <NOTIONAL> [ENTRY_PRICE]" + _fingerprint_footer()
            else:
                tkr = args[0].upper()
                side = _side_norm(args[1])
                notional = float(args[2])
                entry_price = float(args[3]) if len(args) >= 4 and re.match(r"^-?\d+(\.\d+)?$", args[3]) else None
                append_position_row(cfg.positions_csv, ticker=tkr, side=side, notional_usd=notional, entry_price=entry_price)
                state_msg, _ = _portfolio_state_reply(cfg.positions_csv, cfg.allowlist_csv, cfg.symbol_map_yaml)
                ep = f" @ {entry_price:.2f}" if entry_price is not None else ""
                out = f"[danlongshort] Added: {tkr} {side} {_fmt_money(notional)}{ep}\n\n" + state_msg.split("\n\n<code>")[0] + _fingerprint_footer()
            send_telegram_message(out, parse_mode="HTML")

        elif cmd == "/remove":
            if len(args) != 1:
                out = "[danlongshort] Usage: /remove <TICKER>" + _fingerprint_footer()
            else:
                tkr = args[0].upper()
                n = remove_positions_by_ticker(cfg.positions_csv, tkr)
                state_msg, _ = _portfolio_state_reply(cfg.positions_csv, cfg.allowlist_csv, cfg.symbol_map_yaml)
                out = f"[danlongshort] Removed: {tkr} (rows={n})\n\n" + state_msg.split("\n\n<code>")[0] + _fingerprint_footer()
            send_telegram_message(out, parse_mode="HTML")

        elif cmd == "/snapshot":
            snap = build_portfolio_snapshot_text(cfg)
            if not snap:
                snap = "[danlongshort] Portfolio snapshot\nNo positions." + _fingerprint_footer()
            else:
                snap = snap + _fingerprint_footer()
            send_telegram_message(snap, parse_mode="HTML")

        elif cmd == "/rebalance":
            out = _rebalance_reply(cfg.positions_csv, cfg.allowlist_csv, cfg.symbol_map_yaml)
            send_telegram_message(out, parse_mode="HTML")

        else:
            out = "[danlongshort] Unknown command." + _fingerprint_footer()
            send_telegram_message(out, parse_mode="HTML")

    except Exception as e:
        logger.warning("Command failed (non-fatal): %s", e, exc_info=True)
        send_telegram_message("[danlongshort] Error processing command." + _fingerprint_footer(), parse_mode="HTML")

    return update_id if isinstance(update_id, int) else None


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ | %(levelname)s | %(message)s")

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")

    state_json = Path(os.getenv("DANLONGSHORT_BOT_STATE_JSON", "/data/curated/data_lake/danlongshort_bot_state.json"))
    env = BotEnv(bot_token=bot_token, chat_id=chat_id, state_json=state_json)

    state = _load_json(env.state_json, {"offset": None})
    offset = state.get("offset")
    offset_i = int(offset) if isinstance(offset, int) or (isinstance(offset, str) and str(offset).isdigit()) else None

    logger.info("danlongshort bot started. chat_id=%s state=%s", env.chat_id, env.state_json)

    while True:
        try:
            updates = _telegram_get_updates(env, offset=offset_i, timeout_s=30)
            max_update_id: int | None = None
            for u in updates:
                uid = _handle_message(env, u)
                if uid is not None:
                    max_update_id = uid if max_update_id is None else max(max_update_id, uid)
            if max_update_id is not None:
                offset_i = int(max_update_id) + 1
                _save_json(env.state_json, {"offset": offset_i, "updated_at_utc": _utc_now().isoformat().replace("+00:00", "Z")})
        except Exception as e:
            logger.warning("Bot poll failed (non-fatal): %s", e, exc_info=True)
            time.sleep(5.0)


if __name__ == "__main__":
    main()

