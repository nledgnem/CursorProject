from __future__ import annotations

import logging
import os
import requests

logger = logging.getLogger(__name__)


def send_telegram_text(text: str, *, timeout_seconds: float = 5.0) -> bool:
    """
    Send a plain-text Telegram message. Non-fatal: logs and returns False on failure.

    Returns True if the message was accepted by Telegram (HTTP 2xx).
    """
    return send_telegram_message(text, timeout_seconds=timeout_seconds, parse_mode=None)


def send_telegram_message(
    text: str,
    *,
    timeout_seconds: float = 5.0,
    parse_mode: str | None = None,
) -> bool:
    """
    Send a Telegram message. Non-fatal: logs and returns False on failure.

    parse_mode: None | "HTML" | "MarkdownV2"
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token or not chat_id:
        logger.warning("Missing Telegram credentials (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID). Skipping send.")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        resp = requests.post(url, json=payload, timeout=timeout_seconds)
        resp.raise_for_status()
        logger.info("Telegram message dispatched successfully.")
        return True
    except Exception as e:
        logger.error("Telegram delivery failed (non-fatal): %s", e)
        return False


def send_telegram_alert(
    old_regime: str,
    new_regime: str,
    apr: float,
    spread: float,
    gate_on: bool,
    risk_weight: float,
) -> None:
    gate_line = "⚡ GATE: ON" if gate_on else "🔒 GATE: OFF"
    risk_weight_line = f"Risk Weight: {risk_weight * 100:.0f}%"
    text_payload = (
        "MACRO REGIME CHANGE DETECTED\n\n"
        f"{gate_line}\n"
        f"{risk_weight_line}\n\n"
        f"Shift: {old_regime} -> {new_regime}\n"
        f"Environment APR: {apr:.2f}%\n"
        f"Fragmentation Spread: {spread:.6f}\n\n"
        "Check the Streamlit dashboard for full details."
    )
    send_telegram_text(text_payload)


def send_telegram_daily_status(
    regime: str,
    decision_date: str,
    apr: float,
    spread: float,
    gate_on: bool,
    risk_weight: float,
) -> None:
    from datetime import datetime, timezone

    today_utc = datetime.now(timezone.utc).date().isoformat()
    gate_line = "⚡ GATE: ON" if gate_on else "🔒 GATE: OFF"
    risk_weight_line = f"Risk Weight: {risk_weight * 100:.0f}%"
    text_payload = (
        "DAILY MACRO REGIME STATUS\n\n"
        f"UTC Day: {today_utc}\n"
        f"Latest decision_date: {decision_date}\n"
        f"{gate_line}\n"
        f"{risk_weight_line}\n"
        f"Regime: {regime}\n"
        f"Environment APR: {apr:.2f}%\n"
        f"Fragmentation Spread: {spread:.6f}\n\n"
        "Check the Streamlit dashboard for full details."
    )
    send_telegram_text(text_payload)
