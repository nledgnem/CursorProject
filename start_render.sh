#!/usr/bin/env bash
# Render.com (or similar): keep heartbeat in foreground, run alert runner alongside it.
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}$(pwd)"
export PYTHONUNBUFFERED=1

# Seed the Apathy book onto the persistent disk on first boot.
# Never overwrite /data once it exists (book is source of truth).
if [[ -d "/data" ]]; then
  if [[ ! -f "/data/apathy_bleed_book.csv" ]]; then
    if [[ -f "data/curated/data_lake/apathy_bleed_book.csv" ]]; then
      cp "data/curated/data_lake/apathy_bleed_book.csv" "/data/apathy_bleed_book.csv"
      echo "[ALERT RUNNER] seeded /data/apathy_bleed_book.csv from repo snapshot."
    else
      echo "trade_id,cohort,ticker,side,entry_date_utc,entry_price_usd,notional_usd,quantity,stop_price_usd,exit_date_target_utc,status,exit_date_utc,exit_price_usd,pnl_usd,pnl_pct,notes" \
        > "/data/apathy_bleed_book.csv"
      echo "[ALERT RUNNER] created empty /data/apathy_bleed_book.csv header."
    fi
  fi
fi

# Background watchdog: respawn alert runner on crash.
(while true; do
  python scripts/apathy_alert_runner.py
  code=$?
  echo "[ALERT RUNNER] exited with code ${code}, restarting in 10s..."
  sleep 10
done) &
ALERT_PID=$!

cleanup() {
  kill "${ALERT_PID}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Foreground process: Render monitors this for health.
python system_heartbeat.py
