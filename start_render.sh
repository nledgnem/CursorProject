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
  else
    # If both exist, do an append-only merge keyed by trade_id:
    # - Never delete or modify existing /data rows
    # - Only append rows present in the repo snapshot but missing in /data
    if [[ -f "data/curated/data_lake/apathy_bleed_book.csv" ]]; then
      python - <<'PY'
from __future__ import annotations

import csv
from pathlib import Path

repo_path = Path("data/curated/data_lake/apathy_bleed_book.csv")
data_path = Path("/data/apathy_bleed_book.csv")

if not repo_path.exists() or not data_path.exists():
    raise SystemExit(0)

with data_path.open(newline="", encoding="utf-8") as f:
    data_rows = list(csv.DictReader(f))
data_ids = {str(r.get("trade_id", "")).strip() for r in data_rows if str(r.get("trade_id", "")).strip()}

with repo_path.open(newline="", encoding="utf-8") as f:
    repo_reader = csv.DictReader(f)
    repo_fieldnames = list(repo_reader.fieldnames or [])
    repo_rows = list(repo_reader)

to_append = []
for r in repo_rows:
    tid = str(r.get("trade_id", "")).strip()
    if not tid or tid in data_ids:
        continue
    to_append.append(r)

if not to_append:
    raise SystemExit(0)

tmp = data_path.with_suffix(data_path.suffix + ".tmp")
with data_path.open(newline="", encoding="utf-8") as f:
    data_reader = csv.DictReader(f)
    fieldnames = list(data_reader.fieldnames or repo_fieldnames)
    existing_rows = list(data_reader)

with tmp.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    for r in existing_rows:
        w.writerow(r)
    for r in to_append:
        w.writerow(r)

tmp.replace(data_path)
print(f"[ALERT RUNNER] merged {len(to_append)} new trade rows into /data/apathy_bleed_book.csv (append-only).")
PY
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

# Background watchdog: respawn danlongshort alert runner on crash.
(while true; do
  python scripts/danlongshort_alert_runner.py
  code=$?
  echo "[DANLONGSHORT RUNNER] exited with code ${code}, restarting in 10s..."
  sleep 10
done) &
DANLONGSHORT_PID=$!

cleanup() {
  kill "${ALERT_PID}" 2>/dev/null || true
  kill "${DANLONGSHORT_PID}" 2>/dev/null || true
}
trap cleanup EXIT SIGTERM SIGINT

# Foreground process: Render monitors this for health.
python system_heartbeat.py
