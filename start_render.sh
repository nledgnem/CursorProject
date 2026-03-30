#!/usr/bin/env bash
# Render.com (or similar): one process — heartbeat keeps Streamlit + scheduled pipeline.
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}$(pwd)"
exec python system_heartbeat.py
