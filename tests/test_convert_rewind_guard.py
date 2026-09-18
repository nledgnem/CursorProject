"""convert_to_fact_tables.py must not rewind a lake that extends past its wide input.

A full rebuild from the repo's frozen wide files (599e5cb, ending 2026-03-30) truncated the local
lake on 2026-08-17 and would re-inject writer-race-era rows into a live lake.
"""

import subprocess
import sys
from pathlib import Path

import pandas as pd

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "convert_to_fact_tables.py"


def test_full_rebuild_refuses_to_rewind_newer_lake(tmp_path):
    curated, lake = tmp_path / "curated", tmp_path / "lake"
    curated.mkdir(); lake.mkdir()
    idx = pd.date_range("2026-03-01", "2026-03-30", freq="D", name="date")
    pd.DataFrame({"BTC": 90_000.0}, index=idx).to_parquet(curated / "prices_daily.parquet")
    live = pd.DataFrame({"asset_id": "BTC", "date": pd.date_range("2026-03-01", "2026-09-18").date,
                         "close": 90_000.0, "source": "coingecko"})
    live.to_parquet(lake / "fact_price.parquet", index=False)
    before = (lake / "fact_price.parquet").read_bytes()

    r = subprocess.run([sys.executable, str(SCRIPT), "--curated-dir", str(curated), "--data-lake-dir", str(lake)],
                       capture_output=True, text=True, cwd=SCRIPT.parents[1])
    assert r.returncode != 0 and "REFUSED" in (r.stdout + r.stderr)
    assert (lake / "fact_price.parquet").read_bytes() == before
