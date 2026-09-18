"""Regression test: incremental_update must not re-inject stale wide-file values into fact tables.

Asset-identity incident (2026-09-18): the live fact_marketcap for 2026-03-04..30 equalled the
git-committed data/curated/marketcap_daily.parquet (599e5cb, writer-race era: ETH/SOL/DOGE caps
~$2M) in 100% of cells. incremental_update merged each download into the existing wide files with
combine_first (existing wins) and converted the *merged* rows to fact rows, so a run whose start
date fell inside the committed wide range upserted the stale values. Fact rows must come only from
what the run actually downloaded.
"""

import sys

import pandas as pd

import scripts.incremental_update as iu

DATES = pd.date_range("2026-03-01", "2026-03-06", freq="D", name="date")


def _wide(values: dict, dates) -> pd.DataFrame:
    return pd.DataFrame(values, index=dates)


def test_backfill_does_not_upsert_stale_wide_values(tmp_path, monkeypatch):
    curated, lake = tmp_path / "curated", tmp_path / "lake"
    curated.mkdir(); lake.mkdir()

    # Existing wide files (as re-seeded from git on Render): wrong-coin ETH cap, and a DOGE row.
    _wide({"ETH": [2e6] * 6, "DOGE": [2.3e6] * 6}, DATES).to_parquet(curated / "marketcap_daily.parquet")
    _wide({"ETH": [2000.0] * 6, "DOGE": [0.0024] * 6}, DATES).to_parquet(curated / "prices_daily.parquet")
    _wide({"ETH": [1e9] * 6, "DOGE": [1e6] * 6}, DATES).to_parquet(curated / "volume_daily.parquet")

    # Existing fact tables end 2026-03-03.
    base = [{"asset_id": "ETH", "date": d.date(), "source": "coingecko"} for d in DATES[:3]]
    pd.DataFrame([{**r, "marketcap": 250e9} for r in base]).to_parquet(lake / "fact_marketcap.parquet", index=False)
    pd.DataFrame([{**r, "close": 2000.0} for r in base]).to_parquet(lake / "fact_price.parquet", index=False)
    pd.DataFrame([{**r, "volume": 1e9} for r in base]).to_parquet(lake / "fact_volume.parquet", index=False)

    # This run downloads 03-04..03-06: correct ETH; DOGE's fetch fails (absent from the download).
    new_dates = DATES[3:]

    def fake_download(allowlist_path, start_date, end_date, output_dir):
        _wide({"ETH": [250e9] * 3}, new_dates).to_parquet(output_dir / "marketcap_daily.parquet")
        _wide({"ETH": [2000.0] * 3}, new_dates).to_parquet(output_dir / "prices_daily.parquet")
        _wide({"ETH": [1e9] * 3}, new_dates).to_parquet(output_dir / "volume_daily.parquet")

    monkeypatch.setattr(iu, "download_all_coins", fake_download)
    monkeypatch.setattr(sys, "argv", ["incremental_update.py", "--start-date", "2026-03-04", "--end-date", "2026-03-06",
                                      "--curated-dir", str(curated), "--data-lake-dir", str(lake)])
    iu.main()

    mc = pd.read_parquet(lake / "fact_marketcap.parquet")
    mc["date"] = pd.to_datetime(mc["date"])
    eth = mc[mc.asset_id == "ETH"].set_index("date")["marketcap"]
    assert (eth.loc["2026-03-04":"2026-03-06"] == 250e9).all(), "stale wide value won over the fresh download"
    # A coin whose fetch failed gets no fact row, not the stale wide value.
    assert mc[mc.asset_id == "DOGE"].empty
