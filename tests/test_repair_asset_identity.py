"""scripts/repair_asset_identity.py: the apply step must only write what the dry run validated.

A tiny lake with one spliced asset: ETH's March market cap/volume belong to a bridged variant
(price right, cap ~$2M), and a DOGE row holds another coin with no validated replacement.
"""

import argparse
from datetime import date

import pandas as pd
import pyarrow.parquet as pq

import scripts.repair_asset_identity as rep

D = pd.date_range("2026-03-02", "2026-04-01", freq="D")


def _lake(tmp_path):
    lake = tmp_path / "lake"
    lake.mkdir()
    rows = []
    for d in D:
        stale = rep.MASS_WINDOW[0] <= d <= rep.MASS_WINDOW[1]
        rows.append({"asset_id": "ETH", "date": d.date(), "close": 2000.0,
                     "marketcap": 2.1e6 if stale else 2.4e11, "volume": 1e5 if stale else 1e10})
        rows.append({"asset_id": "DOGE", "date": d.date(), "close": 0.0024 if stale else 0.09,
                     "marketcap": 2.3e6 if stale else 1.5e10, "volume": 1e4 if stale else 1e9})
    df = pd.DataFrame(rows)
    for t, c in rep.FACT_TABLES.items():
        df[["asset_id", "date", c]].assign(source="coingecko").to_parquet(lake / f"{t}.parquet", index=False)
    return lake


def _manifest(tmp_path, lake):
    frames = {t: rep._read_fact(lake, t) for t in rep.FACT_TABLES}
    grouped = rep._by_asset(frames)
    win = [d for d in D if rep.MASS_WINDOW[0] <= d <= rep.MASS_WINDOW[1]]
    man = tmp_path / "man"
    man.mkdir()
    segs = [
        {"seg_id": "ETH:mcap", "asset_uid": "ETH", "class": "mass_window_2026_03", "tables": "mcap_volume",
         "start": win[0].date(), "end": win[-1].date(), "apply_default": True,
         "before_hash": rep._segment_hash(grouped, "ETH", win)},
        {"seg_id": "DOGE:q", "asset_uid": "DOGE", "class": "mass_window_2026_03", "tables": "all",
         "start": win[0].date(), "end": win[0].date(), "apply_default": True,
         "before_hash": rep._segment_hash(grouped, "DOGE", win[:1])},
    ]
    repl = [{"seg_id": "ETH:mcap", "asset_id": "ETH", "date": d, "table": t, "value": v}
            for d in win for t, v in (("fact_marketcap", 2.4e11), ("fact_volume", 1e10))]
    quar = [{"seg_id": "DOGE:q", "asset_id": "DOGE", "date": win[0], "table": t, "reason": "wrong coin"}
            for t in rep.FACT_TABLES]
    pd.DataFrame(segs).to_csv(man / "manifest_segments.csv", index=False)
    pd.DataFrame(repl).to_parquet(man / "replacement_rows.parquet", index=False)
    pd.DataFrame(quar).to_parquet(man / "quarantine_keys.parquet", index=False)
    return man


def _args(lake, man, **kw):
    return argparse.Namespace(lake_dir=str(lake), manifest_dir=str(man), classes=None,
                              skip_changed=kw.get("skip_changed", False), yes=kw.get("yes", True))


def test_mass_window_signature():
    rows = pd.DataFrame({"date": D, "close": 2000.0,
                         "marketcap": [2.1e6 if rep.MASS_WINDOW[0] <= d <= rep.MASS_WINDOW[1] else 2.4e11 for d in D]})
    assert rep._mass_window_signature(rows.set_index("date")) == (False, True)


def test_apply_replaces_quarantines_and_backs_up(tmp_path):
    lake = _lake(tmp_path)
    man = _manifest(tmp_path, lake)
    assert rep.cmd_apply(_args(lake, man)) == 0

    mc = rep._read_fact(lake, "fact_marketcap").set_index(["asset_id", "date"])["marketcap"]
    assert (mc.loc["ETH"].loc["2026-03-04":"2026-03-30"] == 2.4e11).all()
    px = rep._read_fact(lake, "fact_price")
    assert (px[px.asset_id == "ETH"]["close"] == 2000.0).all()                       # price untouched (mcap_volume)
    assert not ((px.asset_id == "DOGE") & (px.date == "2026-03-04")).any()          # quarantined
    q = pd.read_parquet(lake / "quarantine_fact_identity.parquet")
    assert set(q["table"]) == set(rep.FACT_TABLES) and (q["asset_id"] == "DOGE").all()
    assert len(list(lake.glob("_backup_asset_identity_*/fact_price.parquet"))) == 1
    assert pq.read_schema(lake / "fact_price.parquet").field("date").type == "date32[day]"


def test_apply_refuses_when_lake_changed_since_dry_run(tmp_path):
    lake = _lake(tmp_path)
    man = _manifest(tmp_path, lake)
    px = pd.read_parquet(lake / "fact_price.parquet")
    px.loc[(px.asset_id == "DOGE") & (px.date == date(2026, 3, 4)), "close"] = 0.0901   # someone fixed it meanwhile
    px.to_parquet(lake / "fact_price.parquet", index=False)
    before = (lake / "fact_marketcap.parquet").read_bytes()
    assert rep.cmd_apply(_args(lake, man)) == 1
    assert (lake / "fact_marketcap.parquet").read_bytes() == before and not list(lake.glob("_backup_*"))
    # --skip-changed applies the unchanged segment only
    assert rep.cmd_apply(_args(lake, man, skip_changed=True)) == 0
    assert (rep._read_fact(lake, "fact_marketcap").query("asset_id == 'ETH'")["marketcap"] == 2.4e11).all()
    assert not (lake / "quarantine_fact_identity.parquet").exists()


def test_apply_without_yes_writes_nothing(tmp_path):
    lake = _lake(tmp_path)
    man = _manifest(tmp_path, lake)
    before = (lake / "fact_price.parquet").read_bytes()
    assert rep.cmd_apply(_args(lake, man, yes=False)) == 0
    assert (lake / "fact_price.parquet").read_bytes() == before
