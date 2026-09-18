"""Nightly lake-integrity job: runs the checks as subprocesses, alerts on FAIL, once per UTC day."""

import sys
from datetime import date, timedelta

import pandas as pd

import src.exports.lake_integrity as li


def _tiny_lake(path, snapshot_last: date):
    path.mkdir()
    today = date.today()
    for f in li_tables():
        last = snapshot_last if f == "fact_markets_snapshot.parquet" else today
        pd.DataFrame({"date": [last - timedelta(days=1), last], "asset_id": ["BTC", "BTC"]}).to_parquet(path / f)


def li_tables():
    return ["fact_price.parquet", "fact_marketcap.parquet", "fact_volume.parquet", "silver_fact_price.parquet",
            "silver_fact_marketcap.parquet", "silver_fact_funding.parquet", "fact_markets_snapshot.parquet"]


def test_freshness_check_flags_stale_snapshot_via_subprocess(tmp_path, monkeypatch):
    lake = tmp_path / "lake"
    _tiny_lake(lake, snapshot_last=date.today() - timedelta(days=45))
    script = str(li.Path(li.__file__).resolve().parents[2] / "scripts" / "verify_ingestion_integrity.py")
    monkeypatch.setattr(li, "_checks", lambda root, wd: [
        ("freshness", [sys.executable, script, "--mode", "freshness", "--lake-dir", str(lake)], wd / "f.json")])
    fails, problems = li.run_checks(li.Path(script).parents[1], tmp_path)
    assert problems == []
    assert [f["description"] for f in fails] == ["fact_markets_snapshot <= 2d old"]
    assert "45d old" in fails[0]["detail"]


def test_run_alerts_once_per_day_and_marks_new(tmp_path, monkeypatch):
    monkeypatch.setattr(li, "DEDUP_MARKER", tmp_path / "marker")
    monkeypatch.setattr(li, "STATE_PATH", tmp_path / "state.json")
    sent = []
    monkeypatch.setattr(li, "send_telegram_text", lambda msg, **k: sent.append(msg) or True)
    fails = [{"mode": "freshness", "name": "F7", "description": "fact_markets_snapshot <= 2d old", "detail": "45d old"}]
    monkeypatch.setattr(li, "run_checks", lambda root, wd: (fails, []))
    li.STATE_PATH.write_text('["asset_identity:A1"]', encoding="utf-8")

    assert li.run(repo_root=tmp_path) is True
    assert len(sent) == 1 and "NEW freshness F7" in sent[0]
    assert li.run(repo_root=tmp_path) is False and len(sent) == 1     # already ran today

    li.DEDUP_MARKER.unlink()
    li.run(repo_root=tmp_path)
    assert "NEW" not in sent[1]                                        # still failing, no longer new


def test_run_is_silent_when_all_pass_and_never_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(li, "DEDUP_MARKER", tmp_path / "marker")
    monkeypatch.setattr(li, "STATE_PATH", tmp_path / "state.json")
    sent = []
    monkeypatch.setattr(li, "send_telegram_text", lambda msg, **k: sent.append(msg) or True)
    monkeypatch.setattr(li, "run_checks", lambda root, wd: ([], []))
    assert li.run(repo_root=tmp_path) is True and sent == []

    def boom(root, wd):
        raise RuntimeError("disk gone")
    li.DEDUP_MARKER.unlink()
    monkeypatch.setattr(li, "run_checks", boom)
    assert li.run(repo_root=tmp_path) is False
