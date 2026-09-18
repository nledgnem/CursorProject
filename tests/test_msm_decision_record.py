"""As-decided MSM records are never rewritten (decision 2026-09-18)."""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import msm_decision_record as rec  # noqa: E402


def _ts(path, rows):
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_log_today_appends_once_and_never_rewrites(tmp_path):
    log = tmp_path / "msm_decision_log.csv"
    ts = _ts(tmp_path / "a.csv", [{"decision_date": "2026-09-14", "funding_regime": "Q3", "is_mrf_active": False},
                                  {"decision_date": "2026-09-18", "funding_regime": "Q3", "is_mrf_active": False}])
    assert rec.log_today(ts, log) is True
    # tomorrow's nightly recompute rewrites history in msm_timeseries -- the log must not follow it
    ts2 = _ts(tmp_path / "b.csv", [{"decision_date": "2026-09-18", "funding_regime": "Q1", "is_mrf_active": True}])
    assert rec.log_today(ts2, log) is False
    ts3 = _ts(tmp_path / "c.csv", [{"decision_date": "2026-09-18", "funding_regime": "Q1", "is_mrf_active": True},
                                   {"decision_date": "2026-09-21", "funding_regime": "Q2", "is_mrf_active": True,
                                    "new_col": 1}])
    assert rec.log_today(ts3, log) is True
    out = pd.read_csv(log)
    assert out["decision_date"].tolist() == ["2026-09-18", "2026-09-21"]
    assert out.set_index("decision_date").loc["2026-09-18", "funding_regime"] == "Q3"   # as decided, not recomputed


def test_freeze_refuses_to_overwrite(tmp_path):
    ts = _ts(tmp_path / "t.csv", [{"decision_date": "2026-03-30", "funding_regime": "Q1"}])
    args = argparse.Namespace(timeseries=str(ts), out=str(tmp_path / "frozen.csv"))
    assert rec.cmd_freeze(args) == 0 and (tmp_path / "frozen.csv.sha256").exists()
    _ts(ts, [{"decision_date": "2026-03-30", "funding_regime": "Q2"}])
    assert rec.cmd_freeze(args) == 1
    assert pd.read_csv(tmp_path / "frozen.csv")["funding_regime"].iloc[0] == "Q1"


def test_compare_keeps_all_three_versions(tmp_path):
    ad = _ts(tmp_path / "ad.csv", [{"decision_date": "2026-03-30", "run_id": "20260330_023757", "commit": "599e5cb",
                                    "funding_regime": "Q2: Weak", "is_mrf_active": False}])
    pre = _ts(tmp_path / "pre.csv", [{"decision_date": "2026-03-30", "funding_regime": "Q1: Negative/Low",
                                      "is_mrf_active": False},
                                     {"decision_date": "2026-04-06", "funding_regime": "Q3", "is_mrf_active": False}])
    post = _ts(tmp_path / "post.csv", [{"decision_date": "2026-03-30", "funding_regime": "Q2: Weak", "is_mrf_active": True},
                                       {"decision_date": "2026-04-06", "funding_regime": "Q3", "is_mrf_active": False}])
    out = tmp_path / "cmp.csv"
    rec.cmd_compare(argparse.Namespace(as_decided=str(ad), pre_repair=str(pre), recomputed=str(post), out=str(out)))
    c = pd.read_csv(out).set_index(["decision_date", "field"])
    r = c.loc[("2026-03-30", "is_mrf_active")]
    assert (str(r["AS_DECIDED"]), str(r["PRE_REPAIR_RECOMPUTE"]), str(r["RECOMPUTED_ON_CORRECTED_DATA"])) == \
        ("False", "False", "True")
    assert r["as_decided_source"] == "20260330_023757@599e5cb"
    assert c.loc[("2026-04-06", "funding_regime"), "as_decided_source"].startswith("not recorded")
