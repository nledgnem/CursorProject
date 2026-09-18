"""The allowlist refresh is frozen (asset-identity incident 2026-09-18).

data_dictionary.yaml -> data_sources.coingecko.ingestion_universe.refresh_frozen is the single
switch: expand_allowlist.py refuses to write and the heartbeat stops nagging to run it.
"""

import pytest
import yaml

import scripts.archive.expand_allowlist as ea
import system_heartbeat as hb


def test_repo_dictionary_has_refresh_frozen():
    cfg = yaml.safe_load(open(hb.REPO_ROOT / "data_dictionary.yaml", encoding="utf-8"))
    iu = cfg["data_sources"]["coingecko"]["ingestion_universe"]
    assert iu["refresh_frozen"] is True and iu["next_refresh_due"] is None


def test_expand_allowlist_refuses_while_frozen(tmp_path, monkeypatch):
    monkeypatch.delenv("ALLOWLIST_REFRESH_UNFROZEN", raising=False)
    monkeypatch.setattr(ea, "fetch_top_coins", lambda **k: pytest.fail("must not fetch while frozen"))
    with pytest.raises(SystemExit, match="FROZEN"):
        ea.expand_allowlist(tmp_path / "perp_allowlist.csv")
    assert not (tmp_path / "perp_allowlist.csv").exists()


def test_override_env_unfreezes(monkeypatch):
    monkeypatch.setenv("ALLOWLIST_REFRESH_UNFROZEN", "1")
    assert ea.refresh_frozen_reason() is None


def _run_reminder(tmp_path, monkeypatch, frozen: bool) -> list[str]:
    iu = {"last_refresh": "2026-05-05", "next_refresh_due": "2026-08-05", "refresh_frozen": frozen}
    (tmp_path / "data_dictionary.yaml").write_text(
        yaml.safe_dump({"data_sources": {"coingecko": {"ingestion_universe": iu}}}), encoding="utf-8")
    monkeypatch.setattr(hb, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(hb, "ALLOWLIST_REMINDER_MARKER", tmp_path / ".marker")
    sent = []
    import src.notifications.telegram_client as tc
    monkeypatch.setattr(tc, "send_telegram_text", lambda msg, **k: sent.append(msg) or True)
    hb._check_and_alert_allowlist_refresh()
    return sent


def test_heartbeat_reminder_silent_when_frozen(tmp_path, monkeypatch):
    assert _run_reminder(tmp_path, monkeypatch, frozen=True) == []


def test_heartbeat_reminder_still_fires_when_not_frozen(tmp_path, monkeypatch):
    sent = _run_reminder(tmp_path, monkeypatch, frozen=False)
    assert len(sent) == 1 and "Allowlist refresh due" in sent[0]
