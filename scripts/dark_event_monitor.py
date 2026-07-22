#!/usr/bin/env python3
r"""
Variational DARK-EVENT MONITOR (stripped manual-trial build).

Per Mads's re-scope this does ONE thing: flag events whose break timestamp falls
inside the traded name's dark window. NO scoring, NO deep-dives, NO PDF. Significance
is a HUMAN call (only "screamers" ~>4% expected belong in the events file); this tool
only answers "was the name dark when it broke?".

Input: a curated events JSON (list). Each event is either
  own-name : {"ticker": "MU",  "event": "...", "source": "...", "broke_at_utc": "..."}
  peer     : {"peer":  "Hynix","event": "...", "source": "...", "broke_at_utc": "..."}
Peer events map to US names via configs/rwa_peer_map_candidate.csv (a peer surprise
counts as news for each mapped US name). Optional "expected_move" free-text is appended
to the event line but never used to filter.

TIMESTAMP HYGIENE IS LOAD-BEARING: any event without a clean, parseable UTC
`broke_at_utc` is DISCARDED (first-seen-on-web != broke-at). Flags are CLUES for manual
verification, not signals.

Cost-floor exclusion (INV-COST): names verdict'd FAIL_TOO_WIDE in
outputs/rwa_cost_floor.csv are dropped (currently HIMS).

Output (flat): outputs/dark_event_flags.csv + .md
  ticker | event | source | broke_at_utc | dark_at_break (Y/N)
plus outputs/dark_event_discarded.csv (audit of dropped events).

Run: venv\Scripts\python.exe scripts\dark_event_monitor.py --events-file inputs\dark_events_<date>.json
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
from src.rwa_offhours.dark_calendar import is_dark, load_coverage_from_config  # noqa: E402

UNIVERSE_FILE = REPO_ROOT / "configs" / "variational_equity_universe.csv"
PEER_MAP_FILE = REPO_ROOT / "configs" / "rwa_peer_map_candidate.csv"
COST_FLOOR_CSV = REPO_ROOT / "outputs" / "rwa_cost_floor.csv"
OUT_DIR = REPO_ROOT / "outputs"


def load_eligible() -> set[str]:
    names = set()
    with UNIVERSE_FILE.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if (r.get("instrument_class") or "").strip().lower() == "equity" and \
               str(r.get("cash_open_eligible", "")).strip().lower() in ("yes", "true", "1"):
                names.add(r["ticker"].strip().upper())
    return names


def load_cost_excluded() -> set[str]:
    excl = set()
    if COST_FLOOR_CSV.exists():
        with COST_FLOOR_CSV.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if r.get("verdict") == "FAIL_TOO_WIDE":
                    excl.add(r["ticker"].strip().upper())
    return excl


def load_peer_map() -> list[dict]:
    rows = []
    if PEER_MAP_FILE.exists():
        with PEER_MAP_FILE.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
    return rows


def map_peer_to_names(peer: str, peer_map: list[dict]) -> list[tuple[str, str]]:
    """Return [(us_name, mechanism_short)] for a peer string (case-insensitive match)."""
    p = (peer or "").strip().lower()
    out = []
    for row in peer_map:
        watch = (row.get("watch_peer") or "").lower()
        # match if the event's peer token appears in the map's watch_peer or vice-versa
        if p and (p in watch or any(tok in watch for tok in p.split() if len(tok) > 2)):
            mech = (row.get("mechanism") or "").split(";")[0][:60]
            out.append((row.get("traded_us_name", "").strip().upper(), mech))
    # de-dup by us_name
    seen, uniq = set(), []
    for name, mech in out:
        if name and name not in seen:
            seen.add(name)
            uniq.append((name, mech))
    return uniq


def parse_utc(s) -> dt.datetime | None:
    """Strict-ish UTC parse. Accepts ISO8601 with Z or +00:00. Returns None if unclean."""
    if not s or not isinstance(s, str):
        return None
    txt = s.strip()
    try:
        d = dt.datetime.fromisoformat(txt.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        return None  # a bare naive timestamp is NOT clean enough -> discard
    return d.astimezone(dt.timezone.utc)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--events-file", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, default=OUT_DIR)
    ap.add_argument("--only-dark", action="store_true", help="Emit only dark_at_break=Y rows.")
    ap.add_argument("--scan-note", type=str, default=None,
                     help="Free-text note on what was checked this run (esp. useful when "
                          "0 events qualify, so the reader isn't left with a blank file).")
    args = ap.parse_args(argv)

    eligible = load_eligible()
    excluded = load_cost_excluded()
    peer_map = load_peer_map()
    load_coverage_from_config(UNIVERSE_FILE)  # CME SSF / Eurex coverage -> dark calendar
    events = json.loads(Path(args.events_file).read_text(encoding="utf-8"))

    flags, discarded = [], []

    def expand(ev) -> list[tuple[str, str]]:
        """-> [(us_name, event_line)]. Own-name or peer-mapped."""
        line = (ev.get("event") or "").strip().replace("\n", " ")
        if ev.get("expected_move"):
            line = f"{line} [exp {ev['expected_move']}]"
        if ev.get("ticker"):
            return [(ev["ticker"].strip().upper(), line)]
        if ev.get("peer"):
            mapped = map_peer_to_names(ev["peer"], peer_map)
            return [(name, f"[peer {ev['peer']} -> {name}] {line}"
                     + (f" (mech: {mech})" if mech else ""))
                    for name, mech in mapped]
        return []

    for ev in events:
        ts = parse_utc(ev.get("broke_at_utc"))
        if ts is None:
            discarded.append({"reason": "no clean UTC broke_at_utc",
                              "raw": json.dumps(ev, ensure_ascii=False)[:300]})
            continue
        targets = expand(ev)
        if not targets:
            discarded.append({"reason": "no ticker and peer did not map to any US name",
                              "raw": json.dumps(ev, ensure_ascii=False)[:300]})
            continue
        for name, line in targets:
            if name not in eligible:
                discarded.append({"reason": f"{name} not in eligible equity universe",
                                  "raw": line[:300]})
                continue
            if name in excluded:
                discarded.append({"reason": f"{name} excluded by INV-COST (spread too wide)",
                                  "raw": line[:300]})
                continue
            dark = is_dark(name, ts).is_dark
            flags.append({
                "ticker": name, "event": line, "source": (ev.get("source") or "").strip(),
                "broke_at_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "dark_at_break": "Y" if dark else "N",
            })

    # dark first, then by time
    flags.sort(key=lambda r: (r["dark_at_break"] != "Y", r["broke_at_utc"]))
    if args.only_dark:
        flags = [f for f in flags if f["dark_at_break"] == "Y"]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    cols = ["ticker", "event", "source", "broke_at_utc", "dark_at_break"]
    with (args.output_dir / "dark_event_flags.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(flags)
    with (args.output_dir / "dark_event_discarded.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["reason", "raw"])
        w.writeheader()
        w.writerows(discarded)

    n_dark = sum(1 for f in flags if f["dark_at_break"] == "Y")
    md = [
        f"# Dark-Event Flags - {dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ}", "",
        f"- Evaluated: {len(flags)} name-events | dark_at_break=Y (FLAGS for manual "
        f"verification): {n_dark} | discarded: {len(discarded)}",
        "- Flags are CLUES, not signals. Verify the event, its real break timestamp, and a "
        "live firm quote before any trade. Indicative quotes are not executable.", "",
    ]
    if args.scan_note:
        md += [f"**Scan notes:** {args.scan_note}", ""]
    md += [
        "| ticker | event | source | broke_at_utc | dark_at_break |",
        "|---|---|---|---|---|",
    ]
    for f in flags:
        md.append("| {ticker} | {event} | {source} | {broke_at_utc} | {dark_at_break} |".format(
            **{k: str(v).replace("|", "/") for k, v in f.items()}))
    (args.output_dir / "dark_event_flags.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[dark_event_monitor] evaluated {len(flags)} name-events | "
          f"DARK flags {n_dark} | discarded {len(discarded)}")
    for f in flags:
        if f["dark_at_break"] == "Y":
            print(f"  FLAG {f['ticker']:<6} {f['broke_at_utc']}  {f['event'][:70]}")
    if discarded:
        print(f"  ({len(discarded)} discarded -> outputs/dark_event_discarded.csv; "
              f"timestamp hygiene is intentional)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
