#!/usr/bin/env python3
"""MSM decision records: keep what the system said at the time apart from later recomputations.

msm_timeseries.csv (and macro_state.db's macro_features) are recomputed over the full 730-day window
every night, so their past rows are NOT what the system said on those dates: they move whenever the
lake changes (asset-identity incident 2026-09-18: the 2026-03-30 row reads Q1/MRF off today, the run
committed on 2026-03-30 said Q2/MRF off, and the corrected-data recompute says Q2/MRF on).

Subcommands:
  log-today   Append the latest decision row of msm_timeseries.csv to msm_decision_log.csv (append-
              only; a decision_date already logged is never rewritten). Run daily after the
              pipeline -- from now on this file IS the as-decided record.
  as-decided  Reconstruct as-decided rows for past dates from MSM runs committed to git
              (reports/msm_funding_v0/<run_id>/msm_timeseries.csv): for each decision date, the
              earliest committed run made on or after it. Source commit, run id and lag are kept.
  freeze      Copy the current msm_timeseries.csv to an immutable, hashed pre-repair file.
  compare     Long-format AS_DECIDED vs PRE_REPAIR_RECOMPUTE vs RECOMPUTED_ON_CORRECTED_DATA.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
FIELDS = ["funding_regime", "is_mrf_active", "funding_pct_rank", "F_tk", "basket_members", "BTCDOM_Trend",
          "Environment_APR"]
RUN_RE = re.compile(r"reports/msm_funding_v0/(\d{8})_(\d{6})/msm_timeseries\.csv$")
# Fixed schema: the log never changes shape when msm_timeseries.csv gains or loses columns.
LOG_COLUMNS = ["logged_utc", "decision_date", *FIELDS, "basket_hash", "n_valid", "coverage",
               "Fragmentation_Spread", "Z_Score_90d", "w_risk", "btcdom_status"]


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout


def log_today(timeseries: Path, log: Path) -> bool:
    """Append the latest decision row once; never rewrite a logged decision. Returns True if written."""
    src = pd.read_csv(timeseries)
    row = src.sort_values("decision_date").tail(1).copy()
    log = Path(log)
    done = set(pd.read_csv(log, usecols=["decision_date"])["decision_date"].astype(str)) if log.exists() else set()
    if str(row["decision_date"].iloc[0]) in done:
        return False
    row.insert(0, "logged_utc", datetime.now(timezone.utc).isoformat(timespec="seconds"))
    row.reindex(columns=LOG_COLUMNS).to_csv(log, mode="a", header=not log.exists(), index=False)
    return True


def latest_timeseries(reports_root: Path) -> Path:
    return max(Path(reports_root).rglob("msm_timeseries.csv"), key=lambda p: p.stat().st_mtime)


def cmd_log_today(args) -> int:
    wrote = log_today(Path(args.timeseries), Path(args.log))
    print(("logged" if wrote else "already logged; nothing written") + f" -> {args.log}")
    return 0


def cmd_as_decided(args) -> int:
    runs = []
    for line in _git("log", "--all", "--format=@%h %cI", "--name-only", "--", "reports/msm_funding_v0").splitlines():
        if line.startswith("@"):
            commit = line[1:].split()[0]
            continue
        m = RUN_RE.search(line.strip())
        if m:
            runs.append((f"{m.group(1)}_{m.group(2)}", commit, line.strip()))
    rows = []
    for run_id, commit, path in sorted(set(runs)):
        run_date = pd.Timestamp(datetime.strptime(run_id[:8], "%Y%m%d"))
        try:
            ts = pd.read_csv(io.StringIO(_git("show", f"{commit}:{path}")))
        except subprocess.CalledProcessError:
            continue
        if "funding_regime" not in ts.columns:
            continue
        ts["decision_date"] = pd.to_datetime(ts["decision_date"])
        for _, r in ts[ts["decision_date"] <= run_date].iterrows():
            rows.append({"decision_date": r["decision_date"], "run_id": run_id, "commit": commit,
                         "lag_days": (run_date - r["decision_date"]).days,
                         **{f: r.get(f) for f in FIELDS}})
    allr = pd.DataFrame(rows)
    if args.since:
        allr = allr[allr["decision_date"] >= pd.Timestamp(args.since)]
    best = allr.sort_values(["decision_date", "lag_days", "run_id"]).groupby("decision_date").head(1)
    best = best[best["lag_days"] <= args.max_lag_days]
    best.to_csv(args.out, index=False)
    print(f"{len(best)} decision dates reconstructed from {allr['run_id'].nunique()} committed runs -> {args.out}")
    return 0


def cmd_freeze(args) -> int:
    src, dst = Path(args.timeseries), Path(args.out)
    if dst.exists():
        print(f"REFUSED: {dst} exists (frozen records are never overwritten)")
        return 1
    data = src.read_bytes()
    dst.write_bytes(data)
    dst.with_suffix(dst.suffix + ".sha256").write_text(hashlib.sha256(data).hexdigest() + "\n", encoding="utf-8")
    print(f"frozen {src} -> {dst}")
    return 0


def cmd_compare(args) -> int:
    def load(p):
        d = pd.read_csv(p)
        d["decision_date"] = pd.to_datetime(d["decision_date"])
        return d.set_index("decision_date")
    ad, pre, post = load(args.as_decided), load(args.pre_repair), load(args.recomputed)
    dates = sorted(set(pre.index) | set(post.index))
    out = []
    for d in dates:
        for f in FIELDS:
            vals = {"AS_DECIDED": ad[f].get(d) if f in ad else None,
                    "PRE_REPAIR_RECOMPUTE": pre[f].get(d) if f in pre else None,
                    "RECOMPUTED_ON_CORRECTED_DATA": post[f].get(d) if f in post else None}
            out.append({"decision_date": d.date(), "field": f, **vals,
                        "as_decided_source": f"{ad['run_id'].get(d)}@{ad['commit'].get(d)}" if d in ad.index else
                        "not recorded (no decision-time record exists)",
                        "corrected_differs_from_pre_repair": str(vals["PRE_REPAIR_RECOMPUTE"]) != str(
                            vals["RECOMPUTED_ON_CORRECTED_DATA"])})
    res = pd.DataFrame(out)
    res.to_csv(args.out, index=False)
    ch = res[res["corrected_differs_from_pre_repair"] & res["field"].isin(["funding_regime", "is_mrf_active"])]
    print(f"{len(dates)} decision dates; {ch['decision_date'].nunique()} with a regime/MRF change -> {args.out}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("log-today"); a.add_argument("--timeseries", required=True); a.add_argument("--log", required=True)
    b = sub.add_parser("as-decided"); b.add_argument("--out", required=True); b.add_argument("--since", default=None)
    b.add_argument("--max-lag-days", type=int, default=7)
    c = sub.add_parser("freeze"); c.add_argument("--timeseries", required=True); c.add_argument("--out", required=True)
    d = sub.add_parser("compare"); d.add_argument("--as-decided", required=True); d.add_argument("--pre-repair", required=True)
    d.add_argument("--recomputed", required=True); d.add_argument("--out", required=True)
    args = p.parse_args()
    return {"log-today": cmd_log_today, "as-decided": cmd_as_decided, "freeze": cmd_freeze, "compare": cmd_compare}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
