"""Reproducible run manifests.

Every research run records: git commit (+ dirty flag), strategy name/version, data
version (content hashes of every input file), parameters, universe, date range,
fees, slippage, results, integrity-check outcomes and a UTC timestamp.

Usage
-----
    with RunLog("track_a_confirmation", version="1.0", params=P, universe=["BTC"],
                start="2016-02-05", end="2026-09-14", fees_bps=5, slippage_bps=5,
                data_files=[path1, path2]) as run:
        ...
        run.result("sharpe_K3", 1.32)
        run.check(check_result)
"""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path

from qlib.paths import REPO, RUNS


def _git(*args) -> str:
    try:
        return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, timeout=30).stdout.strip()
    except Exception:
        return ""


def file_hash(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()[:16]


class RunLog:
    def __init__(self, name: str, version: str, params: dict, universe, start, end,
                 fees_bps: float, slippage_bps: float, data_files=(), notes: str = ""):
        self.ts = datetime.now(timezone.utc)
        self.manifest = {
            "name": name, "version": version, "timestamp_utc": self.ts.isoformat(timespec="seconds"),
            "git_commit": _git("rev-parse", "HEAD"),
            "git_dirty_files": len([l for l in _git("status", "--porcelain").splitlines() if l.strip()]),
            "params": params, "universe": list(universe) if not isinstance(universe, str) else [universe],
            "start": str(start), "end": str(end), "fees_bps": fees_bps, "slippage_bps": slippage_bps,
            "data": {}, "results": {}, "checks": [], "notes": notes,
        }
        for p in data_files:
            p = Path(p)
            if p.exists():
                self.manifest["data"][str(p.relative_to(REPO)) if REPO in p.parents else str(p)] = {
                    "sha256_16": file_hash(p), "bytes": p.stat().st_size}
        self.dir = RUNS / f"{self.ts:%Y%m%dT%H%M%SZ}_{name}"

    def result(self, key: str, value) -> None:
        self.manifest["results"][key] = value

    def check(self, res) -> None:
        self.manifest["checks"].append(asdict(res) if is_dataclass(res) else dict(res))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.manifest["status"] = "error" if exc_type else "ok"
        if exc_type:
            self.manifest["error"] = repr(exc)
        failed = [c for c in self.manifest["checks"] if not c.get("passed", True) and c.get("severity") == "error"]
        self.manifest["failed_error_checks"] = len(failed)
        self.dir.mkdir(parents=True, exist_ok=True)
        (self.dir / "manifest.json").write_text(json.dumps(self.manifest, indent=2, default=str))
        index = RUNS / "run_index.csv"
        new = not index.exists()
        with open(index, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["timestamp_utc", "name", "version", "git_commit", "git_dirty_files", "start", "end",
                            "status", "failed_error_checks", "manifest"])
            m = self.manifest
            w.writerow([m["timestamp_utc"], m["name"], m["version"], m["git_commit"][:10], m["git_dirty_files"],
                        m["start"], m["end"], m["status"], m["failed_error_checks"],
                        str((self.dir / "manifest.json").relative_to(REPO))])
        return False
