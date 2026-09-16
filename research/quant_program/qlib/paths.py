"""Project directories. Everything the program writes lives under research/quant_program/."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]          # research/quant_program
REPO = ROOT.parents[1]                                # repository root
CACHE = ROOT / "cache"                                # raw fetched data (gitignored)
RESULTS = ROOT / "results"
TABLES = RESULTS / "tables"
FIGURES = RESULTS / "figures"
RUNS = ROOT / "runs"                                  # run manifests

for _d in (CACHE, RESULTS, TABLES, FIGURES, RUNS):
    _d.mkdir(parents=True, exist_ok=True)
