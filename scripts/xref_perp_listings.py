#!/usr/bin/env python3
"""
Cross-reference utility for Apathy Bleed candidate tickers against venue perp listings.

Input: candidate tickers (symbols)
Output: coverage per candidate by venue with sizing metadata if available.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from repo_paths import data_lake_root  # noqa: E402
from src.utils.ticker_normalization import build_normalization_rules, normalize_ticker  # noqa: E402


@dataclass(frozen=True)
class OutputPaths:
    curated_dir: Path
    perps_hyperliquid: Path
    perps_variational: Path
    mapping: Path


def _utc_today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _load_paths_from_config() -> OutputPaths:
    cfg_path = REPO_ROOT / "configs" / "perp_listings.yaml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    out = cfg.get("output", {}) or {}
    curated_raw = out.get("curated_data_lake_dir")
    if curated_raw:
        curated_dir = Path(str(curated_raw))
        curated_dir = (REPO_ROOT / curated_dir).resolve() if not curated_dir.is_absolute() else curated_dir.resolve()
    else:
        curated_dir = data_lake_root()
    return OutputPaths(
        curated_dir=curated_dir,
        perps_hyperliquid=(curated_dir / out.get("perps_hyperliquid_csv", "perps_hyperliquid.csv")).resolve(),
        perps_variational=(curated_dir / out.get("perps_variational_csv", "perps_variational.csv")).resolve(),
        mapping=(curated_dir / out.get("perp_ticker_mapping_csv", "perp_ticker_mapping.csv")).resolve(),
    )


def _load_norm_rules_from_config() -> Any:
    cfg_path = REPO_ROOT / "configs" / "perp_listings.yaml"
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    norm = cfg.get("ticker_normalization", {}) or {}
    return build_normalization_rules(norm.get("strip_suffixes", []), norm.get("strip_prefixes", []))


def _read_snapshot(path: Path, snapshot_date_utc: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "snapshot_date_utc" not in df.columns:
        return pd.DataFrame()
    df = df[df["snapshot_date_utc"].astype(str) == snapshot_date_utc].copy()
    return df


def xref_candidates(candidates: list[str], snapshot_date_utc: str) -> pd.DataFrame:
    paths = _load_paths_from_config()
    rules = _load_norm_rules_from_config()

    hl = _read_snapshot(paths.perps_hyperliquid, snapshot_date_utc)
    var = _read_snapshot(paths.perps_variational, snapshot_date_utc)

    hl_by_norm = {}
    if not hl.empty and "ticker" in hl.columns:
        for _, r in hl.iterrows():
            t = str(r.get("ticker", "")).strip()
            if not t:
                continue
            hl_by_norm[normalize_ticker(t, rules)] = r.to_dict()

    var_by_norm = {}
    if not var.empty and "ticker" in var.columns:
        for _, r in var.iterrows():
            t = str(r.get("ticker", "")).strip()
            if not t:
                continue
            var_by_norm[normalize_ticker(t, rules)] = r.to_dict()

    rows = []
    for c in candidates:
        c0 = (c or "").strip()
        cn = normalize_ticker(c0, rules)
        hl_row = hl_by_norm.get(cn)
        var_row = var_by_norm.get(cn)

        rows.append(
            {
                "snapshot_date_utc": snapshot_date_utc,
                "candidate_ticker": c0.upper(),
                "hyperliquid": bool(hl_row is not None),
                "variational": bool(var_row is not None),
                "hyperliquid_max_leverage": hl_row.get("max_leverage") if hl_row else None,
                "hyperliquid_min_order_size": hl_row.get("min_order_size") if hl_row else None,
                "variational_base_spread_bps": var_row.get("base_spread_bps") if var_row else None,
                "variational_funding_interval_s": var_row.get("funding_interval_s") if var_row else None,
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Cross-reference candidate tickers vs venue perp listings.")
    p.add_argument("--snapshot-date-utc", type=str, default=_utc_today_iso(), help="UTC snapshot date (YYYY-MM-DD).")
    p.add_argument("--candidates", nargs="+", required=True, help="Candidate tickers (e.g., ARIA DEXE).")
    p.add_argument("--out", type=Path, default=None, help="Optional CSV output path.")
    args = p.parse_args()

    df = xref_candidates(args.candidates, snapshot_date_utc=args.snapshot_date_utc)
    print(df.to_string(index=False))

    if args.out:
        out_path = args.out if args.out.is_absolute() else (REPO_ROOT / args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()

