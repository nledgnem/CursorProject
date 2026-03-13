#!/usr/bin/env python3
"""
Inspect Bronze and Silver layer schemas without loading full data.
Uses Parquet metadata (read_schema) and CSV nrows=0 to get column names only.
Highlights FDW, FDV, max_supply, total_supply if present.
"""

from pathlib import Path
import sys

# Paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_LAKE = REPO_ROOT / "data" / "curated" / "data_lake"
CURATED = REPO_ROOT / "data" / "curated"

# Target columns to highlight (case-insensitive)
HIGHLIGHT_KEYS = ("fdw", "fdv", "max_supply", "total_supply")


def get_parquet_columns(path: Path) -> list[str] | None:
    """Read Parquet schema only (no row data). Returns column names or None on error."""
    try:
        import pyarrow.parquet as pq
        schema = pq.read_schema(path)
        return list(schema.names)
    except Exception as e:
        print(f"  [ERROR] {e}", file=sys.stderr)
        return None


def get_csv_columns(path: Path) -> list[str] | None:
    """Read CSV header only (nrows=0). Returns column names or None on error."""
    try:
        import pandas as pd
        df = pd.read_csv(path, nrows=0)
        return list(df.columns)
    except Exception as e:
        print(f"  [ERROR] {e}", file=sys.stderr)
        return None


def get_columns(path: Path) -> list[str] | None:
    """Get column names for Parquet or CSV."""
    if not path.exists():
        return None
    suf = path.suffix.lower()
    if suf == ".parquet":
        return get_parquet_columns(path)
    if suf == ".csv":
        return get_csv_columns(path)
    return None


def highlight_columns(columns: list[str]) -> list[str]:
    """Return column names that match FDW, FDV, max_supply, total_supply (case-insensitive)."""
    lower = {c.lower(): c for c in columns}
    out = []
    for key in HIGHLIGHT_KEYS:
        if key in lower:
            out.append(lower[key])
    return out


def main():
    # Bronze (data_lake)
    bronze = [
        ("fact_price", DATA_LAKE / "fact_price.parquet"),
        ("fact_marketcap", DATA_LAKE / "fact_marketcap.parquet"),
    ]
    # Silver (data_lake + curated for universe)
    silver = [
        ("silver_fact_price", DATA_LAKE / "silver_fact_price.parquet"),
        ("silver_fact_marketcap", DATA_LAKE / "silver_fact_marketcap.parquet"),
        ("silver_universe_mask", DATA_LAKE / "silver_universe_mask.parquet"),  # may not exist
        ("universe_eligibility", CURATED / "universe_eligibility.parquet"),
        ("universe_snapshots", CURATED / "universe_snapshots.parquet"),
    ]

    print("=" * 60)
    print("BRONZE & SILVER LAYER SCHEMAS (headers only, no full load)")
    print("=" * 60)
    print(f"Data lake: {DATA_LAKE}")
    print(f"Curated:  {CURATED}")
    print()

    all_highlights = []

    for layer_name, paths in [("Bronze", bronze), ("Silver (and universe)", silver)]:
        print(f"--- {layer_name} ---")
        for name, path in paths:
            if not path.exists():
                print(f"\n  [{name}]")
                print(f"    Path: {path}")
                print("    Status: FILE NOT FOUND")
                continue
            cols = get_columns(path)
            if cols is None:
                print(f"\n  [{name}]")
                print(f"    Path: {path}")
                print("    Status: Could not read schema")
                continue
            highlights = highlight_columns(cols)
            if highlights:
                all_highlights.append((name, path, highlights))
            print(f"\n  [{name}]")
            print(f"    Path: {path}")
            print(f"    Columns ({len(cols)}): {cols}")
            if highlights:
                print(f"    >>> HIGHLIGHT (FDW/FDV/max_supply/total_supply): {highlights}")
        print()

    print("=" * 60)
    print("SUMMARY: FDW / FDV / max_supply / total_supply")
    print("=" * 60)
    if not all_highlights:
        print("  None of the checked datasets contain FDW, FDV, max_supply, or total_supply.")
    else:
        for name, path, highlights in all_highlights:
            print(f"  {name}: {highlights}")
    print()


if __name__ == "__main__":
    main()
