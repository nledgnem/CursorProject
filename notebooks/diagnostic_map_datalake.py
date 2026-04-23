#!/usr/bin/env python3
"""
Data Lake Mapper: Topological map of directory structure + schema for every .parquet and .csv.

Outputs Markdown directory tree with column names and dtypes under each file.
Does not read actual rows (schema / nrows=0 only).
"""

import argparse
import io
import sys
from pathlib import Path

# Windows: ensure UTF-8 output for Arrow dtype strings (e.g. large_string)
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd

# Optional: use pyarrow for parquet schema-only (no row read)
try:
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

# Paths relative to repo root (run from repo root or scripts/)
REPO_ROOT = Path(__file__).resolve().parent.parent


def get_parquet_schema(path: Path) -> list[tuple[str, str]]:
    """Return list of (column_name, dtype_str) without reading any rows."""
    try:
        if HAS_PYARROW:
            schema = pq.read_schema(path)
            return [(f.name, str(f.type)) for f in schema]
        # Fallback: pandas with 0 rows
        df = pd.read_parquet(path, engine="pyarrow")
        return [(c, str(d)) for c, d in df.dtypes.items()]
    except Exception as e:
        return [("(schema error)", str(e))]


def get_csv_schema(path: Path) -> list[tuple[str, str]]:
    """Return list of (column_name, dtype_str) using nrows=0."""
    try:
        df = pd.read_csv(path, nrows=0, encoding="utf-8", on_bad_lines="skip")
        return [(c, str(d)) for c, d in df.dtypes.items()]
    except Exception as e:
        try:
            df = pd.read_csv(path, nrows=0, encoding="latin-1", on_bad_lines="skip")
            return [(c, str(d)) for c, d in df.dtypes.items()]
        except Exception as e2:
            return [("(schema error)", str(e2))]


def format_schema(
    rows: list[tuple[str, str]], indent: str = "    ", max_cols: int | None = None
) -> str:
    """Format schema as markdown list under the file. If max_cols set, truncate with '... and N more'."""
    if not rows:
        return indent + "* (no columns)\n"
    total = len(rows)
    if max_cols is not None and total > max_cols:
        rows = rows[:max_cols]
    lines = []
    for col, dtype in rows:
        lines.append(f"{indent}- `{col}`: {dtype}")
    if max_cols is not None and total > max_cols:
        lines.append(f"{indent}- *... and {total - max_cols} more columns*")
    return "\n".join(lines) + "\n"


def build_tree(root: Path, base_rel: Path) -> list[tuple[str, Path, list[tuple[str, str]] | None]]:
    """
    Walk root, collect (relative_path_str, full_path, schema_or_None).
    schema is None for directories; list of (col, dtype) for files.
    Sorted so parent dirs appear before their children (tree order).
    """
    root = root.resolve()
    if not root.is_dir():
        return []

    out = []
    for path in sorted(root.rglob("*")):
        try:
            rel = path.relative_to(base_rel)
        except ValueError:
            rel = path
        rel_str = rel.as_posix()
        if path.is_dir():
            out.append((rel_str + "/", path, None))
            continue
        if path.suffix.lower() == ".parquet":
            out.append((rel_str, path, get_parquet_schema(path)))
        elif path.suffix.lower() == ".csv":
            out.append((rel_str, path, get_csv_schema(path)))
    # Sort: tree order (parent before child), dirs before files at same level
    def key(item):
        p = item[0]
        is_dir = p.endswith("/")
        # Lexicographic sort gives parent before child; then dirs before files
        return (p.rstrip("/") + ("/" if is_dir else "\xff"), not is_dir)
    out.sort(key=key)
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Map data lake directory structure and schemas (parquet/csv) to Markdown."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=None,
        help="Data lake root directory (default: REPO_ROOT/data)",
    )
    parser.add_argument(
        "--no-schema",
        action="store_true",
        help="Only print directory tree, skip schema extraction",
    )
    parser.add_argument(
        "--max-cols",
        type=int,
        default=None,
        metavar="N",
        help="Show at most N columns per file (truncate wide schemas with '... and N more')",
    )
    args = parser.parse_args()

    base = Path(args.root) if args.root else (REPO_ROOT / "data")
    base = base.resolve()
    if not base.is_dir():
        print(f"Error: not a directory: {base}", file=sys.stderr)
        sys.exit(1)

    print("# Data Lake Topological Map")
    print()
    print(f"**Root:** `{base}`")
    print()

    if args.no_schema:
        # Simple tree without schemas
        for path in sorted(base.rglob("*")):
            try:
                rel = path.relative_to(base)
            except ValueError:
                continue
            depth = len(rel.parts)
            indent = "  " * (depth - 1) if rel.suffix else "  " * depth
            name = path.name + ("/" if path.is_dir() else "")
            print(indent + "- " + name)
        return

    # Full map: directory structure + schemas (parquet/csv only)
    for rel_str, full_path, schema in build_tree(base, base):
        if rel_str.endswith("/"):
            # depth 0 = first level under root
            depth = max(0, rel_str.count("/") - 1)
            name = rel_str.rstrip("/").split("/")[-1] if rel_str.rstrip("/") else rel_str.rstrip("/")
            print("  " * depth + "- **" + name + "/**")
        else:
            depth = rel_str.count("/")  # file depth (same as parent dir depth + 1 in display)
            print("  " * depth + "- `" + full_path.name + "`")
            if schema is not None:
                print(format_schema(schema, indent="    " + "  " * depth, max_cols=args.max_cols))


if __name__ == "__main__":
    main()
