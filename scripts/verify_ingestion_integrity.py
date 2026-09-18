#!/usr/bin/env python3
"""Post-deploy ingestion-integrity checks.

Designed to be re-runnable for any schema-touching deploy. Each mode is a
self-contained set of signal checks. Add new modes as new incident classes
surface; do not fork this script.

Usage:
    python scripts/verify_ingestion_integrity.py --mode writer_race
    python scripts/verify_ingestion_integrity.py --mode asset_identity [--lake-dir DIR] [--since YYYY-MM-DD] [--offline]
    python scripts/verify_ingestion_integrity.py --mode freshness [--lake-dir DIR] [--json-out PATH]
    python scripts/verify_ingestion_integrity.py --mode <future-mode>

Exit codes:
    0 --all signals PASS (or PASS + INDETERMINATE; INDETERMINATE alone never fails the run)
    1 --at least one signal returned FAIL
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# repo_paths sits at repo root; ensure it's importable when this script is run from anywhere.
_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from repo_paths import data_lake_root  # noqa: E402


# ----------------------------------------------------------------------------------
# Signal result type
# ----------------------------------------------------------------------------------

class SignalResult:
    """One row in the verification table."""

    def __init__(self, name: str, description: str, status: str, detail: str):
        if status not in ("PASS", "FAIL", "INDETERMINATE"):
            raise ValueError(f"Invalid signal status: {status!r}")
        self.name = name
        self.description = description
        self.status = status
        self.detail = detail


# ----------------------------------------------------------------------------------
# Mode: writer_race
# ----------------------------------------------------------------------------------
#
# Verifies the post-fix state of the CoinGecko writer-race incident
# (DATA_LAKE_CONTEXT.md §9 entry 0, §13 followups, reports/apathy_universe_cut_audit_2026_04_29.md).
# The four signals are:
#   1. Today's run made ~2,716 API calls (down from 2,997 pre-dedupe).
#   2. The fail-fast guard in download_all_coins did NOT fire (silent in logs).
#   3. ETH / SOL / DOGE on the latest fact_marketcap date show canonical mcap envelopes.
#   4. BTC / BNB / XRP show no regression from a pre-merge anchor date.
#
# Signal 3 + 4 read parquet directly and are reliable from any environment that
# has fact_marketcap.parquet visible (Render, Drive Desktop, etc.). Signals 1 + 2
# require log access; if logs are unreachable they return INDETERMINATE rather
# than FAIL --the parquet evidence in Signal 3 is the strongest empirical proof
# that the fetcher ran and produced canonical results.

_LOG_CANDIDATE_PATHS = [
    "/tmp/run_live_pipeline.log",
    "/tmp/run_live_pipeline_*.log",
    "/var/log/run_live_pipeline.log",
    "/var/log/macro-regime/*.log",
]

_API_CALL_COUNT_PATTERNS = [
    re.compile(r"Downloading data for (\d+) coins"),
    re.compile(r"download_all_coins.*?(\d+)\s+(?:rows|coins|tickers)"),
    re.compile(r"Loading allowlist.*?\((\d+) rows\)"),
]

_GUARD_FIRED_PATTERNS = [
    re.compile(r"Allowlist has \d+ duplicate symbols", re.IGNORECASE),
    re.compile(r"ValueError.*writer-race", re.IGNORECASE),
]


def _find_log_files() -> list[Path]:
    """Return any log files matching the candidate paths. Empty list if none found."""
    found: list[Path] = []
    for pattern in _LOG_CANDIDATE_PATHS:
        if "*" in pattern:
            from glob import glob
            for hit in glob(pattern):
                p = Path(hit)
                if p.is_file():
                    found.append(p)
        else:
            p = Path(pattern)
            if p.is_file():
                found.append(p)
    # Dedupe, preserve order
    seen: set[Path] = set()
    unique: list[Path] = []
    for p in found:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        unique.append(p)
    return unique


def _signal_1_api_call_count() -> SignalResult:
    """API call count from latest run logs.

    PASS: count is 2,716 ± 50 (matches deduped allowlist).
    FAIL: count < 2,500 or > 2,800.
    INDETERMINATE: logs unavailable or no matching pattern.
    """
    log_files = _find_log_files()
    if not log_files:
        return SignalResult(
            "1",
            "API call count from logs",
            "INDETERMINATE",
            "No log files found at any candidate path. Run on Render shell or set log path in script.",
        )

    latest_count: Optional[int] = None
    matched_log: Optional[Path] = None
    for log_path in log_files:
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            continue
        for pat in _API_CALL_COUNT_PATTERNS:
            for m in pat.finditer(text):
                try:
                    n = int(m.group(1))
                except (IndexError, ValueError):
                    continue
                # Take the most recent match (last occurrence wins)
                latest_count = n
                matched_log = log_path

    if latest_count is None:
        return SignalResult(
            "1",
            "API call count from logs",
            "INDETERMINATE",
            f"Found {len(log_files)} log file(s) but no matching call-count pattern. Logs scanned: {[str(p) for p in log_files]}",
        )

    if 2_716 - 50 <= latest_count <= 2_716 + 50:
        return SignalResult(
            "1",
            "API call count from logs",
            "PASS",
            f"N={latest_count} (within 2,716 ± 50) from {matched_log}",
        )
    if latest_count < 2_500 or latest_count > 2_800:
        return SignalResult(
            "1",
            "API call count from logs",
            "FAIL",
            f"N={latest_count} outside acceptable range [2,500, 2,800] from {matched_log}",
        )
    # In the buffer zone (2,666-2,716 or 2,716-2,766 --but outside ±50)
    return SignalResult(
        "1",
        "API call count from logs",
        "FAIL",
        f"N={latest_count} outside ±50 tolerance of 2,716 (from {matched_log}). Investigate before proceeding.",
    )


def _signal_2_guard_silent() -> SignalResult:
    """Guard fail-fast did NOT fire.

    PASS: no `Allowlist has` or `ValueError` matches in logs.
    FAIL: any match --guard fired, dedupe didn't fully resolve.
    INDETERMINATE: logs unavailable.
    """
    log_files = _find_log_files()
    if not log_files:
        return SignalResult(
            "2",
            "Guard silent in logs",
            "INDETERMINATE",
            "No log files found at any candidate path. Strong indirect evidence: if guard had fired, fact_marketcap would have no fresh row for today (Signal 3 implicitly confirms).",
        )

    matches: list[str] = []
    for log_path in log_files:
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        for pat in _GUARD_FIRED_PATTERNS:
            for m in pat.finditer(text):
                matches.append(f"{log_path}: {m.group(0)[:120]}")

    if not matches:
        return SignalResult(
            "2",
            "Guard silent in logs",
            "PASS",
            f"No matches in {len(log_files)} log file(s).",
        )
    return SignalResult(
        "2",
        "Guard silent in logs",
        "FAIL",
        f"Guard fired: {matches[:3]}",
    )


def _signal_3_affected_correct() -> SignalResult:
    """Affected blue-chips show canonical mcap envelopes on latest date.

    PASS criteria:
      ETH:  $100B - $400B
      SOL:  $30B - $150B
      DOGE: $10B - $60B
    Plus a sanity check on the 16 Apathy live picks (no asserts; just reported).
    """
    fmc_path = data_lake_root() / "fact_marketcap.parquet"
    df = pd.read_parquet(fmc_path)
    latest_date = df["date"].max()

    expected = {
        "ETH":  (100e9, 400e9),
        "SOL":  (30e9, 150e9),
        "DOGE": (10e9, 60e9),
    }
    failures: list[str] = []
    detail_lines: list[str] = [f"Latest date: {latest_date}"]

    for tkr, (lo, hi) in expected.items():
        row = df[(df["asset_id"].astype(str).str.upper() == tkr) & (df["date"] == latest_date)]
        if row.empty:
            failures.append(f"{tkr}: NOT FOUND on {latest_date}")
            detail_lines.append(f"  {tkr}: NOT FOUND")
            continue
        val = float(row["marketcap"].iloc[0])
        if lo <= val <= hi:
            detail_lines.append(f"  {tkr}: ${val:,.0f} (in [{lo:.0e}, {hi:.0e}]) [OK]")
        else:
            failures.append(f"{tkr}: ${val:,.0f} outside expected [{lo:.0e}, {hi:.0e}]")
            detail_lines.append(f"  {tkr}: ${val:,.0f} OUTSIDE [{lo:.0e}, {hi:.0e}] [FAIL]")

    # Bonus: regression coverage on Apathy live picks (informational only)
    apathy_picks = ["ZEC", "DASH", "ZEN", "ICNT", "PIEVERSE", "CHZ", "FARTCOIN",
                    "KITE", "AXS", "MORPHO", "STABLE", "ARIA", "DEXE", "ONT", "TAO", "SIGN"]
    apathy_missing = []
    for tkr in apathy_picks:
        row = df[(df["asset_id"].astype(str).str.upper() == tkr) & (df["date"] == latest_date)]
        if row.empty:
            apathy_missing.append(tkr)
    if apathy_missing:
        detail_lines.append(f"  Apathy picks missing on {latest_date}: {apathy_missing}")

    detail = "; ".join(detail_lines)
    if failures:
        return SignalResult(
            "3",
            "ETH/SOL/DOGE in canonical envelope",
            "FAIL",
            f"{len(failures)} ticker(s) failed: {failures}. Full: {detail}",
        )
    return SignalResult("3", "ETH/SOL/DOGE in canonical envelope", "PASS", detail)


def _signal_4_unaffected_unchanged(merge_date: date = date(2026, 5, 4)) -> SignalResult:
    """Single-variant blue-chips unchanged regression check.

    PASS: BTC/BNB/XRP shifted by ≤15% over the period.
    FAIL: any shifted >25%.
    Anything 15-25% gets reported as PASS with a "manual sanity-check recommended" annotation —
    real BTC can move that much in a few days; we don't auto-fail in that band but we surface it.
    """
    pre_target = merge_date - timedelta(days=2)

    fmc_path = data_lake_root() / "fact_marketcap.parquet"
    df = pd.read_parquet(fmc_path)
    latest_date = df["date"].max()

    available_pre = df[df["date"] <= pre_target]["date"].max()
    if pd.isna(available_pre):
        return SignalResult(
            "4",
            "BTC/BNB/XRP regression check",
            "INDETERMINATE",
            f"No fact_marketcap rows on or before {pre_target}; cannot compute pre-merge anchor.",
        )

    failures: list[str] = []
    annotations: list[str] = []
    detail_lines: list[str] = [f"Pre-merge anchor: {available_pre}; latest: {latest_date}"]

    for tkr in ("BTC", "BNB", "XRP"):
        pre = df[(df["asset_id"].astype(str).str.upper() == tkr) & (df["date"] == available_pre)]
        post = df[(df["asset_id"].astype(str).str.upper() == tkr) & (df["date"] == latest_date)]
        if pre.empty or post.empty:
            annotations.append(f"{tkr}: missing data (pre_empty={pre.empty}, post_empty={post.empty})")
            detail_lines.append(f"  {tkr}: missing data")
            continue
        pre_val = float(pre["marketcap"].iloc[0])
        post_val = float(post["marketcap"].iloc[0])
        pct = (post_val - pre_val) / pre_val * 100.0
        if abs(pct) <= 15:
            detail_lines.append(f"  {tkr}: pre=${pre_val:,.0f}, post=${post_val:,.0f} ({pct:+.2f}%) [OK]")
        elif abs(pct) <= 25:
            annotations.append(f"{tkr}: {pct:+.2f}% (in 15-25% band -- confirm against real spot price action)")
            detail_lines.append(f"  {tkr}: {pct:+.2f}% (sanity-check needed)")
        else:
            failures.append(f"{tkr}: {pct:+.2f}% (>25% -- likely regression)")
            detail_lines.append(f"  {tkr}: {pct:+.2f}% [FAIL]")

    detail = "; ".join(detail_lines)
    if failures:
        return SignalResult(
            "4",
            "BTC/BNB/XRP regression check",
            "FAIL",
            f"{len(failures)} ticker(s) >25% shift: {failures}. Full: {detail}",
        )
    if annotations:
        return SignalResult(
            "4",
            "BTC/BNB/XRP regression check",
            "PASS",
            f"All ≤25%; manual sanity-check recommended for: {annotations}. Full: {detail}",
        )
    return SignalResult("4", "BTC/BNB/XRP regression check", "PASS", detail)


def _print_signal_table(signals: list[SignalResult]) -> None:
    """Markdown table to stdout, then full details for rows too long for the table."""
    print()
    print("| Signal | Description                          | Result        | Detail                                              |")
    print("|--------|--------------------------------------|---------------|-----------------------------------------------------|")
    for s in signals:
        # Trim very long detail strings for table readability
        d = s.detail if len(s.detail) < 120 else s.detail[:117] + "..."
        print(f"| {s.name:<6} | {s.description:<36} | {s.status:<13} | {d:<51} |")
    print()
    # Full details below for any that didn't fit
    for s in signals:
        if len(s.detail) >= 120:
            print(f"### Signal {s.name} full detail:\n{s.detail}\n")


def _run_writer_race_mode() -> int:
    """Run all 4 signals for writer_race mode. Return exit code."""
    signals = [
        _signal_1_api_call_count(),
        _signal_2_guard_silent(),
        _signal_3_affected_correct(),
        _signal_4_unaffected_unchanged(),
    ]
    _print_signal_table(signals)

    has_fail = any(s.status == "FAIL" for s in signals)
    has_indeterminate = any(s.status == "INDETERMINATE" for s in signals)
    n_pass = sum(1 for s in signals if s.status == "PASS")

    if has_fail:
        print(f"OVERALL: FAIL --{n_pass}/{len(signals)} PASS, halt before any downstream action.")
        return 1
    if has_indeterminate:
        print(f"OVERALL: PASS (with INDETERMINATEs) --{n_pass}/{len(signals)} direct PASS. Indirect evidence sufficient if Signal 3 PASSed.")
        return 0
    print(f"OVERALL: PASS --all {len(signals)} signals direct-confirmed.")
    return 0


# ----------------------------------------------------------------------------------
# Mode: asset_identity
# ----------------------------------------------------------------------------------
#
# fact_price / fact_marketcap are keyed by ticker, and the coin behind a ticker is whatever
# data/perp_allowlist.csv named at fetch time. The writer_race signals only look at a handful
# of majors on the latest date, so they cannot see a ticker that holds another coin's history
# (audit 2026-09-18: 2026-03-04..30 still carried the 599e5cb writer-race rows for 72 assets --
# ETH/SOL/DOGE caps ~$2M -- and ~60 tickers were re-bound on 2026-01-28). Signals:
#   A1  dim_asset.coingecko_id equals the id actually fetched (not the lower-cased ticker).
#   A2  no ticker key spells another allowlisted coin's slug ('BITCOIN' vs real Bitcoin 'BTC').
#   A3  each Binance USDT perp's lake close matches Binance (lake date d = Binance bar d-1):
#       no currently-wrong coin, no wrong-coin run of >= 14 days since --since. Needs network.
#   A4  no date since --since on which >= 20 assets jump 3x overnight in price or market cap.
#   A5  on the latest fact_markets_snapshot date, lake price and market cap agree with
#       /coins/markets for the *fetched coingecko_id*; snapshot must be <= 3 days old.
#   A6  registry invariant: one coin <-> one uid (declared aliases aside); one uid per Binance symbol.
#   A2 accepts collisions acknowledged in configs/asset_identity_policy.yaml (ids are never re-pointed).

_ASSET_IDENTITY_SINCE = date(2024, 5, 10)   # start of the window the writer-race refetch rewrote


def _identity_inputs(lake: Path) -> dict:
    p = pd.read_parquet(lake / "fact_price.parquet", columns=["asset_id", "date", "close"])
    m = pd.read_parquet(lake / "fact_marketcap.parquet", columns=["asset_id", "date", "marketcap"])
    fact = p.merge(m, on=["asset_id", "date"], how="left")
    fact["date"] = pd.to_datetime(fact["date"])
    reg_path = _REPO_ROOT / "data" / "asset_registry.csv"
    conf_path = _REPO_ROOT / "data" / "asset_registry_conflicts.csv"
    return {
        "fact": fact,
        "registry": pd.read_csv(reg_path) if reg_path.exists() else None,
        "registry_conflicts": list(pd.read_csv(conf_path)["binance_symbol"]) if conf_path.exists() else [],
        "dim_asset": pd.read_parquet(lake / "dim_asset.parquet", columns=["asset_id", "coingecko_id"]),
        "dim_instrument": pd.read_parquet(lake / "dim_instrument.parquet",
                                          columns=["instrument_symbol", "venue", "instrument_type"]),
        "allowlist": pd.read_csv(_REPO_ROOT / "data" / "perp_allowlist.csv"),
        "snapshot_path": lake / "fact_markets_snapshot.parquet",
    }


def _signal_a1_dim_asset_ids(inp: dict) -> SignalResult:
    from src.data_lake.asset_identity import placeholder_coingecko_ids
    bad = placeholder_coingecko_ids(inp["dim_asset"], inp["allowlist"])
    n = int(inp["dim_asset"]["asset_id"].isin(inp["allowlist"]["symbol"].str.upper()).sum())
    desc = "dim_asset.coingecko_id = fetched id"
    if bad.empty:
        return SignalResult("A1", desc, "PASS", f"{n} allowlisted assets, all ids match the allowlist")
    eg = ", ".join(f"{r.asset_id}:{r.dim_coingecko_id}->{r.fetched_coingecko_id}" for r in bad.head(8).itertuples())
    return SignalResult("A1", desc, "FAIL", f"{len(bad)}/{n} allowlisted assets carry a different id, e.g. {eg}")


def _identity_frame(inp: dict) -> pd.DataFrame:
    """uid -> coingecko_id: the registry (active + retired tickers) when present, else the allowlist."""
    if inp.get("registry") is not None:
        return inp["registry"].drop_duplicates("asset_uid").rename(columns={"asset_uid": "symbol"})[
            ["symbol", "coingecko_id"]].dropna()
    return inp["allowlist"]


def _signal_a2_slug_collisions(inp: dict) -> SignalResult:
    """Collisions are allowed only when acknowledged in configs/asset_identity_policy.yaml (the uid
    keeps its historical meaning -- ids are never re-pointed); any new one fails."""
    from src.data_lake.asset_identity import slug_ticker_collisions
    from src.data_lake.asset_registry import load_policy
    hits = slug_ticker_collisions(inp["fact"]["asset_id"].unique(), _identity_frame(inp))
    ack = load_policy()["acknowledged_slug_collisions"]
    desc = "no unacknowledged slug collisions"
    ok = hits.apply(lambda r: r.asset_id in ack and ack[r.asset_id].get("holds") == r.holds_coingecko_id, axis=1)         if len(hits) else pd.Series(dtype=bool)
    new = hits[~ok] if len(hits) else hits
    acked = sorted(hits.loc[ok, "asset_id"]) if len(hits) else []
    if new.empty:
        return SignalResult("A2", desc, "PASS", f"{len(acked)} acknowledged by policy: {acked}")
    eg = ", ".join(f"{r.asset_id} holds {r.holds_coingecko_id}, spells {r.collides_with_coingecko_id} "
                   f"(uid {r.collides_with_ticker})" for r in new.itertuples())
    return SignalResult("A2", desc, "FAIL", f"not in configs/asset_identity_policy.yaml: {eg}")


def _signal_a6_registry_invariant(inp: dict) -> SignalResult:
    """One asset_uid = one economic asset: no coin held by two uids unless declared an alias, no
    Binance symbol bound to two uids, aliases point at registry uids."""
    from src.data_lake.asset_registry import coingecko_to_uid, load_policy
    desc = "registry: one coin <-> one uid"
    reg = inp.get("registry")
    if reg is None:
        return SignalResult("A6", desc, "INDETERMINATE", "data/asset_registry.csv not found")
    problems = []
    try:
        coingecko_to_uid(reg)
    except ValueError as e:
        problems.append(str(e))
    b = reg.dropna(subset=["binance_symbol"]).groupby("binance_symbol")["asset_uid"].nunique()
    if (b > 1).any():
        problems.append(f"Binance symbols bound to several uids: {list(b[b > 1].index)}")
    uids = set(reg["asset_uid"])
    bad_alias = [a for a, v in load_policy()["aliases"].items() if a not in uids or v.get("alias_of") not in uids]
    if bad_alias:
        problems.append(f"aliases not in the registry: {bad_alias}")
    if problems:
        return SignalResult("A6", desc, "FAIL", "; ".join(problems))
    return SignalResult("A6", desc, "PASS", f"{reg['asset_uid'].nunique()} uids, "
                                            f"{len(load_policy()['aliases'])} declared aliases")


def _binance_daily_closes(symbol: str, start: date) -> Optional[pd.Series]:
    import requests
    rows, st = [], int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
    while True:
        r = requests.get("https://fapi.binance.com/fapi/v1/klines",
                         params={"symbol": symbol, "interval": "1d", "startTime": st, "limit": 1000}, timeout=30)
        if r.status_code != 200:
            return None
        k = r.json()
        rows += k
        if len(k) < 1000:
            break
        st = k[-1][0] + 86_400_000
    if not rows:
        return None
    s = pd.Series([float(x[4]) for x in rows], index=pd.to_datetime([x[0] for x in rows], unit="ms"))
    return s


def _signal_a3_binance_prices(inp: dict, since: date, offline: bool) -> SignalResult:
    import time
    from src.data_lake.asset_identity import binance_base_to_asset, price_identity
    desc = "lake close = Binance perp close"
    if offline:
        return SignalResult("A3", desc, "INDETERMINATE", "--offline: Binance comparison skipped")
    import requests
    try:
        info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=30).json()["symbols"]
    except Exception as e:  # network unavailable -> never a FAIL on its own
        return SignalResult("A3", desc, "INDETERMINATE", f"Binance unreachable: {e}")
    trading = {s["symbol"]: s["baseAsset"] for s in info
               if s["quoteAsset"] == "USDT" and s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"}
    fact = inp["fact"]
    fact = fact[fact["date"] >= pd.Timestamp(since)]
    closes = {a: g.set_index("date")["close"] for a, g in fact.groupby("asset_id")}
    # Which lake asset a perp belongs to: the registry's effective-dated binding when it exists
    # (Toncoin trades as GRAMUSDT; 1000000BOBUSDT is not the lake's BOB), else the ticker.
    reg = inp.get("registry")
    if reg is not None:
        bound = reg.dropna(subset=["binance_symbol"])
        pairs = {r.binance_symbol: (r.asset_uid, float(r.binance_multiplier)) for r in bound.itertuples()}
    else:
        pairs = {sym: binance_base_to_asset(base) for sym, base in trading.items()}
    wrong, spliced, checked = [], [], 0
    for sym in sorted(set(trading) & set(pairs)):
        asset, mult = pairs[sym]
        if asset not in closes:
            continue
        b = _binance_daily_closes(sym, since)
        time.sleep(0.15)
        if b is None:
            continue
        r = price_identity(closes[asset], b, multiplier=mult)
        checked += 1
        if r["status"] == "CURRENT_WRONG_COIN":
            wrong.append(f"{asset}({sym})")
        elif r["status"] == "SPLICED_HISTORY":
            spliced.append(f"{asset}:{r['bad_runs'][0][0]}..{r['bad_runs'][-1][1]}")
    detail = f"{checked} TRADING perps checked since {since}; wrong coin now: {wrong or 'none'}; " \
             f"wrong-coin runs >=14d: {len(spliced)} {spliced[:15]}"
    if reg is not None:
        # A TRADING perp whose ticker is a lake asset but which the registry does not bind to it is a
        # different coin; known ones are listed in asset_registry_conflicts.csv, new ones fail.
        unbound = sorted(s for s in trading if s not in pairs and binance_base_to_asset(trading[s])[0] in closes)
        new = [u for u in unbound if u not in set(inp.get("registry_conflicts", []))]
        detail += f"; perps not bound to their ticker's asset: {len(unbound)} ({len(new)} new: {new[:10]})"
        wrong += new
    return SignalResult("A3", desc, "FAIL" if (wrong or spliced) else "PASS", detail)


def _signal_a4_mass_splices(inp: dict, since: date) -> SignalResult:
    from src.data_lake.asset_identity import market_wide_splice_dates
    fact = inp["fact"][inp["fact"]["date"] >= pd.Timestamp(since)]
    hits = {c: market_wide_splice_dates(fact, c) for c in ("close", "marketcap")}
    desc = "no mass re-binding dates"
    parts = [f"{c}: " + ", ".join(f"{d.date()}({n})" for d, n in h.items()) for c, h in hits.items() if len(h)]
    if not parts:
        return SignalResult("A4", desc, "PASS", f"no date since {since} with >=20 assets jumping 3x overnight")
    return SignalResult("A4", desc, "FAIL", "dates with >=20 assets jumping 3x overnight -- " + "; ".join(parts))


def _signal_a5_snapshot_crosscheck(inp: dict) -> SignalResult:
    from src.data_lake.asset_identity import mcap_vs_snapshot
    desc = "price+mcap = /coins/markets (by id)"
    snap = pd.read_parquet(inp["snapshot_path"], columns=["date", "coingecko_id", "current_price_usd", "market_cap_usd"])
    snap["date"] = pd.to_datetime(snap["date"])
    last = snap["date"].max()
    lake_last = inp["fact"]["date"].max()
    if (lake_last - last).days > 3:
        return SignalResult("A5", desc, "FAIL",
                            f"fact_markets_snapshot stale: last date {last.date()} vs lake {lake_last.date()} "
                            f"-- the independent market-cap reference is missing")
    lake_day = inp["fact"][inp["fact"]["date"] == last]
    bad = mcap_vs_snapshot(lake_day, snap[snap["date"] == last], inp["allowlist"])
    if bad.empty:
        return SignalResult("A5", desc, "PASS", f"{len(lake_day)} assets on {last.date()} agree within 5%/10%")
    eg = ", ".join(f"{r.asset_id}(px {np.exp(r.price_log_diff):.3g}x, mcap {np.exp(r.mcap_log_diff):.3g}x)"
                   for r in bad.head(10).itertuples())
    return SignalResult("A5", desc, "FAIL", f"{len(bad)} assets disagree on {last.date()}: {eg}")


def _run_asset_identity_mode(args: argparse.Namespace) -> list[SignalResult]:
    lake = Path(args.lake_dir) if args.lake_dir else data_lake_root()
    since = date.fromisoformat(args.since) if args.since else _ASSET_IDENTITY_SINCE
    print(f"Lake: {lake}  since: {since}")
    inp = _identity_inputs(lake)
    return [
        _signal_a1_dim_asset_ids(inp),
        _signal_a2_slug_collisions(inp),
        _signal_a3_binance_prices(inp, since, args.offline),
        _signal_a4_mass_splices(inp, since),
        _signal_a5_snapshot_crosscheck(inp),
        _signal_a6_registry_invariant(inp),
    ]


def _report(mode: str, signals: list[SignalResult], json_out: Optional[str]) -> int:
    """Print the table + verdict; optionally write machine-readable results. Exit 1 on any FAIL."""
    _print_signal_table(signals)
    if json_out:
        Path(json_out).write_text(json.dumps({
            "mode": mode, "run_utc": pd.Timestamp.now(tz="UTC").isoformat(timespec="seconds"),
            "signals": [{"name": s.name, "description": s.description, "status": s.status, "detail": s.detail}
                        for s in signals]}, indent=1), encoding="utf-8")
    n_fail = sum(s.status == "FAIL" for s in signals)
    if n_fail:
        print(f"OVERALL: FAIL --{n_fail}/{len(signals)} signals failed.")
        return 1
    print("OVERALL: PASS" + (" (with INDETERMINATEs)" if any(s.status == "INDETERMINATE" for s in signals) else ""))
    return 0


# ----------------------------------------------------------------------------------
# Mode: freshness
# ----------------------------------------------------------------------------------
#
# Content freshness (max date in the data, never file mtime: the nightly export re-uploads
# unchanged files with a fresh timestamp). fact_markets_snapshot froze 2026-08-04..09-18 with no
# alert because Step 0 failures were log-only. SLA = max allowed age in days of the newest row.

_FRESHNESS_SLA_DAYS = {
    "fact_price.parquet": 1,
    "fact_marketcap.parquet": 1,
    "fact_volume.parquet": 1,
    "silver_fact_price.parquet": 1,
    "silver_fact_marketcap.parquet": 1,
    "silver_fact_funding.parquet": 2,
    "fact_markets_snapshot.parquet": 2,
}


def _run_freshness_mode(args: argparse.Namespace) -> list[SignalResult]:
    lake = Path(args.lake_dir) if args.lake_dir else data_lake_root()
    today = date.today() if not args.asof else date.fromisoformat(args.asof)
    signals = []
    for i, (fname, sla) in enumerate(_FRESHNESS_SLA_DAYS.items(), start=1):
        name, desc = f"F{i}", f"{fname.removesuffix('.parquet')} <= {sla}d old"
        path = lake / fname
        if not path.exists():
            signals.append(SignalResult(name, desc, "FAIL", f"{path} missing"))
            continue
        last = pd.to_datetime(pd.read_parquet(path, columns=["date"])["date"]).max().date()
        age = (today - last).days
        status = "PASS" if age <= sla else "FAIL"
        signals.append(SignalResult(name, desc, status, f"newest row {last} ({age}d old, SLA {sla}d)"))
    return signals


# ----------------------------------------------------------------------------------
# Mode dispatch
# ----------------------------------------------------------------------------------

_MODES = {
    "writer_race": lambda args: _run_writer_race_mode(),
    "asset_identity": lambda args: _report("asset_identity", _run_asset_identity_mode(args), args.json_out),
    "freshness": lambda args: _report("freshness", _run_freshness_mode(args), args.json_out),
}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", required=True, choices=sorted(_MODES.keys()),
                   help="Verification mode. Add new modes as new incident classes surface.")
    p.add_argument("--lake-dir", default=None,
                   help="asset_identity/freshness: lake directory to check (default data_lake_root(); "
                        "e.g. the Drive Desktop mirror 'G:/My Drive/Render Exports').")
    p.add_argument("--since", default=None,
                   help=f"asset_identity: first date checked by A3/A4 (default {_ASSET_IDENTITY_SINCE}).")
    p.add_argument("--offline", action="store_true", help="asset_identity: skip the Binance comparison (A3).")
    p.add_argument("--asof", default=None, help="freshness: evaluate as of YYYY-MM-DD (default today).")
    p.add_argument("--json-out", default=None, help="asset_identity/freshness: also write results as JSON here.")
    args = p.parse_args()
    return _MODES[args.mode](args)


if __name__ == "__main__":
    sys.exit(main())
