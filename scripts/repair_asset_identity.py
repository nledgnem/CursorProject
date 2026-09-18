#!/usr/bin/env python3
"""Asset-identity repair for the ticker-keyed CoinGecko fact tables (incident 2026-09-18).

The lake keys fact_price / fact_marketcap / fact_volume by asset_id (a ticker string). The coin
behind a ticker was whatever data/perp_allowlist.csv named at fetch time, so re-bindings and a
stale-vintage re-injection left other coins' rows inside several tickers' history
(reports/incidents/2026-09-18_asset_identity/README.md).

Identity model (decision 2026-09-18): asset_uid is ours and immutable -- today's asset_id string,
never re-pointed to another coin. CoinGecko id and Binance symbol are effective-dated attributes
of an asset_uid (data/asset_registry.csv).

Subcommands, in order:
  registry  Build data/asset_registry.csv (+ registry_conflicts.csv) from the allowlist, Binance
            exchangeInfo and price validation. Read-only on the lake.
  dry-run   Re-fetch every registry coin over the recoverable CoinGecko window, diff against the
            lake, write a manifest, replacement rows, quarantine keys and candidate fact tables to
            --out-dir. Never writes to the lake.
  apply     Apply a dry-run manifest to a lake directory (run on Render). Checks each segment's
            before-hash against the live rows, backs the three fact tables up, then replaces and
            quarantines. Refuses to run on any hash mismatch unless --skip-changed.

Usage:
  python scripts/repair_asset_identity.py registry --lake-dir DIR --binance-cache B --cg-cache OUT/cg_cache
  python scripts/repair_asset_identity.py dry-run  --lake-dir DIR --out-dir OUT [--env-file .env]
  python scripts/repair_asset_identity.py apply    --lake-dir /data/curated/data_lake --manifest-dir OUT
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.data_lake.asset_identity import LOG_10PCT, binance_base_to_asset, price_identity  # noqa: E402

FACT_TABLES = {"fact_price": "close", "fact_marketcap": "marketcap", "fact_volume": "volume"}
REGISTRY_PATH = REPO_ROOT / "data" / "asset_registry.csv"
ALLOWLIST_PATH = REPO_ROOT / "data" / "perp_allowlist.csv"
PRE_CUT_ALLOWLIST_PATH = REPO_ROOT / "data" / "perp_allowlist.2716_pre_universe_cut.bak.csv"
RECOVERABLE_DAYS = 725          # CoinGecko Basic depth is 730d; fetch_price_history pads the start by 2 days
MASS_WINDOW = (pd.Timestamp("2026-03-04"), pd.Timestamp("2026-03-30"))
SHORT_SEGMENT_DAYS = 7


# --------------------------------------------------------------------------------- helpers
def _load_env(env_file: str | None) -> None:
    if env_file:
        from dotenv import load_dotenv
        load_dotenv(env_file, override=False)


def _read_fact(lake: Path, table: str) -> pd.DataFrame:
    df = pd.read_parquet(lake / f"{table}.parquet")
    df["date"] = pd.to_datetime(df["date"])
    return df


def _by_asset(frames: dict[str, pd.DataFrame]) -> dict[str, dict[str, pd.DataFrame]]:
    """Per-table {asset_id: rows}, built once so per-segment work never rescans 1.7M-row tables."""
    return {t: dict(tuple(f.groupby("asset_id", sort=False))) for t, f in frames.items()}


def _segment_hash(grouped: dict[str, dict[str, pd.DataFrame]], asset_id: str, dates: list[pd.Timestamp]) -> str:
    """sha256 over the rows of all three fact tables for (asset_id, dates), order-independent."""
    h = hashlib.sha256()
    dset = pd.DatetimeIndex(dates)
    for table, col in FACT_TABLES.items():
        f = grouped[table].get(asset_id)
        if f is None:
            continue
        x = f[f["date"].isin(dset)].sort_values("date")
        for d, v in zip(x["date"], x[col]):
            h.update(f"{table}|{d.date()}|{v!r};".encode())
    return h.hexdigest()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _binance_closes(symbol: str, cache: Path) -> pd.Series | None:
    """Daily closes indexed by bar open date; cached per symbol. Frozen settlement tails dropped."""
    import requests
    f = cache / f"{symbol}.parquet"
    if f.exists():
        b = pd.read_parquet(f)
    else:
        rows, st = [], int(pd.Timestamp("2019-09-01", tz="UTC").timestamp() * 1000)
        while True:
            r = requests.get("https://fapi.binance.com/fapi/v1/klines",
                             params={"symbol": symbol, "interval": "1d", "startTime": st, "limit": 1500}, timeout=30)
            if r.status_code != 200:
                return None
            k = r.json()
            rows += k
            if len(k) < 1500:
                break
            st = k[-1][0] + 86_400_000
            time.sleep(0.2)
        b = pd.DataFrame({"bar_date": pd.to_datetime([x[0] for x in rows], unit="ms"), "close": [float(x[4]) for x in rows]})
        cache.mkdir(parents=True, exist_ok=True)
        b.to_parquet(f)
        time.sleep(0.2)
    if b.empty:
        return None
    b = b.sort_values("bar_date")
    same = (b["close"] == b["close"].iloc[-1])[::-1].cumprod()[::-1].astype(bool)
    if same.sum() >= 3:           # delisted perps keep printing the settlement price
        b = b[~same]
    return b.set_index("bar_date")["close"]


def _binance_perps() -> dict[str, dict]:
    import requests
    info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=30).json()["symbols"]
    return {s["symbol"]: s for s in info if s["quoteAsset"] == "USDT" and s["contractType"] == "PERPETUAL"}


# --------------------------------------------------------------------------------- registry
def _binding_match(bin_closes: pd.Series, mult: float, ref: pd.Series) -> tuple[int, float]:
    """(overlap days, median |log| gap) between a perp (lake-dated, per coin) and a coin's own
    CoinGecko history. The median tolerates CoinGecko's corrupt spells (e.g. 2026-01..03)."""
    b = bin_closes / mult
    b.index = b.index + pd.Timedelta(days=1)
    x = pd.concat([b, ref], axis=1, keys=["b", "r"]).dropna()
    x = x[(x["b"] > 0) & (x["r"] > 0)]
    return len(x), (float(np.log(x["b"] / x["r"]).abs().median()) if len(x) else np.nan)


def _resolve_perp_coin(base: str, last_close: float, coins_list: pd.DataFrame) -> tuple[str | None, str]:
    """CoinGecko id whose current price matches a perp's last close among coins sharing its ticker."""
    from src.providers.coingecko import coingecko_v3_get
    cands = coins_list.loc[coins_list["symbol"].str.upper() == base.upper(), "id"].tolist()[:100]
    if not cands:
        return None, "no CoinGecko coin with this ticker"
    resp = coingecko_v3_get("/coins/markets", {"vs_currency": "usd", "ids": ",".join(cands)})
    if resp.status_code != 200:
        return None, f"/coins/markets HTTP {resp.status_code}"
    hits = [(m["id"], abs(np.log(m["current_price"] / last_close))) for m in resp.json()
            if m.get("current_price") and last_close > 0]
    hits = sorted(h for h in hits if h[1] < np.log(1.05))
    return (hits[0][0], f"current price within {np.exp(hits[0][1]) - 1:.1%} of Binance; {len(cands)} candidates") \
        if len(hits) == 1 else (None, f"{len(hits)} of {len(cands)} candidates match the price")


def cmd_registry(args) -> int:
    """asset_uid = today's asset_id; one row per effective-dated (coingecko_id, binance_symbol) binding.

    A Binance perp binds to an asset_uid only if its closes match that uid's *CoinGecko coin*
    (the dry-run re-fetch in --cg-cache) over >= 14 days with a median gap <= 10%. Validating
    against the lake series instead would bless a splice: uid AI (gensyn) held Sleepless AI's
    history, which matches AIUSDT. Perps that match no uid are resolved to a CoinGecko id via
    /coins/list + /coins/markets price match and listed as conflicts for a decision.
    """
    _load_env(args.env_file)
    lake = Path(args.lake_dir)
    price = _read_fact(lake, "fact_price")
    lake_ids = set(price["asset_id"].unique())
    allow = pd.read_csv(ALLOWLIST_PATH)
    perps = _binance_perps()
    cache, cg_cache = Path(args.binance_cache), Path(args.cg_cache)
    coins_list = pd.read_json(args.coins_list) if args.coins_list else None

    # Symbols cut from the allowlist on 2026-05-05 keep their lake history; bind them to the id they were fetched with.
    cut = pd.read_csv(PRE_CUT_ALLOWLIST_PATH) if PRE_CUT_ALLOWLIST_PATH.exists() else allow.iloc[0:0]
    cut = cut[~cut["symbol"].str.upper().isin(allow["symbol"].str.upper()) & cut["symbol"].str.upper().isin(lake_ids)]
    allow = pd.concat([allow.assign(active=True), cut.assign(active=False)], ignore_index=True)
    cg_of = dict(zip(allow["symbol"].str.upper(), allow["coingecko_id"]))

    def ref_close(uid):
        f = cg_cache / f"{cg_of.get(uid)}.parquet"
        if uid not in cg_of or not f.exists():
            return None
        r = pd.read_parquet(f)
        return r.set_index("date")["close"] if len(r) else None

    bind, conflicts, bound = {}, [], set()

    def try_bind(uid, sym, s, mult, b, how):
        ref = ref_close(uid)
        if ref is None:
            return "no CoinGecko history cached for the uid's coin"
        n, med = _binding_match(b, mult, ref)
        if n < 14 or not med <= LOG_10PCT:
            return f"{how}: {n} overlapping days, median gap {np.exp(med) - 1 if n else float('nan'):.1%}"
        bound.add(sym)
        bind.setdefault(uid, []).append({
            "binance_symbol": sym, "binance_multiplier": mult,
            "valid_from": pd.to_datetime(s["onboardDate"], unit="ms").normalize().date(),
            "valid_to": b.index.max().date() if s["status"] != "TRADING" else None,
            "evidence": f"{how}; vs CoinGecko {cg_of[uid]}: {n} days, median gap {np.exp(med) - 1:.2%}"})
        return None

    failures = {}
    for sym, s in sorted(perps.items()):
        uid, mult = binance_base_to_asset(s["baseAsset"])
        b = _binance_closes(sym, cache)
        if b is None or uid not in cg_of:
            continue
        why = try_bind(uid, sym, s, mult, b, "ticker match")
        if why:
            failures[sym] = (uid, why)
    # Renamed perps (Toncoin: TONUSDT settled, GRAMUSDT listed 2026-07-02; the lake's 'GRAM' is another
    # coin): an unbound TRADING perp may belong to a uid whose own perp was delisted.
    delisted_uids = [u for u, rows in bind.items() if all(r["valid_to"] is not None for r in rows)]
    for sym, s in perps.items():
        if s["status"] != "TRADING" or sym in bound:
            continue
        b = _binance_closes(sym, cache)
        if b is None or len(b) < 30:
            continue
        mult = binance_base_to_asset(s["baseAsset"])[1]
        for uid in delisted_uids:
            if try_bind(uid, sym, s, mult, b, "renamed perp") is None:
                failures.pop(sym, None)
                break
    for sym, (uid, why) in sorted(failures.items()):
        s = perps[sym]
        if s["status"] != "TRADING":
            continue
        rec = {"binance_symbol": sym, "ticker_asset_uid": uid, "uid_coingecko_id": cg_of.get(uid), "why_not_bound": why}
        if coins_list is not None:
            b = _binance_closes(sym, cache)
            rec["proposed_coingecko_id"], rec["resolution"] = _resolve_perp_coin(
                binance_base_to_asset(s["baseAsset"])[0], float(b.iloc[-1]) / binance_base_to_asset(s["baseAsset"])[1],
                coins_list)
        conflicts.append(rec)

    rows = []
    for a in allow.itertuples():
        base = {"asset_uid": a.symbol.upper(), "coingecko_id": a.coingecko_id, "active": a.active,
                "cg_valid_from": None, "cg_valid_to": None}
        for bnd in bind.get(a.symbol.upper(), [None]):
            rows.append({**base, **(bnd or {"binance_symbol": None, "binance_multiplier": None, "valid_from": None,
                                            "valid_to": None, "evidence": "no Binance USDT perp validated"})})
    reg = pd.DataFrame(rows).sort_values(["asset_uid", "valid_from"], na_position="first")
    dup = reg.dropna(subset=["binance_symbol"]).groupby("binance_symbol")["asset_uid"].nunique()
    assert (dup <= 1).all(), f"a Binance symbol bound to several uids: {dup[dup > 1].to_dict()}"
    reg.to_csv(args.registry_out, index=False)
    pd.DataFrame(conflicts).to_csv(Path(args.registry_out).with_name("asset_registry_conflicts.csv"), index=False)
    print(f"registry: {reg.asset_uid.nunique()} asset_uids, {reg.binance_symbol.notna().sum()} Binance bindings, "
          f"{len(conflicts)} TRADING perps not bound (conflicts) -> {args.registry_out}")
    return 0


# --------------------------------------------------------------------------------- dry-run
def _refetch(cg_id: str, start: date, end: date, cache: Path) -> pd.DataFrame:
    """CoinGecko market_chart/range over >90 days (daily 00:00 UTC points, the lake's convention)."""
    f = cache / f"{cg_id}.parquet"
    if f.exists():
        return pd.read_parquet(f)
    from src.providers.coingecko import fetch_price_history
    p, m, v = fetch_price_history(cg_id, start, end)
    df = pd.DataFrame({"close": pd.Series(p), "marketcap": pd.Series(m), "volume": pd.Series(v)})
    df.index = pd.to_datetime(df.index)
    df = df.rename_axis("date").reset_index()
    cache.mkdir(parents=True, exist_ok=True)
    df.to_parquet(f)
    return df


def _runs(dates: pd.DatetimeIndex, max_gap: int = 2) -> list[list[pd.Timestamp]]:
    out, cur = [], []
    for d in sorted(dates):
        if cur and (d - cur[-1]).days > max_gap + 1:
            out.append(cur); cur = []
        cur.append(d)
    if cur:
        out.append(cur)
    return out


def _binance_series(reg_rows: pd.DataFrame, cache: Path) -> pd.Series | None:
    """A uid's Binance closes across all its bindings, re-stamped to the lake's date convention
    (lake date d = UTC close of d-1) and divided by the contract multiplier."""
    parts = []
    for b in reg_rows[reg_rows["binance_symbol"].notna()].itertuples():
        s = _binance_closes(b.binance_symbol, cache)
        if s is None:
            continue
        s = s / float(b.binance_multiplier)
        s.index = s.index + pd.Timedelta(days=1)
        parts.append(s)
    if not parts:
        return None
    out = pd.concat(parts).sort_index()
    return out[~out.index.duplicated(keep="last")]


def _mass_window_signature(g: pd.DataFrame) -> tuple[bool, bool]:
    """(price, market cap) jump >3x into 2026-03-04 AND back out on 2026-03-31: the stale-wide-file
    re-injection fingerprint. g: one asset's rows indexed by date."""
    e = [MASS_WINDOW[0] - pd.Timedelta(days=1), MASS_WINDOW[0], MASS_WINDOW[1], MASS_WINDOW[1] + pd.Timedelta(days=1)]
    if not all(d in g.index for d in e):
        return False, False

    def jump(c, x, y):
        a, b = g.loc[x, c], g.loc[y, c]
        return bool(a > 0 and b > 0 and abs(np.log(b / a)) > np.log(3))
    return tuple(jump(c, e[0], e[1]) and jump(c, e[2], e[3]) for c in ("close", "marketcap"))


def _near(a: float, b: float, tol: float) -> bool:
    return bool(pd.notna(a) and pd.notna(b) and a > 0 and b > 0 and abs(np.log(a / b)) <= np.log(tol))


def cmd_dry_run(args) -> int:
    """Flag wrong-coin rows on independent evidence, then accept a CoinGecko re-fetch as the
    replacement only where it is itself validated; everything else is quarantined, never invented.

    Evidence that a lake row is another coin:
      - price: disagrees with the uid's Binance perp by >10% (lake date d vs bar d-1); or, with no
        perp, the 2026-03-04..30 re-injection signature.
      - market cap only (price right): the re-injection signature in market cap -- bridged variants
        share the price but carry a tiny cap and volume (ETH/SOL/DOGE caps ~$2M).
    A re-fetched value is accepted only if its price agrees with Binance (perps) or it joins the
    lake's good rows either side of the segment within 1.5x (no perp); a re-fetched cap only if its
    implied supply is within 1.25x of the lake's supply just outside the window. CoinGecko's own
    history is not trusted blindly: in 2026-01..03 it is corrupt for some coins (1inch 0.156 vs
    Binance 0.0925 on 2026-03-22) while the lake, captured live, is right.
    """
    _load_env(args.env_file)
    lake, out = Path(args.lake_dir), Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    asof = pd.Timestamp(args.asof) if args.asof else pd.Timestamp(datetime.now(timezone.utc).date())
    w_start = (asof - pd.Timedelta(days=RECOVERABLE_DAYS)).normalize()
    w_end = asof - pd.Timedelta(days=2)       # the latest rows are fresh; today's CG point is intraday
    frames = {t: _read_fact(lake, t) for t in FACT_TABLES}
    grouped = _by_asset(frames)
    reg = pd.read_csv(args.registry)
    reg_by = dict(tuple(reg.groupby("asset_uid")))
    ids = reg.drop_duplicates("asset_uid").set_index("asset_uid")["coingecko_id"]
    fp = frames["fact_price"]
    in_window = set(fp.loc[fp["date"] >= w_start, "asset_id"])
    todo = [u for u in ids.index if u in in_window]
    lk = frames["fact_price"].merge(frames["fact_marketcap"], on=["asset_id", "date"], how="outer") \
        .merge(frames["fact_volume"], on=["asset_id", "date"], how="outer")
    lk_by = {u: g.set_index("date").sort_index() for u, g in lk.groupby("asset_id", sort=False)}
    cache_b = Path(args.binance_cache)
    print(f"window {w_start.date()}..{w_end.date()}; {len(todo)} registry coins with rows in it")

    segs, repl, quar, cg_defects, noise = [], [], [], [], []
    calls = 0

    def add_segment(uid, cg, cls, days, tables, evidence, apply_default, accept, ref):
        sid = f"{uid}:{days[0].date()}:{days[-1].date()}:{tables}"
        n_rep = n_q = 0
        cols = FACT_TABLES if tables == "all" else {"fact_marketcap": "marketcap", "fact_volume": "volume"}
        for d in days:
            for t, c in cols.items():
                ok = accept.get(d) and ref is not None and d in ref.index
                v = ref.at[d, c] if ok else np.nan
                if pd.notna(v) and v > 0:
                    repl.append({"seg_id": sid, "asset_id": uid, "date": d, "table": t, "value": float(v)})
                    n_rep += 1
                else:
                    quar.append({"seg_id": sid, "asset_id": uid, "date": d, "table": t, "reason": evidence})
                    n_q += 1
        segs.append({"seg_id": sid, "asset_uid": uid, "coingecko_id": cg, "class": cls, "tables": tables,
                     "start": days[0].date(), "end": days[-1].date(), "n_days": len(days),
                     "n_rows_replace": n_rep, "n_rows_quarantine": n_q, "evidence": evidence,
                     "apply_default": apply_default, "before_hash": _segment_hash(grouped, uid, days)})

    for i, uid in enumerate(todo):
        cg = ids[uid]
        cached = (out / "cg_cache" / f"{cg}.parquet").exists()
        ref = _refetch(cg, w_start.date(), asof.date(), out / "cg_cache")
        calls += 0 if cached else 1
        if i % 200 == 0:
            print(f"  [{i}/{len(todo)}] {uid} ({cg}) calls={calls}", flush=True)
        g = lk_by[uid]
        a = g[(g.index >= w_start) & (g.index <= w_end)]
        if a.empty:
            continue
        r = ref.set_index("date") if len(ref) else pd.DataFrame(columns=["close", "marketcap", "volume"])
        r = r[~r.index.duplicated(keep="last")]
        bser = _binance_series(reg_by[uid], cache_b)
        j = a[["close", "marketcap", "volume"]].join(r[["close", "marketcap", "volume"]], rsuffix="_ref")
        j["bin"] = bser.reindex(j.index) if bser is not None else np.nan
        has_b = (j["bin"] > 0).fillna(False)
        lake_bad = has_b & (np.log(j["close"] / j["bin"]).abs() > LOG_10PCT).fillna(False)
        ref_ok = has_b & (np.log(j["close_ref"] / j["bin"]).abs() <= LOG_10PCT).fillna(False)
        in_mw = pd.Series((j.index >= MASS_WINDOW[0]) & (j.index <= MASS_WINDOW[1]), j.index)
        sig_px, sig_mc = _mass_window_signature(g)
        # Candidate price days -> keep only segments that look like another coin rather than
        # timing noise (lake 00:00 snapshot vs Binance close differ >10% on some crash days):
        # the March signature, or 3+ days with a median gap >10%, or any day >2x off.
        lr_b = np.log(j["close"] / j["bin"]).abs()
        cand = lake_bad | (~has_b & in_mw & sig_px)
        px_wrong = pd.Series(False, j.index)
        for seg in _runs(j.index[cand.values]):
            strong = (sig_px and all(MASS_WINDOW[0] <= d <= MASS_WINDOW[1] for d in seg)) or                      (len(seg) >= 3 and lr_b.loc[seg].median() > LOG_10PCT) or (lr_b.loc[seg].max() > np.log(2))
            if strong:
                px_wrong.loc[seg] = True
            else:
                noise.append({"asset_uid": uid, "start": seg[0].date(), "end": seg[-1].date(), "n_days": len(seg),
                              "max_gap_vs_binance": float(np.exp(lr_b.loc[seg].max()))})
        # Rows dated before CoinGecko has any history for this coin, with no perp to check: another
        # coin held the ticker. Applied by default only with a >3x break at the hand-over.
        first_ref = r.index[r["close"] > 0].min() if len(r) else pd.NaT
        predates = pd.Series(False, j.index)
        if pd.notna(first_ref) and first_ref > w_start + pd.Timedelta(days=2):
            predates = (j.index < first_ref) & ~has_b & (j["close"] > 0)
        if predates.any():
            days = list(j.index[predates.values])
            last_lake = j.loc[days[-1], "close"]
            brk = not _near(last_lake, r.loc[first_ref, "close"], 3.0)
            add_segment(uid, cg, "predates_coin", days, "all",
                        f"lake rows before CoinGecko history of {cg} starts ({first_ref.date()}); "
                        f"hand-over break {'>3x' if brk else '<=3x'}", brk, {}, None)
        mc_wrong = ~px_wrong & in_mw & sig_mc
        # the lake agrees with Binance but CoinGecko's current history does not: evidence that a
        # re-fetch is not a safe repair source by itself (reported, never applied)
        cg_bad = has_b & ~lake_bad & (np.log(j["close_ref"] / j["bin"]).abs() > LOG_10PCT).fillna(False)
        if cg_bad.any():
            cg_defects.append({"asset_uid": uid, "coingecko_id": cg, "days": int(cg_bad.sum()),
                               "first": cg_bad[cg_bad].index.min().date(), "last": cg_bad[cg_bad].index.max().date()})
        good = j.index[(~px_wrong & (j["close"] > 0)).values]
        for seg in _runs(j.index[px_wrong.values]):
            lo, hi = good[good < seg[0]], good[good > seg[-1]]
            nb = [d for d in seg if not has_b.get(d, False)]
            cont = bool(nb) and all([
                _near(j.at[nb[0], "close_ref"], j.at[lo[-1], "close"], 1.5) if len(lo) else True,
                _near(j.at[nb[-1], "close_ref"], j.at[hi[0], "close"], 1.5) if len(hi) else True,
                not (np.log(j.loc[nb, "close_ref"]).diff().abs() > np.log(3)).any()])
            accept = {d: bool(ref_ok.get(d, False)) if has_b.get(d, False) else cont for d in seg}
            in_m = seg[0] >= MASS_WINDOW[0] and seg[-1] <= MASS_WINDOW[1]
            cls = "mass_window_2026_03" if in_m else (
                "long_splice" if (seg[-1] - seg[0]).days + 1 >= SHORT_SEGMENT_DAYS else "short_splice")
            ev = "price vs Binance >10%" if lake_bad.loc[seg].any() else "re-injection signature (price), no perp"
            add_segment(uid, cg, cls, seg, "all", ev, True, accept, r)
        if mc_wrong.any():
            seg = list(j.index[mc_wrong.values])
            sup = (j["marketcap"] / j["close"]).where((j["marketcap"] > 0) & (j["close"] > 0))
            near_mw = (j.index >= MASS_WINDOW[0] - pd.Timedelta(days=5)) & (j.index <= MASS_WINDOW[1] + pd.Timedelta(days=5))
            anchor = sup[(~in_mw).values & near_mw].median()
            rsup = j["marketcap_ref"] / j["close_ref"]
            accept = {d: _near(rsup.get(d, np.nan), anchor, 1.25) for d in seg}
            add_segment(uid, cg, "mass_window_2026_03", seg, "mcap_volume",
                        "re-injection signature (market cap), price right", True, accept, r)

    # Pre-window history: Binance-evidenced wrong-coin runs cannot be re-fetched -> quarantine.
    for uid, rows in reg_by.items():
        bser = _binance_series(rows, cache_b)
        if bser is None or uid not in lk_by:
            continue
        a = lk_by[uid]["close"]
        a = a[(a.index < w_start) & (a > 0)]
        x = pd.concat([a, bser], axis=1, keys=["l", "b"]).dropna()
        if len(x) < 7:
            continue
        smooth = np.log(x["l"] / x["b"]).abs().rolling(7, center=True, min_periods=3).median()
        for seg in _runs(x.index[(smooth > LOG_10PCT).values], max_gap=0):
            if len(seg) >= 14:
                add_segment(uid, ids.get(uid), "pre_window_unrecoverable", seg, "all",
                            "price vs Binance >10% for 14+ days; outside CoinGecko recoverable window", True, {}, None)

    # Lake assets outside the registry (no CoinGecko id to re-fetch) with the re-injection signature.
    for uid, g in lk_by.items():
        if uid in ids.index:
            continue
        sig_px, sig_mc = _mass_window_signature(g)
        if sig_px or sig_mc:
            seg = list(g.loc[MASS_WINDOW[0]:MASS_WINDOW[1]].index)
            add_segment(uid, None, "mass_window_2026_03", seg, "all" if sig_px else "mcap_volume",
                        "re-injection signature; not in registry, not re-fetchable", True, {}, None)

    seg_df, repl_df, quar_df = pd.DataFrame(segs), pd.DataFrame(repl), pd.DataFrame(quar)
    seg_df.to_csv(out / "manifest_segments.csv", index=False)
    repl_df.to_parquet(out / "replacement_rows.parquet", index=False)
    quar_df.to_parquet(out / "quarantine_keys.parquet", index=False)
    pd.DataFrame(cg_defects).to_csv(out / "coingecko_history_defects.csv", index=False)
    pd.DataFrame(noise).to_csv(out / "timing_noise_not_applied.csv", index=False)

    cand = out / "candidate"
    cand.mkdir(exist_ok=True)
    chosen = set(seg_df.loc[seg_df["apply_default"], "seg_id"]) if len(seg_df) else set()
    summary = {"asof": str(asof.date()), "window": [str(w_start.date()), str(w_end.date())],
               "coingecko_calls": calls,
               "segments_by_class": seg_df.groupby("class").size().to_dict() if len(seg_df) else {},
               "assets_affected": int(seg_df["asset_uid"].nunique()) if len(seg_df) else 0,
               "coingecko_history_defect_assets": len(cg_defects), "tables": {}}
    for table, col in FACT_TABLES.items():
        before = frames[table]
        after, n_rep, n_q = _apply_to_frame(before, table, col, repl_df, quar_df, chosen)
        path = cand / f"{table}.parquet"
        _write_like(after, lake / f"{table}.parquet", path)
        summary["tables"][table] = {"rows_before": len(before), "rows_after": len(after), "rows_replaced": n_rep,
                                    "rows_quarantined": n_q, "sha256_before": _file_sha256(lake / f"{table}.parquet"),
                                    "sha256_after": _file_sha256(path)}
    (out / "summary.json").write_text(json.dumps(summary, indent=1, default=str))
    print(json.dumps(summary, indent=1, default=str))
    return 0


def _apply_to_frame(before: pd.DataFrame, table: str, col: str, repl: pd.DataFrame, quar: pd.DataFrame,
                    chosen: set) -> tuple[pd.DataFrame, int, int]:
    df = before.copy()
    n_q = 0
    q = quar[quar["seg_id"].isin(chosen) & (quar["table"] == table)] if len(quar) else quar
    if len(q):
        key = pd.MultiIndex.from_frame(q[["asset_id", "date"]].assign(date=pd.to_datetime(q["date"])))
        drop = pd.MultiIndex.from_frame(df[["asset_id", "date"]]).isin(key)
        n_q = int(drop.sum())
        df = df[~drop]
    n_rep = 0
    r = repl[repl["seg_id"].isin(chosen) & (repl["table"] == table)] if len(repl) else repl
    if len(r):
        r = r.assign(date=pd.to_datetime(r["date"])).set_index(["asset_id", "date"])["value"]
        idx = pd.MultiIndex.from_frame(df[["asset_id", "date"]])
        hit = idx.isin(r.index)
        df.loc[hit, col] = r.reindex(idx[hit]).values
        n_rep = int(hit.sum())
    return df, n_rep, n_q


def _write_like(df: pd.DataFrame, template: Path, path: Path) -> None:
    """Write with the template's arrow schema (date32, string, double) and atomically replace."""
    schema = pq.read_schema(template).remove_metadata()
    out = df.copy()
    out["date"] = out["date"].dt.date
    out = out.sort_values(["date", "asset_id"])[schema.names]
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(pa.Table.from_pandas(out, schema=schema, preserve_index=False), tmp)
    tmp.replace(path)


# --------------------------------------------------------------------------------- apply
def cmd_apply(args) -> int:
    lake, man = Path(args.lake_dir), Path(args.manifest_dir)
    seg = pd.read_csv(man / "manifest_segments.csv")
    repl = pd.read_parquet(man / "replacement_rows.parquet")
    quar = pd.read_parquet(man / "quarantine_keys.parquet")
    classes = set(args.classes.split(",")) if args.classes else None
    seg = seg[seg["class"].isin(classes)] if classes else seg[seg["apply_default"]]
    frames = {t: _read_fact(lake, t) for t in FACT_TABLES}
    grouped = _by_asset(frames)

    changed = []
    for s in seg.itertuples():
        dates = sorted(set(pd.to_datetime(repl.loc[repl["seg_id"] == s.seg_id, "date"]))
                       | set(pd.to_datetime(quar.loc[quar["seg_id"] == s.seg_id, "date"])))
        if _segment_hash(grouped, s.asset_uid, dates) != s.before_hash:
            changed.append(s.seg_id)
    if changed and not args.skip_changed:
        print(f"ABORT: {len(changed)} segments changed since the dry run (e.g. {changed[:5]}). "
              "Re-run dry-run against the current lake, or pass --skip-changed.")
        return 1
    chosen = set(seg["seg_id"]) - set(changed)
    print(f"applying {len(chosen)} segments ({len(changed)} skipped as changed)")
    if not args.yes:
        print("dry confirmation only: re-run with --yes to write.")
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = lake / f"_backup_asset_identity_{stamp}"
    backup.mkdir()
    for t in FACT_TABLES:
        shutil.copy2(lake / f"{t}.parquet", backup / f"{t}.parquet")
    print(f"backup -> {backup}")

    q_rows = []
    for table, col in FACT_TABLES.items():
        before = frames[table]
        qk = quar[quar["seg_id"].isin(chosen) & (quar["table"] == table)].drop(columns="table")
        key = pd.MultiIndex.from_frame(qk[["asset_id", "date"]].assign(date=pd.to_datetime(qk["date"])))
        moved = before[pd.MultiIndex.from_frame(before[["asset_id", "date"]]).isin(key)]
        q_rows.append(moved.rename(columns={col: "value"}).assign(table=table)
                      .merge(qk.assign(date=pd.to_datetime(qk["date"])), on=["asset_id", "date"], how="left"))
        after, n_rep, n_q = _apply_to_frame(before, table, col, repl, quar, chosen)
        _write_like(after, lake / f"{table}.parquet", lake / f"{table}.parquet")
        print(f"  {table}: replaced {n_rep}, quarantined {n_q}, rows {len(before)} -> {len(after)}")
    qpath = lake / "quarantine_fact_identity.parquet"
    qdf = pd.concat(q_rows, ignore_index=True).assign(quarantined_utc=stamp)
    if len(qdf):
        if qpath.exists():
            qdf = pd.concat([pd.read_parquet(qpath), qdf], ignore_index=True)
        qdf.assign(date=pd.to_datetime(qdf["date"]).dt.date).to_parquet(qpath, index=False)
        print(f"quarantine -> {qpath} ({len(qdf)} rows total)")
    print("Next: python scripts/data_ingestion/build_silver_layer.py && "
          "python scripts/verify_ingestion_integrity.py --mode asset_identity")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("registry")
    r.add_argument("--lake-dir", required=True)
    r.add_argument("--registry-out", default=str(REGISTRY_PATH))
    r.add_argument("--binance-cache", required=True)
    r.add_argument("--cg-cache", required=True, help="dry-run's cg_cache/ (each coin's re-fetched CoinGecko history)")
    r.add_argument("--coins-list", default=None, help="CoinGecko /coins/list JSON, to propose ids for unbound perps")
    r.add_argument("--env-file", default=str(REPO_ROOT / ".env"))
    d = sub.add_parser("dry-run")
    d.add_argument("--lake-dir", required=True)
    d.add_argument("--out-dir", required=True)
    d.add_argument("--registry", default=str(REGISTRY_PATH))
    d.add_argument("--binance-cache", required=True)
    d.add_argument("--env-file", default=str(REPO_ROOT / ".env"))
    d.add_argument("--asof", default=None, help="YYYY-MM-DD; default today UTC")
    d.add_argument("--quarantine-unfetchable", action="store_true",
                   help="quarantine in-window rows of coins CoinGecko returns nothing for (default: leave)")
    a = sub.add_parser("apply")
    a.add_argument("--lake-dir", required=True)
    a.add_argument("--manifest-dir", required=True)
    a.add_argument("--classes", default=None, help="comma list of segment classes (default: apply_default)")
    a.add_argument("--skip-changed", action="store_true")
    a.add_argument("--yes", action="store_true")
    args = p.parse_args()
    return {"registry": cmd_registry, "dry-run": cmd_dry_run, "apply": cmd_apply}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
