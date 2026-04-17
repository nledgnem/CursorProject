from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests
import yaml

from src.utils.ticker_normalization import build_normalization_rules, normalize_ticker
from repo_paths import data_lake_root


@dataclass(frozen=True)
class PerpVenueConfig:
    base_url: str
    timeout_seconds: int = 20


@dataclass(frozen=True)
class PerpListingsConfig:
    hyperliquid: PerpVenueConfig
    hyperliquid_meta_payload: dict[str, Any]
    variational: PerpVenueConfig
    variational_stats_path: str
    curated_data_lake_dir: Path
    perps_hyperliquid_csv: Path
    perps_variational_csv: Path
    perp_ticker_mapping_csv: Path
    perp_coverage_summary_csv: Path
    normalization_strip_suffixes: tuple[str, ...]
    normalization_strip_prefixes: tuple[str, ...]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _snapshot_date_utc() -> str:
    # Stored as ISO date string for stable append-only CSV partitioning.
    return _utc_now().date().isoformat()


def load_perp_listings_config(repo_root: Path) -> PerpListingsConfig:
    cfg_path = repo_root / "configs" / "perp_listings.yaml"
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))

    out = raw.get("output", {}) or {}
    curated_dir_raw = out.get("curated_data_lake_dir")
    if curated_dir_raw:
        curated_dir = Path(str(curated_dir_raw))
        curated_dir = (repo_root / curated_dir).resolve() if not curated_dir.is_absolute() else curated_dir.resolve()
    else:
        curated_dir = data_lake_root()

    def _p(name: str) -> Path:
        return (curated_dir / str(out[name])).resolve()

    hyper = raw.get("hyperliquid", {}) or {}
    var = raw.get("variational", {}) or {}
    norm = raw.get("ticker_normalization", {}) or {}

    return PerpListingsConfig(
        hyperliquid=PerpVenueConfig(
            base_url=str(hyper["base_url"]),
            timeout_seconds=int(hyper.get("timeout_seconds", 20)),
        ),
        hyperliquid_meta_payload=dict(hyper.get("meta_payload", {"type": "meta"})),
        variational=PerpVenueConfig(
            base_url=str(var["base_url"]),
            timeout_seconds=int(var.get("timeout_seconds", 20)),
        ),
        variational_stats_path=str(var.get("stats_path", "/metadata/stats")),
        curated_data_lake_dir=curated_dir,
        perps_hyperliquid_csv=_p("perps_hyperliquid_csv"),
        perps_variational_csv=_p("perps_variational_csv"),
        perp_ticker_mapping_csv=_p("perp_ticker_mapping_csv"),
        perp_coverage_summary_csv=_p("perp_coverage_summary_csv"),
        normalization_strip_suffixes=tuple(norm.get("strip_suffixes", []) or []),
        normalization_strip_prefixes=tuple(norm.get("strip_prefixes", []) or []),
    )


def _csv_has_snapshot(path: Path, snapshot_date_utc: str) -> bool:
    if not path.exists():
        return False
    try:
        df = pd.read_csv(path)
        if "snapshot_date_utc" not in df.columns:
            return False
        return (df["snapshot_date_utc"].astype(str) == snapshot_date_utc).any()
    except Exception:
        logging.exception("Failed reading %s for snapshot check.", path)
        return False


def _append_csv_rows(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    n = 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})
            n += 1
    return n


def fetch_hyperliquid_meta(cfg: PerpListingsConfig) -> dict[str, Any]:
    url = f"{cfg.hyperliquid.base_url.rstrip('/')}/info"
    resp = requests.post(url, json=cfg.hyperliquid_meta_payload, timeout=cfg.hyperliquid.timeout_seconds)
    resp.raise_for_status()
    return resp.json()


def parse_hyperliquid_perps(meta: dict[str, Any], snapshot_date_utc: str) -> pd.DataFrame:
    """
    Hyperliquid meta format includes `universe` (perp markets).
    We store a minimal sizing-oriented snapshot.
    """
    universe = meta.get("universe")
    if not isinstance(universe, list):
        raise ValueError("Hyperliquid meta missing expected `universe` list.")

    rows: list[dict[str, Any]] = []
    for m in universe:
        if not isinstance(m, dict):
            continue
        name = m.get("name")
        if not name:
            continue
        max_lev = m.get("maxLeverage")
        sz_dec = m.get("szDecimals")
        min_order_size = None
        if isinstance(sz_dec, int):
            # Size granularity: 10^-szDecimals. Stored as float for convenience.
            min_order_size = 10 ** (-sz_dec)

        rows.append(
            {
                "snapshot_date_utc": snapshot_date_utc,
                "ticker": str(name).upper(),
                "is_listed": True,
                "max_leverage": float(max_lev) if max_lev is not None else None,
                "min_order_size": float(min_order_size) if min_order_size is not None else None,
                "sz_decimals": sz_dec,
                "px_decimals": m.get("maxPxDecimals"),
                "raw_metadata_json": json.dumps(m, ensure_ascii=False),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["snapshot_date_utc", "ticker"]).sort_values(["snapshot_date_utc", "ticker"])
    return df.reset_index(drop=True)


def fetch_variational_stats(cfg: PerpListingsConfig) -> dict[str, Any]:
    url = f"{cfg.variational.base_url.rstrip('/')}{cfg.variational_stats_path}"
    resp = requests.get(url, timeout=cfg.variational.timeout_seconds)
    resp.raise_for_status()
    return resp.json()


def parse_variational_perps(stats: dict[str, Any], snapshot_date_utc: str) -> pd.DataFrame:
    listings = stats.get("listings")
    if not isinstance(listings, list):
        raise ValueError("Variational stats missing expected `listings` array.")

    rows: list[dict[str, Any]] = []
    for m in listings:
        if not isinstance(m, dict):
            continue
        tkr = m.get("ticker")
        if not tkr:
            continue
        rows.append(
            {
                "snapshot_date_utc": snapshot_date_utc,
                "ticker": str(tkr).upper(),
                "is_listed": True,
                "max_leverage": None,  # Not exposed in public stats docs.
                "min_order_size": None,  # Not exposed in public stats docs.
                "funding_interval_s": m.get("funding_interval_s"),
                "base_spread_bps": m.get("base_spread_bps"),
                "raw_metadata_json": json.dumps(m, ensure_ascii=False),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["snapshot_date_utc", "ticker"]).sort_values(["snapshot_date_utc", "ticker"])
    return df.reset_index(drop=True)


def _load_panel_tickers_from_curated_lake(curated_dir: Path) -> list[str]:
    """
    Panel tickers are sourced strictly from the curated data lake.

    We use `fact_price.parquet` as the canonical set of assets present in the daily panel.
    """
    fact_price_path = curated_dir / "fact_price.parquet"
    if not fact_price_path.exists():
        raise FileNotFoundError(f"Missing curated lake file: {fact_price_path}")
    df = pd.read_parquet(fact_price_path, columns=["asset_id"])
    tickers = sorted({str(x).upper() for x in df["asset_id"].dropna().unique().tolist()})
    return tickers


def build_perp_ticker_mapping(
    panel_tickers: list[str],
    venue_tickers_by_venue: dict[str, list[str]],
    strip_suffixes: tuple[str, ...],
    strip_prefixes: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (mapping_rows, unmatched_venue_rows).

    - mapping_rows: panel_ticker, venue, venue_ticker
    - unmatched_venue_rows: venue_ticker that could not be matched to panel universe
    """
    rules = build_normalization_rules(strip_suffixes, strip_prefixes)
    panel_norm_to_panel: dict[str, str] = {}
    for p in panel_tickers:
        panel_norm_to_panel[normalize_ticker(p, rules)] = p.upper()

    mapping: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []

    for venue, venue_tickers in venue_tickers_by_venue.items():
        for vt in venue_tickers:
            vt_norm = normalize_ticker(vt, rules)
            panel = panel_norm_to_panel.get(vt_norm)
            if panel:
                mapping.append({"panel_ticker": panel, "venue": venue, "venue_ticker": vt})
            else:
                unmatched.append({"panel_ticker": "", "venue": venue, "venue_ticker": vt})

    map_df = pd.DataFrame(mapping).drop_duplicates(subset=["panel_ticker", "venue", "venue_ticker"])
    un_df = pd.DataFrame(unmatched).drop_duplicates(subset=["venue", "venue_ticker"])
    return map_df, un_df


def _load_existing_mapping(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["panel_ticker", "venue", "venue_ticker"])
    df = pd.read_csv(path, dtype=str).fillna("")
    for c in ("panel_ticker", "venue", "venue_ticker"):
        if c not in df.columns:
            df[c] = ""
    return df[["panel_ticker", "venue", "venue_ticker"]]


def update_mapping_append_only(
    path: Path,
    new_rows: pd.DataFrame,
) -> int:
    existing = _load_existing_mapping(path)
    if new_rows.empty:
        return 0
    merged = pd.concat([existing, new_rows], ignore_index=True)
    merged = merged.drop_duplicates(subset=["panel_ticker", "venue", "venue_ticker"])

    # Append-only: write only rows that are not already present.
    existing_keys = set(zip(existing["panel_ticker"], existing["venue"], existing["venue_ticker"]))
    to_append = []
    for _, r in new_rows.iterrows():
        key = (str(r.get("panel_ticker", "")), str(r.get("venue", "")), str(r.get("venue_ticker", "")))
        if key in existing_keys:
            continue
        to_append.append({"panel_ticker": key[0], "venue": key[1], "venue_ticker": key[2]})

    if not to_append:
        return 0

    return _append_csv_rows(path, to_append, fieldnames=["panel_ticker", "venue", "venue_ticker"])


def build_coverage_summary(
    snapshot_date_utc: str,
    panel_tickers: list[str],
    listed_by_venue: dict[str, set[str]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    hl = listed_by_venue.get("hyperliquid", set())
    var = listed_by_venue.get("variational", set())
    for t in panel_tickers:
        rows.append(
            {
                "snapshot_date_utc": snapshot_date_utc,
                "panel_ticker": t,
                "hyperliquid": bool(t in hl),
                "variational": bool(t in var),
            }
        )
    return pd.DataFrame(rows)


def append_coverage_summary(path: Path, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    fieldnames = ["snapshot_date_utc", "panel_ticker", "hyperliquid", "variational"]
    rows = df.to_dict(orient="records")
    return _append_csv_rows(path, rows, fieldnames=fieldnames)


def append_perps_snapshot_csv(path: Path, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    # Keep required columns first; allow extra metadata columns.
    base_cols = ["snapshot_date_utc", "ticker", "is_listed", "max_leverage", "min_order_size"]
    extra_cols = [c for c in df.columns if c not in base_cols]
    fieldnames = base_cols + extra_cols
    rows = df.to_dict(orient="records")
    return _append_csv_rows(path, rows, fieldnames=fieldnames)


def run_daily_perp_ingestion(repo_root: Path) -> None:
    cfg = load_perp_listings_config(repo_root)
    snap = _snapshot_date_utc()

    # If we already produced the coverage summary for today, treat the whole ingestion as done.
    if _csv_has_snapshot(cfg.perp_coverage_summary_csv, snap):
        logging.info("Perp listings snapshot already exists for today UTC=%s. Skipping.", snap)
        return

    panel_tickers = _load_panel_tickers_from_curated_lake(cfg.curated_data_lake_dir)

    # Hyperliquid
    try:
        meta = fetch_hyperliquid_meta(cfg)
        hl_df = parse_hyperliquid_perps(meta, snap)
        n_hl = append_perps_snapshot_csv(cfg.perps_hyperliquid_csv, hl_df)
        logging.info("Hyperliquid perps: appended %s rows for snapshot UTC=%s.", n_hl, snap)
    except Exception:
        logging.exception("Hyperliquid perp ingestion failed for snapshot UTC=%s.", snap)
        hl_df = pd.DataFrame(columns=["snapshot_date_utc", "ticker"])

    # Variational
    try:
        stats = fetch_variational_stats(cfg)
        var_df = parse_variational_perps(stats, snap)
        n_var = append_perps_snapshot_csv(cfg.perps_variational_csv, var_df)
        logging.info("Variational perps: appended %s rows for snapshot UTC=%s.", n_var, snap)
    except Exception:
        logging.exception("Variational perp ingestion failed for snapshot UTC=%s.", snap)
        var_df = pd.DataFrame(columns=["snapshot_date_utc", "ticker"])

    # Build mapping + coverage from today's snapshots (even if one venue failed).
    hl_tickers = sorted({str(x).upper() for x in hl_df.get("ticker", pd.Series(dtype=str)).dropna().tolist()})
    var_tickers = sorted({str(x).upper() for x in var_df.get("ticker", pd.Series(dtype=str)).dropna().tolist()})

    map_df, un_df = build_perp_ticker_mapping(
        panel_tickers=panel_tickers,
        venue_tickers_by_venue={"hyperliquid": hl_tickers, "variational": var_tickers},
        strip_suffixes=cfg.normalization_strip_suffixes,
        strip_prefixes=cfg.normalization_strip_prefixes,
    )

    appended_map = update_mapping_append_only(cfg.perp_ticker_mapping_csv, map_df)
    appended_un = update_mapping_append_only(cfg.perp_ticker_mapping_csv, un_df)
    logging.info("Perp ticker mapping: appended %s matched + %s unmatched rows.", appended_map, appended_un)

    listed_by_venue = {
        "hyperliquid": set(hl_tickers),
        "variational": set(var_tickers),
    }
    cov_df = build_coverage_summary(snap, panel_tickers, listed_by_venue=listed_by_venue)
    n_cov = append_coverage_summary(cfg.perp_coverage_summary_csv, cov_df)

    untradeable = cov_df[(~cov_df["hyperliquid"]) & (~cov_df["variational"])]
    logging.info(
        "Perp coverage summary: appended %s rows. Panel=%s, tradeable_any=%s, untradeable_any=%s.",
        n_cov,
        len(panel_tickers),
        int((cov_df["hyperliquid"] | cov_df["variational"]).sum()),
        len(untradeable),
    )
    if len(untradeable) > 0:
        sample = ", ".join(untradeable["panel_ticker"].head(25).tolist())
        logging.warning("Panel tickers with no perps on either venue (sample up to 25): %s", sample)

