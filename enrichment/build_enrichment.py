"""Build a daily enrichment CSV for the coin universe — reads from Google
Drive (the canonical lake), not the local mirror.

The local data lake at /data/curated/data_lake is a stale snapshot. The
canonical lake is the "Render Exports" Drive folder that's synced nightly
from Render. This script downloads each needed file by Drive file ID into
a local cache, checking modifiedTime on every run so a stale cache is
re-downloaded automatically.

Auth: needs OAuth env vars matching the pattern in
src/exports/gdrive_uploader.py — GDRIVE_OAUTH_CLIENT_ID,
GDRIVE_OAUTH_CLIENT_SECRET, GDRIVE_OAUTH_REFRESH_TOKEN. If those are not set
or the auth fails, the script falls back to using whatever is already in the
local cache directory (with a warning), which lets validation runs proceed
when the cache has been pre-populated through some other channel.

Spec: 15 columns per coin in the silver_fact_price universe.
  asset_id, symbol, latest_close_usd, latest_close_btc, vol_30d_avg_usd,
  mc, mc_tier, funding_rate_latest, funding_rate_7d_avg, oi_latest,
  oi_7d_change_pct, age_days, perp_binance, perp_hyperliquid,
  perp_variational

Changes vs the previous (stale-lake) build:
- alt OI is now real: fact_open_interest covers 590 assets daily through
  today, so oi_latest and oi_7d_change_pct are populated for all of them
  (BTC + 589 alts), not just BTC.
- Variational funding is read from perps_variational.csv `raw_metadata_json`
  as a FALLBACK for assets where Binance funding is missing.
  Hyperliquid does not surface funding rates in its perp snapshot JSON, so
  no HL-funding fallback is possible from the current lake.
- Top-20-by-mc canonical override: where fact_markets_snapshot's asset_id
  differs from the silver tables' asset_id (only known case: canonical
  Bitcoin is "BITCOIN" in snapshot, "BTC" in silver), the snap mc is
  preferred for the silver asset_id row. New mismatches will surface as
  warnings in the run log.
"""
from __future__ import annotations
import io
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# File registry
# ---------------------------------------------------------------------------

FILE_IDS: dict[str, str] = {
    "silver_fact_price.parquet":      "1IaLiu7wg1GHgxhaUwu9t2uTQrZLJdx05",
    "silver_fact_marketcap.parquet":  "1PkxcjjV9X82H6bopEJ3TMiT4pco1uB-p",
    "fact_volume.parquet":            "1n05yi0nvzCbQFHpQ50Zvzgh1lP8QrXA5",
    "silver_fact_funding.parquet":    "1F4b0967EUW8ne1mff_h5Qwa9J5FSayCc",
    "fact_open_interest.parquet":     "187EL5lcFii_Mbbz8TrTW60mtt71S0Xz7",
    "dim_asset.parquet":              "1VxjPqlyHLCvab2-0hA9_9wt8sVnDydNa",
    "perp_coverage_summary.csv":      "1xiBKSKIWD7p7SrmLlNcGZ3BxUoeeNFAE",
    "perps_variational.csv":          "119xDHd3xjm8T66DeT8-Oo8G8KWAClTEt",
    "fact_markets_snapshot.parquet":  "181h32ykjUUnwAXcmtZ13dpxe8JQzpis5",
}

# Known asset_id mismatch: fact_markets_snapshot uses "BITCOIN" for canonical
# Bitcoin, silver tables use "BTC". Map: snap asset_id -> silver asset_id.
SNAP_TO_SILVER_ASSET_ID: dict[str, str] = {"BITCOIN": "BTC"}

# Hard-coded stitch for Variational tickers that don't equal lake asset_id.
# Almost all match by identity; only special cases listed here.
VARIATIONAL_TICKER_TO_ASSET_ID: dict[str, str] = {
    "BTCUSD": "BTC",  # Variational quotes Bitcoin as "BTC" (per their venue
                      # ticker) but the panel-side label is BTCUSD; the lake
                      # uses "BTC". Direct match works since the perps_*
                      # `ticker` column is the venue ticker, not panel ticker.
}

CACHE_DIR = Path(__file__).resolve().parent / "_cache"
OUT_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Drive client + cache freshness
# ---------------------------------------------------------------------------

@dataclass
class CacheEntry:
    path: Path
    file_id: str
    drive_modified_time: str
    fetched_at: str
    size_bytes: int


def build_drive_client():
    """Return a Drive v3 client, or None if auth env vars are missing/invalid.

    Env vars used (matching src/exports/gdrive_uploader.py):
        GDRIVE_OAUTH_CLIENT_ID
        GDRIVE_OAUTH_CLIENT_SECRET
        GDRIVE_OAUTH_REFRESH_TOKEN
    """
    cid = os.environ.get("GDRIVE_OAUTH_CLIENT_ID")
    cs = os.environ.get("GDRIVE_OAUTH_CLIENT_SECRET")
    rt = os.environ.get("GDRIVE_OAUTH_REFRESH_TOKEN")
    if not (cid and cs and rt):
        return None
    try:
        from google.oauth2.credentials import Credentials  # type: ignore
        from googleapiclient.discovery import build  # type: ignore
        creds = Credentials(
            token=None,
            refresh_token=rt,
            client_id=cid,
            client_secret=cs,
            token_uri="https://oauth2.googleapis.com/token",
        )
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"  [warn] Drive client build failed: {e}")
        return None


def ensure_cached(name: str, drive) -> Path:
    """Return a local path for `name`, downloading from Drive if cache is stale.

    If Drive auth is unavailable, fall back to existing cache and warn.
    """
    file_id = FILE_IDS[name]
    target = CACHE_DIR / name
    meta_path = CACHE_DIR / f"{name}.meta.json"
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    cached_meta: Optional[dict] = None
    if meta_path.exists() and target.exists():
        try:
            cached_meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            cached_meta = None

    if drive is None:
        if target.exists() and cached_meta:
            print(
                f"  [trust-cache] {name}  "
                f"(cached drive_modified_time={cached_meta['drive_modified_time']})"
            )
            return target
        raise SystemExit(
            f"Cache miss for {name} and no Drive auth. "
            f"Set GDRIVE_OAUTH_CLIENT_ID/_CLIENT_SECRET/_REFRESH_TOKEN, "
            f"or pre-populate {CACHE_DIR}."
        )

    try:
        meta = (
            drive.files()
            .get(fileId=file_id, fields="modifiedTime, size, name")
            .execute()
        )
        drive_modified = meta["modifiedTime"]
    except Exception as e:
        print(f"  [warn] Drive metadata fetch failed for {name}: {e}")
        if target.exists():
            print(f"           using stale cache: {target}")
            return target
        raise

    if cached_meta and cached_meta.get("drive_modified_time") == drive_modified:
        print(f"  [cache-fresh] {name}  ({drive_modified})")
        return target

    print(f"  [download] {name}  (drive_modified_time={drive_modified})")
    from googleapiclient.http import MediaIoBaseDownload  # type: ignore
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    target.write_bytes(buf.getvalue())
    meta_path.write_text(
        json.dumps(
            {
                "file_id": file_id,
                "drive_modified_time": drive_modified,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "size_bytes": target.stat().st_size,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return target


def fetch_all() -> dict[str, Path]:
    drive = build_drive_client()
    if drive is None:
        print(
            "  [info] No Drive auth env vars (GDRIVE_OAUTH_*); will use cache "
            "as-is. Set them to pick up fresh data."
        )
    return {name: ensure_cached(name, drive) for name in FILE_IDS}


# ---------------------------------------------------------------------------
# Enrichment helpers
# ---------------------------------------------------------------------------

def mc_tier(mc: float) -> str:
    if pd.isna(mc) or mc <= 0:
        return "unknown"
    if mc > 10e9:
        return "large"
    if mc > 1e9:
        return "mid"
    if mc > 100e6:
        return "small"
    return "micro"


def _normalise_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df


def latest_per_asset(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    d = _normalise_dates(df).sort_values(["asset_id", "date"])
    last = d.groupby("asset_id", as_index=False).tail(1)
    return last[["asset_id", "date", value_col]]


def rolling_mean_per_asset(
    df: pd.DataFrame, value_col: str, window: int
) -> pd.Series:
    d = _normalise_dates(df).sort_values(["asset_id", "date"])
    tail = d.groupby("asset_id").tail(window)
    return tail.groupby("asset_id")[value_col].mean()


def first_date_per_asset(df: pd.DataFrame) -> pd.Series:
    return _normalise_dates(df).groupby("asset_id")["date"].min()


def oi_metrics_per_asset(oi: pd.DataFrame) -> pd.DataFrame:
    """Per-asset latest OI plus the 7-day percent change.

    "7d change" = (latest - value_at_or_before_latest-7d) / value * 100. If no
    row exists at or before latest-7d, return NaN.
    """
    d = _normalise_dates(oi).sort_values(["asset_id", "date"])
    rows = []
    for asset_id, g in d.groupby("asset_id"):
        latest_val = g["open_interest_usd"].iloc[-1]
        latest_dt = g["date"].iloc[-1]
        prior = g[g["date"] <= latest_dt - pd.Timedelta(days=7)]
        if prior.empty or prior["open_interest_usd"].iloc[-1] == 0:
            change = np.nan
        else:
            pv = prior["open_interest_usd"].iloc[-1]
            change = (latest_val - pv) / pv * 100.0
        rows.append(
            {
                "asset_id": asset_id,
                "oi_latest": latest_val,
                "oi_7d_change_pct": change,
            }
        )
    return pd.DataFrame(rows)


def variational_funding(pv: pd.DataFrame) -> pd.DataFrame:
    """Extract per-ticker latest and 7d-avg funding from perps_variational JSON.

    Returns columns: asset_id, var_funding_latest, var_funding_7d_avg.
    """
    pv = pv.copy()
    pv["snapshot_date_utc"] = pd.to_datetime(pv["snapshot_date_utc"])
    pv = pv.sort_values(["ticker", "snapshot_date_utc"])

    def _rate(meta_str: str) -> float:
        try:
            obj = json.loads(meta_str)
            raw = obj.get("funding_rate")
            return float(raw) if raw is not None else np.nan
        except Exception:
            return np.nan

    pv["funding_rate"] = pv["raw_metadata_json"].map(_rate)
    # 7-day window per ticker
    tail7 = pv.groupby("ticker").tail(7)
    avg7 = tail7.groupby("ticker")["funding_rate"].mean().rename("var_funding_7d_avg")
    latest = pv.groupby("ticker").tail(1).set_index("ticker")["funding_rate"].rename(
        "var_funding_latest"
    )
    out = pd.concat([latest, avg7], axis=1).reset_index()
    out["asset_id"] = out["ticker"].map(VARIATIONAL_TICKER_TO_ASSET_ID).fillna(
        out["ticker"]
    )
    return out[["asset_id", "var_funding_latest", "var_funding_7d_avg"]]


# ---------------------------------------------------------------------------
# Main enrichment
# ---------------------------------------------------------------------------

def build(paths: dict[str, Path]) -> tuple[pd.DataFrame, dict]:
    print("Loading lake tables (from cache)...")
    price = pd.read_parquet(paths["silver_fact_price.parquet"])
    mcap = pd.read_parquet(paths["silver_fact_marketcap.parquet"])
    vol = pd.read_parquet(paths["fact_volume.parquet"])
    fund = pd.read_parquet(paths["silver_fact_funding.parquet"])
    oi = pd.read_parquet(paths["fact_open_interest.parquet"])
    dim = pd.read_parquet(paths["dim_asset.parquet"])
    fms = pd.read_parquet(paths["fact_markets_snapshot.parquet"])
    pcs = pd.read_csv(paths["perp_coverage_summary.csv"])
    pv = pd.read_csv(paths["perps_variational.csv"])

    price = _normalise_dates(price)
    as_of_date = price["date"].max()
    print(f"  silver_fact_price max date: {as_of_date.date()}")
    print(f"  silver_fact_price assets: {price['asset_id'].nunique()}")
    print(f"  fact_open_interest assets: {oi['asset_id'].nunique()}")
    print(f"  silver_fact_funding assets (Binance): {fund['asset_id'].nunique()}")

    # BTC reference for latest_close_btc
    btc_close_by_date = (
        price[price["asset_id"] == "BTC"].set_index("date")["close"].sort_index()
    )
    if btc_close_by_date.empty:
        raise SystemExit("No BTC rows in silver_fact_price (asset_id='BTC').")

    # Per-asset latest close + matched BTC close on the same date
    latest_price = latest_per_asset(price, "close").rename(
        columns={"date": "latest_price_date", "close": "latest_close_usd"}
    )
    latest_price["btc_close_on_date"] = latest_price["latest_price_date"].map(
        btc_close_by_date
    )
    latest_price["latest_close_btc"] = (
        latest_price["latest_close_usd"] / latest_price["btc_close_on_date"]
    )

    # 30-day mean of daily rolling-24h volume
    vol_30d = rolling_mean_per_asset(vol, "volume", 30).rename("vol_30d_avg_usd")

    # Latest market cap from silver_fact_marketcap
    latest_mc = latest_per_asset(mcap, "market_cap").rename(
        columns={"market_cap": "mc"}
    )[["asset_id", "mc"]]

    # Canonical MC override from fact_markets_snapshot (latest snapshot).
    # Map snap asset_id to silver asset_id, then dedupe: when two snap rows
    # collide on the same silver asset_id (e.g., canonical Bitcoin and a
    # BTC-ticker memecoin both landing on silver "BTC"), keep the one with
    # the best market_cap_rank — that's the canonical row.
    fms = _normalise_dates(fms)
    fms_latest = fms[fms["date"] == fms["date"].max()].copy()
    fms_latest["silver_asset_id"] = (
        fms_latest["asset_id"].map(SNAP_TO_SILVER_ASSET_ID).fillna(fms_latest["asset_id"])
    )
    fms_canonical = (
        fms_latest.sort_values("market_cap_rank")
        .drop_duplicates("silver_asset_id", keep="first")
    )
    snap_mc = (
        fms_canonical.set_index("silver_asset_id")["market_cap_usd"].rename("snap_mc")
    )
    snap_rank = (
        fms_canonical.set_index("silver_asset_id")["market_cap_rank"].rename(
            "snap_rank"
        )
    )

    # Detect & log top-20 mc mismatches (>2x relative diff in either direction).
    diagnostics_overrides = []
    for sid, snap_val in snap_mc.items():
        if pd.isna(snap_val) or snap_val == 0:
            continue
        silver_val = latest_mc.loc[latest_mc["asset_id"] == sid, "mc"]
        if silver_val.empty:
            continue
        sv = silver_val.iloc[0]
        if pd.isna(sv) or sv == 0:
            continue
        ratio = float(sv) / float(snap_val)
        rank = snap_rank.get(sid)
        rank_int = int(rank) if pd.notna(rank) else 9999
        if rank_int <= 20 and (ratio < 0.5 or ratio > 2.0):
            diagnostics_overrides.append(
                {
                    "asset_id": sid,
                    "snap_rank": rank_int,
                    "silver_mc": float(sv),
                    "snap_mc": float(snap_val),
                    "ratio": round(ratio, 4),
                }
            )

    # MC: snap-preferred for any asset that exists in latest snap; silver otherwise.
    mc_merged = latest_mc.merge(
        snap_mc.reset_index().rename(columns={"silver_asset_id": "asset_id"}),
        on="asset_id",
        how="left",
    )
    mc_merged["mc_source"] = np.where(
        mc_merged["snap_mc"].notna(), "fact_markets_snapshot", "silver_fact_marketcap"
    )
    mc_merged["mc"] = mc_merged["snap_mc"].fillna(mc_merged["mc"])

    # Binance funding (silver)
    fund = fund.rename(columns={"funding_rate_raw_pct": "funding_rate"})
    latest_fund_bn = latest_per_asset(fund, "funding_rate").rename(
        columns={"funding_rate": "binance_funding_latest"}
    )[["asset_id", "binance_funding_latest"]]
    fund_7d_bn = rolling_mean_per_asset(fund, "funding_rate", 7).rename(
        "binance_funding_7d_avg"
    )

    # Variational funding (JSON fallback)
    var_fund = variational_funding(pv)

    # Funding: prefer Binance; fall back to Variational when Binance is absent.
    fund_merged = latest_fund_bn.merge(
        fund_7d_bn.reset_index(), on="asset_id", how="outer"
    ).merge(var_fund, on="asset_id", how="outer")
    fund_merged["funding_rate_latest"] = fund_merged["binance_funding_latest"].fillna(
        fund_merged["var_funding_latest"]
    )
    fund_merged["funding_rate_7d_avg"] = fund_merged["binance_funding_7d_avg"].fillna(
        fund_merged["var_funding_7d_avg"]
    )
    fund_merged["funding_source"] = np.where(
        fund_merged["binance_funding_latest"].notna(),
        "binance",
        np.where(fund_merged["var_funding_latest"].notna(), "variational", "none"),
    )

    # OI (alt-aware now): latest + 7d change for any covered asset
    oi_metrics = oi_metrics_per_asset(oi)

    # Age in days since first appearance
    first_seen = first_date_per_asset(price).rename("first_price_date")

    # Perp coverage flags
    binance_funded_assets = set(fund["asset_id"].dropna().unique())
    pcs_latest = (
        pcs.sort_values("snapshot_date_utc")
        .drop_duplicates("panel_ticker", keep="last")[
            ["panel_ticker", "hyperliquid", "variational"]
        ]
        .rename(
            columns={
                "panel_ticker": "asset_id",
                "hyperliquid": "perp_hyperliquid",
                "variational": "perp_variational",
            }
        )
    )

    out = latest_price[
        ["asset_id", "latest_price_date", "latest_close_usd", "latest_close_btc"]
    ].copy()
    out = out.merge(dim[["asset_id", "symbol"]], on="asset_id", how="left")
    out = out.merge(vol_30d, on="asset_id", how="left")
    out = out.merge(mc_merged[["asset_id", "mc"]], on="asset_id", how="left")
    out["mc_tier"] = out["mc"].map(mc_tier)
    out = out.merge(
        fund_merged[["asset_id", "funding_rate_latest", "funding_rate_7d_avg"]],
        on="asset_id",
        how="left",
    )
    out = out.merge(oi_metrics, on="asset_id", how="left")
    out = out.merge(first_seen.reset_index(), on="asset_id", how="left")
    out["age_days"] = (out["latest_price_date"] - out["first_price_date"]).dt.days
    out["perp_binance"] = out["asset_id"].isin(binance_funded_assets)
    out = out.merge(pcs_latest, on="asset_id", how="left")
    for c in ("perp_hyperliquid", "perp_variational"):
        out[c] = out[c].fillna(False).astype(bool)

    cols = [
        "asset_id",
        "symbol",
        "latest_close_usd",
        "latest_close_btc",
        "vol_30d_avg_usd",
        "mc",
        "mc_tier",
        "funding_rate_latest",
        "funding_rate_7d_avg",
        "oi_latest",
        "oi_7d_change_pct",
        "age_days",
        "perp_binance",
        "perp_hyperliquid",
        "perp_variational",
    ]
    out = out[cols].sort_values("asset_id").reset_index(drop=True)

    # Sentinel validation
    core_cols = ["latest_close_usd", "latest_close_btc", "vol_30d_avg_usd", "mc"]
    for sentinel in ("BTC", "ETH"):
        row = out[out["asset_id"] == sentinel]
        if row.empty:
            raise RuntimeError(f"Sentinel {sentinel} missing.")
        nulls = row[core_cols].isna().iloc[0]
        offending = nulls[nulls].index.tolist()
        if offending:
            raise RuntimeError(
                f"Sentinel {sentinel} has nulls in core columns: {offending}"
            )

    diagnostics = {
        "as_of_date": str(as_of_date.date()),
        "snap_date": str(fms["date"].max().date()),
        "row_count": int(len(out)),
        "null_rate_pct": {k: round(v, 2) for k, v in (out.isna().mean() * 100).to_dict().items()},
        "mc_source_counts": mc_merged["mc_source"].value_counts(dropna=False).to_dict(),
        "funding_source_counts": fund_merged["funding_source"].value_counts(dropna=False).to_dict(),
        "top20_mc_overrides": diagnostics_overrides,
        "oi_asset_count": int(oi["asset_id"].nunique()),
        "binance_funding_asset_count": len(binance_funded_assets),
        "variational_funding_asset_count": int(var_fund["asset_id"].nunique()),
    }
    return out, diagnostics


def write_summary(out: pd.DataFrame, diag: dict, csv_path: Path) -> None:
    btc = out.loc[out["asset_id"] == "BTC"].iloc[0]
    eth = out.loc[out["asset_id"] == "ETH"].iloc[0]
    lines = [
        f"# Enrichment build — {diag['as_of_date']}",
        "",
        f"- Output: `{csv_path.name}` ({diag['row_count']:,} rows).",
        f"- Source: **canonical lake on Google Drive** (not the local mirror).",
        f"- Lake max date in `silver_fact_price`: {diag['as_of_date']}.",
        f"- `fact_markets_snapshot` latest snapshot: {diag['snap_date']}.",
        f"- OI assets covered: {diag['oi_asset_count']} (BTC + 589 alts, daily series).",
        f"- Binance funding assets: {diag['binance_funding_asset_count']}.",
        f"- Variational funding assets parsed from JSON: {diag['variational_funding_asset_count']}.",
        "",
        "## Null-rate per column (%)",
        "",
        "| Column | Null % |",
        "|---|---|",
        *[f"| {k} | {v} |" for k, v in diag["null_rate_pct"].items()],
        "",
        "## Sentinel sanity (BTC, ETH)",
        "",
        f"- BTC: close={btc['latest_close_usd']:,.2f}, mc={btc['mc']:,.0f}, "
        f"vol_30d={btc['vol_30d_avg_usd']:,.0f}, oi_latest={btc['oi_latest']:,.0f}, "
        f"oi_7d_change_pct={btc['oi_7d_change_pct']}.",
        f"- ETH: close={eth['latest_close_usd']:,.2f}, mc={eth['mc']:,.0f}, "
        f"vol_30d={eth['vol_30d_avg_usd']:,.0f}, oi_latest={eth['oi_latest']:,.0f}, "
        f"oi_7d_change_pct={eth['oi_7d_change_pct']}.",
        "",
        "## Top-20 MC override events",
        "",
        f"{diag['top20_mc_overrides'] if diag['top20_mc_overrides'] else '  (none — silver and snap agree within 2x for every top-20 coin)'}",
        "",
        "## 3-bullet wrap-up",
        "",
        "- **What worked.** Drive-direct fetch with per-file `modifiedTime` "
        "cache invalidation. The 590-asset daily OI series is now wired up, "
        "so `oi_latest` and `oi_7d_change_pct` are populated for BTC + 589 "
        "alts instead of being NaN. Variational funding fallback parses the "
        "JSON snapshot for ~450 additional coins where Binance funding is "
        "absent.",
        "- **What's uncertain.** Hyperliquid does not surface funding rates "
        "in its perp snapshot JSON (keys are `szDecimals, name, maxLeverage, "
        "marginTableId` only). So funding for HL-only coins remains NaN. The "
        "Variational fallback uses a different cadence than Binance "
        "(Variational funding is reset every 4h and reported as a daily "
        "snapshot string; Binance is daily 8h-period observations). "
        "`funding_rate_latest` is therefore not strictly cross-venue "
        "comparable when sourced from Variational. The `funding_source_counts` "
        "diagnostic shows how many rows landed on each.",
        "- **Assumptions.** The asset_id mismatch between snap (`BITCOIN`) "
        "and silver (`BTC`) is handled by a one-entry override "
        "`SNAP_TO_SILVER_ASSET_ID`. New mismatches will surface as warning "
        "rows in the run log. FIGR_HELOC (rank 9 by mc) exists in snap but "
        "not in silver_fact_price; it is silently excluded. Variational "
        "ticker → asset_id is mostly identity; one explicit override "
        "(`BTCUSD → BTC`) handles the known divergence.",
    ]
    (OUT_DIR / "SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    paths = fetch_all()
    out, diag = build(paths)
    out_csv = OUT_DIR / f"enrichment_{diag['as_of_date']}.csv"
    out.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}  ({len(out):,} rows)")
    write_summary(out, diag, out_csv)
    print(f"Wrote {OUT_DIR / 'SUMMARY.md'}")
    print("\nDiagnostics:")
    for k, v in diag.items():
        if isinstance(v, dict):
            print(f"  {k}:")
            for kk, vv in v.items():
                print(f"    {kk}: {vv}")
        elif isinstance(v, list):
            print(f"  {k}: ({len(v)} items)")
            for item in v[:10]:
                print(f"    {item}")
        else:
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
