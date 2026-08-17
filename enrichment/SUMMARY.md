# Enrichment build — 2026-06-02

- Output: `enrichment_2026-06-02.csv` (2,844 rows).
- Source: **canonical lake on Google Drive** (not the local mirror).
- Lake max date in `silver_fact_price`: 2026-06-02.
- `fact_markets_snapshot` latest snapshot: 2026-06-02.
- OI assets covered: 590 (BTC + 589 alts, daily series).
- Binance funding assets: 513.
- Variational funding assets parsed from JSON: 475.

## Null-rate per column (%)

| Column | Null % |
|---|---|
| asset_id | 0.0 |
| symbol | 4.47 |
| latest_close_usd | 0.04 |
| latest_close_btc | 0.11 |
| vol_30d_avg_usd | 0.0 |
| mc | 0.0 |
| mc_tier | 0.0 |
| funding_rate_latest | 80.38 |
| funding_rate_7d_avg | 80.38 |
| oi_latest | 80.56 |
| oi_7d_change_pct | 80.59 |
| age_days | 0.0 |
| perp_binance | 0.0 |
| perp_hyperliquid | 0.0 |
| perp_variational | 0.0 |

## Sentinel sanity (BTC, ETH)

- BTC: close=71,192.44, mc=1,428,639,591,394, vol_30d=33,681,246,756, oi_latest=54,216,178,339, oi_7d_change_pct=0.7553381869515398.
- ETH: close=1,994.65, mc=241,630,774,740, vol_30d=14,488,429,566, oi_latest=31,036,269,984, oi_7d_change_pct=-0.8305735312178943.

## Top-20 MC override events

  (none — silver and snap agree within 2x for every top-20 coin)

## 3-bullet wrap-up

- **What worked.** Drive-direct fetch with per-file `modifiedTime` cache invalidation. The 590-asset daily OI series is now wired up, so `oi_latest` and `oi_7d_change_pct` are populated for BTC + 589 alts instead of being NaN. Variational funding fallback parses the JSON snapshot for ~450 additional coins where Binance funding is absent.
- **What's uncertain.** Hyperliquid does not surface funding rates in its perp snapshot JSON (keys are `szDecimals, name, maxLeverage, marginTableId` only). So funding for HL-only coins remains NaN. The Variational fallback uses a different cadence than Binance (Variational funding is reset every 4h and reported as a daily snapshot string; Binance is daily 8h-period observations). `funding_rate_latest` is therefore not strictly cross-venue comparable when sourced from Variational. The `funding_source_counts` diagnostic shows how many rows landed on each.
- **Assumptions.** The asset_id mismatch between snap (`BITCOIN`) and silver (`BTC`) is handled by a one-entry override `SNAP_TO_SILVER_ASSET_ID`. New mismatches will surface as warning rows in the run log. FIGR_HELOC (rank 9 by mc) exists in snap but not in silver_fact_price; it is silently excluded. Variational ticker → asset_id is mostly identity; one explicit override (`BTCUSD → BTC`) handles the known divergence.