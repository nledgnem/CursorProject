# Data lake — discoveries log

Facts captured across sessions while building the enrichment dataset on top of the lake. Update as new quirks surface. The Drive folder "Render Exports" (id `1J4qy2zH-bo98A2WsA0wq0AkCXtH7sGEj`) is canonical; the local clone at `C:\Users\Admin\Documents\Cursor\data\curated\data_lake` is a stale snapshot and should not be used as the source of truth.

## Cardinal rule: read from Drive, not from local

The local lake directory mirrors Render's pipeline output only when something pulls it down. As of 2026-05-11 the local copy was frozen at lake-max-date 2026-03-30 while Drive was current through today. The discrepancy went unnoticed for ~6 weeks and led to two wrong reads (claims about OI being BTC-only, and about Ethereum being absent from the universe) before the diagnostic that caught it. Going forward, **always source files via the Drive file ID** (registry below). The enrichment script `enrichment/build_enrichment.py` does this with a per-file `modifiedTime` cache check.

Drive auth needs OAuth env vars matching `src/exports/gdrive_uploader.py`:
- `GDRIVE_OAUTH_CLIENT_ID`
- `GDRIVE_OAUTH_CLIENT_SECRET`
- `GDRIVE_OAUTH_REFRESH_TOKEN`

If they're missing, the script falls back to the local `enrichment/_cache/` directory with a warning.

## File ID registry (canonical Drive copies)

| File | Drive file ID | Latest size | Notes |
|---|---|---|---|
| `silver_fact_price.parquet` | `1IaLiu7wg1GHgxhaUwu9t2uTQrZLJdx05` | 14.8 MB | 2,843 assets, daily 2013-04-28 → today |
| `silver_fact_marketcap.parquet` | `1PkxcjjV9X82H6bopEJ3TMiT4pco1uB-p` | 13.9 MB | Same grain as price |
| `fact_volume.parquet` | `1n05yi0nvzCbQFHpQ50Zvzgh1lP8QrXA5` | 14.7 MB | Rolling 24h snapshot, NOT calendar-day bars |
| `silver_fact_funding.parquet` | `1F4b0967EUW8ne1mff_h5Qwa9J5FSayCc` | 1.3 MB | Binance-only (by ingestion design) via CoinGlass |
| `fact_open_interest.parquet` | `187EL5lcFii_Mbbz8TrTW60mtt71S0Xz7` | 1.5 MB | 590 assets, daily series. Was BTC-only in stale mirror; current Drive has alts. |
| `fact_liquidations.parquet` | `1XUkiBdtxaSsUZ_-ppzftVu5Ujwr544Xj` | 5.9 MB | NEW — 593 assets, daily 2024-01-01 → today. Schema below. Not yet wired into enrichment. |
| `dim_asset.parquet` | `1VxjPqlyHLCvab2-0hA9_9wt8sVnDydNa` | 105 KB | Per-asset metadata |
| `fact_markets_snapshot.parquet` | `181h32ykjUUnwAXcmtZ13dpxe8JQzpis5` | 5.0 MB | Top ~2,500 daily, with canonical `coingecko_id`. 22 snapshots so far. |
| `perp_coverage_summary.csv` | `1xiBKSKIWD7p7SrmLlNcGZ3BxUoeeNFAE` | 1.6 MB | Per-coin HL/Variational listing flags |
| `perps_hyperliquid.csv` | `1tE4oWtpJQQCZYpQA9o3xIOD1H0FwOCQR` | 455 KB | HL listings snapshot. **JSON does NOT contain funding rate** (keys: `szDecimals, name, maxLeverage, marginTableId`). |
| `perps_variational.csv` | `119xDHd3xjm8T66DeT8-Oo8G8KWAClTEt` | 6.6 MB | Variational listings. JSON contains `funding_rate`, `funding_interval_s`, `open_interest` (long/short). |
| `perp_ticker_mapping.csv` | `1pxfgA--oFh7BFenax5liE8m1K7BRYiLA` | 16 KB | panel_ticker ↔ venue_ticker. Most rows identity; one notable exception `BTCUSD ↔ BTC`. |

## fact_liquidations.parquet — schema (not yet in enrichment)

```
asset_id              object
date                  object   2024-01-01 → today (862 distinct daily values)
long_liquidation_usd  float64
short_liquidation_usd float64
source                object   coinglass (only)
```

593 unique `asset_id`, daily granularity. Same source and grain as `fact_open_interest`. Not wired into the enrichment CSV (per the brief that introduced it); flagged here for future decision on whether to add liquidation metrics.

## Asset ID conventions — what's actually true

Earlier versions of this file claimed `coingecko_id` was lowercased symbol and that Ethereum was effectively missing from the canonical universe. Both claims were derived from the stale local mirror and are **withdrawn** based on fresh Drive data:

- `silver_fact_price` and `silver_fact_marketcap` use `asset_id = ticker symbol` for nearly all coins. Real Ethereum is `asset_id="ETH"` with close ~$2,350 and mc ~$286B on 2026-05-11. Real Solana is `asset_id="SOL"`. Real Bitcoin is `asset_id="BTC"` with close ~$81k and mc ~$1.6T.
- `fact_markets_snapshot` uses `asset_id = ticker symbol` for nearly all coins **with one known exception**: canonical Bitcoin appears with `asset_id="BITCOIN"` (not `BTC`), `coingecko_id="bitcoin"`. The `asset_id="BTC"` row in markets_snapshot is a separate, low-ranked memecoin.
- The cross-table asset_id mismatch for Bitcoin is the only one observed in the latest top-20. The enrichment script handles it via a one-entry override map `SNAP_TO_SILVER_ASSET_ID = {"BITCOIN": "BTC"}`. If new mismatches surface, they'll appear in the script's run log under `top20_mc_overrides`.
- `dim_asset` carries the same `asset_id` values as `silver_fact_price`. The `coingecko_id` column appears mostly to be the lowercased symbol for the asset_ids in the silver tables (e.g., `asset_id="ETH" → coingecko_id="eth"`), but `fact_markets_snapshot.coingecko_id` uses canonical CoinGecko slugs (`"ethereum"`, `"binancecoin"`, `"tron"` etc.). So `dim_asset.coingecko_id` is NOT reliable as a join key against external CoinGecko slugs; `fact_markets_snapshot.coingecko_id` is.

## Other conventions and gotchas

- `silver_fact_price` is one row per `(asset_id, date)`. No duplicates observed.
- `is_winsorized` and `is_ffilled` flags exist on silver rows. `is_ffilled=True` rows are synthetic (price carried forward from the prior bar). BTC has zero ffilled rows; some altcoins have many.
- `fact_funding` (bronze) mixes funding-rate units across the 2026-01-13 cutover (decimal pre, percent post). Always read `silver_fact_funding`, which normalises to a uniform percent.
- `fact_volume` is rolling 24h volume at snapshot time, not a calendar-day bar. Means/sums treat it as a smoothed liquidity proxy, not summable across consecutive days.
- `silver_fact_funding` is Binance-only by design (the ingestion script `fetch_coinglass_funding.py` is configured with `exchange: 'Binance'`; verified via `funding_metadata.json`).
- Hyperliquid funding rates are **not** ingested anywhere in the lake — the HL perp snapshot JSON does not contain funding info. Variational funding IS in the JSON (`funding_rate` string field).
- `fact_open_interest` aggregates OI across exchanges per asset (CoinGlass methodology), not per-venue. PEPE for example has `oi_latest > 0` despite not appearing on Binance, HL, or Variational in our coverage tables.
- `pd.read_csv` default `na_values` includes the literal string `"None"`. If a state/category column uses `"None"` as a label, reads will silently turn it into `NaN`. Use `keep_default_na=False, na_values=[""]` (or rename the label) when round-tripping such columns.

## File freshness on Drive (2026-05-11)

All files in the registry above have `modifiedTime` ≈ 2026-05-11 01:54–01:56 UTC (last night's sync). Lake-internal `max(date)` values:
- `silver_fact_price`, `silver_fact_marketcap`, `fact_volume`, `fact_funding`, `silver_fact_funding`, `fact_markets_snapshot`, `fact_open_interest`, `fact_liquidations`: **2026-05-11**.
- `perps_hyperliquid`, `perps_variational`, `perp_coverage_summary`: latest snapshot 2026-05-11 (accumulated history goes back to mid-April).
- `fact_derivative_open_interest`, `fact_derivative_volume`, `fact_derivative_exchange_details`: still frozen at 2026-01-28 (single-snapshot CoinGecko endpoints, not on the daily pipeline). Different provider semantics from `fact_open_interest` and explicitly quarantined in DATA_LAKE_CONTEXT.md; ignored by the enrichment.

## Retractions from prior versions of this file

- ❌ "Altcoin OI is not in the lake" / "OI is BTC-only by design" — wrong. Caused by reading the stale local copy. The Drive copy has 590-asset daily OI.
- ❌ "Ethereum is effectively missing from the canonical universe" — wrong. Caused by the same stale read. Real Ethereum is `asset_id="ETH"` in silver, correctly priced and capitalised.
- ❌ "Lake max date is 2026-03-30" — wrong for the lake; correct only for the stale local mirror.
- ❌ "dim_asset.coingecko_id is the lowercased symbol everywhere, so any external CoinGecko slug match fails" — partially wrong. `dim_asset.coingecko_id` does look like lowercased ticker for many rows, but the canonical CoinGecko slugs live in `fact_markets_snapshot.coingecko_id`. Use that for canonical joins.
