# Data Lake Inspection Report

**Inspection Date:** 2026-01-27 23:58:33
**Data Lake Directory:** `data\curated\data_lake`

---

## Executive Summary

- **Files Found:** 9/9 expected files
- **Total Price Records:** 1,271,237
- **Total Assets:** 2717
- **Price Data Last Updated:** 2026-01-27 20:20:16
- **Latest Price Data:** 2026-01-05

## 1. File Inventory

### dim_asset.parquet

- **Size:** 0.1 MB
- **Last Modified:** 2026-01-06 11:28:48

### dim_instrument.parquet

- **Size:** 0.03 MB
- **Last Modified:** 2026-01-06 11:28:48

### fact_price.parquet

- **Size:** 11.23 MB
- **Last Modified:** 2026-01-27 20:20:16

### fact_marketcap.parquet

- **Size:** 10.64 MB
- **Last Modified:** 2026-01-27 20:20:17

### fact_volume.parquet

- **Size:** 11.3 MB
- **Last Modified:** 2026-01-27 20:20:17

### fact_funding.parquet

- **Size:** 0.94 MB
- **Last Modified:** 2026-01-13 21:41:41

### fact_open_interest.parquet

- **Size:** 0.01 MB
- **Last Modified:** 2026-01-13 21:41:41

### map_provider_asset.parquet

- **Size:** 0.04 MB
- **Last Modified:** 2026-01-06 11:28:48

### map_provider_instrument.parquet

- **Size:** 0.01 MB
- **Last Modified:** 2026-01-06 11:28:48


## 2. Fact Table Analysis

### fact_price

- **Row Count:** 1,271,237

- **Date Range:** 2024-01-07 to 2026-01-05
- **Unique Dates:** 730

- **Unique Assets:** 2718

**Top 10 Assets by Record Count:**

1. MOCHI: 730 records
2. WMNT: 730 records
3. BLUR: 730 records
4. SPELL: 730 records
5. VVS: 730 records
6. MCOIN: 730 records
7. BBTC: 730 records
8. FNCT: 730 records
9. ACX: 730 records
10. GFAL: 730 records

**Data Sources:**

- coingecko: 1,271,237 records

**Sample Data (first 3 rows):**

```
Row 1: {'asset_id': '$MYRO', 'date': datetime.date(2024, 1, 7), 'close': 0.06929142601020269, 'source': 'coingecko'}
Row 2: {'asset_id': '0X0', 'date': datetime.date(2024, 1, 7), 'close': 0.12833259844895092, 'source': 'coingecko'}
Row 3: {'asset_id': '10SET', 'date': datetime.date(2024, 1, 7), 'close': 0.6146228844448931, 'source': 'coingecko'}
```

### fact_marketcap

- **Row Count:** 1,261,471

- **Date Range:** 2024-01-07 to 2026-01-05
- **Unique Dates:** 730

- **Unique Assets:** 2718

**Top 10 Assets by Record Count:**

1. AGVE: 730 records
2. OM: 730 records
3. CTK: 730 records
4. RAY: 730 records
5. ACS: 730 records
6. CAKE: 730 records
7. JUNO: 730 records
8. VTHO: 730 records
9. AXL: 730 records
10. RSR: 730 records

**Data Sources:**

- coingecko: 1,261,471 records

**Sample Data (first 3 rows):**

```
Row 1: {'asset_id': '$MYRO', 'date': datetime.date(2024, 1, 7), 'marketcap': 69866353.4961432, 'source': 'coingecko'}
Row 2: {'asset_id': '0X0', 'date': datetime.date(2024, 1, 7), 'marketcap': 114587335.2880524, 'source': 'coingecko'}
Row 3: {'asset_id': '10SET', 'date': datetime.date(2024, 1, 7), 'marketcap': 102906152.68791604, 'source': 'coingecko'}
```

### fact_volume

- **Row Count:** 1,265,420

- **Date Range:** 2024-01-07 to 2026-01-05
- **Unique Dates:** 730

- **Unique Assets:** 2718

**Top 10 Assets by Record Count:**

1. LRC: 730 records
2. TRUMP: 730 records
3. BITCOIN: 730 records
4. UOS: 730 records
5. USD+: 730 records
6. KUJI: 730 records
7. NTX: 730 records
8. GHO: 730 records
9. FLR: 730 records
10. GAS: 730 records

**Data Sources:**

- coingecko: 1,265,420 records

**Sample Data (first 3 rows):**

```
Row 1: {'asset_id': '$MYRO', 'date': datetime.date(2024, 1, 7), 'volume': 29767294.7131797, 'source': 'coingecko'}
Row 2: {'asset_id': '0X0', 'date': datetime.date(2024, 1, 7), 'volume': 730544.4921561241, 'source': 'coingecko'}
Row 3: {'asset_id': '10SET', 'date': datetime.date(2024, 1, 7), 'volume': 571070.7437057175, 'source': 'coingecko'}
```

### fact_funding

- **Row Count:** 263,399

- **Date Range:** 2023-04-19 to 2026-01-13
- **Unique Dates:** 1,001

- **Unique Assets:** 507

**Top 10 Assets by Record Count:**

1. KAVA: 1,000 records
2. USDC: 1,000 records
3. TLM: 1,000 records
4. ASTR: 1,000 records
5. RUNE: 1,000 records
6. ZRX: 1,000 records
7. IMX: 1,000 records
8. RVN: 1,000 records
9. ICP: 1,000 records
10. DOGE: 1,000 records

**Data Sources:**

- coinglass: 263,399 records

**Sample Data (first 3 rows):**

```
Row 1: {'asset_id': '1INCH', 'instrument_id': None, 'date': datetime.date(2023, 4, 19), 'funding_rate': 0.01, 'exchange': 'Binance', 'source': 'coinglass'}
Row 2: {'asset_id': 'AAVE', 'instrument_id': 'binance_perp_AAVEUSDT', 'date': datetime.date(2023, 4, 19), 'funding_rate': 0.01, 'exchange': 'Binance', 'source': 'coinglass'}
Row 3: {'asset_id': 'ACH', 'instrument_id': 'binance_perp_ACHUSDT', 'date': datetime.date(2023, 4, 19), 'funding_rate': 0.01, 'exchange': 'Binance', 'source': 'coinglass'}
```

### fact_open_interest

- **Row Count:** 334

- **Date Range:** 2025-02-14 to 2026-01-13
- **Unique Dates:** 334

- **Unique Assets:** 1

**Top 10 Assets by Record Count:**

1. BTC: 334 records

**Data Sources:**

- coinglass: 334 records

**Sample Data (first 3 rows):**

```
Row 1: {'asset_id': 'BTC', 'date': datetime.date(2025, 2, 14), 'open_interest_usd': 60865420868.0, 'source': 'coinglass'}
Row 2: {'asset_id': 'BTC', 'date': datetime.date(2025, 2, 15), 'open_interest_usd': 61162601361.0, 'source': 'coinglass'}
Row 3: {'asset_id': 'BTC', 'date': datetime.date(2025, 2, 16), 'open_interest_usd': 60250326985.0, 'source': 'coinglass'}
```


## 3. Dimension Table Analysis

### dim_asset

- **Row Count:** 2,717

**Column Statistics:**

- `asset_id`: 2717 unique, 0 nulls
- `symbol`: 2717 unique, 0 nulls
- `name`: 2717 unique, 0 nulls
- `chain`: 1 unique, 2717 nulls
- `contract_address`: 1 unique, 2717 nulls
- `coingecko_id`: 2717 unique, 0 nulls
- `is_stable`: 2 unique, 0 nulls
- `is_wrapped_stable`: 1 unique, 0 nulls
- `metadata_json`: 2717 unique, 0 nulls

### dim_instrument

- **Row Count:** 605

**Column Statistics:**

- `instrument_id`: 605 unique, 0 nulls
- `venue`: 1 unique, 0 nulls
- `instrument_symbol`: 605 unique, 0 nulls
- `instrument_type`: 1 unique, 0 nulls
- `quote`: 1 unique, 0 nulls
- `base_asset_symbol`: 602 unique, 0 nulls
- `asset_id`: 554 unique, 50 nulls
- `multiplier`: 8 unique, 585 nulls
- `metadata_json`: 390 unique, 0 nulls


## 4. Temporal Coverage Analysis

### fact_price

- **Total Assets:** 2718

**Top 10 Assets by Date Coverage:**

1. **ALUSD**: 2024-01-07 to 2026-01-05 (730 dates)
2. **DADDYDOGE**: 2024-01-07 to 2026-01-05 (730 dates)
3. **XRP**: 2024-01-07 to 2026-01-05 (730 dates)
4. **SRX**: 2024-01-07 to 2026-01-05 (730 dates)
5. **BEL**: 2024-01-07 to 2026-01-05 (730 dates)
6. **HOT**: 2024-01-07 to 2026-01-05 (730 dates)
7. **DMTR**: 2024-01-07 to 2026-01-05 (730 dates)
8. **WAVAX**: 2024-01-07 to 2026-01-05 (730 dates)
9. **KNCL**: 2024-01-07 to 2026-01-05 (730 dates)
10. **DOGEKING**: 2024-01-07 to 2026-01-05 (730 dates)

### fact_marketcap

- **Total Assets:** 2718

**Top 10 Assets by Date Coverage:**

1. **PYUSD**: 2024-01-07 to 2026-01-05 (730 dates)
2. **SPELL**: 2024-01-07 to 2026-01-05 (730 dates)
3. **AAVE**: 2024-01-07 to 2026-01-05 (730 dates)
4. **PSP**: 2024-01-07 to 2026-01-05 (730 dates)
5. **CRV**: 2024-01-07 to 2026-01-05 (730 dates)
6. **AURORA**: 2024-01-07 to 2026-01-05 (730 dates)
7. **CELL**: 2024-01-07 to 2026-01-05 (730 dates)
8. **GLMR**: 2024-01-07 to 2026-01-05 (730 dates)
9. **SUN**: 2024-01-07 to 2026-01-05 (730 dates)
10. **HNT**: 2024-01-07 to 2026-01-05 (730 dates)

### fact_volume

- **Total Assets:** 2718

**Top 10 Assets by Date Coverage:**

1. **ZIG**: 2024-01-07 to 2026-01-05 (730 dates)
2. **CORE**: 2024-01-07 to 2026-01-05 (730 dates)
3. **ATLAS**: 2024-01-07 to 2026-01-05 (730 dates)
4. **DUSK**: 2024-01-07 to 2026-01-05 (730 dates)
5. **STRX**: 2024-01-07 to 2026-01-05 (730 dates)
6. **VINU**: 2024-01-07 to 2026-01-05 (730 dates)
7. **KAS**: 2024-01-07 to 2026-01-05 (730 dates)
8. **BUCK**: 2024-01-07 to 2026-01-05 (730 dates)
9. **MANA**: 2024-01-07 to 2026-01-05 (730 dates)
10. **CORGIAI**: 2024-01-07 to 2026-01-05 (730 dates)

### fact_funding

- **Total Assets:** 507

**Top 10 Assets by Date Coverage:**

1. **ONE**: 2023-04-19 to 2026-01-12 (1,000 dates)
2. **ONT**: 2023-04-19 to 2026-01-12 (1,000 dates)
3. **COMP**: 2023-04-19 to 2026-01-12 (1,000 dates)
4. **JASMY**: 2023-04-19 to 2026-01-12 (1,000 dates)
5. **BEL**: 2023-04-19 to 2026-01-12 (1,000 dates)
6. **GTC**: 2023-04-19 to 2026-01-12 (1,000 dates)
7. **IOTX**: 2023-04-19 to 2026-01-12 (1,000 dates)
8. **CHZ**: 2023-04-19 to 2026-01-12 (1,000 dates)
9. **CHR**: 2023-04-19 to 2026-01-12 (1,000 dates)
10. **ZIL**: 2023-04-19 to 2026-01-12 (1,000 dates)


## 5. Data Quality Assessment

### Outlier Detection

**Price Data:**
- Min Price: $0.00
- Max Price: $174,463,063.78

**Funding Rate Data:**
- Min Funding Rate: -3.000000
- Max Funding Rate: 2.000000
- Mean Funding Rate: 0.001758


## 6. Asset Universe Analysis

- **Total Assets:** 2717

- **Stablecoins:** 76 (2.8%)

**Chain Distribution:**

- None: 2717 assets

**Asset Coverage by Fact Table:**

- fact_price: 2718 unique assets
- fact_marketcap: 2718 unique assets
- fact_volume: 2718 unique assets
- fact_funding: 507 unique assets


## 7. Funding Data Analysis

- **Unique Assets:** 507
- **Unique Instruments:** 498

**Exchange Coverage:**

- Binance: 263,399 records

**Top 10 Assets by Funding Data Coverage:**

1. **SAND**: 1,000 records, mean funding rate: 0.007977
2. **COMP**: 1,000 records, mean funding rate: -0.001457
3. **STG**: 1,000 records, mean funding rate: 0.000488
4. **ATOM**: 1,000 records, mean funding rate: -0.001100
5. **APE**: 1,000 records, mean funding rate: 0.003891
6. **FLOW**: 1,000 records, mean funding rate: 0.007004
7. **SPELL**: 1,000 records, mean funding rate: -0.014715
8. **VET**: 1,000 records, mean funding rate: 0.008021
9. **NKN**: 1,000 records, mean funding rate: 0.006112
10. **SUSHI**: 1,000 records, mean funding rate: 0.007938


## 8. Recommendations

### Data Freshness

**OK:** Data appears to be regularly updated.

**OK:** No duplicate records found in fact tables.

### Coverage Recommendations

- Monitor data coverage for top assets (BTC, ETH) to ensure completeness
- Consider backfilling any identified gaps in temporal coverage
- Validate funding data coverage for assets used in MSM v0

