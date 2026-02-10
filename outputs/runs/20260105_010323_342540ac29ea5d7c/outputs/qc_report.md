# QC Curation Report

Generated: 2026-01-05T01:03:25.302498+00:00

## PRICES

- Date range: 2024-01-01 00:00:00 to 2025-12-23 00:00:00
- Number of days: 723
- Number of symbols: 939
- Missingness before QC: 29.46%
- Missingness after QC: 29.47%

## MARKETCAP

- Date range: 2024-01-01 00:00:00 to 2025-12-23 00:00:00
- Number of days: 723
- Number of symbols: 939
- Missingness before QC: 29.46%
- Missingness after QC: 29.94%

## VOLUME

- Date range: 2024-01-01 00:00:00 to 2025-12-23 00:00:00
- Number of days: 723
- Number of symbols: 939
- Missingness before QC: 29.46%
- Missingness after QC: 29.74%

## Repair Summary

### Edits by Rule
- mcap_spike: 3243 edits
- vol_spike: 1930 edits
- return_spike: 65 edits

### Top 20 Symbols by Number of Edits
- BPX: 177 edits
- MTBILL: 68 edits
- BTU: 57 edits
- EURCV: 57 edits
- MMEV: 54 edits
- REAL: 53 edits
- USYC: 53 edits
- PINKSALE: 51 edits
- AURA: 43 edits
- USUALUSDC+: 42 edits
- SUSDAI: 41 edits
- AINTI: 41 edits
- CUSDC: 40 edits
- WHITE: 39 edits
- ANKRETH: 37 edits
- AB: 35 edits
- MWC: 33 edits
- USDAI: 33 edits
- ELEPHANT: 30 edits
- YOUSD: 29 edits

## QC Configuration

- MCAP_MULT: 20
- RET_SPIKE: 5.0
- VOL_MULT: 50
- allow_ffill: False
- allow_interpolate: False
- allow_post_align_ffill: False
- max_ffill_days: 2
- mcap_volume_allow_zero: True
- prices_must_be_positive: True
