# QC Curation Report

Generated: 2026-03-26T05:22:44.078302+00:00

## PRICES

- Date range: 2024-01-07 00:00:00 to 2026-03-25 00:00:00
- Number of days: 809
- Number of symbols: 2718
- Missingness before QC: 35.12%
- Missingness after QC: 35.15%

## MARKETCAP

- Date range: 2024-01-07 00:00:00 to 2026-03-25 00:00:00
- Number of days: 809
- Number of symbols: 2718
- Missingness before QC: 35.50%
- Missingness after QC: 35.90%

## VOLUME

- Date range: 2024-01-07 00:00:00 to 2026-03-25 00:00:00
- Number of days: 809
- Number of symbols: 2718
- Missingness before QC: 35.36%
- Missingness after QC: 35.61%

## Repair Summary

### Edits by Rule
- mcap_spike: 8833 edits
- vol_spike: 5605 edits
- return_spike: 646 edits

### Top 20 Symbols by Number of Edits
- OMNI: 258 edits
- LMEOW: 204 edits
- MIU: 178 edits
- SIREN: 172 edits
- SQD: 170 edits
- UBTC: 163 edits
- PNUT: 145 edits
- H: 125 edits
- CONAN: 111 edits
- DUCK: 97 edits
- BIG: 96 edits
- PUMP: 87 edits
- F: 85 edits
- BLOCK: 83 edits
- MBC: 79 edits
- USD1: 77 edits
- CLANKER: 76 edits
- BOOP: 71 edits
- JOJO: 71 edits
- BERT: 70 edits

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
