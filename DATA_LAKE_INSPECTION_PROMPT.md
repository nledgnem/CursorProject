# Data Lake Inspection Prompt

Please perform a comprehensive inspection and analysis of my cryptocurrency data lake. I need you to:

## 1. Directory Structure Discovery
- Locate the data lake directory (likely at `data/curated/data_lake/` or similar)
- List all parquet files present
- Identify any related schema files or documentation

## 2. Table Inventory & Schema Analysis
For each parquet file found, please provide:

### Dimension Tables (if present):
- **dim_asset.parquet**: Asset metadata (symbols, names, chains, stablecoin flags, etc.)
- **dim_instrument.parquet**: Trading instrument metadata (exchanges, symbols, types)

### Fact Tables (time-series data):
- **fact_price.parquet**: Daily closing prices by asset and date
- **fact_marketcap.parquet**: Daily market capitalization by asset and date
- **fact_volume.parquet**: Daily trading volume by asset and date
- **fact_funding.parquet**: Perpetual futures funding rates by asset, instrument, exchange, and date

For each table, report:
- **Schema**: Column names, data types, and what each column represents
- **Row count**: Total number of records
- **Date range**: Earliest and latest dates (for fact tables)
- **Asset/Instrument coverage**: Number of unique assets/instruments
- **Data sources**: Unique values in the `source` column (if present)
- **Sample data**: Show 3-5 sample rows to illustrate the structure

## 3. Temporal Coverage Analysis
For each fact table, provide:
- **Earliest date**: First available data point
- **Latest date**: Most recent data point
- **Date coverage**: Are there gaps? Show a sample of date ranges per asset
- **Asset-specific coverage**: Which assets have the longest/shortest history?
- **Coverage quality**: Percentage of expected dates that have data (e.g., if daily data, how many days are covered vs. total possible days)

## 4. Data Quality Assessment
- **Missing values**: Count of null/NA values per column
- **Duplicate detection**: Are there duplicate (asset_id, date) or (asset_id, instrument_id, date) combinations?
- **Data consistency**: Do prices, marketcaps, and volumes align across tables for the same assets/dates?
- **Outlier detection**: Any obviously incorrect values (negative prices, extreme values, etc.)?

## 5. Asset Universe Analysis
- **Total unique assets**: How many distinct assets are tracked?
- **Major assets**: List top 20 assets by data coverage (most days of data)
- **Asset categories**: Breakdown by:
  - Stablecoins vs. non-stablecoins
  - Major cryptocurrencies (BTC, ETH) vs. altcoins
  - Assets with funding data vs. those without
- **Asset metadata**: What tags/categories are available in dim_asset (e.g., is_stable, is_wrapped_stable, chain, etc.)?

## 6. Funding Data Specifics
If fact_funding.parquet exists:
- **Exchange coverage**: Which exchanges are represented?
- **Instrument coverage**: How many unique instruments have funding data?
- **Asset coverage**: Which assets have funding rate data?
- **Temporal gaps**: Are there periods where funding data is missing for certain assets/exchanges?
- **Funding rate statistics**: Min, max, mean, median funding rates per asset

## 7. Data Source Analysis
- **Source diversity**: What data sources are used (e.g., "coingecko", "binance", "coinglass")?
- **Source distribution**: How many records come from each source?
- **Source-specific coverage**: Which sources provide which types of data?

## 8. Summary Report
Provide a concise executive summary including:
- **Overall data lake health**: Is the data complete, consistent, and ready for analysis?
- **Key strengths**: What data is most comprehensive?
- **Key gaps**: What data is missing or incomplete?
- **Recommended use cases**: What types of analyses can be performed with this data?
- **Data freshness**: How recent is the data? When was it last updated?

## Technical Requirements
- Use Polars or Pandas to read parquet files efficiently
- Handle large datasets appropriately (don't load everything into memory if not needed)
- Provide code snippets for any complex analyses
- Include visualizations if helpful (date range plots, coverage heatmaps, etc.)
- Be specific with numbers, dates, and examples

## Expected Output Format
Please organize your findings in a clear, structured report with:
1. Executive Summary
2. Table-by-Table Analysis
3. Temporal Coverage Details
4. Data Quality Findings
5. Asset Universe Overview
6. Recommendations

Thank you! Please start by exploring the directory structure and then proceed systematically through each table.
