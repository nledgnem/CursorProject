# Response to Data Lake Inspection AI Agent

## Data Availability

The parquet files **DO exist** in `data/curated/data_lake/`. Please check that directory directly. Based on my codebase, the following files should be present:

**Dimension Tables:**
- `data/curated/data_lake/dim_asset.parquet`
- `data/curated/data_lake/dim_instrument.parquet`
- `data/curated/data_lake/map_provider_asset.parquet` (optional)
- `data/curated/data_lake/map_provider_instrument.parquet` (optional)

**Fact Tables:**
- `data/curated/data_lake/fact_price.parquet` (~11.8 MB, last updated 2026-01-27)
- `data/curated/data_lake/fact_marketcap.parquet` (~11.2 MB, last updated 2026-01-27)
- `data/curated/data_lake/fact_volume.parquet` (~11.8 MB, last updated 2026-01-27)
- `data/curated/data_lake/fact_funding.parquet` (~983 KB, last updated 2026-01-13)
- `data/curated/data_lake/fact_open_interest.parquet` (smaller, optional)

**Additional Files:**
- `data/curated/data_lake/funding_metadata.json` (metadata about funding data)
- `data/curated/data_lake/canonical_id_validation.json`
- `data/curated/data_lake/mapping_validation.json`

If you don't see these files, they may be:
1. In a different location (check `data/curated/` for wide-format files like `prices_daily.parquet`)
2. Not yet generated (unlikely, but possible)
3. Hidden or filtered by your file system

**Action:** Please use `list_dir` or `glob_file_search` to find all `.parquet` files in the workspace, starting from the root directory.

---

## Execution vs. Review

**Execute the inspection now.** The data exists and I want a comprehensive analysis. However, if you encounter issues finding the files, please:
1. Report what you find
2. Suggest alternative locations to check
3. Proceed with whatever data you can locate

---

## Output Format

**Preferred: Markdown report with embedded code snippets**

Please provide:
1. **A well-structured Markdown report** (`DATA_LAKE_INSPECTION_REPORT.md`) with:
   - Executive summary
   - Detailed findings
   - Code snippets for reproducibility
   - Visualizations (if helpful)

2. **Reusable Python functions** in a separate file (`inspect_data_lake.py`) that:
   - Can be run independently
   - Generate the same report
   - Include helper functions for common queries

**Why this format?**
- Markdown is readable and version-controllable
- Code snippets allow verification and reuse
- Separate script enables automation and updates

---

## Scope and Priorities

### Priority Order:
1. **Fact tables first** (fact_price, fact_marketcap, fact_funding, fact_volume)
   - These are the core time-series data
   - Most critical for analysis workflows
   
2. **Dimension tables** (dim_asset, dim_instrument)
   - Essential for understanding asset universe
   - Needed for joins and filtering

3. **Mapping tables** (map_provider_asset, map_provider_instrument)
   - Lower priority but useful for data lineage

### Memory Constraints:
- **Use Polars** (preferred) or **Pandas with chunking** for large files
- For fact tables, use lazy evaluation where possible:
  ```python
  import polars as pl
  df = pl.scan_parquet("fact_price.parquet")  # Lazy
  # Then filter/aggregate before collect()
  ```
- Don't load entire tables into memory if you only need summaries
- Use `.head()`, `.sample()`, or filtered queries for exploration

### Specific Focus Areas:
- **BTC and ETH coverage** (critical for MSM v0 benchmark)
- **Funding data completeness** (key for MSM v0 features)
- **Date range gaps** (especially for 2024, as we recently backfilled ETH)
- **Asset coverage** (top 30-50 assets by market cap)

---

## Data Freshness

**Check when data was last updated.** Please:
1. Report file modification times for each parquet file
2. Check the latest dates in each fact table
3. Identify any obvious gaps (e.g., missing recent dates)
4. Note if data appears stale (e.g., no data in last 7 days)

This is important because:
- We recently backfilled ETH data for 2024
- Incremental updates should be running regularly
- Stale data indicates pipeline issues

---

## Visualization

**Yes, include visualizations** where helpful:
- **Date range plots**: Show coverage timeline per asset (especially BTC, ETH, top alts)
- **Coverage heatmap**: Assets × Dates matrix showing data availability
- **Source distribution**: Pie/bar charts of data sources
- **Funding coverage**: Which assets/exchanges have funding data over time

**Format:** Save plots as PNG files in a `data_lake_inspection_plots/` directory, and reference them in the markdown report.

**Tools:** Use matplotlib or plotly. Keep plots simple and informative.

---

## Code Snippets

**Include reusable Python functions.** Please create `inspect_data_lake.py` with:

1. **Helper functions** for common queries:
   ```python
   def get_table_summary(filepath: Path) -> Dict
   def get_date_range(df: pl.DataFrame) -> Tuple[date, date]
   def get_asset_coverage(df: pl.DataFrame) -> Dict[str, int]
   def check_duplicates(df: pl.DataFrame, keys: List[str]) -> int
   ```

2. **Analysis functions** that generate specific insights:
   ```python
   def analyze_temporal_coverage(data_lake_dir: Path) -> Dict
   def analyze_data_quality(data_lake_dir: Path) -> Dict
   def analyze_asset_universe(data_lake_dir: Path) -> Dict
   ```

3. **Main function** that generates the full report:
   ```python
   def generate_inspection_report(data_lake_dir: Path, output_path: Path) -> None
   ```

**Why:** This allows me to:
- Re-run the inspection after data updates
- Integrate checks into CI/CD
- Use functions in other scripts
- Verify your findings independently

---

## Best Practices (Based on MSM v0 Structure)

### 1. **Use Polars for Efficiency**
MSM v0 uses Polars throughout. Follow this pattern:
```python
import polars as pl
df = pl.read_parquet(path)  # Fast, memory-efficient
# Use lazy evaluation for large operations
df_lazy = pl.scan_parquet(path).filter(...).collect()
```

### 2. **Respect Data Lake Schema**
Reference `src/data_lake/schema.py` for expected schemas. Validate:
- Column names match schema
- Data types are correct
- Required columns are present

### 3. **Handle Missing Data Gracefully**
MSM v0 handles missing data with coverage rules (≥60% threshold). Report:
- Which assets have incomplete coverage
- Date ranges with gaps
- Impact on potential analyses

### 4. **Focus on Canonical IDs**
The data lake uses canonical `asset_id`s (e.g., "BTC", "ETH", not "bitcoin" or "ethereum"). 
- Always use `asset_id` for joins
- Report any inconsistencies in ID usage
- Check alignment between dim_asset and fact tables

### 5. **Time-Series Best Practices**
- Report date ranges per asset (not just global min/max)
- Identify gaps in daily coverage
- Check for duplicate dates per asset
- Validate date ordering (should be chronological)

### 6. **Data Source Tracking**
The `source` column tracks data provenance. Analyze:
- Which sources provide which data types
- Source-specific coverage and quality
- Any source-specific issues

---

## Expected Workflow

1. **Discovery Phase:**
   - Find all parquet files
   - List file sizes and modification times
   - Identify schema files

2. **Schema Analysis:**
   - Read each table's schema
   - Validate against expected schemas
   - Report any discrepancies

3. **Data Profiling:**
   - Row counts, date ranges, unique values
   - Missing value analysis
   - Duplicate detection

4. **Coverage Analysis:**
   - Temporal coverage per asset
   - Asset coverage per table
   - Cross-table alignment

5. **Quality Assessment:**
   - Data consistency checks
   - Outlier detection
   - Source reliability

6. **Report Generation:**
   - Create markdown report
   - Generate visualizations
   - Save reusable code

---

## Questions to Answer

Please ensure your report addresses:
1. ✅ Can I run MSM v0 successfully with this data? (BTC/ETH prices, funding data, market caps)
2. ✅ What date ranges are available for analysis?
3. ✅ Which assets have the most complete data?
4. ✅ Are there any data quality issues that would affect analysis?
5. ✅ How fresh is the data? When was it last updated?
6. ✅ What analyses are possible with the current data coverage?

---

## Final Notes

- **Be thorough but efficient**: Don't spend excessive time on low-priority tables
- **Be specific**: Use exact numbers, dates, and examples
- **Be actionable**: Highlight issues that need attention
- **Be reproducible**: Include code that can verify your findings

Thank you! Please proceed with the inspection and report back with your findings.
