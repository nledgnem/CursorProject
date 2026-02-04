# Markdown Files Cleanup Guide

## Files to KEEP (Active/Important)

### Core Documentation
- `README.md` - Main project readme
- `docs/architecture.md` - Architecture documentation
- `docs/PARQUET_VS_DATABASE.md` - Technical docs
- `docs/query_examples.md` - Query examples
- `docs/snapshot_schema.md` - Schema documentation
- `docs/VALIDATION_FRAMEWORK.md` - Validation docs
- `GITHUB_SETUP_GUIDE.md` - Setup guide

### Recently Created (Keep)
- `DETAILED_EXPLANATION_FOR_MADS.md` - Just created, important for Mads
- `MONITOR_EVAL_SUMMARY_FOR_CHATGPT.md` - Just created, important
- `MONITOR_EVAL_FRAMEWORK.md` - Monitor evaluation framework docs

### Active Guides
- `INSTRUMENT_ASSET_LINKAGE.md` - Active reference
- `DATA_LAKE_FORMAT_EXPLANATION.md` - Active reference
- `PRE_COMMIT_CHECKLIST.md` - Active checklist
- `UPDATE_SCRIPTS_GUIDE.md` - Active guide

---

## Files to DELETE (Outdated Status/Summary Reports)

### Old Status Reports (Historical, no longer needed)
- `ASSET_COUNT_ANALYSIS.md`
- `AUDIT_REPORT.md`
- `AUDIT_SUMMARY.md`
- `CANONICAL_ID_VALIDATION_STATUS.md`
- `CHATGPT_RESPONSE_FINAL.md`
- `CHATGPT_RESPONSE.md`
- `CHATGPT_VERIFICATION_PACKAGE.md`
- `COINGLASS_INTEGRATION_SUMMARY.md`
- `DATA_LAKE_STATUS.md`
- `DATA_LAKE_SUMMARY.md`
- `DATA_LAKE_MIGRATION_SUMMARY.md`
- `DATA_LAKE_IMPLEMENTATION_STATUS.md`
- `DATA_SOURCES_ANALYSIS.md`
- `DATA_SOURCES_INTEGRATION_COMPLETE.md`
- `DATA_TABLES_LOCATION.md`
- `DATA_UPDATE_BEHAVIOR.md`
- `DATA_UPDATE_SUMMARY.md`
- `DIFF_AND_PROOF_BUNDLE.md`
- `DIFF_SUMMARY.md`
- `ELIGIBILITY_REFACTOR_SUMMARY.md`
- `FILES_NOT_ALIGNED.md`
- `FILES_TO_SEND_CHATGPT.md`
- `FILES_TO_SEND_TO_CHATGPT.md`
- `FIXES_APPLIED.md`
- `FIXES_SUMMARY.md`
- `GET_RLS_NET_QUICK_GUIDE.md`
- `HOW_TO_GET_RLS_NET.md`
- `ID_STANDARDIZATION_ANALYSIS.md`
- `IMPLEMENTATION_COMPLETE.md`
- `IMPLEMENTATION_STATUS.md`
- `LEGACY_FILES_DELETION_GUIDE.md`
- `LEGACY_FILES_DELETION_STATUS.md`
- `LEGACY_FILES_STATUS.md`
- `MAPPING_VALIDATION_IMPLEMENTATION.md`
- `MAPPING_VALIDATION_STATUS.md`
- `MIGRATION_COMPLETE_SUMMARY.md`
- `OUTPUT_FILES_ALIGNMENT.md`
- `PARQUET_VALIDATION_GUIDE.md`
- `PERP_LISTINGS_ALIGNMENT.md`
- `PHASE_0_TO_2_IMPLEMENTATION_SUMMARY.md`
- `PHASE_2.5_FIXES_APPLIED.md`
- `PIPELINE_ENHANCEMENTS.md`
- `PROGRESS_UPDATE_FOR_MADS.md`
- `PROJECT_CONTEXT_PROMPT.md`
- `QUERY_STATUS_SUMMARY.md`
- `STATUS.md`
- `UTILITY_SCRIPTS_UPDATE_SUMMARY.md`
- `VALIDATION_RESULTS.md`
- `VALIDATION_SUMMARY.md`
- `VERIFICATION_PACKAGE_FOR_MADS.md`
- `VERIFICATION_SNIPPETS_FOR_CHATGPT.md`
- `WHY_WIDE_FORMAT_IS_OK.md`

### Generated Output Files (Can be cleaned)
- `outputs/runs/*/outputs/*.md` - All generated reports per run (can be cleaned periodically)
- `outputs/losers_rebound_analysis.md`
- `outputs/report.md`

---

## Recommendation

**Safe to delete**: ~60+ status/summary files that are historical snapshots
**Keep**: Core docs, recent work, and active guides

The status files were useful during development/migration phases but are now outdated. The important information is either:
1. In the code itself
2. In the active documentation (docs/ folder)
3. In the recent detailed explanations

You can safely delete all the "Files to DELETE" listed above.


