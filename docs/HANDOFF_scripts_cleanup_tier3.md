# Handoff: Scripts Cleanup — Tier 3

**Context to load first:** `docs/DATA_LAKE_CONTEXT.md`, `.cursorrules`, `docs/HANDOFF_scripts_cleanup.md` (the original handoff that defined Tiers 1-3).

## Where we are

- Tier 1 commit: `8087120` — 32 chart PNGs deleted, 38 diagnostics moved to `notebooks/`, 2 CoinGlass orphans (`fetch_coinglass_funding.py`, `fetch_coinglass_oi.py`) moved to `scripts/archive/`.
- Tier 2 commit: `5f15367` — 10 orphans moved to `scripts/archive/`; 3 files explicitly kept (`run_backtest.py`, `convert_to_fact_tables.py`, `download_data.py`) because `scripts/run_pipeline.py` calls them; 4 files flagged for Tier 3.
- Branch: `claude/epic-swirles-c781e7`.
- Production pipeline (`run_live_pipeline.py`) and alert runners are unaffected — verified by module-import smoke test after each tier.

## The principle for Tier 3: err on the safer side

Deeper investigation of `scripts/run_pipeline.py` (done outside this branch, documented below) changed the picture:

- `outputs/runs/` shows 53 run directories produced by `run_pipeline.py` between 2025-12-19 and 2026-01-06. Pattern: heavy research/backtest usage Dec-Jan, then abrupt stop ~3.5 months ago.
- This is NOT dead code that never worked. It is recently-active code that went dormant when Dan pivoted to live Apathy Bleed.
- Dan has decided: leave `run_pipeline.py` and its dependency chain intact. The research pipeline may be revisited later.

The safety rule for Tier 3:

> Only archive a file when either (a) grep shows zero references from live code AND zero activity in the last 6 months, or (b) the file has a structural defect that proves it never successfully executed (e.g. parse-time syntax error).

Stale `.md` progress reports in the repo root do NOT count as live references. They are historical artifacts. But recent run-directory production in `outputs/runs/` DOES count as activity.

## Tier 3 Step 0 — Fix the latent break Tier 1 introduced (REQUIRED BEFORE ANYTHING ELSE)

Tier 1 archived `scripts/fetch_coinglass_funding.py`. But `scripts/run_pipeline.py` (Step 2.5) references it:

```python
funding_cmd = [sys.executable, str(script_dir / "fetch_coinglass_funding.py")]
```

If anyone re-runs `run_pipeline.py --coinglass-api-key <key>` after the cleanup merges to main, the subprocess will fail with file-not-found. This is a latent bug Tier 1 introduced that current tests don't catch (no test passes `--coinglass-api-key`).

### Fix

Replace the reference to `fetch_coinglass_funding.py` with a call to the combined fetcher, which is on the production path and therefore stable:

**In `scripts/run_pipeline.py` Step 2.5 (around the funding_cmd construction):**

```python
# Before
funding_cmd = [sys.executable, str(script_dir / "fetch_coinglass_funding.py")]
funding_output = data_lake_dir / "fact_funding.parquet"
funding_cmd.extend(["--api-key", args.coinglass_api_key])
funding_cmd.extend(["--output", str(funding_output)])
if args.incremental:
    funding_cmd.append("--incremental")

# After
funding_cmd = [sys.executable, str(script_dir / "fetch_coinglass_data.py"), "--fetch-funding"]
funding_output = data_lake_dir / "fact_funding.parquet"
funding_cmd.extend(["--api-key", args.coinglass_api_key])
funding_cmd.extend(["--funding-output", str(funding_output)])
if args.incremental:
    funding_cmd.extend(["--incremental", "--merge-existing"])
```

Three changes:

1. Script name: `fetch_coinglass_funding.py` → `fetch_coinglass_data.py --fetch-funding` (combined fetcher scoped to funding only)
2. Output flag name: `--output` → `--funding-output` (the combined script's flag name; verify by reading `scripts/fetch_coinglass_data.py` arg parser around line 870-900)
3. Added `--merge-existing` when `--incremental` is set (combined script pattern; prevents accidental full rewrite)

**Verify the flag names by reading `scripts/fetch_coinglass_data.py` arg parser first.** The specifics above are what I expect from prior reads, but Claude Code should confirm and adjust.

**Smoke test after this change:**

```bash
python scripts/run_pipeline.py --mode smoke --skip-download --skip-qc --skip-snapshots --skip-backtest --skip-validation --help
```

A `--help` invocation verifies the script parses; nothing needs to actually run. If Claude Code has time + API budget, a smoke-mode run with `--skip-funding` would exercise more of the pipeline without touching the changed code path.

**Commit message:** `fix(scripts): update run_pipeline.py to use combined CoinGlass fetcher after Tier 1 archived the standalone`

## Tier 3 Step 1 — Easy archive case: `validate_canonical_ids.py`

51 KB file with a pre-existing `SyntaxError` at line 1066 ("expected an indented block after 'else'") present since initial commit.

A file with a parse-time syntax error has never been successfully imported or executed. Any live code depending on it would have failed at import time. Therefore by definition nothing live depends on this file.

**Action:** `git mv scripts/validate_canonical_ids.py scripts/archive/validate_canonical_ids.py`

Update `scripts/archive/README.md` with a note:

```
- validate_canonical_ids.py (51 KB): Archived 2026-04-23. Contains a pre-existing
  SyntaxError at line 1066 present since initial commit. Never successfully
  imported or executed; no live code depends on it. If revived, the syntax
  error at line 1066 must be fixed first.
```

**Commit message:** `chore(scripts): archive validate_canonical_ids.py (broken since initial commit)`

## Tier 3 Step 2 — Process the 4 original Tier 3 candidates

In your Tier 2 summary, you flagged 4 files for Tier 3 review but didn't list them. For each one:

1. Run:
   ```bash
   grep -rn "<script_name>" \
       --include="*.py" --include="*.sh" --include="*.yaml" --include="*.yml" \
       --include="*.toml" . | grep -v "scripts/archive/"
   ```
   (excluding already-archived files, which shouldn't count as live references)

2. For each hit, categorize:
   - **Live code reference** (production pipeline, alert runners, bots, heartbeat, `start_render.sh`, `run_live_pipeline.py`, anything under `src/`, `configs/`, `dashboards/`) → KEEP. Stop investigating this file.
   - **Research code reference** (`scripts/run_pipeline.py`, `scripts/run_backtest.py`, etc.) → KEEP. The research pipeline is intact by Dan's decision. Stop investigating.
   - **Test code reference** — check if the test is run by CI (there is no CI currently) or imported from anywhere else. If neither: NOT a live reference.
   - **`.md` doc reference** — NOT a live reference. Most `.md` files in the repo root are stale progress reports.
   - **Archived-script reference** (file already in `scripts/archive/`) — NOT a live reference.

3. For each candidate, produce a verdict block of this form:

   ```
   scripts/<name>.py (<size>):
     mtime: YYYY-MM-DD
     grep hits (live refs only): <count>
         <file:line> — <brief reason it's a live ref, or why not>
     verdict: ARCHIVE | KEEP
     reasoning: <one sentence>
   ```

4. Only ARCHIVE files where zero hits are live references AND mtime is > 6 months ago. If either fails, KEEP.

5. Move all ARCHIVE verdicts in a single commit. Update `scripts/archive/README.md` with one bullet per archived file.

**Commit message:** `chore(scripts): Tier 3 — archive N verified orphans (see verdicts in commit body)` with the verdict blocks in the commit body.

## Explicit NOT-dos for Tier 3

- **Do NOT archive `scripts/run_pipeline.py`** — active research pipeline, used through 2026-01-06, Dan may return to it.
- **Do NOT re-evaluate `run_backtest.py`, `convert_to_fact_tables.py`, `download_data.py`** — they're dependencies of `run_pipeline.py`, which is staying. They stay.
- **Do NOT touch `tests/test_pipeline_modes.py`** — lives with `run_pipeline.py`.
- **Do NOT delete anything.** Archive (`git mv` to `scripts/archive/`) only.
- **Do NOT modify the fetch path for funding in any way beyond Step 0 above.** The production daily pipeline calls `fetch_coinglass_data.py` with its own flags via `run_live_pipeline.py`; do not change that.
- **Do NOT touch anything under `scripts/data_ingestion/`, `scripts/live/`, `scripts/research/`, `scripts/ls_basket_low_vol/`.** These are subdirectories with their own contents not yet audited; leave for a future pass.

## Smoke tests required between steps

After Step 0 (the fix):
```bash
python -c 'import run_live_pipeline'  # exit 0
python scripts/run_pipeline.py --help  # should print help without error
```

After Step 1 (validate_canonical_ids archive):
```bash
python -c 'import run_live_pipeline'  # exit 0
```

After Step 2 (Tier 3 candidates):
```bash
python -c 'import run_live_pipeline'  # exit 0
```

If any smoke test fails, STOP and revert that step's commit before proceeding.

## When done

Report back with:

1. Step 0: confirmation of the fix and smoke test result
2. Step 1: confirmation of archive
3. Step 2: the 4 verdict blocks, and which were archived vs kept
4. Final state: `scripts/` file count, `scripts/archive/` file count
5. Branch head commit hash

Pause for review before merging to main.

## Out of scope for Tier 3 (future passes)

Flag for a separate future handoff, don't action now:

- Audit of ~60 `.md` files in repo root (most are stale progress reports from earlier project phases).
- Audit of scripts/data_ingestion/, scripts/live/, scripts/research/, scripts/ls_basket_low_vol/ subdirectories.
- Fix hardcoded `PROJECT_ROOT / "scripts" / "chart_*.png"` output paths in the moved diagnostic scripts (so they write to `notebooks/out/` instead of back into `scripts/`).
- Delete or fix `scripts/validate_canonical_ids.py`'s syntax error if the script is ever revived.
