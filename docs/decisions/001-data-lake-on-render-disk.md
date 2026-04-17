# ADR 001 — Data lake on Render persistent disk

- **Date**: 2026-04-17
- **Status**: Accepted

## Context

Render’s default filesystem is ephemeral across deploys/restarts. As a workaround, Parquet outputs were being committed into git so the app could “ship data” with the code.

This caused:

- Repo bloat and noisy diffs
- Confusing “source of truth” (git history vs. runtime state)
- Operational fragility (data accidentally overwritten by deploys)

## Decision

Move the curated data lake to Render’s persistent disk:

- **Production location**: `/data/curated/data_lake/` (Render disk mounted at `/data`)
- **Local fallback**: `<repo_root>/data/curated/data_lake/`
- All ingestion scripts resolve the path via `repo_paths.data_lake_root()` — never hardcode `data/curated/data_lake/`.

Bootstrapping:

- On first boot when `/data` is empty, `start_render.sh` seeds `/data/...` from the repo snapshot.
- Seeding is **non-destructive**: it never overwrites existing files on `/data`.

Git policy:

- New Parquet outputs are **gitignored**; do not commit new `.parquet` data to the repo.

## Consequences

- The repo stays lean (code + small snapshots only).
- Data persists across Render deploys/restarts.
- The single source of truth in production is the persistent disk under `/data`.

## Alternatives considered

- **Keep Parquet in git**: rejected (repo bloat, history churn, unclear runtime truth).
- **External object storage (S3 / R2)**: deferred (could be added later as a backup layer or replication target).

