# ADR 002 — OAuth delegation for Drive export

- **Date**: 2026-04-17
- **Status**: Accepted

## Context

The repo originally used a **Google service account** for Google Drive uploads.

In practice, service-account uploads to a personal Gmail Drive can fail with `storageQuotaExceeded` because:

- Service accounts don’t have their own “My Drive” quota
- Service accounts typically need a **Google Workspace** environment and/or Shared Drives for quota-managed storage

We also want exports to land in a human-owned Drive so future analysis tooling can access them.

## Decision

Switch Drive export auth from service accounts to **user-delegated OAuth refresh token**.

- Auth is via env vars: `GDRIVE_OAUTH_CLIENT_ID`, `GDRIVE_OAUTH_CLIENT_SECRET`, `GDRIVE_OAUTH_REFRESH_TOKEN`
- Upload target is the owning user’s **My Drive** (folder `Render Exports`)

## Consequences

- Drive export works with personal Gmail Drive storage quotas (files live under the user account).
- Exports are easier to consume in future analysis chats/tools that can access user Drive.
- Tokens may need operational maintenance:
  - Refresh-token regeneration if the OAuth app is in **Testing** mode (short refresh-token lifetime)
  - Re-auth if the account security posture changes (e.g., password reset / revoked consent)

## Alternatives considered

- **Google Workspace**: rejected (recurring cost ~\$270/year; not currently justified).
- **Cloudflare R2**: rejected (future analysis tooling cannot directly read from R2 in the intended workflow).
- **Microsoft 365 / OneDrive**: not evaluated.

