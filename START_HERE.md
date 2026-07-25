# START_HERE

> **⚠️ Edits to the Drive copy are overwritten nightly.** Edit the repo copy,
> commit, push to main. Render's nightly export propagates to Drive.

**This file contains no facts.** It tells you what to read and, when two docs
disagree, which one wins. It holds no schemas, no rule text, no file IDs, no
counts — those go stale; pointers don't.

**Owner:** Dan · **Authority map last reconciled:** 2026-07-24

---

## Before you touch any data

Three non-negotiables. The full list is `DATA_LAKE_CONTEXT.md` §11 — its
numbering is canonical (see *Rule numbering* below).

1. **Freshness is content, never timestamps.** The nightly export re-uploads
   files whether or not they changed, so Drive `modifiedTime` advances daily on
   dead files. Read the actual max date. Two incidents came from this.
2. **Halt on unknown units.** Do not infer a unit. Check
   `data_dictionary.yaml`; if the column isn't there, stop and ask.
3. **Check standing before use.** A column existing does not mean it is live,
   enforced, or trustworthy. Tables carry `status` / `do_not_use` /
   `use_instead`; signals carry gotchas. Read them.

Corollary that has bitten twice: **an observed correlation is not a gate.** Do
not promote a signal to a decision trigger unless a doc says it is one.

---

## Authority map — who wins when docs disagree

| Topic | Authoritative source | Notes |
|---|---|---|
| Column semantics, units, per-column gotchas | `data_dictionary.yaml` | Byte-verified for `fact_*` / `silver_*`. Its `map_*`, `dim_*`, `single_coin_panel` entries are acknowledged stubs — verify against the file. |
| Lake catalog, tier limits, quality landmines, analysis rules, Drive file IDs | `DATA_LAKE_CONTEXT.md` | The catalog. Large; read the section you need, not the whole file. |
| Apathy Bleed — methodology, parameters, results, **current status** | `BACKTEST.md` | Newest strategy doc. Wins over `STRATEGIES.md` on anything numeric or status-related. |
| Apathy Bleed — thesis and design intent | `STRATEGIES.md` | Intent only. **Stale on status and results** — see Open Reconciliations. |
| `danlongshort` | `ARCHITECTURE.md` | Not in `STRATEGIES.md`, despite the name. |
| Repo structure, paths, pipeline shape | `ARCHITECTURE.md` | |
| Rule enforcement text | `.cursorrules` + `ARCHITECTURE.md` | But cite rules **by name**, numbered per `DATA_LAKE_CONTEXT.md` §11. |

**Rule numbering.** Three docs number the rules differently and they collide
(Curated-Lake-Only is §11 Rule 8 but ARCHITECTURE "Rule 1"). Cite rules by
name. When a number is unavoidable, pin it: "§11 Rule 8."

**Name collision.** `data_dictionary.yaml` (lowercase, YAML) is the lake
dictionary and is authoritative. `DATA_DICTIONARY.md` (uppercase, Markdown) is
a superseded legacy glossary. Do not confuse them.

---

## Reading order by task

- **Any data work at all** → this file, then `data_dictionary.yaml` for the
  columns you'll touch.
- **Backtest / strategy analysis** → add `BACKTEST.md`, then `STRATEGIES.md`
  for intent (reading Open Reconciliations first).
- **Pipeline / ingestion / data-quality work** → add `DATA_LAKE_CONTEXT.md`
  (§4 tier limits, §9 landmines, §11 rules) and `ARCHITECTURE.md`.
- **Anything touching BTC dominance** → `docs/runbooks/btcdom_macro_index.md`
  first. The signal was silently fabricated for ~6 months; the runbook explains
  what's trustworthy and from when. Two implementations exist: production is
  `scripts/data_ingestion/btcdom_backfill.py` (Step 3, hardened);
  `btcdom/btcdom_lake_native.py` is research, not wired in — don't promote it to
  production without inheriting the same guards (runbook: "Two BTCDOM implementations").
- **Before writing to the lake or the export** → `ARCHITECTURE.md` paths and
  `configs/gdrive_export.yaml`. Path resolution has caused three incidents.

Read the docs your task touches, not only the one it names. Listing the folder
first costs one call.

---

## Open reconciliations

*Temporary. Delete each line when the source doc is fixed. Dated so staleness
here is visible.*

| Conflict | Winner | Opened |
|---|---|---|
| `STRATEGIES.md` and `DATA_LAKE_CONTEXT.md` §5 describe Apathy Bleed as live with open positions | **VENUE-CONFIRMED exited 2026-05-12** (Variational export, not just §22). Source docs corrected in this change (STRATEGIES banner + §5 note). `apathy_bleed_book.csv` reconciled to 0 OPEN 2026-07-24 | 2026-07-24 |
| `apathy_bleed_book.csv` realized P&L (−$4,669.05, notional-based) vs exchange records | Venue is ground truth: **−$5,424.66** (price −$4,619.81 + funding −$804.85, ex-fees). Book's ~$49 gap = intended-size vs actual-fill qty; funding is a separate stream, not in the book | 2026-07-24 |
| A second strategy (**LLPROtrades** — discretionary, market-neutral) has run on the same Variational account since 2026-05-12; no repo doc describes it | `LLPRO_STRATEGY.md` (branch `docs/llpro-strategy-enrichment`, **pending merge**). Distinct from the systematic books; add to the authority map once merged | 2026-07-24 |
| `STRATEGIES.md` cites alpha 75.5% as `[VERIFIED]` | `BACKTEST.md` §22 — retracted as unverifiable, likely overstated 1.7–2.5×; auditable ≈30% mean / 41% peak | 2026-07-24 |
| `STRATEGIES.md` thesis claims 100% win rate | `BACKTEST.md` §22 — deploy-honest with-events baseline: ~79–80% win, Sharpe ~1.1, worst DD −61.7% | 2026-07-24 |
| `DATA_LAKE_CONTEXT.md` §5 calls `msm_timeseries` paused / 48 rows | `data_dictionary.yaml` (2026-07-22) — alive daily, 106 rows | 2026-07-24 |

---

## The failure mode to watch for

This system's characteristic bug is **silent degradation**: a component emits a
plausible value instead of admitting it has none. Confirmed instances — a price
column that went entirely null while timestamps stayed fresh; `NaN` rendering
as the string `"Falling"` for six months; dead files re-uploaded nightly.

None were caught by null-checks. All were caught by comparing a value against
an independent reference and finding it implausible.

If a number looks wrong, verify it against something outside the pipeline
before building on it — and report it rather than working around it.
