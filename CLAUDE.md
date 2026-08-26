# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

A collection of independent **OpenHEXA pipelines** for Niger's RISP (vaccination campaign
tracking) project. Each top-level directory is a self-contained pipeline: it extracts and
transforms data from an IASO form filled out by health workers during vaccination campaigns
(polio, rougeole, méningite, TCV, fièvre jaune, albendazole, vitamine A) and feeds an OpenHEXA
database that powers PowerBI dashboards (Couverture, Complétude, Stocks, Surveillance,
Communications, Comparaison des rounds).

There is no single build/test/run command for the whole repo — each pipeline directory is
deployed and versioned independently. Read `README.md` at the repo root first: it has a mermaid
diagram of how data flows between pipelines.

## Repository layout

Each pipeline lives in its own top-level folder and is a standalone OpenHEXA pipeline package:

```
<pipeline_name>/
  pipeline.py          # entry point — the @pipeline / @parameter decorated function
  config.py            # workspace paths + static config/lookup dicts for this pipeline
  shared_utils.py       # local copy of load_data/save_file/export_to_dataset (see below)
  utils.py             # pipeline-specific helper functions (optional)
  requirements.txt     # extra deps beyond the base openhexa.sdk image (optional)
  .vscode/launch.json  # debugpy "attach" config for remote pipeline debugging
```

Deploy workflows themselves live at the repo root (`.github/workflows/push-<pipeline-name>.yml`),
one file per pipeline that has one — GitHub Actions only ever discovers workflows under the
repo-root `.github/workflows/`, never inside a subdirectory, so a per-pipeline nested `.github`
folder would silently never run. Each workflow's `paths:` filter scopes it to its own pipeline
folder (plus `shared/utils.py`/`scripts/sync_shared_utils.py`, since a canonical-source change
should re-check every pipeline that syncs from it).

Pipelines, in rough data-flow order (see README.md diagram for the full picture):

- `extract_org_units` — pulls the IASO org-unit tree, produces raw + cleaned versions used
  by almost every other pipeline for org-unit matching. **Manual**, run rarely and on its own —
  deliberately excluded from `orchestrate_pipelines_flow`'s chain: the tree changes only
  occasionally (health-facility openings/closures/renames), while the pipeline itself is
  comparatively memory- and time-consuming, so it isn't worth re-running on every orchestration.
  Every downstream pipeline just keeps reading whichever org-unit tree was last extracted,
  however long ago that was.
- `extract_iaso_form_data` → `process_iaso_form_data` — pulls raw IASO form submissions,
  matches them to org units, cleans/reshapes them into `combined_iaso_data.parquet`.
- `extract_target_data` — the sole **manual** step of the whole flow (renamed from
  `process_historical_target_data_v2`, then from `process_target_data` once compilation was
  split out — see below); absorbs the "Configure" stage of the v2 architecture
  (`docs/ARCHITECTURE.md`). Imports ONE uploaded target spreadsheet (historical or new campaign
  alike, arbitrary/inconsistent layouts) via an auto-detecting engine, generates the matching
  expected-data-structure rows for that same run, and saves both as per-run files — it does
  **not** compile the combined datasets itself anymore (see `process_target_data` below).
  Absorbed and replaced five now-deleted v1 pipelines: `generate_targets_templates` (blank
  target-entry Excel templates — no longer needed, since new campaigns now go through this same
  auto-detecting engine as historical ones), the original `process_target_data` (turned
  filled-in templates into `combined_target_data.parquet`), `configure_new_campaign` (wrote new
  campaign config to `inputs/config` — replaced by the `campaign_start_date`/`campaign_end_date`
  parameters), and `combine_expected_data_structures` plus
  `create_expected_data_structure_for_historical_campaigns` (built the expected-data-structure
  rows separately). See "`extract_target_data` internals" below.
- `process_target_data` — a separate, lightweight, **automated** pipeline (not the same one as
  above despite the reused name — see the note in `extract_target_data`'s history): compiles
  `combined_target_data.parquet` / `expected_data_structure.parquet` from every per-run file
  `extract_target_data` has produced so far, under "historical targets processed" / "expected
  data structure processed". No parameters. Split out because recompiling both multi-million-row
  combined datasets from scratch was the expensive part of what used to be one pipeline, and a
  single file extraction shouldn't have to pay that cost synchronously every time. Runs
  automatically as the **first** step of `orchestrate_pipelines_flow`'s chain (before
  `extract_iaso_form_data`), since `process_iaso_form_data`/`build_visualisation_tables` downstream
  both expect these combined datasets to already reflect every extraction done so far.
- `build_visualisation_tables` — the Transform stage: builds the 17 coverage/completeness/stocks/
  surveillance/communications/filter tables that back the PowerBI dashboards, and saves/exports
  each as this pipeline's output (same parquet+dataset convention as every other pipeline). Does
  **not** write to the database itself.
- `load_visualisation_tables` — the Load stage: reads those same 17 tables back
  (`config.VISUALISATION_TABLE_NAMES`) and pushes each to the OpenHEXA database, replacing any
  existing table of the same name. Split out of `build_visualisation_tables`, which used to do
  both in one pipeline; the two lists of 17 table names (that pipeline's `outputs_dict` keys and
  this one's `VISUALISATION_TABLE_NAMES`) aren't shared code, so keep them in sync by hand if a
  table is ever added, renamed or removed.
- `orchestrate_pipelines_flow` — meta-pipeline that runs the other pipelines in sequence via
  the OpenHEXA API (`openhexa.toolbox` `OpenHEXAClient`), using `papermill` for notebook-based
  steps. Chain: `process_target_data` → `extract_iaso_form_data` → `process_iaso_form_data` →
  `build_visualisation_tables` → `load_visualisation_tables`. Two pipelines are deliberately not
  part of this chain, both manual: `extract_target_data` (the one per-campaign step) and
  `extract_org_units` (run rarely, whenever the org-unit tree actually needs refreshing — see
  its own entry above for why).
- `population_analysis/` — newer, separate line of work (WorldPop/INS raster-based population
  estimation), not yet wired into the main data flow above.

## Conventions shared across every pipeline

- **OpenHEXA SDK.** Pipelines are defined with `@pipeline(...)` from `openhexa.sdk`, with
  `@parameter(...)` decorators for run-time inputs (choices are static literals — the SDK
  parses them without executing code, so parameter `choices=[...]` lists cannot be built
  dynamically or imported from another module at decoration time).
  `current_run.log_info/log_warning/log_error` is the only logging mechanism — there is no
  `print`/`logging` usage. OpenHEXA collapses newlines within a single log call into one
  block, so multi-line messages are emitted as **one log call per line**, not one call with
  embedded `\n`.
- **Workspace paths.** Every `config.py` derives paths from `openhexa.sdk.workspace.files_path`
  and a shared `PROJECT_FOLDER = "multi-campagne"` root, e.g.
  `OUTPUTS_PATH = workspace.files_path / multi-campagne / outputs`. When running locally against
  a real workspace you need a `.env` with `HEXA_WORKSPACE`, `HEXA_SERVER_URL`, `HEXA_TOKEN` (do
  not commit real tokens).
- **I/O helpers.** `shared_utils.py` provides `load_data(name)` / `save_file(df, name)` for
  parquet round-tripping under `OUTPUTS_PATH`, and `export_to_dataset(df, path, dataset_name)`
  to publish a dataset (parquet+csv, versioned) to OpenHEXA — no xlsx export: Excel's
  1,048,576-row sheet limit crashes it on any large table (hit in practice on
  `expected_data_structure`), and parquet+csv cover the same need without that ceiling.
  `shared/utils.py` (repo root) is the single canonical source; every active pipeline's own
  `shared_utils.py` is a **generated copy** of it (OpenHEXA's deploy action uploads one pipeline
  folder at a time, so each pipeline still needs its own physical file at runtime — it just isn't
  hand-edited anymore). Never edit a pipeline's `shared_utils.py` directly: edit `shared/utils.py`
  and run `python scripts/sync_shared_utils.py` to regenerate every copy (`--check` verifies
  without writing; wired into every pipeline's deploy workflow that has one
  (`.github/workflows/push-<pipeline-name>.yml`, repo root), so a stale copy fails CI instead of
  deploying).
- **Org-unit matching.** Multiple pipelines fuzzy-match free-text place names (CSI/district
  names from Excel/IASO) against the IASO org-unit tree to attach `org_unit_id`. Unmatched
  names are logged as warnings and dropped rather than silently mismatched — preserve this
  behavior when touching matching code.
- **Deployment.** Every pipeline has a workflow file at `.github/workflows/push-<pipeline-name>.yml`
  (repo root) that auto-deploys it to OpenHEXA workspace `pev-niger-7cc1fb` on push to `main`
  (or manually via `workflow_dispatch`): `blsq/openhexa-cli-action` configures the OpenHEXA CLI
  using the `OH_TOKEN` repo secret, then `openhexa pipelines push <folder> -c <code> --yes` does
  the actual push. (An earlier version of these workflows used `blsq/openhexa-push-pipeline-action`
  directly instead — switched away from after it started failing with "Input 'token' is required"
  despite the secret being passed correctly.)
- **Local debugging.** `.vscode/launch.json` in each pipeline attaches `debugpy` on
  `localhost:5678` with `remoteRoot: /home/hexa/pipeline`, matching how the OpenHEXA pipeline
  runner mounts code inside its container — debugging is done against a running remote/container
  pipeline, not a plain local `python pipeline.py` invocation (though `if __name__ == "__main__"`
  blocks exist for quick local smoke-testing where the pipeline doesn't need real IASO/DB access).
- **Language.** All user-facing log messages, parameter names/help text, and Excel outputs are
  in French (this is a Niger MoH-facing project) — keep new log/parameter text in French to
  match the existing ones.

## `extract_target_data` internals (current focus of active work)

This is the pipeline most likely to be under active development, and — per the v2 architecture
migration (`docs/ARCHITECTURE.md`, plan at the time of writing in
`/home/lio_gdb/.claude/plans/fuzzy-wondering-cherny.md`) — the pipeline that absorbs the whole
"Configure" stage: target import AND expected-data-structure generation, for both historical and
new campaigns alike, through one unified path. It was renamed from
`process_historical_target_data_v2` once the five v1 pipelines it superseded
(`generate_targets_templates`, the original `process_target_data`, `configure_new_campaign`,
`create_expected_data_structure_for_historical_campaigns`, `combine_expected_data_structures`)
were deleted from the repo, leaving it the only target-processing pipeline — then renamed again,
from `process_target_data` to `extract_target_data`, when compiling the combined datasets was
split out into a new, separate, automated pipeline that reused the `process_target_data` name
(see below). `extract_target_data` remains the sole **manual** step; the OpenHEXA-deployed
pipeline's `name=` (and therefore its deployed identity/run history) was deliberately left
unchanged across that second rename — only the module/pipeline-id changed — to avoid orphaning
it the way an earlier, unrelated rename did (see `docs/WEBAPP.md` §3's "stale orphaned pipeline"
finding for that precedent).

Target-import engine — historical target spreadsheets arrive in inconsistent, ad hoc layouts
(different header rows, column orders, age-bracket labelings, district vs. CSI level,
typos/accents in place names). Rather than one hardcoded parser per file, it uses a **generic
auto-detecting engine**:

- `target_import.py` — the engine. Discovers header row(s), geographic columns, district/CSI
  level, and age-bracket columns purely from the sheet's own content; uses the run's derived
  `products` list (see below) plus `layouts.PRODUCT_DEFS`/`PRODUCT_SYNONYMS` to know which age
  columns to read for a given product and how to label them. Tracks every inference it makes
  ("assumptions") via `note_assumption`, deduplicated and logged once per distinct assumption
  plus a recap — extend this tracking rather than adding silent heuristics when handling new file
  quirks.
- `layouts.py` — static product/age-bracket definitions and synonyms, plus `CAMPAIGN_CHOICES`
  (the `campaign_name` parameter's choices → internal `campaign_name` + derived `products` list —
  kept in sync manually with the `choices` list in `pipeline.py`'s `campaign_name` `@parameter`;
  there's a comment marking this coupling).
- `text_match.py` — token-matching helpers (`match_token`, `find_token`, `matches`,
  `any_token_matches`) used by the engine to recognize labels despite wording variance.
- `geo_match.py` — fuzzy district-name reconciliation (`build_district_mapping`) against the
  IASO org-unit tree, plus the district-level matching stage that consumes it
  (`match_district_to_org_unit_id` and its reporting helpers) — independent from `utils.py`
  (used for CSI-level matching).
- `utils.py` — CSI-level org-unit matching: `org_unit_matching` (carried over unchanged from the
  pre-rename pipeline), the CSI-level matching stage that consumes it
  (`match_csi_to_org_unit_id` and its helpers, including the hand-curated `csi_matching_failed`
  corrections dict), and the post-matching org-unit cleanup shared by both levels
  (`add_region_names`, `clean_org_unit_id`).
- `run_persistence.py` — processed-file bookkeeping for this pipeline's own per-run save:
  detecting whether a run's (year, produit, round) combination already exists
  (`check_for_existing_slices`, checked against the last *compiled* combined dataset — see the
  staleness caveat in `pipeline.py`'s module docstring), and locating/removing superseded slices
  in overwrite mode. Compiling every per-run file in a folder into one combined dataset
  (`compile_processed_files`) used to live here too, but moved to `process_target_data`'s own
  copy of this module (same name, different, smaller content) once that became a separate
  pipeline — `extract_target_data` never calls it anymore.
- `validate.py` — validation helpers.
- `test_robustness.py` — a standalone robustness suite (not pytest-based): takes real workbooks,
  applies synthetic mutations (extra header rows, renamed/reordered/blank columns, typos, moved
  columns, accent/casing changes) and asserts the engine still produces identical per-product
  totals, plus checks that mutations which *should* fail raise `TargetImportError` with a clear
  message. Run it against a folder of real `.xlsx` target files:
  ```
  python test_robustness.py <folder_with_xlsx>
  ```
  There is no CI wiring for this — run it manually after touching `target_import.py`,
  `layouts.py`, or `text_match.py`.

Expected-data-structure module (`expected_structure.py`) — builds the product/site/status/sex/
age/period combinatorial rows for the SAME run's org-unit-matched target data (`matched`), not the
full combined dataset; that scoping is what makes a separate regional-restriction special case
unnecessary (a district that only has a target for one product can't get a spurious expected row
for a different product, since the cross-join never sees org units outside `matched`). Config
(`SITE_TYPE`/`PRODUCT_STATUS`/`SEX_TYPE`/`HISTORICAL_CAMPAIGNS_CONFIG`) lives in this file as one
named block, right alongside the functions that consume it — not in `config.py`, which (per
`docs/ARCHITECTURE.md` §14.2) is reserved for workspace paths and OpenHEXA connection details
only. This module also owns the date-overlap check for new campaign periods
(`check_for_date_overlap`), since it's a period/expected-structure concern.

Pipeline-level behavior worth knowing before modifying `pipeline.py`:
- `products` is **not** a user-facing parameter — it's derived from `campaign_name` (see
  `layouts.CAMPAIGN_CHOICES`). The polio choice is split into "couplée avec Albendazole et
  Vitamine A" / "non couplée" rather than one "coupled or not" option specifically so the
  expected products are always explicit — no product is ever silently optional.
- Each run's processed output is saved as its **own** parquet file (named by a deterministic
  slug of year/rounds/products) under both `outputs/historical targets processed/` (targets) and
  `outputs/expected data structure processed/` (expected structure). Compiling the combined
  datasets (`combined_target_data.parquet`, `expected_data_structure.parquet`) from every file
  in those folders is `process_target_data`'s job now, not this pipeline's — see below.
- Re-running the same (year, products, rounds) combination is guarded: by default the run
  aborts if that combination already exists (to avoid silent duplication); the
  `overwrite_existing` parameter allows replacing it, and the old data is only deleted *after*
  the new data has been produced successfully (so a failed run can't destroy existing targets).
  A separate check (`check_for_date_overlap`) guards against a *new* campaign's supplied dates
  overlapping an already-recorded round for the same product/year — only relevant when
  `campaign_start_date`/`campaign_end_date` are actually used (i.e. the combination isn't already
  in `HISTORICAL_CAMPAIGNS_CONFIG`). Both this check and `check_for_existing_slices` above read
  the last *compiled* combined dataset, which can be stale if `extract_target_data` has run more
  than once since `process_target_data` last compiled — an accepted, documented consequence of
  the split (see `pipeline.py`'s module docstring), not an oversight.

## `process_target_data` internals

The lightweight, **automated** compile-only pipeline split out of `extract_target_data` (see
above) — not the same pipeline as the one that used to have this name. No parameters; every run
updates `combined_target_data.parquet` / `expected_data_structure.parquet` from whatever per-run
files currently exist in `outputs/historical targets processed/` /
`outputs/expected data structure processed/` — **incrementally**, not by recompiling from
scratch: `run_persistence.compile_processed_files` keeps a small JSON manifest (per-run filename
→ mtime) recording which files backed the last compiled output, so a run reads and merges in
only the per-run files that are new or have a different mtime since then, and a run where
nothing changed skips the read/write entirely. A changed file's `(year, produit, round)`
combinations are dropped from the existing compiled output before its current rows are added
back — covers `extract_target_data`'s overwrite mode replacing stale data for a combination with
fresh data, not only the case of a genuinely new one. A previously-tracked file that's vanished
outright (not just replaced — `extract_target_data`'s overwrite mode always replaces, it never
removes a combination without writing its replacement elsewhere) is treated as unexpected enough
to fall back to a full recompile rather than risk keeping stale rows forever. This replaced an
earlier from-scratch-every-run implementation, which got slower and more memory-hungry with every
new campaign and made the pipeline's one large write (expected_data_structure alone can run to
~50M rows) more exposed to transient infrastructure failures — a real
`OSError: [Errno 116] Stale file handle` failure during that large a write is what prompted this,
alongside `shared/utils.py`'s `save_file`/`export_to_dataset` now retrying a write a few times on
`OSError` before giving up, for the same underlying class of transient mount error. Its
`run_persistence.py` holds only `compile_processed_files` (plus the manifest/incremental-merge
helpers) — a smaller module of the same name as `extract_target_data`'s own, not a shared file
between them (each pipeline needs its own physical copy, per this repo's usual convention of no
cross-pipeline imports at runtime). Runs as the first step of `orchestrate_pipelines_flow`'s
chain (see `orchestrate_pipelines_flow/config.py`).
