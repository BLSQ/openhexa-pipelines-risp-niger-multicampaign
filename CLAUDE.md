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
  .github/workflows/push-pipeline.yml   # deploys to OpenHEXA on push to main
  .vscode/launch.json  # debugpy "attach" config for remote pipeline debugging
```

Pipelines, in rough data-flow order (see README.md diagram for the full picture):

- `extract_org_units` — pulls the IASO org-unit tree, produces raw + cleaned versions used
  by almost every other pipeline for org-unit matching.
- `extract_iaso_form_data` → `process_iaso_form_data` — pulls raw IASO form submissions,
  matches them to org units, cleans/reshapes them into `combined_iaso_data.parquet`.
- `generate_targets_templates` — produces blank target-entry Excel templates per campaign.
- `process_target_data` — turns filled-in target templates into `combined_target_data.parquet`.
- `process_historical_target_data_v2` — current, actively-developed pipeline for importing
  *historical* target spreadsheets (arbitrary/inconsistent layouts). Supersedes
  `process_historical_target_data` (v1, kept for reference/rollback). See "process_historical_target_data_v2 internals" below.
- `configure_new_campaign` — writes new campaign configuration to `inputs/config`.
- `combine_expected_data_structures` / `create_expected_data_structure_for_historical_campaigns`
  — build the "expected data structure" (every valid combination of product/site/age/sex/
  round/year) used to detect missing or invalid IASO submissions.
- `build_visualisation_tables` — final aggregation step; writes coverage/completeness/stocks/
  surveillance/communications tables that back the PowerBI dashboards.
- `orchestrate_pipelines_flow` — meta-pipeline that runs the other pipelines in sequence via
  the OpenHEXA API (`openhexa.toolbox` `OpenHEXAClient`), using `papermill` for notebook-based
  steps.
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
- **I/O helpers.** `shared_utils.py` (duplicated per pipeline, not a shared package) provides
  `load_data(name)` / `save_file(df, name)` for parquet round-tripping under `OUTPUTS_PATH`, and
  `export_to_dataset(df, path, dataset_name)` to publish a dataset (parquet+xlsx+csv, versioned)
  to OpenHEXA. There's also a repo-root `shared/utils.py` with the same functions — treat it as
  the canonical version if consolidating; pipelines have not all been migrated to import from it.
- **Org-unit matching.** Multiple pipelines fuzzy-match free-text place names (CSI/district
  names from Excel/IASO) against the IASO org-unit tree to attach `org_unit_id`. Unmatched
  names are logged as warnings and dropped rather than silently mismatched — preserve this
  behavior when touching matching code.
- **Deployment.** Each pipeline with a `.github/workflows/push-pipeline.yml` auto-deploys to
  OpenHEXA workspace `pev-niger-7cc1fb` (or `risp-rpj` for `population_analysis/ins_population_extraction`)
  on push to `main`, via `blsq/openhexa-push-pipeline-action`, using the `OH_TOKEN` repo secret.
  Not every pipeline directory has this workflow yet (e.g. `build_visualisation_tables`,
  `process_target_data`, `orchestrate_pipelines_flow`, `generate_targets_templates` are deployed
  some other way or not yet wired to CI) — check before assuming a push auto-deploys.
- **Local debugging.** `.vscode/launch.json` in each pipeline attaches `debugpy` on
  `localhost:5678` with `remoteRoot: /home/hexa/pipeline`, matching how the OpenHEXA pipeline
  runner mounts code inside its container — debugging is done against a running remote/container
  pipeline, not a plain local `python pipeline.py` invocation (though `if __name__ == "__main__"`
  blocks exist for quick local smoke-testing where the pipeline doesn't need real IASO/DB access).
- **Language.** All user-facing log messages, parameter names/help text, and Excel outputs are
  in French (this is a Niger MoH-facing project) — keep new log/parameter text in French to
  match the existing ones.

## `process_historical_target_data_v2` internals (current focus of active work)

This is the pipeline most likely to be under active development. Historical target
spreadsheets arrive in inconsistent, ad hoc layouts (different header rows, column orders,
age-bracket labelings, district vs. CSI level, typos/accents in place names). Rather than one
hardcoded parser per file, it uses a **generic auto-detecting engine**:

- `target_import.py` — the engine. Discovers header row(s), geographic columns, district/CSI
  level, and age-bracket columns purely from the sheet's own content; uses the run's `products`
  parameter plus `layouts.PRODUCT_DEFS`/`PRODUCT_SYNONYMS` to know which age columns to read for
  a given product and how to label them. Tracks every inference it makes ("assumptions") via
  `note_assumption`, deduplicated and logged once per distinct assumption plus a recap — extend
  this tracking rather than adding silent heuristics when handling new file quirks.
- `layouts.py` — static product/age-bracket definitions and synonyms (kept in sync manually
  with the `products` parameter `choices` list in `pipeline.py` — there's a comment marking
  this coupling).
- `text_match.py` — token-matching helpers (`match_token`, `find_token`, `matches`,
  `any_token_matches`) used by the engine to recognize labels despite wording variance.
- `geo_match.py` — fuzzy district-name reconciliation (`build_district_mapping`) against the
  IASO org-unit tree, independent from `utils.org_unit_matching` (used for CSI-level matching).
- `utils.py` — CSI-level org-unit matching (`org_unit_matching`), used unchanged from the v1
  pipeline.
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

Pipeline-level behavior worth knowing before modifying `pipeline.py`:
- Each run's processed output is saved as its **own** parquet file (named by a deterministic
  slug of year/rounds/products) under `outputs/historical targets processed/`; the combined
  dataset (`combined_historical_target_data.parquet`) is always **rebuilt from scratch** by
  concatenating every file in that folder, not incrementally appended.
- Re-running the same (year, products, rounds) combination is guarded: by default the run
  aborts if that combination already exists (to avoid silent duplication); the
  `overwrite_existing` parameter allows replacing it, and the old data is only deleted *after*
  the new data has been produced successfully (so a failed run can't destroy existing targets).
