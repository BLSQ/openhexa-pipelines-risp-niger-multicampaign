# Pipeline inventory (Session 1, per ARCHITECTURE.md §11)

**Purpose:** validate `docs/ARCHITECTURE.md` §2 (fate mapping) and §6/D6 (table list) against what
the code actually does, and catalogue functions over ~50 lines for the eventual Refactor session
(§7). This is read-only inventory — no design decisions are made here (see the plan file for those:
`/home/lio_gdb/.claude/plans/fuzzy-wondering-cherny.md`).

**Corrections this inventory makes to the rest of the document**, stated up front so they aren't
missed reading linearly:

- §6/D6 lists 14 tables from `build_visualisation_tables`; it actually writes **17** (see that
  pipeline's section below).
- The v1→v2 handoff for target data (`process_historical_target_data` → `_v2`) is a filename-level
  operational switch, not a code dependency — downstream consumers `load_data("combined_historical_
  target_data")` by name, agnostic to which pipeline wrote it last.

---

## `process_historical_target_data` (v1)

**Files:** `config.py`, `pipeline.py` (1180 lines), `utils.py` (198 lines), `shared_utils.py` (139 lines)

**Inputs:** no `@parameter`s — fully automated, one-shot import of 7 hardcoded Excel filenames under
`TARGETS_HISTORICAL_PATH` (`inputs/cibles/historique/`): `Population JNV JNM ET DEPRARASITAGE.xlsx`,
`cible_niger_et_refugies_2025.xlsx`, `cible_csi_fj_dosso_tahoua.xlsx`, `Cible Men5-TCV CSI.xlsx`,
`cible_jnv_polio_2025.xlsx`, `Cible CSI JNV Avril 2026.xlsx`, `Population Niger_2026.xlsx`. Also
reads `iaso_org_unit_tree_raw`/`_clean` via `load_data`.

**Outputs:** `combined_historical_target_data` (parquet + dataset).

**Consumers:** `process_target_data`, `create_expected_data_structure_for_historical_campaigns`
(both via `load_data("combined_historical_target_data")`).

**Functions over ~50 lines:**

| Function | File | Lines |
|---|---|---|
| `add_rounds_and_products` | pipeline.py:785–1052 | 268 |
| `org_unit_matching` | utils.py:43–197 | 155 |
| `match_csi_to_org_unit_id` | pipeline.py:602–726 | 125 |
| `process_historical_target_data` (main) | pipeline.py:37–132 | 96 |
| `import_target_data_for_yellow_fever_2025_2026_r1` | pipeline.py:266–341 | 76 |
| `import_target_data_for_polio_2024_r1_r4` | pipeline.py:135–202 | 68 |
| `import_target_data_for_polio_2026_r1` | pipeline.py:404–471 | 68 |
| `import_target_data_for_albendazole_vitA_polio_2026_r3` | pipeline.py:535–599 | 65 |
| `clean_org_unit_id` | pipeline.py:1115–1175 | 61 |
| `import_target_data_for_polio_2026_r2` | pipeline.py:474–532 | 59 |
| `import_target_data_for_polio_and_rougeole_2025_r1_r2` | pipeline.py:205–263 | 59 |
| `import_target_data_for_men5_and_tcv_2025_r1_r2` | pipeline.py:344–401 | 58 |
| `match_district_to_org_unit_id` | pipeline.py:729–782 | 54 |

**Summary:** one bespoke parsing/melt function per historical file, fuzzy-matched to org units,
concatenated into the canonical `combined_historical_target_data`. Superseded by
`process_historical_target_data_v2`'s generic auto-detecting engine — not migrated, deleted once v2
absorbs its role (v1 stays read-only/untouched per §12/§13 until then).

---

## `process_historical_target_data_v2` (current)

Not re-inventoried here in the same table form — this is the pipeline under active development this
session; see `target_import.py`/`layouts.py`/`geo_match.py`/`text_match.py`/`pipeline.py`/`utils.py`
directly, and the plan file's "Session 4a" section for what it's absorbing next. Current scope:
single-file historical target import via a generic auto-detecting engine (no per-file config),
CSI/district org-unit matching, per-run-file + rebuild-from-scratch persistence for
`combined_historical_target_data`.

---

## `process_target_data`

**Files:** `config.py`, `pipeline.py` (562 lines), `utils.py` (68 lines), `shared_utils.py` (137 lines)

**Inputs:** no `@parameter`s. Scans `TARGET_OTHER_DATA_PATH` (`inputs/cibles/autres/`) for
`Cibles_{campaign}_{year}_{scale}_{level}.xlsx` files (validated by
`utils.validate_campaign_filename`). Also loads `iaso_org_unit_tree_raw`/`_clean` and
`combined_historical_target_data`.

**Outputs:** `combined_configured_target_data` (parquet only), `combined_target_data` (parquet +
dataset).

**Consumers:** `combined_target_data` → `build_visualisation_tables`, `generate_targets_templates`.
`combined_configured_target_data` → `configure_new_campaign`.

**Functions over ~50 lines:**

| Function | File | Lines |
|---|---|---|
| `add_round_info_to_configured_target_data` | pipeline.py:480–557 | 78 |
| `add_org_unit_ids` | pipeline.py:299–374 | 76 |
| `clean_org_unit_id` | pipeline.py:411–477 | 67 |
| `validate_campaign_filename` | utils.py:5–67 | 63 |
| `process_target_data` (main) | pipeline.py:32–91 | 60 |
| `process_single_target_file` | pipeline.py:189–246 | 58 |

**Summary:** imports user-filled new-campaign target files (matched by filename convention), infers
missing round numbers from the max historical round for the same product/year, unions with
historical data into `combined_target_data`. Per the approved plan, the round-inference logic is
dead code once v2 absorbs this (rounds become explicit required input); the filename-convention
scan is eliminated along with the template workflow.

---

## `generate_targets_templates`

**Files:** `config.py`, `pipeline.py` (406 lines). No `utils.py`.

**Inputs:** `@parameter`s — `campaign` (str, 7 choices), `campaign_scale` (str, multi-select, 8
choices), `year` (int, 2026–2050), `aggregation_level` (str: CSI/District). Reads
`iaso_org_unit_tree_clean`, `combined_target_data` (for overlap checks).

**Outputs:** a formatted Excel template file (`Cibles_{campaign}_{year}_{scale}_{level}_template.xlsx`)
registered via `current_run.add_file_output`. No parquet/dataset.

**Consumers:** none programmatic — pure manual hand-off (human fills it in, renames it, drops it in
`inputs/cibles/autres/` for `process_target_data` to pick up by filename pattern).

**Functions over ~50 lines:**

| Function | File | Lines |
|---|---|---|
| `create_template_file` | pipeline.py:285–401 | 117 |
| `validate_coherence_of_params` | pipeline.py:176–252 | 77 |

**Summary:** builds a formatted blank Excel template for a campaign/scale/year/level combination.
Eliminated per the approved plan — `process_historical_target_data_v2`'s auto-detecting engine reads
arbitrary layouts, so new campaigns no longer need a rigid template to fill in.

---

## `configure_new_campaign`

**Files:** `config.py`, `pipeline.py` (531 lines), `shared_utils.py` (139 lines).

**Inputs:** `@parameter`s — `campaign` (str, 7 choices), `year` (int), `campaign_scale` (str,
multi-select), `campaign_round_start_date`/`campaign_round_end_date` (str, `YYYY-MM-DD`),
`overwrite_existing_round` (bool). Reads `combined_configured_target_data`,
`expected_data_structure`, `iaso_org_unit_tree_clean`.

**Outputs:** `config_{campaign}_{year}_{round}` (parquet, dataset, and a physical copy dropped into
`CONFIG_PATH` = `inputs/config/`).

**Consumers:** `combine_expected_data_structures` scans `inputs/config/config_*.parquet` directly by
filename pattern.

**Functions over ~50 lines:**

| Function | File | Lines |
|---|---|---|
| `create_configuration_df` | pipeline.py:352–478 | 127 |
| `validate_coherence_of_params` | pipeline.py:239–349 | 111 |
| `inspect_params` | pipeline.py:168–236 | 69 |

**Summary:** validates a new campaign's round dates against existing config for overlap, builds a
fully expanded per-round configuration table (day × status × age × site × sex × org-unit), and drops
it where `combine_expected_data_structures` picks it up. Being folded into
`process_historical_target_data_v2` per the approved plan — see the plan's Open Items for the
`campaign_name` and site/status-value questions this raises, and Session 4a for the new
`campaign_start_date`/`campaign_end_date` parameters replacing this pipeline's dedicated ones.

**Config drift found (see plan "Open items"):** its `campaign_config_dict`'s `site`/
`vaccination_status`/`sexe` values disagree, product by product, with both
`create_expected_data_structure_for_historical_campaigns/config.py` and `ARCHITECTURE.md` §4/D2 —
not yet reconciled.

---

## `create_expected_data_structure_for_historical_campaigns`

**Files:** `pipeline.py` (366 lines), `config.py` (108 lines), `shared_utils.py` (139 lines).

**Inputs:** no `@parameter`s. Reads `combined_historical_target_data`. Hardcoded in `config.py`:
`product_site_config` (13-37), `sex_types_config` = `["TOUS"]` (39), `product_status_config`
(41-64), `historical_campaigns_config` (66-107 — keyed by `(year, round, campaign_name, produit)`,
**not** `(year, round, produit)` — see plan finding #6).

**Outputs:** `expected_data_structure_historical_campaigns` (parquet only, no dataset export).

**Consumers:** `combine_expected_data_structures` only.

**Functions over ~50 lines:**

| Function | File | Lines |
|---|---|---|
| `combine_dfs` | pipeline.py:221–325 | 105 |
| `export_to_dataset` | shared_utils.py:74–138 | 65 |
| `create_campaign_period_df` (borderline) | pipeline.py:170–220 | 51 |

**Summary:** cross-joins org units, sex, age/product/year/round, site-type, product-status, and
campaign date ranges into the expected combinatorial dataset for historical campaigns; applies one
manual restriction (yellow-fever 2025/2026 round 1 → Dosso/Tahoua only — load-bearing status
unconfirmed, see plan Open Items). Being folded into `process_historical_target_data_v2`.

---

## `combine_expected_data_structures`

**Files:** `pipeline.py` (141 lines), `config.py` (12 lines), `shared_utils.py` (139 lines).

**Inputs:** no `@parameter`s. Scans `inputs/config/config_*.parquet` (from `configure_new_campaign`).
Reads `expected_data_structure_historical_campaigns`.

**Outputs:** `expected_data_structure` (parquet only).

**Consumers:** `process_iaso_form_data`, `build_visualisation_tables`, `configure_new_campaign`
(three consumers — this is the most widely-depended-on intermediate artifact in the current
architecture).

**Functions over ~50 lines:**

| Function | File | Lines |
|---|---|---|
| `generate_expected_data_structure_for_new_campaigns` | pipeline.py:47–101 | 55 |
| `export_to_dataset` | shared_utils.py:74–138 | 65 |

**Summary:** merges the historical and newly-configured halves of the expected data structure into
one dataset. First stage in today's automated `orchestrate_pipelines_flow` chain. Eliminated per the
approved plan — its job becomes `process_historical_target_data_v2`'s combined
target-data-plus-expected-structure rebuild.

---

## `extract_iaso_form_data`

**Files:** `pipeline.py` (271 lines), `config.py` (32 lines), `utils.py` (521 lines),
`shared_utils.py` (139 lines).

**Inputs:** no `@parameter`s. Hardcoded: `workspace.get_connection("iaso-pev-niger")`,
`iaso_form_id = 1186`. Live IASO REST API calls via `IASOConnectionHandler`. Reads any pre-existing
`*.feather` cache files under `IASO_EXTRACTION_PATH` to skip already-extracted months.

**Outputs:** per-month `.feather` caches, `combined_iaso_data_raw` (parquet + dataset).

**Consumers:** `process_iaso_form_data` only.

**Functions over ~50 lines:**

| Function | File | Lines |
|---|---|---|
| `process_historical_and_current_data` | pipeline.py:180–270 | 91 |
| `extract_iaso_data_for_other_months` | pipeline.py:94–179 | 86 |
| `_get_data_structure_from_form_tuple` | utils.py:242–314 | 73 |
| `export_to_dataset` | shared_utils.py:74–138 | 65 |
| `_json_request_extract` | utils.py:420–476 | 57 |

**Summary:** extracts and caches IASO form 1186 submissions month by month, backfilling missing
columns against the live form schema, and combines everything into `combined_iaso_data_raw`. Matches
the target Extract stage as-is — no structural change expected.

---

## `extract_org_units`

Already deeply covered by this session's work (the CSI-name-collision fix). **Outputs:**
`iaso_org_unit_tree_raw`, `iaso_org_unit_tree_clean` (both parquet + dataset). **Consumers:**
essentially every other pipeline in the repo. Matches the target Extract stage as-is.

---

## `process_iaso_form_data`

Already deeply covered by this session's work (`align_to_clean_org_tree`). **Inputs:**
`iaso_org_unit_tree_raw`/`_clean`, `expected_data_structure`, `combined_iaso_data_raw`. **Outputs:**
`combined_iaso_data`. **Consumers:** `build_visualisation_tables`. Matches the target Transform
stage as-is — no structural change expected beyond whatever `expected_data_structure`'s new source
(process_historical_target_data_v2 instead of combine_expected_data_structures) requires, which is
none at the consumer side (same filename).

---

## `build_visualisation_tables`

**Files:** `pipeline.py` (1181 lines), `utils.py` (262 lines), `shared_utils.py` (138 lines),
`config.py` (838 lines).

**Inputs (all via `load_data`):** `combined_iaso_data` (← `process_iaso_form_data`),
`combined_target_data` (← `process_target_data`), `expected_data_structure` (←
`combine_expected_data_structures`), `iaso_org_unit_tree_clean`/`_raw` (← `extract_org_units`).

**Outputs — 17 DB tables, not 14** (write mode: `df.to_sql(..., if_exists="replace")`, full
replace every run; each table is also saved as a workspace parquet and exported as an OpenHEXA
dataset):

| Table | In ARCHITECTURE.md §6/D6? |
|---|---|
| `ner_vaccination_couverture` | Yes |
| `ner_vaccination_couverture_csi_district_cibled` | Yes |
| `ner_vaccination_completude` | Yes |
| `ner_vaccination_stock` | Yes |
| `ner_vaccination_supervision` | Yes |
| `ner_vaccination_communications_long` | Yes |
| `ner_vaccination_communications` | Yes |
| `ner_vaccination_cibles_district` | Yes |
| `ner_vaccination_campaign_filter_table` | Yes |
| `ner_vaccination_round_filter_table` | Yes |
| `ner_vaccination_year_filter_table` | Yes |
| `ner_vaccination_products_filter_table` | Yes |
| `ner_vaccination_combination_filter_table` | Yes |
| `ner_spatial_units` | Yes |
| `ner_vaccination_month_filter_table` | **No** |
| `ner_spatial_units_non_dynamic` | **No** |
| `ner_vaccination_campaign_round_summary` | **No** |

Per the approved plan, all 17 are preserved; §6/D6 should be corrected from 14 to 17 when
`ARCHITECTURE.md` is next updated.

**Structure:** ~8 largely-independent logic sections (coverage, completeness, stocks, supervision,
communications, filter tables, spatial units, campaign-round summary), sharing the same 5 loaded
input dataframes. Two light cross-section dependencies: stocks and campaign-round-summary both pull
from coverage's `cvrg_total`.

**Functions over ~50 lines** (12 total — the largest concentration of long functions in the repo):

| Function | File | Lines |
|---|---|---|
| `add_target_data` | pipeline.py:273–440 | 168 |
| `create_coverage_dataset` | pipeline.py:150–270 | 121 |
| `create_stocks_dataset` | pipeline.py:511–631 | 121 |
| `create_communication_dataset` | pipeline.py:722–834 | 113 |
| `build_visualisation_tables` (main) | pipeline.py:57–147 | 91 |
| `create_filter_tables` | pipeline.py:837–923 | 87 |
| `create_supervision_dataset` | pipeline.py:634–719 | 86 |
| `create_dynamic_org_unit_table` | pipeline.py:926–1000 | 75 |
| `create_completeness_dataset` | pipeline.py:443–508 | 66 |
| `export_to_dataset` (pipeline.py version) | pipeline.py:1114–1177 | 64 |
| `export_to_dataset` (shared_utils.py version) | shared_utils.py:74–138 | 65 |
| `process_target_level` | utils.py:191–262 | 72 |

**Code smells worth carrying into the Refactor session:** `pipeline.py` defines its own local
`export_to_dataset` (64 lines) instead of reusing the one already imported from `shared_utils.py`
(65 lines, near-identical) — duplicated, not shared. A `df = add_month_column(df)` reassignment loop
(pipeline.py:111-121) reassigns a local loop variable rather than the original dataframe names; it
happens to work today only because `add_month_column` mutates in place before returning, making the
reassignment dead code.

**Summary:** the largest, most complex pipeline in the repo. **Split done (Session 4b):**
`build_visualisation_tables` kept the table-generation logic (Transform) and now only
saves/exports its 17 outputs; the `write_to_db` DB-push loop moved to a new
`load_visualisation_tables` pipeline (Load), which reads those same 17 outputs back and pushes
each to the DB. Validated against the real local data in this repo (50M-row
`expected_data_structure`, 615K-row `combined_target_data`, etc.): all 17 tables still generate
correctly with the DB write removed, and Load correctly reads and would push each of them. The
12 over-50-line functions and the duplicated `export_to_dataset` noted above are unchanged by
this split - left for the later Refactor session, per §7.

---

## `orchestrate_pipelines_flow`

**Files:** `pipeline.py` (308 lines).

**Launch mechanism:** a hand-rolled `OpenHEXAClient` (defined inline) — bearer-token auth against
`https://app.openhexa.org`, pipeline triggers via plain `requests.post` to hardcoded, opaque
per-pipeline REST run-trigger URLs at `https://api.openhexa.org/pipelines/<base64-blob>/run`, status
polled via GraphQL (up to 3 retries, 10s apart). Not the OpenHEXA SDK's own pipeline-run API; not
papermill (imported, but its branch in `launch_action` is dead code — no action currently uses
`"type": "papermill"`).

**Current sequence (params `{}` for all four):** `combine_expected_data_structures` →
`extract_iaso_form_data` → `process_iaso_form_data` → `build_visualisation_tables`. Notably,
`generate_targets_templates`, `process_target_data`, `configure_new_campaign`, `extract_org_units`,
and `process_historical_target_data(_v2)` are **not** in this automated chain — they run manually/
out-of-band today.

**Config notes:** custom connection name used in code is `"risp-ner-campagnes-connection"`; this
directory's own local `workspace.yaml` defines a *different* name (`multi-campaign-workspace`) —
a local run against the checked-in `workspace.yaml` would not resolve the connection the code
actually asks for. Pre-existing staleness, not something this migration needs to fix, but worth
knowing before attempting a local run of this pipeline.

**Summary:** to be updated per §2 to run Extract → Transform → Load (Configure excluded, since it's
the one manual human-triggered step per §5). Whether to keep the current hand-rolled REST+GraphQL
mechanism or move to a more maintainable one is an open call for whoever picks up Session 4c — not
mandated either way by ARCHITECTURE.md.
