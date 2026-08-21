# WEBAPP.md — execution plan for the campaign-target web app

**Status: implemented and deployed** (`app/`, live at
`https://gestion-des-cibles-de-campagne.openhexa.io/`) **but blocked on a platform-side issue —
see §9.** This supersedes the original one-page instructions (kept in git history) with a plan
informed by two sources: the `OpenHEXA-WebApp-Diary` reference project's
[`PRODUCT_SPEC.md`](https://github.com/BLSQ/OpenHEXA-WebApp-Diary/blob/main/docs/PRODUCT_SPEC.md)
(a live, user-facing OpenHEXA static webapp already in production) and its
[`app/flowchart/`](https://github.com/BLSQ/OpenHEXA-WebApp-Diary/tree/main/app/flowchart)
implementation, plus the OpenHEXA GraphQL schema. Section 6 below flags where this plan departs
from the original instructions and why.

## 1. Goal, unchanged from the original brief

A no-code static webapp, deployed inside OpenHEXA workspace `pev-niger-7cc1fb`, that lets a
Niger MoH end user (Excel-skill floor, no code, not an OpenHEXA power user) push a new
campaign's targets into the dashboard without leaving a guided, single-purpose UI. Three
stages, unchanged from the brief:

1. **Upload** a target Excel file into the workspace.
2. **Run** `process_target_data` against that file, with its other parameters, and watch it
   to completion.
3. **Automate** the recurring `orchestrate_pipelines_flow` re-run once the campaign is live, and
   watch its 6-pipeline chain.

## 2. What this app is and is not (borrowed from PRODUCT_SPEC §5)

The reference project's most consequential product decision was drawing this boundary early,
and it transfers directly here:

- The app **runs and watches two specific pipelines**. It does not install, configure, or edit
  pipelines, connections, or workspace files in general — those stay in the native OpenHEXA UI.
- The app does **not** proactively check readiness. If a prerequisite is missing (e.g. the file
  is malformed, an org unit can't be matched), the user finds out from the pipeline's own error,
  the same way they would in the OpenHEXA UI — this app just makes that error legible without
  requiring OpenHEXA fluency to find it.
- **No confirmation modal before a run.** `process_target_data` already exposes its own
  `overwrite_existing` parameter and aborts on an unintended duplicate — that guard belongs to
  the pipeline, not a second gate in the app. A re-run's consequences should be legible in the
  UI's copy, not gated behind an "are you sure?" dialog.
- Single fixed language: **French**, no toggle. Unlike the reference project (English-first,
  building to an EN/FR toggle), every pipeline in this repo already logs and labels parameters in
  French only (`CLAUDE.md`'s repo-wide convention) — matching that rather than introducing a
  second language is both less work and more consistent.

## 3. Deployment mechanics (learned from the reference repo)

- A static webapp is a **multi-file bundle**: `index.html` is HTML-injected by OpenHEXA at serve
  time; every other file (`.js`, `.css`, `.json`) is served as-is, same origin as the GraphQL
  API. OpenHEXA injects `window.OPENHEXA.workspaceSlug` into the page at load.
- The app calls the OpenHEXA GraphQL API through a **same-origin proxy** at `/graphql/` — plain
  `fetch("/graphql/", {method:"POST", body: JSON.stringify({query, variables})})`, no separate
  auth token to manage client-side.
- **Deployment is exclusively via OpenHEXA MCP tools** — `list_static_webapps`,
  `get_static_webapp`, `update_static_webapp`, `edit_static_webapp_file`,
  `get_static_webapp_file`. There is **no CLI push path for static webapps** (unlike pipelines,
  which this repo already deploys via `blsq/openhexa-push-pipeline-action`). `update_static_webapp`
  takes full file contents inline and is used for new files or wholesale rewrites;
  `edit_static_webapp_file` does a targeted server-side find/replace for small edits without
  loading the whole file. Whichever agent implements this will need MCP access to the OpenHEXA
  instance to deploy — flagging this now since it's a different mechanism from every other
  pipeline in this repo.
- **Webapp scopes are granted when the webapp is created/edited in the OpenHEXA UI** (a workspace
  admin action, outside any tool this plan can drive) from the closed enum `WebappOperationScope`:
  `DATASETS_READ, DATASETS_WRITE, FILES_READ, FILES_WRITE, PIPELINES_READ, PIPELINES_RUN,
  USER_READ`, plus an eighth, `DATABASE_READ` → `executeSavedQuery` (read-only, saved queries by
  slug only, no ad-hoc SQL) — confirmed via both the MCP `get_help_or_doc` tool and OpenHEXA's
  public docs (docs.openhexa.com/static-webapps), which agree exactly on the scope table. Not
  needed by any of this plan's three stages, noted here only in case a future version wants the
  webapp to read something straight from the workspace database. This app needs: **`PIPELINES_READ`** (status polling), **`PIPELINES_RUN`**
  (launching `process_target_data` and `orchestrate_pipelines_flow`), **`FILES_WRITE`** +
  **`FILES_READ`** (uploading the target Excel file to the workspace bucket, and reading it back
  / reading run outputs). `USER_READ` and `DATASETS_READ`/`WRITE` are not needed — neither
  pipeline's parameters include a connection dropdown or dataset selector.
- No companion "catalog pipeline" (the reference project's `create_pipeline_cards`) is needed
  here. That pattern earns its cost when a webapp must generically render ~18 pipelines across
  many workspaces without a redeploy; this app targets exactly two fixed, known pipelines in one
  fixed workspace. Simpler: resolve each pipeline's UUID and current parameter spec **live** at
  page load via the `pipelines(workspaceSlug:, code:)` query (`PIPELINES_READ`), keyed on each
  pipeline's stable `code` (confirmed live in the real workspace: `process_target_data`'s current
  code is `multi-campagne-01-import-et-traitement-d-un-fichier-de-cibles`, the orchestrator's is
  `multi-campagne-02-orchestrate-etl-pipelines` — the hyphenated slug, not the human-readable
  `name` string, which is what actually needs to be looked up). That also means a pipeline's
  parameters changing on redeploy require no webapp change — the form is always generated from
  what's actually installed.
- **Confirmed live (2026-08-20) via the MCP connection to the real workspace**, beyond what the
  reference project's docs alone could show:
  - `process_target_data`'s 7 real parameters (`input_file`, `campaign_name` with its 7 choices,
    `year`, `rounds`, `campaign_start_date`, `campaign_end_date`, `overwrite_existing`) match
    what this plan assumed — the parameter-form design in §6 needs no rework.
  - The orchestrator (`multi-campagne-02-orchestrate-etl-pipelines`) currently has `schedule:
    null` and **zero run history** — it has never been run in production since this version was
    deployed. The "never run yet" state from §6 isn't a hypothetical edge case to design for
    speculatively; it's the current live state the webapp will show on day one.
  - Real uploaded target files live under `multi-campagne/inputs/cibles/historique/` (seen in a
    real run's `input_file` config value) — the webapp's upload box should default to writing
    there, matching existing convention, **unless** a separate folder is preferred for
    newly-configured (non-historical) campaigns specifically — worth confirming rather than
    assuming "historique" fits every case.
  - A pre-existing static webapp already exists in this workspace: `tableaux-de-bords`
    ("Tableaux de bords"), an **IFRAME** type (not static), public, pointing at
    `iaso.bluesquare.org/pages/routine/` — the *routine*-vaccination dashboard, unrelated to this
    plan's multi-campaign PowerBI dashboard. Not a conflict, but this plan's new webapp needs a
    visibly distinct name/slug so the two aren't confused in the workspace's webapp list.
  - **Housekeeping finding, outside this plan's scope but worth flagging separately:** the
    workspace still has a stale, orphaned pipeline literally named `process_target_data` (code
    `process-target-data`, 14 real historical runs, most recently 2026-08-18) that is *not* the
    pipeline the current repo deploys to anymore. Renaming the pipeline's `name=` in a recent
    commit caused OpenHEXA to deploy it under a **new** code
    (`multi-campagne-01-import-et-traitement-d-un-fichier-de-cibles`, only 1 run so far) rather
    than version the existing one — leaving the old one fully runnable, undeleted, and no longer
    receiving deploys. Separately, six more pipelines from the pre-reorg v1/v2 architecture
    (`multi-campagne-01-pipeline-de-creation-de-fichier-template-pour-les-cibles`,
    `multi-campagne-02-pipeline-d-importation-et-traitement-des-donnees-de-cibles`,
    `multi-campagne-03-pipeline-de-configuration-d-une-nouvelle-campagne`,
    `multi-campagne-etablissement-de-la-structure-des-donnees-attendues`,
    `multi-campagne-etablissement-de-la-structure-des-donnees-attendues-pour-les-campagnes-historiques`,
    `multi-campagne-import-et-traitement-des-donnees-historiques-de-cibles`) are also still live
    in the workspace despite being deleted from the repo per `CLAUDE.md`'s migration history.
    None of this blocks the webapp (resolving by the *current* code, as this plan already does,
    naturally targets the live one), but someone could still run a stale pipeline by mistake from
    the native OpenHEXA UI. Recommend a separate cleanup pass (archive/delete the orphaned ones)
    — not bundled into this webapp work unless you'd like it to be.

## 4. Feasibility findings that change the plan

Four of the original brief's requirements were checked directly against the GraphQL schema,
OpenHEXA's own static-webapp documentation, and the reference app's actual, shipped behavior.
Two are now definitively settled (one confirmed buildable as specified, one confirmed not
achievable as an in-app write), and two are scope/vocabulary calls rather than open feasibility
questions.

### 4.1 Automate toggle (stage 3) — **resolved: not achievable in-app, confirmed by OpenHEXA's own docs**

The brief asks for an in-app button that turns the `orchestrate_pipelines_flow`'s hourly cron
schedule on/off. The schema does have a real notion of a pipeline's schedule
(`Pipeline.schedule: String`, `PipelinePermissions.schedule: Boolean!`, and an `updatePipeline`
mutation that accepts a `schedule` field) — so OpenHEXA supports scheduling as a platform
concept. But OpenHEXA's own static-webapp documentation (`get_help_or_doc(topic:
"static-webapps")`) publishes the exhaustive scope-to-field table, and `updatePipeline` appears
under **none** of the seven scopes (`USER_READ, PIPELINES_READ, PIPELINES_RUN, FILES_READ,
FILES_WRITE, DATASETS_READ, DATASETS_WRITE`). This isn't a permission a webapp is merely missing
today — there is no scope combination that grants it. Consistent with this, the reference
project — which has shipped real scheduling-adjacent UI to real users — never implemented
schedule read or write from the webapp at all, and places even just **displaying**
"scheduled/automated runs" (read-only) at v2.

**Decided:** the "Automate" control is a **read-only display** of the orchestrator's current
schedule (`pipeline.schedule`, via `PIPELINES_READ`) plus a **deep link** to that pipeline's
native OpenHEXA "Scheduling and Notifications" page, where the actual toggle happens. The
campaign-period warning copy from the brief is shown next to this link. No spike needed — this
is confirmed from OpenHEXA's own documented scope contract, not inferred.

### 4.2 Stop button (stages 2 and 3) — **resolved: fully supported, build as specified**

`stopPipeline(input: {runId})` is a real mutation, and OpenHEXA's static-webapp scope table
confirms it directly: **`PIPELINES_RUN` grants `runPipeline` *and* `stopPipeline`** — the same
scope already planned for launching runs, no extra grant needed. (The reference app's own "Stop"
button only stops client-side polling rather than calling `stopPipeline` — that was their choice,
not a platform limitation; this plan doesn't need to repeat it.)

**Decided:** build the Stop button as originally specified, calling `stopPipeline` on the active
run's id, for both stage 2 (`process_target_data`) and stage 3 (`orchestrate_pipelines_flow`).

### 4.3 Color-coded info/warning/error log windows (stages 2 and 3) — **decided: lighter v1**

Good news: the schema supports this well. `PipelineRun.messages: [PipelineRunMessage!]!` returns
`{message, priority, timestamp}`, and `MessagePriority` is `CRITICAL | DEBUG | ERROR | INFO |
WARNING` — an almost exact match for the brief's info/warning/error split (fold `CRITICAL` into
the error bucket; hide `DEBUG` unless `enableDebugLogs` is set on the run). This is a real,
queryable field, not something inferred only from raw text — so this was a scope call, not a
feasibility one.

That said, the reference app **deliberately did not build this**, in either variant, even at v1:
its own poll query fetches only `{id status executionDate duration}` and links out to the native
OpenHEXA run page for "logs, messages, full detail." Their PRODUCT_SPEC explicitly places
"in-app failure help... run Messages / log excerpts" at **v2**, after user feedback, specifically
because it was judged more effort than the v1 bar warranted — not because it's infeasible.

**Decided (§8, Q2): go lighter for v1.** v1 shows only the run's `status` (§4.4) plus, on
`failed`, the Action box pulled from the run's most recent `ERROR`/`CRITICAL` message (a single
targeted `messages` fetch on failure, not a live scrolling window) — no live-updating,
color-coded, per-type sub-windows. Those sub-windows move to v2, matching the reference
project's own v1/v2 split. The stage-3 flowchart likewise shows only per-node status color in
v1, no per-node message windows.

### 4.4 Status vocabulary — reconcile, don't invent

`PipelineRunStatus` is `queued | running | success | failed | stopped | skipped | terminating` —
richer than the brief's four-word list (`running, succeeded, stopped, failed`). The app should
use the real enum throughout (mapping `success`→"Réussi", `failed`→"Échoué", etc. in copy) rather
than a simplified vocabulary that can't represent an actual state OpenHEXA reports, and should
have a defined visual treatment for `queued` (pending, not yet started) and `terminating`
(mid-stop, transient) even though the brief didn't ask for one — otherwise those states have to
fall back to something misleading (e.g. showing `queued` as if it were `running`).

## 5. Technical building blocks confirmed for reuse

These map directly onto the reference app's `app/flowchart/app.js` and the schema, and should be
adapted rather than re-invented:

| Need | GraphQL basis | Scope |
|---|---|---|
| Resolve pipeline UUID + current parameter spec live | `pipelines(workspaceSlug:, code:)` | `PIPELINES_READ` |
| Upload the target Excel file | `prepareObjectUpload(input:{workspaceSlug, objectKey, contentType})` → signed PUT URL | `FILES_WRITE` |
| Launch a pipeline run | `runPipeline(input:{id, config})` | `PIPELINES_RUN` |
| Poll one run's status/progress | `pipelineRun(id){status executionDate duration progress}` | `PIPELINES_READ` |
| Poll one run's color-coded messages | `pipelineRun(id){messages{message priority timestamp}}` | `PIPELINES_READ` |
| One query for all 5 orchestrated pipelines' latest run (stage-3 flowchart) | `pipelines(workspaceSlug:){items{id runs(orderBy: EXECUTION_DATE_DESC, perPage:1){status}}}` — the reference app's "cross-session status query" pattern, one round-trip for every node's badge | `PIPELINES_READ` |
| Run outputs / report link | `pipelineRun(id){outputs{...}}` + `prepareObjectDownload` for a signed URL | `FILES_READ` |
| Stop a run | `stopPipeline(input:{runId})` — confirmed covered by `PIPELINES_RUN` (§4.2) | `PIPELINES_RUN` |
| Read (display-only) the orchestrator's current schedule | `pipeline.schedule` — write (`updatePipeline`) is confirmed unreachable from any webapp scope (§4.1); this is deliberately read-only | `PIPELINES_READ` |

The 6 pipelines for stage 3's flowchart, in their fixed run order (from
`orchestrate_pipelines_flow/config.py`'s `PIPELINE_ACTIONS`; updated from the original 5 when
`process_target_data` was added as the chain's first step):

1. `multi-campagne-compilation-des-cibles-et-de-la-structure-attendue` (`process_target_data`)
2. `multi-campagne-extraction-des-unites-organisationnelles-iaso` (`extract_org_units`)
3. `multi-campagne-extraction-des-donnees-du-formulaire-iaso` (`extract_iaso_form_data`)
4. `multi-campagne-traitement-des-donnees-du-formulaire-iaso` (`process_iaso_form_data`)
5. `multi-campagne-construction-des-tableaux-pour-la-visualisation` (`build_visualisation_tables`)
6. `multi-campagne-envoi-des-tables-de-visualisation-vers-la-base-de-donnees` (`load_visualisation_tables`)

Note that `orchestrate_pipelines_flow` itself triggers these 6 via its own hand-rolled REST
client (`openhexa_client.py`), not the GraphQL `runPipeline` mutation — the webapp only ever
calls `runPipeline`/polls status on the **orchestrator pipeline** for stage 3's Launch/Stop/
Automate controls, and separately polls each of the 5 sub-pipelines' own latest run (via the
cross-session query above) purely to paint the flowchart. The two are independent data sources
that happen to be shown together.

## 6. Gaps identified versus the original brief (PRODUCT_SPEC-informed)

Beyond the three feasibility findings in §4, going through PRODUCT_SPEC.md surfaced requirements
the original brief didn't mention but that a real deployed instance of this pattern needed:

- **Missing/misconfigured pipeline detection.** If `process_target_data` or
  `orchestrate_pipelines_flow` isn't installed in this workspace (e.g. after a template update,
  or in a not-yet-fully-set-up workspace), the app should say so plainly rather than fail
  obscurely — mirroring the reference app's greyed-node + explanatory panel. Given this app only
  ever targets one fixed workspace, this is a smaller concern than for the reference app's
  multi-workspace case, but the failure mode is still worth a deliberate message rather than a
  silent blank screen.
- **Params-match-what's-installed.** Resolving the parameter form live from the installed
  pipeline version (§3) — rather than hardcoding the parameter list in the webapp bundle — avoids
  the exact drift the reference project called out as a real risk: a form offering a parameter
  the currently-installed pipeline version doesn't actually accept.
- **Report/output link-out.** The brief only asks for a dashboard link at the very end; the
  reference app additionally surfaces each run's own outputs (files/datasets) and — if useful —
  can embed an HTML report inline via `<iframe>` on a freshly-signed URL (GCS signed URLs don't
  send restrictive `X-Frame-Options`, confirmed feasible by the reference project's own spike).
  **Decided (§8, Q3): dropped for now.** `process_target_data` currently registers no outputs at
  all (confirmed live — see §8), so there's nothing to embed today. Not pursued for this round;
  worth revisiting if the pipeline is later extended to produce a registered HTML report.
- **"Never run yet" state.** The brief's status list has no state for "this pipeline exists but
  has no run history in this workspace" — needed on first load before any user has ever run
  either pipeline here.
- **What happens on page reload / across sessions.** The brief describes the UI as if always
  freshly launched; per the reference app's own explicit requirement, run status must **persist
  across reloads and different teammates** (it's read from OpenHEXA's own run history via
  `PIPELINES_READ`, not from in-memory webapp state), so a user reopening the app mid-run — or a
  different NMP staff member opening it — sees the real current state, not a blank slate.

## 7. Proposed phasing

Mirroring the reference project's own v0/v1/v2 discipline rather than promising everything at
once. No pre-implementation spike is needed — §4.1 and §4.2 are now settled directly from
OpenHEXA's own documented scope contract (confirmed 2026-08-20 via `get_help_or_doc(topic:
"static-webapps")`), not inferred or pending live validation.

- **v1 (first real version):** stages 1–2 in full — upload, parameter form, launch, poll,
  status display (§4.4's full vocabulary), Stop (§4.2, confirmed), and the Action box on failure
  (from the run's latest error message, not a live window — §4.3 decided) — plus stage 3's
  flowchart (status-colored nodes only), Launch/Stop on the orchestrator, the read-only schedule
  display + deep link for Automate (§4.1, confirmed), global-status messaging, and the static
  PowerBI link (§8 Q4). No embedded HTML report (§8 Q3 — dropped for now, nothing to embed).
- **v2 (only if real usage asks for it):** the full color-coded, live-updating, per-type
  info/warning/error message windows from §4.3 (both stage 2 and per-node in stage 3's
  flowchart) — deferred, not dropped. A real in-app schedule toggle is not on this roadmap at
  all unless OpenHEXA's platform-level scope model changes (§4.1) — that would need to come from
  the OpenHEXA team, not from this app.
- **Later, conditional on a pipeline-side change:** `process_target_data`'s embedded HTML report
  (§8 Q3) — parked, not designed out. If `process_target_data` is ever extended to register a
  generated HTML summary as a real output, the technical basis for embedding it (signed-URL
  `<iframe>`, confirmed frameable by the reference project's spike) is already known and just
  needs slotting into a stage-2 UI box.

## 8. Decisions (resolved)

All four open questions from the review draft are now settled; recorded here for traceability
rather than as open items:

1. **Automate/Stop designs (§4.1/§4.2) — settled, not just accepted.** Both are now confirmed
   directly from OpenHEXA's documented webapp scope contract, not spiked or assumed: Stop is
   fully supported (`PIPELINES_RUN` covers `stopPipeline`) and is built as originally specified;
   Automate is confirmed unreachable as an in-app write (`updatePipeline` isn't covered by any
   scope), so it ships as the read-only-display-plus-deep-link design. No platform change is
   being requested from the OpenHEXA team as a precondition.
2. **Message windows (§4.3) — lighter v1.** Status display + an Action box sourced from the
   run's latest error message ships in v1; the full color-coded, live per-type message windows
   are a v2 item (§7).
3. **HTML report embedding (§5/§6/§7) — dropped for now, revisit later.** The original premise
   (`process_target_data` has an HTML report worth embedding) turned out false: checking the two
   real, recent successful runs of this pipeline live (2026-08-18 and 2026-08-19) shows `outputs:
   []` on both — **the pipeline currently registers no outputs of any kind, HTML report
   included.** It writes parquet/csv files and dataset versions directly but never calls the
   SDK's output-registration step, so there's nothing for `pipelineRun.outputs` to return and
   nothing to embed today. **Decided: drop the feature from this plan entirely for now** — not
   built, not stubbed dark. If `process_target_data` is later extended to register a generated
   HTML report as a real output, the embedding technique itself is already validated (signed-URL
   `<iframe>`, confirmed frameable by the reference project's spike) and can be added as a
   self-contained follow-up without touching anything else in this plan.
4. **Dashboard link (§7) — static.** The PowerBI URL from the original brief is hardcoded in the
   bundle, not resolved dynamically.

**Nothing in this plan has been implemented.** With §4.1/§4.2 now settled (no spike needed), the
plan is ready to implement as written.

## 9. Post-deployment finding: file upload blocked by GCS bucket CORS (confirmed 2026-08-20)

Discovered testing the deployed webapp for real, after implementation — recorded here because
it materially blocks Étape 1's upload box as designed, not because it changes anything about
the plan's correctness up to this point.

**Symptom:** selecting a real `.xlsx` file gives "Échec de l'envoi du fichier" (or, before a
diagnostic-improving patch, the less specific "Failed to fetch").

**Confirmed root cause**, from the browser's own console error:

```
Access to fetch at 'https://storage.googleapis.com/hexa-data-pev-niger-7cc1fb/...'
from origin 'https://<webapp-uuid>.openhexa.io' has been blocked by CORS policy:
Response to preflight request doesn't pass access control check: No
'Access-Control-Allow-Origin' header is present on the requested resource.
```

- `prepareObjectUpload` (§5) correctly returns a valid, well-formed GCS V4 signed URL
  (`X-Goog-SignedHeaders=content-type;host`, matching exactly the headers this app sends).
- The failure is not in this webapp's code or the OpenHEXA GraphQL layer: it's the **GCS
  bucket `hexa-data-pev-niger-7cc1fb` itself having no CORS configuration** permitting
  cross-origin requests at all. A `PUT` is never a CORS-"simple" request (per the Fetch spec,
  regardless of headers), so the browser always preflights it with an `OPTIONS` request first —
  and that preflight gets no `Access-Control-Allow-Origin` back, so the browser aborts before
  the real `PUT` is ever sent. This is an infrastructure-level GCS setting, not fixable from
  webapp JavaScript or via any OpenHEXA MCP tool available to this session.
- **The gap likely isn't fixable per-webapp even on OpenHEXA's side.** The blocked request's
  origin was a webapp UUID subdomain (`https://u9txlgdimz9wx0lxzrh1hz09q6a5me13.openhexa.io`),
  not the friendly slug — a different, unpredictable value per webapp. A CORS allowlist can't
  enumerate these ahead of time; it would need a wildcard (e.g. `*.openhexa.io`) or some other
  mechanism. This suggests the officially-documented `FILES_WRITE` upload pattern
  (`docs.openhexa.com/static-webapps`) may not actually work from *any* OpenHEXA static webapp
  today, not just this one — worth stating plainly when this is reported upstream.
- **Checked for a same-origin workaround, found none viable.** `FILES_WRITE` also grants
  `writeFileContent` (via the `/graphql/` proxy, so no cross-origin PUT involved) — but its
  input is `content: String!` with **no encoding parameter**, unlike other file-input types in
  the same schema that explicitly offer `encoding: BASE64`. It reads as built for small text
  files, not binary `.xlsx` uploads; using it here would risk silently corrupting the file
  rather than actually fixing the problem.

**Status:**
1. **Report upstream — still outstanding.** This needs escalating to whoever administers this
   OpenHEXA workspace/instance's GCP infrastructure (BLSQ/OpenHEXA platform support), asking for
   a CORS rule on bucket `hexa-data-pev-niger-7cc1fb` (or wherever this generalizes) permitting
   `PUT` with header `content-type` from webapp origins - framed as a platform gap affecting the
   documented `FILES_WRITE` feature generally (the blocked request's origin was a per-webapp
   UUID subdomain, `https://<uuid>.openhexa.io`, not a predictable value a CORS allowlist could
   enumerate one webapp at a time), not a one-off workspace config request.
2. **Interim fallback — built and deployed.** Rather than a plain text-path field, Étape 1's
   upload box became a **file picker**: it lists `.xlsx` files already present under
   `multi-campagne/inputs/cibles/` (via `workspace.bucket.objects`, gated by `USER_READ` — swapped
   in for the now-unused `FILES_WRITE`) and the user selects one, with a link out to OpenHEXA's
   native Files browser to add a file first if needed. Selecting a file sets exactly the same
   `input_file` value a real upload would have, so nothing downstream (parameter form, launch,
   Étape 2) needed any change. The original upload-box code is preserved in git history to
   restore once the CORS gap is fixed upstream.
