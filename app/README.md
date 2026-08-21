# Campaign-target webapp

A static OpenHEXA webapp implementing the plan in
[`../docs/WEBAPP.md`](../docs/WEBAPP.md) — read that first for the full rationale, decisions,
and confirmed feasibility findings behind what's built here.

**Deployed** (2026-08-20): `gestion-des-cibles-de-campagne`, private, at
<https://gestion-des-cibles-de-campagne.openhexa.io/> in workspace `pev-niger-7cc1fb` — webapp
id `7d3c43b9-c628-4289-8672-29985ffda140` (needed for `update_static_webapp`/
`edit_static_webapp_file`; re-fetch via `list_static_webapps` if this ever changes). Being
private, it's only reachable by an authenticated member of that workspace — open the URL while
logged into OpenHEXA to see it.

**⚠️ Known blocker (confirmed 2026-08-20), worked around for now.** Real browser→GCS upload
(`prepareObjectUpload` + `PUT` to a signed URL) is blocked by a CORS gap on the GCS bucket
backing this workspace's files — confirmed via a real failed upload, not fixable from `app.js`
or via any OpenHEXA API. This is a platform infrastructure gap; full diagnosis in
[`../docs/WEBAPP.md`](../docs/WEBAPP.md) §9 — still needs escalating to BLSQ/OpenHEXA platform
support to actually restore real upload-from-your-machine.

**Interim fix, built and live**: Étape 1's upload box is currently a **file picker**, not an
upload — it lists `.xlsx` files already present under `UPLOAD_FOLDER`
(`multi-campagne/inputs/cibles/`) via `workspace.bucket.objects` and lets the user select one,
with a link out to OpenHEXA's native Files browser to add a file first if it isn't there yet.
Revert to the original upload-box design (code still in git history) once the CORS gap is
fixed upstream.

## Files

- `index.html` — page shell (Étape 1 / Étape 2 cards). Injected with `window.OPENHEXA` by
  OpenHEXA at serve time.
- `app.js` — all logic: GraphQL calls, form generation, run/poll/stop, the flowchart. See its
  header comment for the two independent flows it implements.
- `styles.css` — plain, dependency-free CSS. No build step.

This is **not** a pipeline — it has no `pipeline.py`/`config.py` and doesn't follow the
per-pipeline layout described in the repo root `CLAUDE.md`. It's a separate deployable unit,
kept in its own `app/` folder precisely so it isn't mistaken for one.

## Deployment

Static webapps have **no CLI push path** and are not wired into this repo's GitHub Actions
(unlike every pipeline here). Deploying is exclusively via OpenHEXA MCP tools, from whichever
agent/session has MCP access to the `pev-niger-7cc1fb` workspace:

- **First deploy**: `create_static_webapp` with `workspace_slug: "pev-niger-7cc1fb"`, a `name`,
  and `files_json` built from this folder's three files. Pass `allowed_operations` at creation
  time — see "Required scopes" below.
- **Subsequent updates**: `update_static_webapp` with the webapp's `id` (from
  `list_static_webapps`) and only the changed files in `files_json` — files are updated
  incrementally, no need to resend the whole bundle. Use `edit_static_webapp_file` instead for a
  small, targeted change to one existing file.

After deploying, verify with `get_static_webapp` that the live files match this folder.

## Required scopes (`allowed_operations`)

Currently granted: `PIPELINES_READ, PIPELINES_RUN, USER_READ`.

- `PIPELINES_READ` — resolving pipeline UUIDs/parameters, polling run status, reading messages,
  reading the orchestrator's schedule.
- `PIPELINES_RUN` — launching and stopping runs (`runPipeline` *and* `stopPipeline` are both
  covered by this one scope).
- `USER_READ` — listing existing files under `UPLOAD_FOLDER` via `workspace.bucket.objects`,
  for the file-picker fallback described below (`workspace` is gated by this scope, per
  `docs.openhexa.com/static-webapps`).

`FILES_READ`, `DATASETS_READ`/`WRITE`, `DATABASE_READ` are **not** needed. `FILES_WRITE` was
granted initially (for the originally-designed direct upload) but has been **dropped** since
that upload path is currently parked (see the known blocker below) and nothing in the code
calls `prepareObjectUpload` anymore. Re-add it if/when direct upload is restored.

## Naming

The workspace already has one static webapp-adjacent entry, `tableaux-de-bords` (an **iframe**
type embedding the unrelated routine-vaccination dashboard). Give this webapp a name/slug that
reads as clearly distinct — e.g. something built from "cibles" / "campagnes", not "tableaux de
bords" — so the two aren't confused in the workspace's webapp list.

## Known assumptions to verify on first real use

- **Upload folder**: files are written to `multi-campagne/inputs/cibles/<original filename>`
  (`UPLOAD_FOLDER` in `app.js`). This matches the folder seen in real historical-campaign runs;
  it hasn't been confirmed as the right convention for newly-configured (non-historical)
  campaigns specifically. Adjust the constant if that turns out to matter.
- **Schedule settings deep link**: the "Automate" section links to
  `.../workspaces/<slug>/pipelines/<code>/` (the pipeline's own detail page) rather than a
  specific "Scheduling and Notifications" sub-path, since that exact URL wasn't confirmed. Check
  it lands somewhere the user can actually find the scheduling controls, and narrow the link if
  OpenHEXA exposes a more specific URL for it.
- **`extract_target_data` currently has no `output`** (confirmed live — see `WEBAPP.md` §8 Q3;
  this pipeline was renamed from `process_target_data` after this note was written — see
  `docs/WEBAPP.md`'s note on the `extract_target_data`/`process_target_data` split),
  so no report/output section is built for Étape 1 beyond a link to the run itself. Revisit if
  that pipeline is ever extended to register one.

## Local development

`index.html` includes the `dev.js` script tag OpenHEXA provides for developing against real
workspace data before deploying (see `docs.openhexa.com/static-webapps` → "Local Development").
Open `index.html` directly in a browser (or serve it with any static file server), click
"Connect to OpenHEXA", and approve — the page then talks to the real `pev-niger-7cc1fb`
workspace under this webapp's actual granted scopes, so nothing will behave differently once
deployed. The script tag currently pins `data-workspace-slug` only; add `data-webapp-slug` once
the webapp exists and its slug is known, to skip the picker.
