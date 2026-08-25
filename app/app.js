/*
 * Campaign-target webapp - app logic. See WEBAPP.md for the full plan/decisions this
 * implements; the section numbers referenced in comments below point back to that doc.
 *
 * Two independent flows, matching the two cards in index.html:
 *   - Étape 1 (WEBAPP.md §1, stages 1-2): upload the target Excel file, fill in
 *     process_target_data's other parameters, launch/stop it, watch its status.
 *   - Étape 2 (WEBAPP.md §1, stage 3): once Étape 1 has succeeded, show orchestrate_pipelines_flow's
 *     current schedule (read-only - §4.1), launch/stop it, and watch its 5-pipeline chain as a
 *     flowchart (status color only, no per-node message windows - §4.3 decided "lighter v1").
 *
 * Everything here talks to OpenHEXA exclusively through the same-origin GraphQL proxy
 * (POST /graphql/), scope-gated by this webapp's allowed_operations. No token handling -
 * the browser session cookie authenticates every call. See docs.openhexa.com/static-webapps.
 */

// ---------------------------------------------------------------------------
// Configuration (WEBAPP.md §5) - the pipeline codes and folder/link constants this app is
// wired to. All confirmed live against the real workspace on 2026-08-20 (WEBAPP.md §3).
// ---------------------------------------------------------------------------

const WORKSPACE_SLUG =
  (window.OPENHEXA && window.OPENHEXA.workspaceSlug) || "pev-niger-7cc1fb";

const TARGET_PIPELINE_CODE = "multi-campagne-01-import-et-traitement-d-un-fichier-de-cibles";
const ORCHESTRATOR_CODE = "multi-campagne-02-orchestrate-etl-pipelines";

// The 5 pipelines orchestrate_pipelines_flow runs in sequence (WEBAPP.md §5) - used only to
// paint the Étape 2 flowchart; orchestrate_pipelines_flow itself launches them via its own
// REST client, not through this webapp. extract_org_units is deliberately not part of this
// chain (run manually, rarely - the org-unit tree changes only occasionally and the pipeline
// is comparatively memory-/time-consuming), so it has no node here either.
const ORCHESTRATED_PIPELINES = [
  {
    code: "multi-campagne-compilation-des-cibles-et-de-la-structure-attendue",
    label: "Compilation des cibles",
    description: "Compile les cibles et la structure de données attendue à partir de tous les fichiers déjà importés.",
  },
  {
    code: "multi-campagne-extraction-des-donnees-du-formulaire-iaso",
    label: "Extraction du formulaire IASO",
    description: "Récupère les soumissions brutes du formulaire de campagne depuis IASO.",
  },
  {
    code: "multi-campagne-traitement-des-donnees-du-formulaire-iaso",
    label: "Traitement du formulaire IASO",
    description: "Nettoie les soumissions et les associe aux unités organisationnelles.",
  },
  {
    code: "multi-campagne-construction-des-tableaux-pour-la-visualisation",
    label: "Construction des tableaux",
    description: "Construit les tableaux de couverture, complétude, stocks, etc. pour le tableau de bord.",
  },
  {
    code: "multi-campagne-envoi-des-tables-de-visualisation-vers-la-base-de-donnees",
    label: "Envoi vers la base de données",
    description: "Envoie les tableaux construits vers la base de données du tableau de bord.",
  },
];

// Where uploaded target files are written in the workspace bucket - one flat folder for every
// campaign's target file, historical or newly-configured alike (extract_target_data treats them
// identically, so there's no need for separate subfolders per campaign type).
const UPLOAD_FOLDER = "multi-campagne/inputs/cibles/";

// Excluded from the auto-generated parameter form: the brief asks for a dedicated upload box
// for this one, handled separately (WEBAPP.md §1).
const HIDDEN_PARAM_CODES = new Set(["input_file"]);

const POLL_INTERVAL_MS = 4000;
// Matches the reference project's own "stop watching after ~40 min, the run keeps going"
// pattern (WEBAPP.md §5) - these pipelines can legitimately run for well over an hour.
const MAX_POLL_MS = 90 * 60 * 1000;

const OPENHEXA_APP_BASE = "https://app.openhexa.org";

// ---------------------------------------------------------------------------
// Status vocabulary (WEBAPP.md §4.4) - the real PipelineRunStatus enum, not a simplified list,
// plus two app-level states ("never run yet" - §6 - and "unknown").
// ---------------------------------------------------------------------------

const STATUS_LABELS = {
  never: "Non exécuté",
  unknown: "Statut inconnu",
  queued: "En attente",
  running: "En cours d'exécution",
  success: "Réussi",
  failed: "Échoué",
  stopped: "Arrêté",
  skipped: "Ignoré",
  terminating: "Arrêt en cours…",
};

const TERMINAL_STATUSES = new Set(["success", "failed", "stopped", "skipped"]);

function statusClass(status) {
  return "status-" + (STATUS_LABELS[status] ? status : "unknown");
}

// Separate from statusClass(): the bare "status-xxx" classes above already carry a `background`
// rule (for the pill-shaped badges), so reusing them on the flowchart description text/arrows
// would give those elements that same solid background - with the text/arrow drawn in the exact
// same color as its own background, i.e. invisible. This produces a same-named-but-independent
// "flow-status-xxx" class that only ever carries a plain `color`, never a `background`.
function flowStatusClass(status) {
  return "flow-status-" + (STATUS_LABELS[status] ? status : "unknown");
}

function statusLabel(status) {
  return STATUS_LABELS[status] || STATUS_LABELS.unknown;
}

function statusBadgeHtml(status) {
  return `<span class="status-badge ${statusClass(status)}">${statusLabel(status)}</span>`;
}

// ---------------------------------------------------------------------------
// GraphQL helper (same shape as the reference project's - see WEBAPP.md §3/§5)
// ---------------------------------------------------------------------------

async function gql(query, variables) {
  const res = await fetch("/graphql/", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables: variables || {} }),
  });
  const json = await res.json();
  if (json.errors) {
    throw new Error(json.errors.map((e) => e.message).join("; "));
  }
  return json.data;
}

// ---------------------------------------------------------------------------
// Queries / mutations (WEBAPP.md §5) - field shapes confirmed against the real OpenHEXA
// GraphQL schema, not guessed.
// ---------------------------------------------------------------------------

// One round-trip on load: process_target_data's parameters + last run, the orchestrator's
// schedule + last run, and each of the 5 orchestrated pipelines' last run (for the flowchart).
// Mirrors the reference app's "cross-session status query" efficiency (WEBAPP.md §5).
function buildInitQuery() {
  const nodeFields = (alias, code) => `
    ${alias}: pipelineByCode(code: "${code}", workspaceSlug: $ws) {
      id
      code
      runs(orderBy: EXECUTION_DATE_DESC, perPage: 1) { items { id status executionDate } }
    }
  `;
  return `
    query Init($ws: String!) {
      target: pipelineByCode(code: "${TARGET_PIPELINE_CODE}", workspaceSlug: $ws) {
        id
        code
        currentVersion {
          id
          parameters { code name type required help choices default multiple }
        }
        runs(orderBy: EXECUTION_DATE_DESC, perPage: 1) { items { id status } }
      }
      orchestrator: pipelineByCode(code: "${ORCHESTRATOR_CODE}", workspaceSlug: $ws) {
        id
        code
        schedule
        runs(orderBy: EXECUTION_DATE_DESC, perPage: 1) { items { id status executionDate } }
      }
      ${ORCHESTRATED_PIPELINES.map((p, i) => nodeFields("node" + i, p.code)).join("\n")}
    }
  `;
}

const RUN_MUTATION = `
  mutation Run($input: RunPipelineInput!) {
    runPipeline(input: $input) {
      success
      errors
      run { id status executionDate }
    }
  }
`;

const STOP_MUTATION = `
  mutation Stop($input: StopPipelineInput!) {
    stopPipeline(input: $input) {
      success
      errors
    }
  }
`;

const POLL_RUN_QUERY = `
  query Poll($id: UUID!) {
    pipelineRun(id: $id) {
      id
      status
      progress
      hasErrorMessages
    }
  }
`;

const RUN_MESSAGES_QUERY = `
  query RunMessages($id: UUID!) {
    pipelineRun(id: $id) {
      messages { message priority timestamp }
    }
  }
`;

// Direct-upload-from-browser (prepareObjectUpload + a PUT to a signed GCS URL) is parked: the
// bucket has no CORS policy allowing that cross-origin PUT, confirmed via a real failed upload
// (WEBAPP.md §9) - not fixable from this webapp's code. Interim fallback: let the user pick a
// file already present in the workspace (uploaded via OpenHEXA's native Files browser) instead
// of uploading a new one from here. Needs USER_READ (workspace.bucket.objects is reached
// through the workspace query, gated by that scope per docs.openhexa.com/static-webapps).
const BUCKET_FILES_QUERY = `
  query BucketFiles($ws: String!, $prefix: String!) {
    workspace(slug: $ws) {
      bucket {
        objects(prefix: $prefix, perPage: 200, ignoreHiddenFiles: true) {
          items { key name type }
          hasNextPage
        }
      }
    }
  }
`;

// One round-trip refresh of the 5 flowchart nodes + the orchestrator's own run, used while
// polling an active orchestration run.
function buildFlowchartPollQuery() {
  const nodeFields = (alias, code) => `
    ${alias}: pipelineByCode(code: "${code}", workspaceSlug: $ws) {
      runs(orderBy: EXECUTION_DATE_DESC, perPage: 1) { items { id status executionDate } }
    }
  `;
  return `
    query FlowchartPoll($ws: String!) {
      ${ORCHESTRATED_PIPELINES.map((p, i) => nodeFields("node" + i, p.code)).join("\n")}
    }
  `;
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

function openHexaRunUrl(pipelineCode, runId) {
  return `${OPENHEXA_APP_BASE}/workspaces/${WORKSPACE_SLUG}/pipelines/${pipelineCode}/runs/${runId}/`;
}

function openHexaPipelineUrl(pipelineCode) {
  return `${OPENHEXA_APP_BASE}/workspaces/${WORKSPACE_SLUG}/pipelines/${pipelineCode}/`;
}

// The single most useful message to surface in the Action box on failure (WEBAPP.md §4.3): the
// most recent ERROR/CRITICAL message, falling back to the very last message if none is flagged
// at that priority (a pipeline can fail without ever emitting an ERROR-priority line).
function pickActionMessage(messages) {
  if (!messages || !messages.length) return null;
  const severe = messages.filter((m) => m.priority === "ERROR" || m.priority === "CRITICAL");
  return severe.length ? severe[severe.length - 1] : messages[messages.length - 1];
}

// Same idea as pickActionMessage, but returns every ERROR/CRITICAL message instead of just the
// last one - messages arrive oldest first (the same assumption pickActionMessage and the
// orchestrator's own display_new_messages already make), so this is already earliest-to-latest.
function pickAllErrorMessages(messages) {
  if (!messages || !messages.length) return [];
  const severe = messages.filter((m) => m.priority === "ERROR" || m.priority === "CRITICAL");
  return severe.length ? severe : [messages[messages.length - 1]];
}

// ---------------------------------------------------------------------------
// Étape 1 - process_target_data
// ---------------------------------------------------------------------------

const state = {
  target: null, // { id, code, parameters, lastRun }
  orchestrator: null, // { id, code, schedule, lastRun, shown, currentRunStartedAt }
  uploadedObjectKey: null,
  targetRunId: null,
  targetPollTimer: null,
  targetPollDeadline: null,
  orchestratorRunId: null,
  orchestratorPollTimer: null,
  orchestratorPollDeadline: null,
};

function el(id) {
  return document.getElementById(id);
}

// A pipeline's real run history persists across reloads (by design - see the GraphQL init
// query), but showing a stale terminal status (e.g. "Réussi" from yesterday) right when the
// page opens reads as if that just happened. `section.shown` tracks whether the CURRENT browser
// session has actually seen this pipeline live - either because it was launched from this page,
// or because a run was already in progress when the page loaded (worth surfacing, since it's
// happening right now). Until then, sessionRun() withholds the real run so the caller falls
// back to a "never" display; once shown, every status (including a freshly-reached terminal one)
// is shown for real.
function sessionRun(section, run) {
  if (run && !TERMINAL_STATUSES.has(run.status)) {
    section.shown = true;
    return run;
  }
  return section.shown ? run : null;
}

// A flowchart node's "latest run" is that sub-pipeline's own independent run history - it may
// well predate the orchestration currently in progress (or any orchestration at all). Only a
// run whose executionDate is at or after the current orchestration run's own start can actually
// be a run THIS orchestration triggered; anything older is stale and must display as "never".
function isCurrentOrchestrationRun(run) {
  if (!run || !state.orchestrator || !state.orchestrator.currentRunStartedAt) return false;
  return new Date(run.executionDate) >= new Date(state.orchestrator.currentRunStartedAt);
}

async function init() {
  el("workspaceLabel").textContent = WORKSPACE_SLUG;

  let data;
  try {
    data = await gql(buildInitQuery(), { ws: WORKSPACE_SLUG });
  } catch (err) {
    el("paramForm").innerHTML = `<p class="form-errors">Impossible de charger les pipelines depuis OpenHEXA : ${escapeHtml(err.message)}</p>`;
    return;
  }

  if (!data.target) {
    el("paramForm").innerHTML =
      '<p class="form-errors">Le pipeline "process_target_data" n\'est pas installé dans cet espace de travail.</p>';
    return;
  }
  if (!data.orchestrator) {
    el("step2Locked").textContent =
      "Le pipeline d'orchestration n'est pas installé dans cet espace de travail.";
  }

  state.target = {
    id: data.target.id,
    code: data.target.code,
    parameters: data.target.currentVersion ? data.target.currentVersion.parameters : [],
    lastRun: (data.target.runs.items || [])[0] || null,
    shown: false,
  };

  renderParamForm(state.target.parameters);
  const initialTargetRun = sessionRun(state.target, state.target.lastRun);
  renderGlobalStatus1(initialTargetRun ? initialTargetRun.status : "never", null, initialTargetRun);
  updateLaunchEnabled1();

  if (data.orchestrator) {
    state.orchestrator = {
      id: data.orchestrator.id,
      code: data.orchestrator.code,
      schedule: data.orchestrator.schedule,
      lastRun: (data.orchestrator.runs.items || [])[0] || null,
      shown: false,
    };
    renderScheduleInfo(state.orchestrator.schedule);
    renderRunStatus2(state.orchestrator.lastRun);
    const initialOrchestratorRun = sessionRun(state.orchestrator, state.orchestrator.lastRun);
    renderGlobalStatus(initialOrchestratorRun ? initialOrchestratorRun.status : "never");
    // Set before renderFlowchartInitial below, so a resumed active run's nodes render with
    // their real statuses immediately instead of "never" (isCurrentOrchestrationRun needs
    // this set first - see startOrchestratorPolling for why plain state.orchestrator.shown
    // is no longer sufficient here).
    if (initialOrchestratorRun) {
      state.orchestrator.currentRunStartedAt = initialOrchestratorRun.executionDate;
    }
  }

  renderFlowchartInitial(data);

  // Stage 3 is only shown once stage 1 has actually succeeded (brief's explicit gating rule -
  // WEBAPP.md §1). Persists across reloads since it's read from OpenHEXA's own run history, not
  // in-memory webapp state (WEBAPP.md §6).
  updateStep2Visibility();

  wireEvents();

  // If either pipeline's last run is still active (e.g. the user reloaded mid-run), resume
  // polling it immediately instead of showing a stale status.
  if (state.target.lastRun && !TERMINAL_STATUSES.has(state.target.lastRun.status)) {
    startTargetPolling(state.target.lastRun.id);
  }
  if (
    state.orchestrator &&
    state.orchestrator.lastRun &&
    !TERMINAL_STATUSES.has(state.orchestrator.lastRun.status)
  ) {
    startOrchestratorPolling(state.orchestrator.lastRun.id, state.orchestrator.lastRun.executionDate);
  }
}

function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = String(s);
  return div.innerHTML;
}

// --- Parameter form, generated live from process_target_data's installed parameters ---------

function renderParamForm(parameters) {
  const form = el("paramForm");
  form.innerHTML = "";

  const visible = parameters.filter((p) => !HIDDEN_PARAM_CODES.has(p.code));
  if (!visible.length) {
    form.innerHTML = '<p class="muted">Aucun paramètre à renseigner.</p>';
    return;
  }

  visible.forEach((param) => {
    const field = document.createElement("div");
    field.className = "param-field";
    field.dataset.code = param.code;

    const label = document.createElement("label");
    label.textContent = param.name + (param.required ? " *" : "");
    label.setAttribute("for", "param-" + param.code);
    field.appendChild(label);

    field.appendChild(renderParamInput(param));

    if (param.help) {
      const help = document.createElement("p");
      help.className = "param-help";
      help.textContent = param.help;
      field.appendChild(help);
    }

    form.appendChild(field);
  });

  form.addEventListener("input", updateLaunchEnabled1);
  form.addEventListener("change", updateLaunchEnabled1);
}

function renderParamInput(param) {
  const inputId = "param-" + param.code;

  if (param.type === "bool") {
    const wrap = document.createElement("div");
    wrap.className = "bool-field";
    const label = document.createElement("label");
    const input = document.createElement("input");
    input.type = "checkbox";
    input.id = inputId;
    if (param.default) input.checked = true;
    label.appendChild(input);
    label.appendChild(document.createTextNode(" Activer"));
    wrap.appendChild(label);
    return wrap;
  }

  if (param.multiple && param.choices && param.choices.length <= 15) {
    // Checkbox group - clearer than a native <select multiple> for a small, fixed choice set
    // (e.g. round numbers).
    const wrap = document.createElement("div");
    wrap.className = "checkbox-group";
    wrap.id = inputId;
    param.choices.forEach((choice) => {
      const label = document.createElement("label");
      const input = document.createElement("input");
      input.type = "checkbox";
      input.value = choice;
      input.name = param.code;
      label.appendChild(input);
      label.appendChild(document.createTextNode(" " + choice));
      wrap.appendChild(label);
    });
    return wrap;
  }

  if (param.choices && param.choices.length) {
    const select = document.createElement("select");
    select.id = inputId;
    select.multiple = !!param.multiple;
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "— Sélectionner —";
    if (!param.multiple) select.appendChild(placeholder);
    param.choices.forEach((choice) => {
      const opt = document.createElement("option");
      opt.value = choice;
      opt.textContent = choice;
      select.appendChild(opt);
    });
    return select;
  }

  // Fallback: plain text input (dates, free-text strings, and any parameter type this app
  // doesn't have a dedicated widget for yet).
  const input = document.createElement("input");
  input.type = "text";
  input.id = inputId;
  if (param.type === "str" && param.code.includes("date")) {
    input.placeholder = "AAAA-MM-JJ";
  }
  return input;
}

function readParamValue(param) {
  const field = el("paramForm").querySelector(`.param-field[data-code="${param.code}"]`);
  if (!field) return undefined;

  if (param.type === "bool") {
    return field.querySelector("input[type=checkbox]").checked;
  }

  if (param.multiple && param.choices && param.choices.length <= 15) {
    const checked = Array.from(field.querySelectorAll("input[type=checkbox]:checked")).map(
      (cb) => cb.value,
    );
    if (!checked.length) return param.required ? undefined : null;
    return param.type === "int" ? checked.map(Number) : checked;
  }

  const select = field.querySelector("select");
  if (select) {
    if (select.multiple) {
      const values = Array.from(select.selectedOptions).map((o) => o.value);
      if (!values.length) return param.required ? undefined : null;
      return param.type === "int" ? values.map(Number) : values;
    }
    if (!select.value) return param.required ? undefined : null;
    return param.type === "int" ? Number(select.value) : select.value;
  }

  const input = field.querySelector("input[type=text]");
  const value = input.value.trim();
  if (!value) return param.required ? undefined : null; // omit blank optional fields
  return param.type === "int" ? Number(value) : value;
}

// Returns { config, errors } - mirrors the reference project's validateForm convention
// (required-and-missing -> error; optional-and-blank -> omitted, never sent as "").
function buildConfig() {
  const errors = [];
  const config = {};

  if (!state.uploadedObjectKey) {
    errors.push("Chargez d'abord le fichier Excel des cibles.");
  } else {
    config.input_file = state.uploadedObjectKey;
  }

  state.target.parameters
    .filter((p) => !HIDDEN_PARAM_CODES.has(p.code))
    .forEach((param) => {
      const value = readParamValue(param);
      if (value === undefined) {
        if (param.required) errors.push(`${param.name} est requis.`);
        return;
      }
      if (value !== null) config[param.code] = value;
    });

  return { config, errors };
}

function updateLaunchEnabled1() {
  if (!state.target) return;
  const { errors } = buildConfig();
  const running = state.targetRunId && !!state.targetPollTimer;
  el("launchBtn1").disabled = errors.length > 0 || running;
}

// --- Upload box --------------------------------------------------------------------------

// Interim fallback for the blocked upload (WEBAPP.md §9): instead of sending a new file from
// the user's machine, list .xlsx files already present under UPLOAD_FOLDER and let them pick
// one. The picked file's key becomes state.uploadedObjectKey exactly as a real upload would
// have set it, so buildConfig()/launchTarget() need no changes at all.
function wireFileBrowser() {
  el("filesLink").href = `${OPENHEXA_APP_BASE}/workspaces/${WORKSPACE_SLUG}/files/`;
  el("uploadFolderHint").textContent = UPLOAD_FOLDER;

  const select = el("fileBrowseSelect");
  select.addEventListener("change", () => {
    const status = el("uploadStatus");
    state.uploadedObjectKey = select.value || null;
    if (select.value) {
      status.textContent = `✓ Fichier sélectionné : ${select.value.slice(UPLOAD_FOLDER.length)}`;
      status.className = "upload-status ok";
    } else {
      status.textContent = "";
      status.className = "upload-status";
    }
    updateLaunchEnabled1();
  });

  el("refreshFilesBtn").addEventListener("click", loadBucketFiles);
  loadBucketFiles();
}

async function loadBucketFiles() {
  const select = el("fileBrowseSelect");
  const status = el("uploadStatus");
  select.disabled = true;
  select.innerHTML = '<option value="">Chargement des fichiers…</option>';

  let workspace;
  try {
    const result = await gql(BUCKET_FILES_QUERY, { ws: WORKSPACE_SLUG, prefix: UPLOAD_FOLDER });
    workspace = result.workspace;
  } catch (err) {
    console.error("Étape 1 - échec du chargement de la liste des fichiers :", err);
    select.innerHTML = '<option value="">Erreur de chargement</option>';
    status.textContent = `Impossible de charger la liste des fichiers : ${err.message}`;
    status.className = "upload-status error";
    return;
  }

  const files = (workspace.bucket.objects.items || [])
    .filter((o) => o.type === "FILE" && /\.xlsx$/i.test(o.name))
    .sort((a, b) => a.key.localeCompare(b.key));

  if (!files.length) {
    select.innerHTML = '<option value="">Aucun fichier .xlsx trouvé</option>';
    select.disabled = true;
    return;
  }

  select.innerHTML =
    '<option value="">— Sélectionner un fichier —</option>' +
    files
      .map(
        (f) =>
          `<option value="${escapeHtml(f.key)}">${escapeHtml(f.key.slice(UPLOAD_FOLDER.length))}</option>`,
      )
      .join("");
  select.disabled = false;

  if (workspace.bucket.objects.hasNextPage) {
    status.textContent =
      "Remarque : d'autres fichiers existent au-delà des 200 premiers affichés ici.";
    status.className = "upload-status";
  }
}

// --- Run status (Étape 1) ------------------------------------------------------------------

// Same shape as Étape 2's renderGlobalStatus: a status badge plus a plain-language message
// explaining what that status means, and (once there's a run to point at) a button to open it
// in OpenHEXA. Consolidates what used to be a separate badge-only status box plus an "Action
// requise" box shown only on failure.
// failureMessages (only meaningful for status "failed") is an array of every ERROR/CRITICAL
// log message, oldest to newest - not just the last one - so a multi-step failure shows its
// whole error trail, not only the final symptom.
function renderGlobalStatus1(status, failureMessages, run) {
  const box = el("statusBox1");
  box.className = "global-status";
  const openBtn = run
    ? `<a class="btn btn-openhexa" href="${openHexaRunUrl(state.target.code, run.id)}" target="_blank" rel="noopener">Voir dans OpenHEXA</a>`
    : "";

  if (status === "success") {
    box.classList.add("success");
    box.innerHTML = `${statusBadgeHtml(status)}<p>Le fichier de cibles a été importé et traité avec succès. Vous pouvez passer à l'étape 2.</p>${openBtn}`;
  } else if (status === "failed") {
    box.classList.add("failed");
    const messages = failureMessages && failureMessages.length ? failureMessages : ["raison inconnue"];
    const list = `<ul class="failure-messages">${messages.map((m) => `<li>${escapeHtml(m)}</li>`).join("")}</ul>`;
    box.innerHTML = `${statusBadgeHtml(status)}<p>Le traitement du fichier de cibles a échoué pour la raison suivante:</p>${list}${openBtn}`;
  } else if (status === "stopped") {
    box.classList.add("stopped");
    box.innerHTML = `${statusBadgeHtml(status)}<p>L'exécution a été arrêtée. Appuyez sur « Lancer » pour recommencer.</p>${openBtn}`;
  } else if (status === "running" || status === "queued" || status === "terminating") {
    box.innerHTML = `${statusBadgeHtml(status)}<p>Le traitement du fichier de cibles est en cours.</p>${openBtn}`;
  } else {
    box.innerHTML = statusBadgeHtml(status);
  }
}

// On failure, fetches every error-priority log message (oldest to newest) before rendering, so
// the box's explanation is never left at a generic "raison inconnue" when real ones exist, and
// shows the whole error trail rather than only the last symptom.
async function renderGlobalStatus1Failed(run) {
  let messages = null;
  try {
    const { pipelineRun } = await gql(RUN_MESSAGES_QUERY, { id: run.id });
    messages = pickAllErrorMessages(pipelineRun.messages).map((m) => m.message);
  } catch (_err) {
    // fall through with messages == null - renderGlobalStatus1 has its own fallback text
  }
  renderGlobalStatus1("failed", messages, run);
}

function startTargetPolling(runId) {
  state.target.shown = true;
  state.targetRunId = runId;
  state.targetPollDeadline = Date.now() + MAX_POLL_MS;
  el("stopBtn1").disabled = false;
  el("launchBtn1").disabled = true;
  renderGlobalStatus1("running", null, null);

  clearInterval(state.targetPollTimer);
  state.targetPollTimer = setInterval(pollTarget, POLL_INTERVAL_MS);
  pollTarget();
}

async function pollTarget() {
  if (Date.now() > state.targetPollDeadline) {
    stopTargetPolling();
    el("statusBox1").innerHTML +=
      ' <small>(suivi interrompu - le pipeline continue de s\'exécuter dans OpenHEXA)</small>';
    return;
  }

  let run;
  try {
    const { pipelineRun } = await gql(POLL_RUN_QUERY, { id: state.targetRunId });
    run = pipelineRun;
  } catch (_err) {
    return; // transient network error - try again on the next tick
  }

  if (run.status === "failed") {
    await renderGlobalStatus1Failed(run);
  } else {
    renderGlobalStatus1(run.status, null, run);
  }

  if (TERMINAL_STATUSES.has(run.status)) {
    stopTargetPolling();
    if (run.status === "success") {
      // Refresh the cached last-run and unlock Étape 2 immediately, no reload needed.
      state.target.lastRun = run;
      updateStep2Visibility();
    }
  }
}

function stopTargetPolling() {
  clearInterval(state.targetPollTimer);
  state.targetPollTimer = null;
  el("stopBtn1").disabled = true;
  updateLaunchEnabled1();
}

async function launchTarget() {
  const { config, errors } = buildConfig();
  const errBox = el("formErrors");
  if (errors.length) {
    errBox.textContent = errors.join(" ");
    errBox.classList.remove("hidden");
    return;
  }
  errBox.classList.add("hidden");

  el("launchBtn1").disabled = true;
  try {
    const { runPipeline } = await gql(RUN_MUTATION, {
      input: { id: state.target.id, config },
    });
    if (!runPipeline.success) {
      errBox.textContent = "Échec du lancement : " + (runPipeline.errors || []).join(", ");
      errBox.classList.remove("hidden");
      el("launchBtn1").disabled = false;
      return;
    }
    startTargetPolling(runPipeline.run.id);
  } catch (err) {
    errBox.textContent = "Échec du lancement : " + err.message;
    errBox.classList.remove("hidden");
    el("launchBtn1").disabled = false;
  }
}

async function stopTarget() {
  if (!state.targetRunId) return;
  el("stopBtn1").disabled = true;
  try {
    await gql(STOP_MUTATION, { input: { runId: state.targetRunId } });
  } catch (_err) {
    // The next poll tick will reflect the real state regardless of whether this call itself
    // reported an error (e.g. the run had already finished).
  }
}

// ---------------------------------------------------------------------------
// Étape 2 - orchestrate_pipelines_flow + flowchart
// ---------------------------------------------------------------------------

function updateStep2Visibility() {
  const unlocked = state.target.lastRun && state.target.lastRun.status === "success";
  el("step2Locked").classList.toggle("hidden", !!unlocked && !!state.orchestrator);
  el("step2Content").classList.toggle("hidden", !unlocked || !state.orchestrator);
}

// Best-effort plain-French description of a 5-field cron expression, for schedules set up via
// OpenHEXA's "Edit scheduling" UI (see the step-by-step instructions above). Only covers the
// common shapes a person is actually likely to set by hand; anything else falls back to the raw
// cron syntax in renderScheduleInfo below rather than risk describing an unusual schedule wrong.
function describeCron(cron) {
  if (!cron) return null;
  const parts = cron.trim().split(/\s+/);
  if (parts.length !== 5) return null;
  const [min, hour, dom, month, dow] = parts;
  const isNum = (s) => /^\d+$/.test(s);
  const pad2 = (n) => String(n).padStart(2, "0");
  const DAYS = ["dimanche", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi"];

  if (month !== "*") return null; // a specific month is rare enough to not bother describing

  const everyNMinutes = min.match(/^\*\/(\d+)$/);
  if (everyNMinutes && hour === "*" && dom === "*" && dow === "*") {
    return `toutes les ${everyNMinutes[1]} minutes`;
  }
  if (min === "0" && hour === "*" && dom === "*" && dow === "*") {
    return "toutes les heures pleines";
  }
  if (isNum(min) && hour === "*" && dom === "*" && dow === "*") {
    return `toutes les heures, à la ${min}e minute`;
  }
  if (isNum(min) && isNum(hour) && dom === "*" && /^[0-6]$/.test(dow)) {
    return `tous les ${DAYS[Number(dow)]} à ${pad2(hour)}h${pad2(min)}`;
  }
  if (isNum(min) && isNum(hour) && isNum(dom) && dow === "*") {
    const dayLabel = dom === "1" ? "1er" : dom;
    return `le ${dayLabel} de chaque mois à ${pad2(hour)}h${pad2(min)}`;
  }
  if (isNum(min) && isNum(hour) && dom === "*" && dow === "*") {
    return `tous les jours à ${pad2(hour)}h${pad2(min)}`;
  }
  return null;
}

function renderScheduleInfo(schedule) {
  const description = describeCron(schedule);
  const statusEl = el("scheduleStatus");
  statusEl.textContent = schedule ? `active (${description || schedule})` : "inactive";
  statusEl.classList.toggle("schedule-active", !!schedule);
  statusEl.classList.toggle("schedule-inactive", !schedule);
  el("scheduleLink").href = openHexaPipelineUrl(state.orchestrator.code);
}

// No dedicated status box for the orchestrator beyond the flowchart + global status text - the
// flowchart nodes plus renderGlobalStatus cover the same information (WEBAPP.md §4.3). This just
// sets the initial Stop-button state from the real (session-gating-independent) last run - a run
// genuinely in progress must stay stoppable even before sessionRun() decides what to display.
function renderRunStatus2(run) {
  el("stopBtn2").disabled = !run || TERMINAL_STATUSES.has(run.status);
}

// The flowchart visualizes THIS session's orchestration run, not each sub-pipeline's own
// independent run history (a node could have last run standalone, outside any orchestration,
// or from a previous orchestration run) - so a node only ever shows a real run when that run
// is actually part of the orchestration currently in progress (isCurrentOrchestrationRun).
function renderFlowchartInitial(initData) {
  const container = el("flowchart");
  container.innerHTML = "";
  ORCHESTRATED_PIPELINES.forEach((p, i) => {
    const nodeData = initData["node" + i];
    const rawRun = nodeData ? (nodeData.runs.items || [])[0] : null;
    const run = isCurrentOrchestrationRun(rawRun) ? rawRun : null;
    container.appendChild(buildFlowRow(p, run));
    if (i < ORCHESTRATED_PIPELINES.length - 1) {
      container.appendChild(buildFlowArrow(p.code, run));
    }
  });
}

// Appended to each pipeline's static outcome description so it reads as "in progress" /
// "done" rather than always describing the step in the abstract.
const FLOW_DESC_SUFFIX = {
  never: "",
  queued: " — en attente…",
  running: " — en cours…",
  terminating: " — arrêt en cours…",
  success: " — terminé.",
  failed: " — échoué.",
  stopped: " — arrêté.",
  skipped: " — ignoré.",
};

// A node plus its outcome description, side by side - the vertical layout leaves enough room
// next to each box to explain what that pipeline actually produces.
function buildFlowRow(pipelineDef, run) {
  const row = document.createElement("div");
  row.className = "flow-row";
  row.appendChild(buildFlowNode(pipelineDef, run));
  const desc = document.createElement("p");
  desc.dataset.code = pipelineDef.code;
  updateFlowDesc(desc, pipelineDef, run);
  row.appendChild(desc);
  return row;
}

// Text + color both follow the node's current status, same tokens as the badges/arrows.
function updateFlowDesc(desc, pipelineDef, run) {
  const status = run ? run.status : "never";
  desc.className = "flow-node-desc " + flowStatusClass(status);
  desc.textContent = pipelineDef.description + (FLOW_DESC_SUFFIX[status] || "");
}

function buildFlowNode(pipelineDef, run) {
  const a = document.createElement("a");
  a.className = "flow-node";
  a.href = run ? openHexaRunUrl(pipelineDef.code, run.id) : openHexaPipelineUrl(pipelineDef.code);
  a.target = "_blank";
  a.rel = "noopener";
  a.dataset.code = pipelineDef.code;
  a.innerHTML = `
    <span class="flow-node-label">${escapeHtml(pipelineDef.label)}</span>
    ${statusBadgeHtml(run ? run.status : "never")}
  `;
  return a;
}

// The arrow following a node is colored by THAT node's own status - it represents whether the
// chain has actually handed off to the next step yet. Built from two small shapes (a line and
// a triangular head) rather than a text glyph, so it renders as an actual arrowhead regardless
// of font/platform. Wrapped so it stays centered under the node column specifically, not the
// wider row (which also carries the description text).
function buildFlowArrow(sourceCode, sourceRun) {
  const wrap = document.createElement("div");
  wrap.className = "flow-arrow-wrap";
  const arrow = document.createElement("div");
  arrow.className = "flow-arrow " + flowStatusClass(sourceRun ? sourceRun.status : "never");
  arrow.dataset.after = sourceCode;
  arrow.setAttribute("aria-hidden", "true");
  arrow.innerHTML = '<span class="flow-arrow-line"></span><span class="flow-arrow-head"></span>';
  wrap.appendChild(arrow);
  return wrap;
}

function updateFlowNode(pipelineDef, run) {
  const code = pipelineDef.code;
  const node = document.querySelector(`.flow-node[data-code="${code}"]`);
  if (!node) return;
  node.href = run ? openHexaRunUrl(code, run.id) : openHexaPipelineUrl(code);
  const badge = node.querySelector(".status-badge");
  badge.outerHTML = statusBadgeHtml(run ? run.status : "never");
  const arrow = document.querySelector(`.flow-arrow[data-after="${code}"]`);
  if (arrow) arrow.className = "flow-arrow " + flowStatusClass(run ? run.status : "never");
  const desc = document.querySelector(`.flow-node-desc[data-code="${code}"]`);
  if (desc) updateFlowDesc(desc, pipelineDef, run);
}

async function refreshFlowchart() {
  let data;
  try {
    data = await gql(buildFlowchartPollQuery(), { ws: WORKSPACE_SLUG });
  } catch (_err) {
    return;
  }
  // Tracks whichever node is currently non-terminal (there's normally at most one, since the
  // orchestrator triggers its chain sequentially) - stopOrchestrator() needs this run's id to
  // actually stop that sub-pipeline too, not just the orchestrator's own run.
  state.orchestrator.activeNode = null;
  ORCHESTRATED_PIPELINES.forEach((p, i) => {
    const nodeData = data["node" + i];
    const rawRun = nodeData ? (nodeData.runs.items || [])[0] : null;
    const run = isCurrentOrchestrationRun(rawRun) ? rawRun : null;
    updateFlowNode(p, run);
    if (run && !TERMINAL_STATUSES.has(run.status)) {
      state.orchestrator.activeNode = { pipelineDef: p, runId: run.id };
    }
  });
}

// Exact copy templates from the brief (translated to French per this repo's convention -
// CLAUDE.md), driven by the orchestrator's own run status, not any individual sub-pipeline.
function renderGlobalStatus(status, failureReason) {
  const box = el("globalStatus");
  box.className = "global-status";

  if (status === "success") {
    box.classList.add("success");
    box.innerHTML = `${statusBadgeHtml(status)}<p>Le tableau de bord a été mis à jour avec succès avec la nouvelle configuration de campagne.</p>`;
  } else if (status === "failed") {
    box.classList.add("failed");
    box.innerHTML = `${statusBadgeHtml(status)}<p>L'exécution de la mise à jour du tableau de bord a échoué, pour la raison suivante : ${escapeHtml(failureReason || "raison inconnue")}</p>`;
  } else if (status === "stopped") {
    box.classList.add("stopped");
    box.innerHTML = `${statusBadgeHtml(status)}<p>L'exécution a été arrêtée. Appuyez sur « Lancer l'orchestration » pour recommencer.</p>`;
  } else {
    box.innerHTML = statusBadgeHtml(status);
  }
}

function startOrchestratorPolling(runId, startedAt) {
  // Every flowchart node's "latest run" fetched from here on is only real if it started at or
  // after this run - fixes nodes flashing a stale previous-run status the instant this run
  // starts (they haven't been re-triggered by THIS run yet, so they must show "never" until they
  // are). Falls back to the current time if the caller didn't have a server timestamp yet.
  const resolvedStartedAt = startedAt || new Date().toISOString();
  // Only reset the flowchart to blank when this is genuinely a NEW run (a fresh launch, or a
  // different run than the one already tracked) - not when resuming polling on the very run
  // init() just rendered real statuses for, which would otherwise flash them back to "never".
  const isNewRun = state.orchestrator.currentRunStartedAt !== resolvedStartedAt;
  state.orchestrator.shown = true;
  state.orchestrator.currentRunStartedAt = resolvedStartedAt;
  state.orchestratorRunId = runId;
  state.orchestratorPollDeadline = Date.now() + MAX_POLL_MS;
  el("stopBtn2").disabled = false;
  el("launchBtn2").disabled = true;
  renderGlobalStatus("running");
  if (isNewRun) {
    state.orchestrator.activeNode = null;
    ORCHESTRATED_PIPELINES.forEach((p) => updateFlowNode(p, null));
  }

  clearInterval(state.orchestratorPollTimer);
  state.orchestratorPollTimer = setInterval(pollOrchestrator, POLL_INTERVAL_MS);
  pollOrchestrator();
}

async function pollOrchestrator() {
  if (Date.now() > state.orchestratorPollDeadline) {
    stopOrchestratorPolling();
    return;
  }

  let run;
  try {
    const { pipelineRun } = await gql(POLL_RUN_QUERY, { id: state.orchestratorRunId });
    run = pipelineRun;
  } catch (_err) {
    return;
  }

  refreshFlowchart();

  if (TERMINAL_STATUSES.has(run.status)) {
    stopOrchestratorPolling();
    if (run.status === "failed") {
      let reason = null;
      try {
        const { pipelineRun: withMessages } = await gql(RUN_MESSAGES_QUERY, { id: run.id });
        const msg = pickActionMessage(withMessages.messages);
        reason = msg ? msg.message : null;
      } catch (_err) {
        // fall through with reason == null - renderGlobalStatus has its own fallback text
      }
      renderGlobalStatus("failed", reason);
    } else {
      renderGlobalStatus(run.status);
    }
  } else {
    renderGlobalStatus(run.status);
  }
}

function stopOrchestratorPolling() {
  clearInterval(state.orchestratorPollTimer);
  state.orchestratorPollTimer = null;
  el("stopBtn2").disabled = true;
  el("launchBtn2").disabled = false;
}

async function launchOrchestrator() {
  el("launchBtn2").disabled = true;
  try {
    const { runPipeline } = await gql(RUN_MUTATION, {
      input: { id: state.orchestrator.id, config: {} },
    });
    if (!runPipeline.success) {
      renderGlobalStatus("failed", (runPipeline.errors || []).join(", "));
      el("launchBtn2").disabled = false;
      return;
    }
    startOrchestratorPolling(runPipeline.run.id, runPipeline.run.executionDate);
  } catch (err) {
    renderGlobalStatus("failed", err.message);
    el("launchBtn2").disabled = false;
  }
}

// Stopping the orchestrator's own run does NOT stop whichever sub-pipeline it already
// triggered - that's a separate, independent OpenHEXA run, so it just keeps executing on its
// own unless stopped explicitly too. Stop that one as well when there is one, so the flowchart
// node currently in progress actually reaches "Arrêté" instead of running to completion anyway.
async function stopOrchestrator() {
  if (!state.orchestratorRunId) return;
  el("stopBtn2").disabled = true;
  const active = state.orchestrator.activeNode;
  try {
    await gql(STOP_MUTATION, { input: { runId: state.orchestratorRunId } });
  } catch (_err) {
    // next poll tick reflects real state
  }
  if (active) {
    try {
      await gql(STOP_MUTATION, { input: { runId: active.runId } });
    } catch (_err) {
      // best effort - refreshFlowchart's next tick shows the real outcome either way
    }
  }
}

// ---------------------------------------------------------------------------
// Wiring + boot
// ---------------------------------------------------------------------------

function wireEvents() {
  wireFileBrowser();
  el("launchBtn1").addEventListener("click", launchTarget);
  el("stopBtn1").addEventListener("click", stopTarget);
  el("launchBtn2").addEventListener("click", launchOrchestrator);
  el("stopBtn2").addEventListener("click", stopOrchestrator);
}

init();
