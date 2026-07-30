"""
Auto-detecting engine that turns a vaccination target spreadsheet into the tidy
long format expected downstream - WITHOUT any per-file configuration.

How it stays generic
---------------------
Everything mechanical is discovered from the sheet itself: the header row(s), the
geographic columns, the district/CSI level, and which columns hold which age
bracket (read from the header labels).

The one thing a spreadsheet can't tell us - what a campaign *means* - comes from
the run parameters. Knowing the ``products`` lets the engine decide which age
columns to read and how to label them, using the global PRODUCT_DEFS. Product
names found in section headers (e.g. "POPULATION CIBLE VPO") and "(corrigé)"
markers are used to disambiguate when a single sheet mixes several products or
raw/corrected columns. A column that doesn't unambiguously state the exact age
bracket a product needs - a combined lower-bound notation, a differently-scoped
adult bracket, a bare total with no per-product breakdown - is rejected rather
than approximated: the file must be corrected to state it explicitly.

Output columns: LVL_3_NAME[, LVL_6_NAME], age, cible, year, round, produit.
"""

import re
import unicodedata

import numpy as np
import pandas as pd

from layouts import (
    PRODUCT_DEFS,
    PRODUCT_SYNONYMS,
)
from text_match import any_token_matches, find_token, match_token, matches


class TargetImportError(Exception):
    """Raised when a spreadsheet cannot be interpreted. The message is written for
    the person running the pipeline: what went wrong and how to fix it."""


# --------------------------------------------------------------------------- #
# Assumption tracking.                                                         #
#                                                                              #
# Whenever the engine interprets the data rather than reading it literally      #
# (equating an age bracket, splitting a total, tolerating a typo, choosing      #
# between candidate columns), it records the assumption and logs it as a        #
# warning. Messages are de-duplicated, so a rule applied to 1 500 rows is       #
# reported once, and a recap is logged at the end of the import.                #
# --------------------------------------------------------------------------- #
_ASSUMPTIONS = {}


def reset_assumptions():
    _ASSUMPTIONS.clear()


def note_assumption(*parts):
    """
    Log a data assumption once (subsequent identical ones are just counted).

    Each argument is one sentence and is emitted as its OWN log entry: the
    OpenHexa interface collapses newlines inside a single message, so splitting
    across calls is what actually produces readable, separated lines.
    """
    sentences = [str(p).strip() for p in parts if p and str(p).strip()]
    key = " ".join(sentences)
    if key not in _ASSUMPTIONS:
        _ASSUMPTIONS[key] = 0
        for i, sentence in enumerate(sentences):
            _warn(
                f"HYPOTHÈSE: {sentence}" if i == 0 else f"HYPOTHÈSE (suite): {sentence}"
            )
    _ASSUMPTIONS[key] += 1


def assumptions_summary() -> list:
    return list(_ASSUMPTIONS)


# --------------------------------------------------------------------------- #
# Logging shim: use OpenHexa's current_run when available, else stdlib logging.#
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    from openhexa.sdk import current_run

    def _info(m):
        current_run.log_info(m)

    def _warn(m):
        current_run.log_warning(m)

    def _error(m):
        current_run.log_error(m)
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _log = logging.getLogger("target_import")
    _info, _warn, _error = _log.info, _log.warning, _log.error


# --------------------------------------------------------------------------- #
# Vocabularies. Lookups go through text_match, so spelling variants and typos   #
# ("distrct", "totall", "corigé") still resolve. Short words are matched         #
# exactly (see text_match) so codes like "csi"/"ds"/"hp" stay unambiguous.       #
# --------------------------------------------------------------------------- #
GEO_FIRST_TOKEN = {
    "region": "LVL_2_NAME",
    "regions": "LVL_2_NAME",
    "district": "LVL_3_NAME",
    "districts": "LVL_3_NAME",
    "csi": "LVL_6_NAME",
    "aire": "LVL_6_NAME",
    "aires": "LVL_6_NAME",
    "formation": "LVL_6_NAME",
    "centre": "LVL_6_NAME",
}
GEO_QUALIFIERS = {
    "sanitaire",
    "sanitaires",
    "de",
    "des",
    "du",
    "la",
    "sante",
    "s",
    "nom",
    "libelle",
}
GEO_NOISE_RE = re.compile(r"\b(csi|cs|ds|chr|hd|creni|crenam|di|dr|drsp|hp)\b")
SITE_KEYWORDS = (
    "autochtone",
    "refugier",
    "refugie",
    "urbain",
    "avancee",
    "mobile",
    "fixe",
    "total",
)

# Age units, fuzzily canonicalised before the age regexes run.
AGE_UNITS = {
    "mois": "mois",
    "ans": "ans",
    "an": "ans",
    "annee": "ans",
    "annees": "ans",
    "mo": "mois",
}
AGE_TOKEN_RE = re.compile(r"\d+\s*[-/]\s*\d+\s*(mois|ans|an)\b")
AGE_FULL_RE = re.compile(r"(\d+)\s*[-/]\s*(\d+)\s*(mois|ans|an)\b")
# A combined lower-bound bracket such as "6/9-11 mois" (6-or-9 to 11 months) is
# the infant window; canonicalise it to start at 0 -> "0-11 mois".
AGE_SLASH_RE = re.compile(r"\d+\s*/\s*\d+\s*-\s*(\d+)\s*(mois|ans|an)\b")

# Aggregate/subtotal row markers.
#  - STRONG: recognised anywhere in the cell ("Total Abala", "Région Agadez").
#  - WHOLE : only when they make up the entire cell, because they also occur
#            inside legitimate facility names (e.g. the CSI "Garde Nationale"
#            must NOT be treated as a national total).
AGGREGATE_STRONG = {
    "total",
    "totaux",
    "totale",
    "ensemble",
    "region",
    "regions",
    "refugie",
    "refugier",
    "refugiers",
    "refugies",
    "drs",
    "hp",
}
AGGREGATE_WHOLE = {"national", "nationale", "pays", "cumul", "somme"}

# Header words marking a precomputed "total" section (spans the columns to their
# right until the next filled header cell).
TOTAL_MARKERS = {"total", "totale", "totaux", "ensemble", "cumul"}

# Marker for corrected/adjusted columns, preferred over their raw counterparts.
CORRECTED_MARKERS = {"corrige", "corrigee", "corr", "ajuste", "ajustee"}


def normalize(text) -> str:
    """Lowercase, strip accents, drop punctuation (keep - and / for age tokens)."""
    if not isinstance(text, str):
        if pd.isna(text):
            return ""
        text = str(text)
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^a-z0-9/ -]", " ", text)
    return " ".join(text.split())


def canonicalize_units(label: str) -> str:
    """
    Rewrite fuzzy age-unit spellings to their canonical form so the age regexes
    match despite typos ("12-59 moiss" -> "12-59 mois", "5-14 annees" -> "5-14 ans").
    """
    out = []
    for tok in label.split():
        hit = match_token(tok, AGE_UNITS, threshold=90, min_len=4, max_len_diff=2)
        if hit and AGE_UNITS[hit] != tok:
            note_assumption(
                f"L'unité d'âge '{tok}' n'est pas une orthographe attendue.",
                f"Le traitement continue en l'interprétant comme '{AGE_UNITS[hit]}'.",
            )
        out.append(AGE_UNITS[hit] if hit else tok)
    return " ".join(out)


def is_aggregate_label(norm: str) -> bool:
    """
    True if a geo cell denotes an aggregate/subtotal row rather than a place.

    Separators inside compound codes are split so that e.g. "DRS/HP" (a regional
    subtotal line) is seen as the tokens "drs" and "hp".
    """
    if not norm:
        return False
    if matches(norm, AGGREGATE_WHOLE):  # whole-cell ambiguous markers
        return True
    tokenised = norm.replace("/", " ")
    return any_token_matches(tokenised, AGGREGATE_STRONG)  # strong markers anywhere


def _geo_level_of(norm: str):
    toks = norm.split()
    if not toks:
        return None
    level_key = match_token(toks[0], GEO_FIRST_TOKEN)
    if level_key is None:
        return None
    level = GEO_FIRST_TOKEN[level_key]
    return level if all(matches(t, GEO_QUALIFIERS) for t in toks[1:]) else None


def _row_label_kinds(row) -> set:
    kinds = set()
    for cell in row:
        norm = normalize(cell)
        if not norm:
            continue
        toks = norm.split()
        if _geo_level_of(norm) is not None:
            kinds.add("geo")
        if len(toks) <= 6 and AGE_TOKEN_RE.search(canonicalize_units(norm)):
            kinds.add("age")
        if len(toks) <= 3 and any_token_matches(norm, SITE_KEYWORDS):
            kinds.add("site")
    return kinds


def sheet_geo_level(raw: pd.DataFrame, rows: int = 25) -> str:
    """CSI-level if a CSI/aire header exists, else district."""
    for r in range(min(rows, len(raw))):
        for cell in raw.iloc[r]:
            if _geo_level_of(normalize(cell)) == "LVL_6_NAME":
                return "csi"
    return "district"


def fail(message: str):
    """
    Log an actionable error for the pipeline operator, then abort the import.

    The message is split on newlines and each line is logged SEPARATELY, because
    the OpenHexa interface renders a multi-line message as one collapsed block.
    One call per line is what makes the diagnostic readable there. The exception
    still carries the whole text.
    """
    lines = [ln.strip() for ln in message.splitlines() if ln.strip()]
    for line in lines:
        _error(line)
    raise TargetImportError("\n".join(lines))


def _preview_rows(raw: pd.DataFrame, n: int = 6) -> str:
    """First non-empty cells of the first rows, to help the user see the file."""
    lines = []
    for r in range(min(n, len(raw))):
        cells = [str(v)[:24] for v in raw.iloc[r] if pd.notna(v) and str(v).strip()]
        if cells:
            lines.append(f"    ligne {r}: {' | '.join(cells[:6])}")
    return "\n".join(lines) if lines else "    (feuille vide)"


def detect_header_end(raw: pd.DataFrame, max_scan: int = 25) -> int:
    scan = min(max_scan, len(raw))
    labelled = {r: k for r in range(scan) if (k := _row_label_kinds(raw.iloc[r]))}
    geo_rows = [r for r, k in labelled.items() if "geo" in k]
    if not geo_rows:
        fail(
            "Impossible de trouver la ligne d'en-tête du fichier.\n"
            "CAUSE: aucune colonne géographique n'a été reconnue dans les "
            f"{scan} premières lignes. L'en-tête doit contenir un intitulé de type "
            "'Régions', 'Districts' / 'Districts sanitaires' ou 'CSI' / 'Aire de santé'.\n"
            "À FAIRE: ouvrez le fichier et vérifiez que la ligne d'en-tête existe, "
            "qu'elle se situe dans les 25 premières lignes, et que la colonne des "
            "districts (ou des CSI) porte bien un de ces intitulés. "
            "Les fautes de frappe légères sont tolérées, mais un intitulé totalement "
            "différent (ex: 'Zone') ne peut pas être deviné.\n"
            f"APERÇU DU FICHIER:\n{_preview_rows(raw)}"
        )
    end = geo_rows[0]
    while end + 1 in labelled:
        end += 1
    return end


def find_geo_columns(raw: pd.DataFrame, header_end: int, geo_level: str) -> dict:
    wanted = {"LVL_3_NAME"} if geo_level == "district" else {"LVL_3_NAME", "LVL_6_NAME"}
    found = {}
    for r in range(header_end + 1):
        for col, cell in enumerate(raw.iloc[r]):
            level = _geo_level_of(normalize(cell))
            if level is not None:
                found.setdefault(level, col)
    missing = wanted - set(found)
    if missing:
        human = {
            "LVL_3_NAME": "districts sanitaires",
            "LVL_6_NAME": "CSI / aires de santé",
        }
        missing_txt = ", ".join(human.get(m, m) for m in sorted(missing))
        detected = (
            ", ".join(
                f"{human.get(k, k)} -> colonne {v}" for k, v in sorted(found.items())
            )
            or "aucune"
        )
        fail(
            f"Colonne(s) géographique(s) manquante(s): {missing_txt}.\n"
            f"CAUSE: le fichier a été identifié comme de niveau '{geo_level}' mais "
            f"l'en-tête ne contient pas d'intitulé pour {missing_txt}. "
            f"Colonnes géographiques détectées: {detected}.\n"
            "À FAIRE: ajoutez/corrigez l'intitulé de cette colonne dans la ligne "
            f"d'en-tête (ligne {header_end}) du fichier, par exemple 'Districts "
            "sanitaires' ou 'CSI', puis relancez le pipeline."
        )
    return {lvl: found[lvl] for lvl in wanted}


def _is_bare_aggregate(cell) -> bool:
    norm = normalize(cell)
    if not norm:
        return True
    return " ".join(GEO_NOISE_RE.sub(" ", norm).split()) == ""


def drop_aggregate_rows(df: pd.DataFrame, geo_cols: list) -> pd.DataFrame:
    keep = pd.Series(True, index=df.index)
    for col in geo_cols:
        keep &= ~df[col].map(normalize).map(is_aggregate_label)
    keep &= ~df[geo_cols[-1]].map(_is_bare_aggregate)
    return df[keep]


# --------------------------------------------------------------------------- #
# Header interpretation.                                                        #
# --------------------------------------------------------------------------- #
def column_labels(raw: pd.DataFrame, header_end: int) -> dict:
    """
    Build a normalised label per column from the header block.

    Merged section cells (e.g. "POPULATION CIBLE VPO" spanning three age columns)
    leave blanks to their right; those blanks inherit the section text so each
    column gets a full label.

    A blank inherits ONLY when a lower header row gives that column its own label.
    That is what distinguishes a merged section header (its span sits above the
    per-column age labels) from a genuine gap such as the standalone
    "NOMBRE D'AIRE DE SANTE" column, which must not inherit an age bracket.
    This rule is structural, so it is unaffected by extra/blank columns elsewhere.
    """
    start = max(0, header_end - 3)
    block = raw.iloc[start : header_end + 1].reset_index(drop=True)
    norm = block.applymap(normalize).replace("", np.nan)
    nrow, ncol = norm.shape
    filled = norm.copy()
    for r in range(nrow):
        carried = None
        for c in range(ncol):
            value = norm.iat[r, c]
            if isinstance(value, str) and value:
                carried = value
                continue
            if carried is None:
                continue
            has_own_label_below = any(
                isinstance(norm.iat[r2, c], str) and norm.iat[r2, c]
                for r2 in range(r + 1, nrow)
            )
            if has_own_label_below:
                filled.iat[r, c] = carried
    labels = {}
    for c in range(ncol):
        toks = []
        for r in range(filled.shape[0]):
            v = filled.iloc[r, c]
            if isinstance(v, str) and v:
                toks.extend(v.split())
        # de-duplicate while keeping order
        labels[c] = " ".join(dict.fromkeys(toks))
    return labels


def detect_total_columns(raw: pd.DataFrame, header_end: int) -> set:
    """
    Columns belonging to a precomputed "TOTAL" section.

    A standalone TOTAL marker in a header cell tags itself and every empty cell
    to its right (the merged span), so a "TOTAL" over several age columns marks
    them all. Used to prefer a total over its component columns.
    """
    start = max(0, header_end - 3)
    total_cols = set()
    for r in range(start, header_end + 1):
        active = False
        for c in range(raw.shape[1]):
            val = normalize(raw.iloc[r, c])
            if val and matches(val, TOTAL_MARKERS):
                active = True
                total_cols.add(c)
            elif val == "":
                if active:
                    total_cols.add(c)
            else:
                active = False
    return total_cols


def parse_age(label: str):
    """Return the canonical age bracket (e.g. '0-11 mois') from a header label.

    Age-unit spellings are fuzzily canonicalised first, so '12-59 moiss' or
    '5-14 annees' are still understood. A combined lower-bound notation (e.g.
    '6/9-11 mois', used when two antigens start at different ages) is rejected
    rather than guessed at: whether it should be read as '0-11 mois' or
    something else changes what population is actually counted, so the file
    must state the bracket explicitly instead.
    """
    label = canonicalize_units(label)
    slash = AGE_SLASH_RE.search(label)
    if slash:
        unit = "mois" if slash.group(2) == "mois" else "ans"
        observed = slash.group(0).strip()
        fail(
            "Tranche d'âge ambiguë détectée dans l'en-tête.\n"
            f"CAUSE: une colonne intitulée '{label}' utilise une notation combinée "
            f"de borne inférieure ('{observed}'), qui indique un âge de départ "
            "différent selon l'antigène plutôt qu'une tranche d'âge unique et "
            "explicite.\n"
            "À FAIRE: renommez cette colonne dans le fichier pour qu'elle indique "
            f"la tranche exacte qu'elle représente (par exemple '0-"
            f"{int(slash.group(1))} {unit}' si elle couvre bien l'ensemble de la "
            f"population depuis 0 {unit}), puis relancez le pipeline."
        )
    found = AGE_FULL_RE.findall(label)
    if not found:
        return None
    a, b, unit = found[-1]
    unit = "mois" if unit == "mois" else "ans"
    return f"{int(a)}-{int(b)} {unit}"


def detect_product(label: str):
    """Return the product named in a column's header, if any (typo-tolerant)."""
    hit = find_token(label, PRODUCT_SYNONYMS)
    return PRODUCT_SYNONYMS[hit] if hit else None


def is_corrected(label: str) -> bool:
    """True if the column header marks a corrected/adjusted value."""
    return any_token_matches(label, CORRECTED_MARKERS)


def age_match(found: str, needed: str) -> bool:
    """Exact match only.

    A found bracket must state precisely the range a product's age group is
    defined against: treating a nearby-but-different bracket as equivalent
    (e.g. a wider adult upper bound like '15-94 ans' for a needed '15-60 ans')
    would misstate who was actually targeted, so nothing is absorbed here.
    """
    return found == needed


# --------------------------------------------------------------------------- #
# Extraction.                                                                  #
# --------------------------------------------------------------------------- #
def _coerce(series: pd.Series, rounding: str) -> pd.Series:
    num = pd.to_numeric(series, errors="coerce").fillna(0)
    num = np.trunc(num) if rounding == "trunc" else np.round(num, 0)
    return num.astype(np.int64)


def build_candidates(raw, header_end, labels, data, total_cols):
    """List of age-bearing value columns: {age, produit, corrige, is_total, series}."""
    candidates = []
    for c in range(raw.shape[1]):
        age = parse_age(labels.get(c, ""))
        if age is None:
            continue
        series = pd.to_numeric(data[c], errors="coerce")
        if series.notna().sum() == 0:
            continue
        candidates.append(
            {
                "age": age,
                "produit": detect_product(labels[c]),
                "corrige": is_corrected(labels[c]),
                "is_total": c in total_cols,
                "label": labels[c],
                "series": series.fillna(0),
            }
        )
    return candidates


def drop_redundant_total(cols):
    """
    If one column equals the row-wise sum of the others, it is a precomputed
    total (e.g. a TOTAL column alongside Autochtone/Réfugiés) - keep only it.
    Otherwise the columns are genuine disaggregations (e.g. site strategies) and
    are all returned to be summed.
    """
    if len(cols) < 2:
        return cols
    series = [c["series"].reset_index(drop=True).astype(float) for c in cols]
    for i, s in enumerate(series):
        others = sum((series[j] for j in range(len(series)) if j != i))
        if s.sum() > 0 and np.allclose(s.values, others.values, rtol=0.001, atol=1.0):
            note_assumption(
                f"La colonne '{cols[i]['label'][:40]}' est égale à la somme des "
                f"{len(cols) - 1} autre(s) colonne(s) de même tranche d'âge.",
                "Le traitement continue en la considérant comme un total déjà calculé.",
                "Les colonnes de détail correspondantes sont ignorées pour éviter un "
                "double comptage.",
            )
            return [cols[i]]
    return cols


def select_columns(candidates, product, sources):
    """Pick the series feeding one target age of one product."""
    for src in sources:
        hits = [
            c
            for c in candidates
            if age_match(c["age"], src)
            and (c["produit"] is None or c["produit"] == product)
        ]
        if not hits:
            continue
        tagged = [c for c in hits if c["produit"] == product]
        use = tagged if tagged else hits
        corrected = [c for c in use if c["corrige"]]
        if corrected and len(corrected) < len(use):
            note_assumption(
                f"Pour la tranche '{src}', le fichier contient à la fois des colonnes "
                "brutes et des colonnes corrigées.",
                "Le traitement continue en retenant uniquement les colonnes corrigées "
                "(mention 'corrigé' ou 'ajusté' dans l'en-tête).",
            )
            use = corrected
        totals = [c for c in use if c.get("is_total")]
        if totals and len(totals) < len(use):
            note_assumption(
                f"Pour la tranche '{src}', le fichier contient une colonne de section "
                "TOTAL et des colonnes de détail.",
                "Le traitement continue en retenant uniquement la colonne TOTAL.",
                "Les colonnes de détail sont ignorées pour éviter un double comptage.",
            )
            use = totals
        use = drop_redundant_total(use)  # collapse a precomputed total + its parts
        if len(use) > 1:
            note_assumption(
                f"{len(use)} colonnes distinctes correspondent à la tranche '{src}' "
                f"pour le produit '{product}'.",
                "Il s'agit vraisemblablement de stratégies de vaccination distinctes "
                "(par ex. poste fixe / avancée / mobile).",
                "Le traitement continue en additionnant ces colonnes.",
            )
        return use
    return []


def import_target_file(
    file, products, year: int, rounds, rounding: str = "round"
) -> pd.DataFrame:
    """
    Auto-detect the structure of ``file`` and produce the tidy long target frame
    for the requested ``products`` / ``year`` / ``rounds``.
    """
    if isinstance(products, str):
        products = [products]
    if isinstance(rounds, (int, np.integer, str)):
        rounds = [rounds]
    round_labels = [f"round {int(r)}" for r in rounds]

    reset_assumptions()

    try:
        raw = pd.read_excel(file, sheet_name=0, header=None)
    except Exception as e:
        fail(
            "Le fichier n'a pas pu être ouvert comme classeur Excel.\n"
            f"CAUSE TECHNIQUE: {e}\n"
            "À FAIRE: vérifiez que le fichier envoyé est bien un .xlsx (et non un "
            ".csv, un .xls ancien format ou un fichier corrompu), puis réessayez. "
            "Si le fichier s'ouvre dans Excel, réenregistrez-le au format "
            "'Classeur Excel (.xlsx)'."
        )
    if raw.dropna(how="all").empty:
        fail(
            "La première feuille du classeur est vide.\n"
            "CAUSE: aucune donnée trouvée dans la feuille traitée (la première).\n"
            "À FAIRE: placez les données de cibles dans la PREMIÈRE feuille du "
            "classeur, ou supprimez les feuilles vides qui la précèdent."
        )

    header_end = detect_header_end(raw)
    geo_level = sheet_geo_level(raw)
    geo_map = find_geo_columns(raw, header_end, geo_level)
    geo_levels = (
        ["LVL_3_NAME"] if geo_level == "district" else ["LVL_3_NAME", "LVL_6_NAME"]
    )
    geo_cols = [geo_map[l] for l in geo_levels]
    labels = column_labels(raw, header_end)

    data = raw.iloc[header_end + 1 :].reset_index(drop=True)
    data = data.dropna(subset=[geo_cols[0]])
    data = drop_aggregate_rows(data, geo_cols)

    # keep only rows that carry at least one numeric value in an age column
    age_cols = [c for c in range(raw.shape[1]) if parse_age(labels.get(c, ""))]
    if not age_cols:
        seen = [f"colonne {c}: '{l}'" for c, l in sorted(labels.items()) if l][:12]
        fail(
            "Aucune colonne de tranche d'âge n'a été reconnue dans ce fichier.\n"
            "CAUSE: le pipeline identifie les colonnes de cibles grâce à une "
            "tranche d'âge dans leur intitulé, au format '<début>-<fin> mois' ou "
            "'<début>-<fin> ans' (ex: '0-11 mois', '12-59 mois', '5-14 ans'). "
            "Aucun intitulé de ce type n'a été trouvé.\n"
            "À FAIRE: dans la ligne d'en-tête, renommez les colonnes de cibles pour "
            "qu'elles indiquent explicitement leur tranche d'âge et son unité "
            "(mois ou ans). Exemple: remplacez 'Enfants cible' par '0-59 mois'.\n"
            f"INTITULÉS DÉTECTÉS: {'; '.join(seen) if seen else 'aucun'}"
        )
    valmat = pd.concat(
        [pd.to_numeric(data[c], errors="coerce") for c in age_cols], axis=1
    )
    data = data[valmat.notna().any(axis=1)]

    if data.empty:
        fail(
            "Aucune ligne de données exploitable n'a été trouvée.\n"
            f"CAUSE: sous la ligne d'en-tête (ligne {header_end}), toutes les lignes "
            "ont été écartées car elles étaient vides, sans nom de district/CSI, "
            "sans valeur numérique, ou identifiées comme des totaux.\n"
            "À FAIRE: vérifiez que les données commencent juste sous l'en-tête, que "
            "la colonne des districts/CSI est bien remplie et que les cibles sont "
            "des nombres (et non du texte)."
        )

    geo_frame = data[geo_cols].copy()
    geo_frame.columns = geo_levels
    for c in geo_levels:
        geo_frame[c] = geo_frame[c].astype(str).str.strip()

    total_cols = detect_total_columns(raw, header_end)
    candidates = build_candidates(
        raw, header_end, labels, data.reset_index(drop=True), total_cols
    )
    geo_frame = geo_frame.reset_index(drop=True)

    # If the sheet provides an explicit TOTAL section, its columns are the
    # authoritative aggregates - use them and ignore the partial disaggregations
    # (e.g. Autochtone/Réfugiés component columns).
    if any(c["is_total"] for c in candidates):
        dropped = sorted({c["age"] for c in candidates if not c["is_total"]})
        candidates = [c for c in candidates if c["is_total"]]
        note_assumption(
            "Le fichier contient une section 'TOTAL' dans son en-tête.",
            "Le traitement continue en n'utilisant que les colonnes de cette section, "
            "considérées comme les agrégats de référence.",
            (f"Colonnes de détail ignorées: {', '.join(dropped)}." if dropped else ""),
        )

    available_ages = sorted({c["age"] for c in candidates})
    _info(
        f"Niveau géographique: {geo_level}; en-tête ligne {header_end}; "
        f"colonnes d'âge détectées: {available_ages}."
    )

    # Build every requested product. A product is only acceptable if EVERY age
    # bracket it is delivered in could be resolved to a column: a partially
    # extracted product would silently under-count the campaign, so it aborts.
    pieces = []
    unknown_products = []
    incomplete = {}  # product -> {output age: accepted source brackets}
    for product in products:
        defn = PRODUCT_DEFS.get(product)
        if defn is None:
            unknown_products.append(product)
            continue
        product_pieces, missing_ages = [], {}
        for out_age, sources in defn.items():
            cols = select_columns(candidates, product, sources)
            if not cols:
                missing_ages[out_age] = sources
                continue
            total = sum((c["series"] for c in cols[1:]), cols[0]["series"])
            piece = geo_frame.copy()
            piece["age"] = out_age
            piece["produit"] = product
            piece["cible"] = _coerce(total, rounding)
            product_pieces.append(piece)
        if missing_ages:
            incomplete[product] = missing_ages
        else:
            pieces.extend(product_pieces)

    if unknown_products:
        fail(
            f"Produit(s) non reconnu(s): {', '.join(map(str, unknown_products))}.\n"
            f"CAUSE: ces produits ne font pas partie des produits gérés.\n"
            f"À FAIRE: sélectionnez un ou plusieurs produits parmi: "
            f"{sorted(PRODUCT_DEFS)}."
        )

    if incomplete:
        details = []
        for product, missing_ages in incomplete.items():
            expected = list(PRODUCT_DEFS[product].keys())
            found = [a for a in expected if a not in missing_ages]
            details.append(f"> Produit '{product}':")
            details.append(
                f"  - tranche(s) MANQUANTE(S): {', '.join(sorted(missing_ages))}"
            )
            details.append(
                f"  - tranche(s) trouvée(s): {', '.join(sorted(found)) or 'aucune'}"
            )
            for out_age, sources in sorted(missing_ages.items()):
                details.append(
                    f"  - pour '{out_age}', le pipeline cherche une colonne intitulée "
                    f"{' ou '.join(repr(s) for s in sources)}"
                )
        fail(
            "Le fichier ne contient pas toutes les tranches d'âge nécessaires aux "
            "produits sélectionnés.\n"
            "Traitement interrompu pour éviter de produire des cibles incomplètes.\n"
            "CAUSE: pour chaque produit sélectionné, TOUTES ses tranches d'âge "
            "doivent être présentes dans le fichier.\n"
            "DÉTAIL:\n" + "\n".join(details) + "\n"
            "TRANCHES D'ÂGE DÉTECTÉES DANS LE FICHIER: "
            f"{', '.join(available_ages) if available_ages else 'aucune'}\n"
            "À FAIRE: deux options possibles.\n"
            f"OPTION 1: corrigez l'intitulé de la ou des colonnes concernées dans la "
            f"ligne d'en-tête (ligne {header_end}) pour qu'il corresponde exactement "
            "à la tranche attendue ci-dessus.\n"
            "Attention aux bornes: '12-60 mois' n'est pas '12-59 mois'.\n"
            "OPTION 2: si la tranche est réellement absente du fichier, "
            "désélectionnez le produit concerné dans le paramètre 'Produit(s)'."
        )

    if not pieces:
        fail(
            "Aucune donnée n'a pu être extraite de ce fichier.\n"
            f"CAUSE: les produits demandés ({', '.join(map(str, products))}) n'ont "
            "donné aucune colonne exploitable. Tranches d'âge détectées: "
            f"{', '.join(available_ages) if available_ages else 'aucune'}.\n"
            "À FAIRE: vérifiez que le fichier correspond bien à la campagne choisie "
            "et que les colonnes de cibles indiquent leur tranche d'âge dans "
            "l'en-tête."
        )

    long_df = pd.concat(pieces, ignore_index=True)
    # sum any columns that mapped to the same geo + age + product
    long_df = long_df.groupby(geo_levels + ["age", "produit"], as_index=False)[
        "cible"
    ].sum()

    long_df["year"] = int(year)
    tidy = pd.concat(
        [long_df.assign(round=lbl) for lbl in round_labels], ignore_index=True
    )

    ordered = [
        c
        for c in [
            "LVL_3_NAME",
            "LVL_6_NAME",
            "age",
            "cible",
            "year",
            "round",
            "produit",
        ]
        if c in tidy.columns
    ]
    tidy = tidy[ordered]
    _info(f"{len(tidy)} lignes produites (cible totale {int(tidy['cible'].sum())}).")

    made = assumptions_summary()
    if made:
        # One call per line: OpenHexa collapses newlines within a single message.
        _warn(
            f"RÉCAPITULATIF: {len(made)} hypothèse(s) ont été nécessaires pour "
            "interpréter ce fichier."
        )
        _warn(
            "Vérifiez que ces hypothèses correspondent bien à votre intention "
            "avant d'utiliser ces cibles."
        )
    else:
        _info(
            "Aucune hypothèse nécessaire: toutes les tranches d'âge ont été "
            "lues directement dans le fichier."
        )
    return tidy
