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
raw/corrected columns; a lone 0-59 total is split with fixed demographic ratios.

Output columns: LVL_3_NAME[, LVL_6_NAME], age, cible, year, round, produit.
"""

import re
import unicodedata

import numpy as np
import pandas as pd

from layouts import (
    POLIO_INFANT_SPLIT,
    PRODUCT_DEFS,
    PRODUCT_SYNONYMS,
)

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
# Text + structural detectors.                                                 #
# --------------------------------------------------------------------------- #
GEO_FIRST_TOKEN = {
    "region": "LVL_2_NAME",
    "regions": "LVL_2_NAME",
    "district": "LVL_3_NAME",
    "districts": "LVL_3_NAME",
    "csi": "LVL_6_NAME",
    "aire": "LVL_6_NAME",
    "aires": "LVL_6_NAME",
}
GEO_QUALIFIERS = {"sanitaire", "sanitaires", "de", "des", "du", "la", "sante"}
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
AGE_TOKEN_RE = re.compile(r"\d+\s*[-/]\s*\d+\s*(mois|ans|an)\b")
AGE_FULL_RE = re.compile(r"(\d+)\s*[-/]\s*(\d+)\s*(mois|ans|an)\b")
# A combined lower-bound bracket such as "6/9-11 mois" (6-or-9 to 11 months) is
# the infant window; canonicalise it to start at 0 -> "0-11 mois".
AGE_SLASH_RE = re.compile(r"\d+\s*/\s*\d+\s*-\s*(\d+)\s*(mois|ans|an)\b")
AGGREGATE_RE = re.compile(
    r"\b(total|totaux|ensemble|region|pays|national|refugie[rs]*|drs|hp)\b"
)
# Standalone header words marking a precomputed "total" section (spans the
# columns to their right until the next filled header cell).
TOTAL_MARKERS = {"total", "totale", "totaux"}


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


def _geo_level_of(norm: str):
    toks = norm.split()
    if not toks:
        return None
    level = GEO_FIRST_TOKEN.get(toks[0])
    if level is None:
        return None
    return level if all(t in GEO_QUALIFIERS for t in toks[1:]) else None


def _row_label_kinds(row) -> set:
    kinds = set()
    for cell in row:
        norm = normalize(cell)
        if not norm:
            continue
        toks = norm.split()
        if _geo_level_of(norm) is not None:
            kinds.add("geo")
        if len(toks) <= 6 and AGE_TOKEN_RE.search(norm):
            kinds.add("age")
        if len(toks) <= 3 and any(k in norm for k in SITE_KEYWORDS):
            kinds.add("site")
    return kinds


def sheet_geo_level(raw: pd.DataFrame, rows: int = 25) -> str:
    """CSI-level if a CSI/aire header exists, else district."""
    for r in range(min(rows, len(raw))):
        for cell in raw.iloc[r]:
            if _geo_level_of(normalize(cell)) == "LVL_6_NAME":
                return "csi"
    return "district"


def detect_header_end(raw: pd.DataFrame, max_scan: int = 25) -> int:
    scan = min(max_scan, len(raw))
    labelled = {r: k for r in range(scan) if (k := _row_label_kinds(raw.iloc[r]))}
    geo_rows = [r for r, k in labelled.items() if "geo" in k]
    if not geo_rows:
        raise ValueError(
            "Aucune ligne d'en-tête géographique détectée (region/district/CSI)."
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
        raise ValueError(f"Colonnes géographiques introuvables: {missing}")
    return {lvl: found[lvl] for lvl in wanted}


def _is_bare_aggregate(cell) -> bool:
    norm = normalize(cell)
    if not norm:
        return True
    return " ".join(GEO_NOISE_RE.sub(" ", norm).split()) == ""


def drop_aggregate_rows(df: pd.DataFrame, geo_cols: list) -> pd.DataFrame:
    keep = pd.Series(True, index=df.index)
    for col in geo_cols:
        keep &= ~df[col].map(normalize).str.contains(AGGREGATE_RE)
    keep &= ~df[geo_cols[-1]].map(_is_bare_aggregate)
    return df[keep]


# --------------------------------------------------------------------------- #
# Header interpretation.                                                        #
# --------------------------------------------------------------------------- #
def column_labels(raw: pd.DataFrame, header_end: int) -> dict:
    """
    Build a normalised label per column from the header block.

    Sparse header rows (merged section cells such as "POPULATION CIBLE VPO") are
    forward-filled across columns so every column inherits its section text;
    dense rows (the per-column age labels) are left as-is.
    """
    start = max(0, header_end - 3)
    block = raw.iloc[start : header_end + 1].reset_index(drop=True)
    norm = block.applymap(normalize).replace("", np.nan)
    ncol = norm.shape[1]
    filled = norm.copy()
    for r in range(norm.shape[0]):
        if norm.iloc[r].notna().sum() <= ncol / 2:  # sparse -> merged sections
            filled.iloc[r] = norm.iloc[r].ffill()
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
            if val in TOTAL_MARKERS:
                active = True
                total_cols.add(c)
            elif val == "":
                if active:
                    total_cols.add(c)
            else:
                active = False
    return total_cols


def parse_age(label: str):
    """Return the canonical age bracket (e.g. '0-11 mois') from a header label."""
    slash = AGE_SLASH_RE.search(label)
    if slash:  # "6/9-11 mois" -> "0-11 mois"
        unit = "mois" if slash.group(2) == "mois" else "ans"
        return f"0-{int(slash.group(1))} {unit}"
    matches = AGE_FULL_RE.findall(label)
    if not matches:
        return None
    a, b, unit = matches[-1]
    unit = "mois" if unit == "mois" else "ans"
    return f"{int(a)}-{int(b)} {unit}"


def detect_product(label: str):
    """Return the product named in a column's header, if any."""
    toks = set(label.split())
    for token, product in PRODUCT_SYNONYMS.items():
        if token in toks:
            return product
    return None


def _age_parts(age: str):
    m = AGE_FULL_RE.match(age)
    return (int(m.group(1)), int(m.group(2)), m.group(3)) if m else None


def age_match(found: str, needed: str) -> bool:
    """Exact match, or same open adult bracket (same start, 'ans', start >= 15)
    to absorb inconsistent upper bounds like '15-94' vs '15-60'."""
    if found == needed:
        return True
    pf, pn = _age_parts(found), _age_parts(needed)
    if (
        pf
        and pn
        and pf[2] == "ans"
        and pn[2] == "ans"
        and pf[0] == pn[0]
        and pf[0] >= 15
    ):
        return True
    return False


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
                "corrige": "corrige" in labels[c],
                "is_total": c in total_cols,
                "label": labels[c],
                "series": series.fillna(0),
            }
        )
    return candidates


def add_split_if_needed(candidates):
    """If a sheet only offers a 0-59 total (no component brackets), synthesise
    virtual 0-11 / 12-59 columns via fixed demographic ratios."""
    component = {
        "0-11 mois",
        "6-11 mois",
        "9-11 mois",
        "12-59 mois",
        "12-23 mois",
        "24-59 mois",
        "12-24 mois",
    }
    if any(c["age"] in component for c in candidates):
        return candidates
    totals = [
        c for c in candidates if c["age"] == "0-59 mois" and "km" not in c["label"]
    ]
    if not totals:
        return candidates
    total = max(totals, key=lambda c: c["series"].sum())
    _info("Colonne total 0-59 mois détectée: répartition 0-11 / 12-59 appliquée.")
    for age, ratio in POLIO_INFANT_SPLIT.items():
        candidates.append(
            {
                "age": age,
                "produit": None,
                "corrige": False,
                "is_total": False,
                "label": "split",
                "series": total["series"] * ratio,
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
            return [cols[i]]
    return cols


def select_columns(candidates, product, sources):
    """Pick the series feeding one target age of one product."""
    for src in sources:
        matches = [
            c
            for c in candidates
            if age_match(c["age"], src)
            and (c["produit"] is None or c["produit"] == product)
        ]
        if not matches:
            continue
        tagged = [c for c in matches if c["produit"] == product]
        use = tagged if tagged else matches
        if any(c["corrige"] for c in use):  # prefer corrected over raw columns
            use = [c for c in use if c["corrige"]]
        totals = [c for c in use if c.get("is_total")]
        if totals and len(totals) < len(use):
            # an explicit TOTAL column exists alongside components -> use the total
            use = totals
        use = drop_redundant_total(use)  # collapse a precomputed total + its parts
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

    raw = pd.read_excel(file, sheet_name=0, header=None)
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
    if age_cols:
        valmat = pd.concat(
            [pd.to_numeric(data[c], errors="coerce") for c in age_cols], axis=1
        )
        data = data[valmat.notna().any(axis=1)]

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
        candidates = [c for c in candidates if c["is_total"]]
        _info("Section TOTAL détectée: seules les colonnes totales sont utilisées.")

    candidates = add_split_if_needed(candidates)

    _info(
        f"Niveau géographique: {geo_level}; "
        f"colonnes d'âge détectées: {sorted({c['age'] for c in candidates})}."
    )

    pieces = []
    for product in products:
        defn = PRODUCT_DEFS.get(product)
        if defn is None:
            _warn(f"Produit inconnu '{product}' (ignoré).")
            continue
        produced = False
        for out_age, sources in defn.items():
            cols = select_columns(candidates, product, sources)
            if not cols:
                continue
            total = sum((c["series"] for c in cols[1:]), cols[0]["series"])
            piece = geo_frame.copy()
            piece["age"] = out_age
            piece["produit"] = product
            piece["cible"] = _coerce(total, rounding)
            pieces.append(piece)
            produced = True
        if not produced:
            _warn(
                f"Produit '{product}' non dérivable de ce fichier "
                f"(âges disponibles: {sorted({c['age'] for c in candidates})})."
            )

    if not pieces:
        raise ValueError(
            "Aucun produit demandé n'a pu être dérivé du fichier. "
            f"Produits: {products}; âges détectés: {sorted({c['age'] for c in candidates})}."
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
    return tidy
