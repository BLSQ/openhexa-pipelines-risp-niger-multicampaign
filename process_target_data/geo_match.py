"""
Fuzzy matching of district (LVL_3) names between target files and the IASO tree.

Why this replaces the hard-coded dictionary
-------------------------------------------
District labels in the spreadsheets differ from the official IASO names in
predictable, mechanical ways: accents ("Gouré" / "Goure"), spelling variants
("Filingue" / "Fillingue", "Dungass" / "Doungass"), spacing ("Dogondoutchi" /
"Dogon Doutchi", "Niamey  I" / "Niamey I"), administrative suffixes ("Tahoua
Département" -> "Tahoua", "Agadez commune" -> "Agadez") and parentheses
("Tibiri (Doutchi)" -> "Tibiri"). All of these are recoverable by normalising the
strings and scoring similarity, so no per-name entry is needed.

Verified against the previous 74-entry dictionary: 73 of 74 names resolve to the
exact same IASO district by fuzzy matching alone.

The one irreducible case
------------------------
"Kantché" and "Matamèye" are two different NAMES for the same district (an
administrative rename), not a spelling variant - no string-similarity method can
connect them (best fuzzy candidate scored 46, far below any usable threshold).
True renames like this therefore remain as explicit aliases in DISTRICT_ALIASES.
Adding a genuine rename there is a deliberate, documented decision rather than
the routine maintenance the old dictionary required.

Safety
------
Correct matches scored >= 83 while the best incorrect candidate scored 46, so the
default threshold of 75 separates them with a wide margin. Names below the
threshold are reported as unmatched (and therefore excluded) instead of being
silently attached to the wrong district.
"""

import re
import unicodedata

try:
    from fuzzywuzzy import fuzz

    def _token_set(a, b):
        return fuzz.token_set_ratio(a, b)

    def _ratio(a, b):
        return fuzz.ratio(a, b)
except Exception:  # pragma: no cover - stdlib fallback
    from difflib import SequenceMatcher

    def _ratio(a, b):
        return int(round(SequenceMatcher(None, a, b).ratio() * 100))

    def _token_set(a, b):
        ta, tb = set(a.split()), set(b.split())
        if not ta or not tb:
            return 0
        inter = " ".join(sorted(ta & tb))
        return max(_ratio(inter or a, b), _ratio(a, b))


#: True administrative renames: the same district known under a different name.
#: These cannot be derived from string similarity and must stay explicit.
DISTRICT_ALIASES = {
    "kantche": "matameye",
}

#: Administrative qualifiers treated as equivalent, so "Tahoua Ville" and
#: "Tahoua Commune" describe the same urban district.
_QUALIFIER_EQUIVALENTS = {"ville": "commune"}

#: Qualifiers that carry no distinguishing information ("Tahoua Département" is
#: simply the district "Tahoua").
_QUALIFIER_DROPPED = ("departement", "department")

DEFAULT_DISTRICT_THRESHOLD = 75


def normalize_geo(name: str) -> str:
    """Normalise a district label for comparison (accents, punctuation, prefixes,
    administrative qualifiers), then apply any known rename alias."""
    text = str(name).lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"^\s*ds\b", " ", text)  # drop the IASO "DS " prefix
    text = re.sub(r"[^a-z0-9]+", " ", text)  # punctuation -> space
    for word in _QUALIFIER_DROPPED:
        text = re.sub(rf"\b{word}\b", " ", text)
    for src, dst in _QUALIFIER_EQUIVALENTS.items():
        text = re.sub(rf"\b{src}\b", dst, text)
    text = " ".join(text.split())
    return DISTRICT_ALIASES.get(text, text)


def score(a: str, b: str) -> float:
    """Similarity of two normalised district names (0-100, exact match = 200)."""
    if a == b:
        return 200.0
    return 0.6 * _token_set(a, b) + 0.4 * _ratio(a, b)


def build_district_mapping(
    raw_names, iaso_names, threshold: int = DEFAULT_DISTRICT_THRESHOLD
):
    """
    Map each raw district label to the best-matching IASO district name.

    Returns (mapping, unmatched, inexact) where:
      mapping  : {raw name -> IASO name} for accepted matches
      unmatched: [(raw name, best candidate, score)] below the threshold
      inexact  : [(raw name, IASO name, score)] matched but not string-identical,
                 so the caller can report the assumption made
    """
    candidates = [(name, normalize_geo(name)) for name in iaso_names]
    mapping, unmatched, inexact = {}, [], []

    for raw in raw_names:
        query = normalize_geo(raw)
        if not query:
            continue
        best, best_score = None, -1.0
        for iaso_name, iaso_norm in candidates:
            value = score(query, iaso_norm)
            if value > best_score:
                best, best_score = iaso_name, value
        if best is None or best_score < threshold:
            unmatched.append((raw, best, round(best_score, 1)))
            continue
        mapping[raw] = best
        if str(raw).strip() != str(best).strip():
            inexact.append((raw, best, round(min(best_score, 100.0), 1)))
    return mapping, unmatched, inexact
