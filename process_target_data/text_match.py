"""
Fuzzy vocabulary matching for spreadsheet header/label detection.

Why
---
The structural detectors used to test header words by exact membership
("total", "district", "corrigé", ...). A single typo in a new file ("totall",
"distrct", "corigé") silently broke detection. Here every vocabulary lookup goes
through an approximate matcher instead, so spelling variants still resolve.

Guarding against false positives
--------------------------------
Loose fuzzy matching on short words is dangerous: "pays" vs the real district
"Say" scores ~86%. Three guards keep this safe:

  * min_len   - words shorter than this are compared EXACTLY (so "csi", "vpo",
                "hp", "drs" can never fuzzy-match a place name);
  * max_len_diff - candidates whose length differs too much are rejected outright;
  * threshold - similarity must be high (default 88/100).

Backed by fuzzywuzzy when available (already a project dependency, used by the
org-unit matcher) and by the stdlib difflib otherwise, so the module works in any
environment.
"""

from difflib import SequenceMatcher

try:  # pragma: no cover - prefer the library already used by utils.org_unit_matching
    from fuzzywuzzy import fuzz

    def _ratio(a: str, b: str) -> int:
        return int(fuzz.ratio(a, b))
except Exception:  # pragma: no cover - stdlib fallback

    def _ratio(a: str, b: str) -> int:
        return int(round(SequenceMatcher(None, a, b).ratio() * 100))


DEFAULT_THRESHOLD = 88
DEFAULT_MIN_LEN = 5
DEFAULT_MAX_LEN_DIFF = 2


def similarity(a: str, b: str) -> int:
    """Similarity of two strings on a 0-100 scale."""
    return _ratio(a, b)


def match_token(
    token: str,
    vocabulary,
    threshold: int = DEFAULT_THRESHOLD,
    min_len: int = DEFAULT_MIN_LEN,
    max_len_diff: int = DEFAULT_MAX_LEN_DIFF,
):
    """
    Return the vocabulary entry best matching ``token``, or None.

    ``vocabulary`` may be any iterable of words or a dict (its keys are used).
    Exact matches always win. Words shorter than ``min_len`` - on either side -
    are only ever matched exactly, which prevents short codes from being
    confused with real place names.
    """
    if not token:
        return None
    words = list(vocabulary)
    if token in words:
        return token

    best, best_score = None, 0
    for word in words:
        if len(token) < min_len or len(word) < min_len:
            continue  # too short to fuzzy-match safely
        if abs(len(token) - len(word)) > max_len_diff:
            continue
        score = _ratio(token, word)
        if score >= threshold and score > best_score:
            best, best_score = word, score
    return best


def matches(
    token: str,
    vocabulary,
    threshold: int = DEFAULT_THRESHOLD,
    min_len: int = DEFAULT_MIN_LEN,
    max_len_diff: int = DEFAULT_MAX_LEN_DIFF,
) -> bool:
    """True if ``token`` matches any vocabulary entry (exactly or approximately)."""
    return match_token(token, vocabulary, threshold, min_len, max_len_diff) is not None


def any_token_matches(
    label: str,
    vocabulary,
    threshold: int = DEFAULT_THRESHOLD,
    min_len: int = DEFAULT_MIN_LEN,
    max_len_diff: int = DEFAULT_MAX_LEN_DIFF,
) -> bool:
    """True if ANY whitespace-separated token of ``label`` matches the vocabulary."""
    return any(
        matches(tok, vocabulary, threshold, min_len, max_len_diff)
        for tok in label.split()
    )


def find_token(
    label: str,
    vocabulary,
    threshold: int = DEFAULT_THRESHOLD,
    min_len: int = DEFAULT_MIN_LEN,
    max_len_diff: int = DEFAULT_MAX_LEN_DIFF,
):
    """Return the vocabulary entry matched by the first matching token of ``label``."""
    for tok in label.split():
        hit = match_token(tok, vocabulary, threshold, min_len, max_len_diff)
        if hit is not None:
            return hit
    return None
