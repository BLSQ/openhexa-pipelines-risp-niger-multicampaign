import pandas as pd
import re
from fuzzywuzzy import fuzz, process
import unicodedata
from openhexa.sdk import current_run


def normalize_string(text: str) -> str:
    """
    Normalizes a string:
        - Lowercase & Accent removal
        - Removes suffixes even if glued to text (e.g., 'CSITagadofet' -> 'tagadofet')
        - Removes special characters
        - Collapses internal spaces

    Args:
        text (str): The string to normalize.

    Returns:
        str: The normalized string.
    """
    try:
        if not isinstance(text, str):
            return ""

        # \b on both ends: without the trailing boundary this would also strip
        # these as PREFIXES of unrelated words (e.g. "ds" inside a longer token).
        noisy_words = (
            r"\b(csi|cs|ds|chr|hd|creni|crenam|cloture|departement|region|ville|"
            r"commune)\b"
        )

        text = text.lower()
        text = unicodedata.normalize("NFD", text)
        text = "".join([c for c in text if unicodedata.category(c) != "Mn"])
        text = re.sub(noisy_words, "", text, flags=re.IGNORECASE)
        text = re.sub(r"[^a-z0-9\s]", " ", text)

        return " ".join(text.split()).strip()
    except Exception as e:
        msg = f"Erreur lors de la normalisation de la chaîne '{text}': {str(e)}"
        current_run.log_error(msg)
        raise


def _prepare_target_and_spatial(
    target_df: pd.DataFrame, spatial_unit_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Rename target's LVL_3/LVL_6 to *_original (so they survive the later merge
    back onto target_df unambiguously), then build the normalized LVL_3+LVL_6
    concatenation each side will be fuzzy-matched on."""
    rename_map = {
        "LVL_3_NAME": "LVL_3_NAME_original",
        "LVL_6_NAME": "LVL_6_NAME_original",
    }
    target_df = target_df.rename(columns=rename_map)
    target = target_df[["LVL_3_NAME_original", "LVL_6_NAME_original"]].copy()
    target = target.drop_duplicates()
    spatial = spatial_unit_df.copy()

    target["cleansed_target"] = target.apply(
        lambda r: f"{normalize_string(str(r['LVL_3_NAME_original']))} {normalize_string(str(r['LVL_6_NAME_original']))}",
        axis=1,
    )
    spatial["cleansed_spatial"] = spatial.apply(
        lambda r: f"{normalize_string(str(r['LVL_3_NAME']))} {normalize_string(str(r['LVL_6_NAME']))}",
        axis=1,
    )
    return target_df, target, spatial


def _exact_candidates(
    idx_t, query: str, spatial_list: list, spatial_indices: list
) -> list:
    """Every spatial row whose cleansed string exactly equals the query - always
    the winning candidate in the greedy pass below (score 101, above any fuzzy
    score's max of 100)."""
    exact_indices = [i for i, x in enumerate(spatial_list) if x == query]
    return [
        {"target_idx": idx_t, "spatial_idx": spatial_indices[list_idx], "score": 101}
        for list_idx in exact_indices
    ]


def _fuzzy_candidates(
    idx_t, query: str, spatial_list: list, spatial_indices: list, threshold: int
) -> list:
    """Every fuzzy candidate for ``query`` whose length-adjusted score clears
    ``threshold``."""
    # limit=20 (not 5): process.extract ranks by the RAW score, before the
    # length penalty below is applied. A correct candidate can score high
    # on content but rank outside a too-small top-N once a stray extra
    # word elsewhere in the query lengthens it - a wider net makes it
    # more likely that candidate is still there to be re-scored.
    matches = process.extract(
        query,
        spatial_list,
        scorer=lambda s1, s2: (fuzz.token_set_ratio(s1, s2) * 0.7)
        + (fuzz.ratio(s1, s2) * 0.3),
        limit=20,
    )

    candidates = []
    for match in matches:
        matched_str = match[0]
        score = match[1]
        if len(match) > 2:
            list_idx = match[2]
        else:
            list_idx = spatial_list.index(matched_str)

        len_penalty = 1 - (
            abs(len(query) - len(matched_str)) / max(len(query), len(matched_str))
        )
        adjusted_score = score * len_penalty

        # Gate on the length-adjusted score, not the raw one: otherwise a
        # candidate that only clears the bar before its length penalty is
        # applied gets accepted anyway, once it happens to be the best
        # (or only) candidate left standing for its target row.
        if adjusted_score >= threshold:
            idx_s = spatial_indices[list_idx]
            candidates.append(
                {"target_idx": idx_t, "spatial_idx": idx_s, "score": adjusted_score}
            )
    return candidates


def _collect_match_candidates(
    target: pd.DataFrame, spatial: pd.DataFrame, threshold: int
) -> list:
    """For every non-empty target query, collect every spatial row that could be
    its match: an exact cleansed-string match, or every fuzzy candidate whose
    length-adjusted score clears ``threshold``."""
    all_potential_candidates = []
    spatial_list = spatial["cleansed_spatial"].tolist()
    spatial_indices = spatial.index.tolist()

    for idx_t, query in target["cleansed_target"].items():
        if not query or not query.strip():
            continue

        if query in spatial_list:
            all_potential_candidates.extend(
                _exact_candidates(idx_t, query, spatial_list, spatial_indices)
            )
            continue

        all_potential_candidates.extend(
            _fuzzy_candidates(idx_t, query, spatial_list, spatial_indices, threshold)
        )
    return all_potential_candidates


def _greedy_assign(all_potential_candidates: list) -> dict:
    """Highest-score candidates first, each target and each spatial row used at
    most once - a global greedy 1-to-1 assignment rather than each target row
    independently picking its own best (possibly already-claimed) match."""
    all_potential_candidates.sort(key=lambda x: x["score"], reverse=True)
    assigned_target_indices = set()
    assigned_spatial_indices = set()
    final_assignment = {}

    for match in all_potential_candidates:
        t_idx = match["target_idx"]
        s_idx = match["spatial_idx"]

        if (
            t_idx not in assigned_target_indices
            and s_idx not in assigned_spatial_indices
        ):
            final_assignment[t_idx] = (s_idx, match["score"])
            assigned_target_indices.add(t_idx)
            assigned_spatial_indices.add(s_idx)

    return final_assignment


def _finalize_matches(
    target_df: pd.DataFrame,
    target: pd.DataFrame,
    spatial: pd.DataFrame,
    final_assignment: dict,
) -> pd.DataFrame:
    """Pull the matched org unit's columns onto each target row, then merge back
    onto the original (non-deduplicated) target_df. Fails loudly if that merge
    doesn't preserve the row count - it must, since it joins on the same columns
    target was deduplicated from."""
    target["match_index"] = target.index.map(
        lambda x: final_assignment[x][0] if x in final_assignment else None
    )
    target["match_score"] = target.index.map(
        lambda x: final_assignment[x][1] if x in final_assignment else 0
    )

    cols_to_pull = [
        "org_unit_id",
        "LVL_3_NAME",
        "LVL_6_NAME",
        "cleansed_spatial",
    ]

    final_df = target.merge(
        spatial[cols_to_pull],
        left_on="match_index",
        right_index=True,
        how="left",
        suffixes=("", "_matched"),
    )

    final_df = final_df.rename(columns={"cleansed_spatial": "cleansed_spatial_match"})
    final_df = target_df.merge(
        final_df, on=["LVL_3_NAME_original", "LVL_6_NAME_original"], how="left"
    )

    # Make sure that all entries merged back to target_df
    count_initial = target_df.shape[0]
    count_final = final_df.shape[0]
    if count_initial != count_final:
        raise ValueError(
            f"Erreur lors de la fusion des données: le nombre d'entrées initial ({count_initial}) ne correspond pas au nombre d'entrées final ({count_final})"
        )
    return final_df.drop(columns=["match_index"])


def org_unit_matching(
    target_df: pd.DataFrame, spatial_unit_df: pd.DataFrame, threshold: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Matches organization unit levels (LVL 3 (ds), LVL 6 (csi)) between two DataFrames using fuzzy string matching.

    Args:
        target_df (pd.DataFrame): DataFrame containing the target data for each combination of LVL 2, LVL 3, and LVL 6 names.
        spatial_unit_df (pd.DataFrame): DataFrame containing the org unit IDs for each combination of LVL 2, LVL 3, LVL 4, LVL 5 and LVL 6 names.
        threshold (int): The minimum fuzzy matching score required to consider a match valid.

    Returns:
        final_df (pd.DataFrame): DataFrame containing the original target data along with matched org unit IDs and names from the spatial unit DataFrame.
        spatial (pd.DataFrame): The original spatial unit DataFrame, returned for reference.
    """
    current_run.log_info(
        "Début du processus de matching des unités organisationnelles."
    )
    try:
        target_df, target, spatial = _prepare_target_and_spatial(
            target_df, spatial_unit_df
        )
        candidates = _collect_match_candidates(target, spatial, threshold)
        final_assignment = _greedy_assign(candidates)
        final_df = _finalize_matches(target_df, target, spatial, final_assignment)
    except ValueError:
        raise
    except Exception as e:
        msg = f"Erreur lors du matching des unités organisationnelles: {str(e)}"
        current_run.log_error(msg)
        raise

    return final_df, spatial
