import os

import numpy as np
import pandas as pd
import re
from fuzzywuzzy import fuzz, process
import unicodedata
from openhexa.sdk import current_run

from config import TEMP_PATH

# CSI fuzzy-match corrections (used by the CSI matcher below). Entries found via a
# systematic sweep of every distinct target CSI name across the real historical
# files against the clean tree: each is a case where the correct candidate scored
# higher on raw string similarity than the eventual (wrong) match, but lost once
# the length-penalty was applied because its official tree name is longer than
# the target file's shorthand for it.
csi_matching_failed = {
    "abalak fachi": "abalak fachi tabalack",
    "abalak urbain2": "abalak urbain 2 abalak",
    "agadez sabon gari agadez": "agadez sabongari",
    "aguie guidanmalambakabe": "aguie guidan malam bakabe",
    "aguie maiguizaouakagnou": "aguie maiguizaoua kagnou",
    "boboye birni i": "boboye birni ngaoure",
    "boboye birni ii": "boboye birni 2",
    "diffa chateau": "diffa chateau centre",
    "dosso bella1": "dosso bella i",
    "dosso bellaii": "dosso bella ii",
    "gotheye tchawa": "gotheye tchawa ferme insecurite",
    "guidan roumdji g roumdji": "guidan roumdji guidan roumdji 1",
    "illela zourare": "illela zourare chafa",
    "kollo lakabia": "kollo latakabia sonrai",
    "madaoua galma": "madaoua galma sedentaire",
    "madarounfa harounawa": "madarounfa harounaoua",
    "madarounfa madarounfa": "madarounfa madarounfa 1",
    "madarounfa madeini": "madarounfa madeini tadeta",
    "magaria adamawa": "magaria adamaoua",
    "magaria baoure": "magaria baoure sarkin gako",
    "malbaza laweygoge": "malbaza lawey goge",
    "maradi sabongari": "maradi sabongari maradi",
    "maradi zariai": "maradi zaria i",
    "maradi zariaii": "maradi zaria ii",
    "maradi zariaiii": "maradi zaria iii",
    "matameye danbarto": "matameye dan barto",
    "matameye matameye 1": "matameye matameye",
    "say ganki": "say ganki bassarou",
    "tchintabaraden darha": "tchintabaraden zigat darha",
    "zinder sabongarizinder": "zinder sabon gari",
    "zinder sabongari zinder": "zinder sabon gari",
}


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
        # clotur(e|ee)s? covers all 4 accent-stripped agreement forms of
        # "clôturé(e)(s)" (cloture/cloturee/clotures/cloturees) - plain "cloture"
        # alone never matched the real, always-feminine "clôturée" facility
        # marker, so it survived normalization as a literal token and inflated
        # similarity between two otherwise-unrelated closed facilities that
        # happened to share it. "commune" stays in this list - it's decorative
        # noise for every district except one (see below).
        noisy_words = (
            r"\b(csi|cs|ds|chr|hd|creni|crenam|clotur(e|ee)s?|departement|region|"
            r"ville|commune)\b"
        )

        text = text.lower()
        text = unicodedata.normalize("NFD", text)
        text = "".join([c for c in text if unicodedata.category(c) != "Mn"])

        # "DS Tahoua Commune" is a real district, distinct from "DS Tahoua" (each
        # with its own org units, confusingly including its own "CSI Sabon
        # Gari") - stripping "commune" as generic noise collapses the two into
        # the same normalized district, so a target row for the Commune could
        # win a match in the wrong one. Fusing it into one word BEFORE the
        # general strip below preserves just this one real distinction, without
        # leaving "commune" as a bare shared token that would instead falsely
        # attract UNRELATED "X Commune" districts to each other (confirmed: it
        # did, for several "DS Diffa Commune" rows, when tried as a plain kept
        # word rather than fused).
        text = re.sub(r"\btahoua\s+commune\b", "tahouacommune", text)

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


# =========================================================================== #
# CSI-level matching stage + post-matching org-unit cleanup (moved from        #
# pipeline.py - both are org-unit-matching concerns, this module's theme).     #
# District-level matching is geo_match.py's counterpart.                      #
# =========================================================================== #
def match_csi_to_org_unit_id(
    csi_level_target_df: pd.DataFrame, iaso_org_unit_tree_df_clean: pd.DataFrame
) -> pd.DataFrame:
    """Match CSI names to org-unit IDs via fuzzy matching + known corrections."""
    current_run.log_info(
        "Appariement des noms CSI aux identifiants des unités organisationnelles..."
    )
    try:
        target_df_matched, org_unit_tree_check = _fuzzy_match_csi(
            csi_level_target_df, iaso_org_unit_tree_df_clean
        )
        target_df_matched = _apply_manual_csi_corrections(
            target_df_matched, org_unit_tree_check
        )
        _report_unmatched_csi(target_df_matched)

        target_df_matched = target_df_matched.drop(
            columns=[
                "LVL_3_NAME_original",
                "LVL_6_NAME_original",
                "match_score",
                "cleansed_target",
                "cleansed_spatial_match",
            ]
        ).dropna(subset=["org_unit_id"])

        current_run.log_info("Appariement des noms CSI terminé.")
        return target_df_matched
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'appariement des noms CSI: {str(e)}")
        raise


def _fuzzy_match_csi(
    csi_level_target_df: pd.DataFrame, iaso_org_unit_tree_df_clean: pd.DataFrame
) -> tuple:
    """Run the fuzzy matcher and dump its working tables to TEMP_PATH for debugging."""
    iaso_org_unit_tree_for_matching = iaso_org_unit_tree_df_clean[
        ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
    ].drop_duplicates()

    target_df_matched, org_unit_tree_check = org_unit_matching(
        csi_level_target_df, iaso_org_unit_tree_for_matching, threshold=50
    )

    target_df_matched_check = target_df_matched[
        [
            "org_unit_id",
            "LVL_3_NAME_original",
            "LVL_6_NAME_original",
            "LVL_3_NAME",
            "LVL_6_NAME",
            "cleansed_target",
            "cleansed_spatial_match",
            "match_score",
        ]
    ].drop_duplicates()

    if not os.path.exists(TEMP_PATH):
        os.makedirs(TEMP_PATH)
    target_df_matched_check.to_csv(
        os.path.join(TEMP_PATH, "target_df_matched_check.csv"), index=False
    )
    org_unit_tree_check = org_unit_tree_check.drop_duplicates()
    org_unit_tree_check.to_csv(
        os.path.join(TEMP_PATH, "org_unit_tree_check.csv"), index=False
    )
    return target_df_matched, org_unit_tree_check


def _apply_manual_csi_corrections(
    target_df_matched: pd.DataFrame, org_unit_tree_check: pd.DataFrame
) -> pd.DataFrame:
    """Apply csi_matching_failed's hand-curated overrides on top of the fuzzy
    match, then fall back LVL_6_NAME to its original (unmatched) spelling wherever no
    org_unit_id was ultimately resolved."""
    cols = ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
    for csi_concat_original, csi_concat_correct in csi_matching_failed.items():
        mask = target_df_matched["cleansed_target"] == csi_concat_original

        if csi_concat_correct is None:
            target_df_matched.loc[mask, cols] = None
            continue

        org_unit_tree_row = org_unit_tree_check.loc[
            org_unit_tree_check["cleansed_spatial"] == csi_concat_correct
        ]
        if org_unit_tree_row.empty:
            # This correction was written because the automated match for
            # csi_concat_original was known to be wrong; its intended target
            # no longer exists in the current IASO tree (renamed/removed
            # since). Fall back to unmatched rather than silently keeping
            # that already-distrusted automated match in place.
            current_run.log_warning(
                f"CORRECTION CSI OBSOLÈTE: la correction manuelle prévue pour "
                f"'{csi_concat_original}' pointe vers '{csi_concat_correct}', "
                "qui n'existe plus dans l'arbre IASO actuel."
            )
            current_run.log_warning(
                f"CORRECTION CSI OBSOLÈTE: '{csi_concat_original}' est donc "
                "traité comme non apparié plutôt que de conserver "
                "l'appariement automatique initial, déjà connu comme "
                "incorrect."
            )
            target_df_matched.loc[mask, cols] = None
            continue

        target_df_matched.loc[mask, cols] = org_unit_tree_row[cols].values[0]

    target_df_matched["LVL_6_NAME"] = np.where(
        target_df_matched["org_unit_id"].isna(),
        target_df_matched["LVL_6_NAME_original"],
        target_df_matched["LVL_6_NAME"],
    )
    return target_df_matched


def _report_unmatched_csi(target_df_matched: pd.DataFrame) -> None:
    """Warn about every CSI that still has no org_unit_id, with the target value
    (cible) that won't appear in the dashboard as a result."""
    unmatched_count = int(target_df_matched["org_unit_id"].isna().sum())
    total_count = len(target_df_matched)
    if unmatched_count == 0:
        return

    unmatched_rows = target_df_matched[target_df_matched["org_unit_id"].isna()]
    total_cible_lost = int(unmatched_rows["cible"].sum())
    per_csi = (
        unmatched_rows.groupby(
            ["LVL_3_NAME_original", "LVL_6_NAME_original"], dropna=False
        )["cible"]
        .sum()
        .reset_index()
    )
    current_run.log_warning(
        f"CSI NON APPARIÉS: {unmatched_count} sur {total_count} entrées "
        "n'ont pu être associées à aucune unité d'organisation de l'arbre "
        f"IASO nettoyé, représentant une cible cumulée de "
        f"{total_cible_lost} qui n'apparaîtra pas dans le tableau de bord."
    )
    # One log call per CSI: OpenHexa collapses newlines within a message.
    for district, csi, cible_sum in per_csi.itertuples(index=False):
        current_run.log_warning(
            f"CSI NON APPARIÉ: '{csi}' (district '{district}') n'est pas "
            "enregistré comme une unité d'organisation valide dans IASO; "
            f"sa cible ({int(cible_sum)}, tous âges/rounds/produits "
            "confondus pour cette entrée) ne sera pas remontée dans le "
            "tableau de bord."
        )
    current_run.log_warning(
        "À FAIRE: corrigez l'orthographe de ces CSI dans le fichier source, "
        "ou faites-les créer dans IASO s'il s'agit de formations sanitaires "
        "réellement manquantes."
    )


def add_region_names(
    target_df: pd.DataFrame, iaso_org_unit_tree_clean_df: pd.DataFrame
) -> pd.DataFrame:
    """Add LVL_2_NAME (region) by merging on org_unit_id."""
    current_run.log_info("Ajout des noms de région aux données de cibles...")
    try:
        regions_df = iaso_org_unit_tree_clean_df[
            ["org_unit_id", "LVL_2_NAME"]
        ].drop_duplicates()
        target_with_regions_df = target_df.merge(
            regions_df, on="org_unit_id", how="left"
        )
        current_run.log_info("Ajout des noms de région terminé.")
        return target_with_regions_df
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'ajout des noms de région: {e}")
        raise


def clean_org_unit_id(
    target_data_combined: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """Remap org_unit_id to the final LVL_6_UID-based id (one-to-many cleanup)."""
    current_run.log_info(
        "Récupération des identifiants des unités d'organisation (correspondance un-à-plusieurs)..."
    )
    try:
        uid_to_org_id_df_clean = iaso_org_unit_tree_clean_df[
            ["LVL_6_UID", "org_unit_id"]
        ].drop_duplicates()
        uid_to_org_id_df_raw = iaso_org_unit_tree_raw_df.copy()
        uid_to_org_id_df_raw["LVL_6_UID"] = uid_to_org_id_df_raw.groupby("LVL_6_NAME")[
            "LVL_6_UID"
        ].transform("first")
        uid_to_org_id_df_raw = (
            uid_to_org_id_df_raw[["LVL_6_UID", "org_unit_id"]]
            .drop_duplicates()
            .rename(columns={"org_unit_id": "final_org_unit_id"})
        )
        mapping_df = uid_to_org_id_df_clean.merge(
            uid_to_org_id_df_raw, on="LVL_6_UID", how="inner"
        )[["org_unit_id", "final_org_unit_id"]].drop_duplicates()

        target_data_combined = pd.merge(
            target_data_combined, mapping_df, on="org_unit_id", how="left"
        )
        target_data_combined["org_unit_id"] = target_data_combined[
            "final_org_unit_id"
        ].fillna(target_data_combined["org_unit_id"])
        target_data_combined.drop(columns=["final_org_unit_id"], inplace=True)

        current_run.log_info(
            "Récupération des identifiants des unités d'organisation terminée."
        )
        return target_data_combined
    except Exception as e:
        current_run.log_error(
            f"Erreur lors du processus de récupération des identifiants: {e}"
        )
        raise
