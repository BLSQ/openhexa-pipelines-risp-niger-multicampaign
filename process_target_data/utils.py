import pandas as pd
import re
from fuzzywuzzy import fuzz, process
import unicodedata


def normalize_string(text):
    """
    Normalizes a string:
    - Lowercase & Accent removal
    - Removes suffixes even if glued to text (e.g., 'CSITagadofet' -> 'tagadofet')
    - Removes special characters
    - Collapses internal spaces
    """
    if not isinstance(text, str):
        return ""

    noisy_words = r"\b(csi|cs|ds|chr|hd|creni|crenam|cloture|departement|region|ville)"

    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join([c for c in text if unicodedata.category(c) != "Mn"])
    text = re.sub(noisy_words, "", text, flags=re.IGNORECASE)
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    return " ".join(text.split()).strip()


def org_unit_matching(
    target_df: pd.DataFrame, spatial_unit_df: pd.DataFrame, threshold: int = 90
) -> pd.DataFrame:
    """
    Matches organization unit levels (LVL 3 (ds), LVL 6 (csi)) between two DataFrames using fuzzy string matching.

    Args:
        target_df (pd.DataFrame): DataFrame containing the target data for each combination of LVL 2, LVL 3, and LVL 6 names.
        spatial_unit_df (pd.DataFrame): DataFrame containing the org unit IDs for each combination of LVL 2, LVL 3, LVL 4, LVL 5 and LVL 6 names.

    Returns:
        pd.DataFrame: target_df with matched organization units and their corresponding org unit IDs
    """
    # 1. Preparation
    rename_map = {
        "LVL_3_NAME": "LVL_3_NAME_original",
        "LVL_6_NAME": "LVL_6_NAME_original",
    }

    target_df = target_df.rename(columns=rename_map)
    target = target_df[["LVL_3_NAME_original", "LVL_6_NAME_original"]].copy()
    target = target.drop_duplicates()
    spatial = spatial_unit_df.copy()

    # 2. Create Cleansed Concatenations
    target["cleansed_target"] = target.apply(
        lambda r: f"{normalize_string(str(r['LVL_3_NAME_original']))} {normalize_string(str(r['LVL_6_NAME_original']))}",
        axis=1,
    )
    spatial["cleansed_spatial"] = spatial.apply(
        lambda r: f"{normalize_string(str(r['LVL_3_NAME']))} {normalize_string(str(r['LVL_6_NAME']))}",
        axis=1,
    )

    # 3. Collect ALL Potential Match Candidates
    all_potential_candidates = []
    spatial_list = spatial["cleansed_spatial"].tolist()
    spatial_indices = spatial.index.tolist()

    for idx_t, query in target["cleansed_target"].items():
        if not query or not query.strip():
            continue

        if query in spatial_list:
            exact_indices = [i for i, x in enumerate(spatial_list) if x == query]
            for list_idx in exact_indices:
                idx_s = spatial_indices[list_idx]
                all_potential_candidates.append(
                    {"target_idx": idx_t, "spatial_idx": idx_s, "score": 101}
                )
            continue

        matches = process.extract(
            query,
            spatial_list,
            scorer=lambda s1, s2: (fuzz.token_set_ratio(s1, s2) * 0.7)
            + (fuzz.ratio(s1, s2) * 0.3),
            limit=5,
        )

        for match in matches:
            matched_str = match[0]
            score = match[1]
            if len(match) > 2:
                list_idx = match[2]
            else:
                list_idx = spatial_list.index(matched_str)

            if score >= threshold:
                idx_s = spatial_indices[list_idx]
                len_penalty = 1 - (
                    abs(len(query) - len(matched_str))
                    / max(len(query), len(matched_str))
                )
                adjusted_score = score * len_penalty

                all_potential_candidates.append(
                    {"target_idx": idx_t, "spatial_idx": idx_s, "score": adjusted_score}
                )

    # 4. Global Greedy Matching Logic
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

    # 5. Finalize Results
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
            f"Row count mismatch after merging back to target_df: {count_initial} vs {count_final}"
        )

    return final_df.drop(columns=["match_index"]), spatial
