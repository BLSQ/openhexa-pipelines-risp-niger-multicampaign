"""
Coverage theme: melting/categorizing raw IASO coverage columns into
ner_vaccination_couverture, and attaching District/CSI target data on top of it
for ner_vaccination_couverture_csi_district_cibled.
"""

import pandas as pd
from openhexa.sdk import current_run

from data_cleaning import (
    align_categories_for_merge,
    drop_duplicates_low_memory,
    drop_zero_values,
    EXPECTED_STRUCTURE_CATEGORY_COLS,
    melt_campaign_columns,
    new_cols,
)

# coverage table
cvrg_polio_cols = (
    [
        "vitamine_a_12_24_mois_site_ordinaire",
        "vitamine_a_12_24_mois_site_speciaux",
        "vitamine_a_12_24_mois_site_speciaux_autre",
        "vitamine_a_12_24_mois_site_speciaux_deplace_int",
        "vitamine_a_12_24_mois_site_speciaux_gares",
        "vitamine_a_12_24_mois_site_speciaux_marche",
        "vitamine_a_12_24_mois_site_speciaux_nomad",
        "vitamine_a_12_24_mois_site_speciaux_point_eau",
        "vitamine_a_12_24_mois_site_speciaux_postefron",
        "vitamine_a_12_24_mois_site_speciaux_refugie",
        "vitamine_a_12_24_mois_site_trans_front_cote_front",
        "vitamine_a_12_24_mois_site_trans_front_cote_niger",
        "vitamine_a_6_11_mois_site_ordinaire",
        "vitamine_a_6_11_mois_site_speciaux",
        "vitamine_a_6_11_mois_site_speciaux_autre",
        "vitamine_a_6_11_mois_site_speciaux_deplace_int",
        "vitamine_a_6_11_mois_site_speciaux_gares",
        "vitamine_a_6_11_mois_site_speciaux_marche",
        "vitamine_a_6_11_mois_site_speciaux_nomad",
        "vitamine_a_6_11_mois_site_speciaux_point_eau",
        "vitamine_a_6_11_mois_site_speciaux_postefron",
        "vitamine_a_6_11_mois_site_speciaux_refugie",
        "vitamine_a_6_11_mois_site_trans_front_cote_front",
        "vitamine_a_6_11_mois_site_trans_front_cote_niger",
    ]
    + [
        "zero_dose_vpo_0_11_mois_fois_site_ordinaire",
        "zero_dose_vpo_0_11_mois_fois_site_speciaux",
        "zero_dose_vpo_0_11_mois_fois_site_speciaux_postefron",
        "zero_dose_vpo_0_11_mois_fois_site_trans_front_cote_front",
        "zero_dose_vpo_0_11_mois_fois_site_trans_front_cote_niger",
        "zero_dose_vpo_12_59_mois_fois_site_ordinaire",
        "zero_dose_vpo_12_59_mois_fois_site_speciaux",
        "zero_dose_vpo_12_59_mois_fois_site_speciaux_postefron",
        "zero_dose_vpo_12_59_mois_fois_site_trans_front_cote_front",
        "zero_dose_vpo_12_59_mois_fois_site_trans_front_cote_niger",
    ]
    + [
        "deja_recu_vpo_0_11_mois_site_ordinaire",
        "deja_recu_vpo_0_11_mois_site_speciaux",
        "deja_recu_vpo_0_11_mois_site_speciaux_postefron",
        "deja_recu_vpo_0_11_mois_site_trans_front_cote_front",
        "deja_recu_vpo_0_11_mois_site_trans_front_cote_niger",
        "deja_recu_vpo_12_59_mois_site_ordinaire",
        "deja_recu_vpo_12_59_mois_site_speciaux",
        "deja_recu_vpo_12_59_mois_site_speciaux_postefron",
        "deja_recu_vpo_12_59_mois_site_trans_front_cote_front",
        "deja_recu_vpo_12_59_mois_site_trans_front_cote_niger",
    ]
    + [
        "depara_12_23_site_ordinaire",
        "depara_24_59_site_ordinaire",
        "depara_24_59_site_speciaux",
        "depara_12_23_site_speciaux_gares",
        "depara_24_59_site_speciaux_gares",
        "depara_12_23_site_speciaux_marche",
        "depara_24_59_site_speciaux_marche",
        "depara_12_23_site_speciaux_point_eau",
        "depara_24_59_site_speciaux_point_eau",
        "depara_12_23_site_speciaux_nomad",
        "depara_24_59_site_speciaux_nomad",
        "depara_12_23_site_speciaux_deplace_int",
        "depara_24_59_site_speciaux_deplace_int",
        "depara_12_23_site_speciaux_refugie",
        "depara_24_59_site_speciaux_refugie",
        "depara_12_23_site_speciaux_autre",
        "depara_24_59_site_speciaux_autre",
        "depara_12_23_site_trans_front_cote_niger",
        "depara_24_59_site_trans_front_cote_niger",
        "depara_12_23_site_trans_front_cote_front",
        "depara_24_59_site_trans_front_cote_front",
        "depara_12_23_site_speciaux_postefron",
        "depara_24_59_site_speciaux_postefron",
    ]
)

cvrg_jnm_cols = [
    "vitamine_a_12_24_mois_site_ordinaire",
    "vitamine_a_12_24_mois_site_speciaux",
    "vitamine_a_12_24_mois_site_speciaux_autre",
    "vitamine_a_12_24_mois_site_speciaux_deplace_int",
    "vitamine_a_12_24_mois_site_speciaux_gares",
    "vitamine_a_12_24_mois_site_speciaux_marche",
    "vitamine_a_12_24_mois_site_speciaux_nomad",
    "vitamine_a_12_24_mois_site_speciaux_point_eau",
    "vitamine_a_12_24_mois_site_speciaux_postefron",
    "vitamine_a_12_24_mois_site_speciaux_refugie",
    "vitamine_a_12_24_mois_site_trans_front_cote_front",
    "vitamine_a_12_24_mois_site_trans_front_cote_niger",
    "vitamine_a_6_11_mois_site_ordinaire",
    "vitamine_a_6_11_mois_site_speciaux",
    "vitamine_a_6_11_mois_site_speciaux_autre",
    "vitamine_a_6_11_mois_site_speciaux_deplace_int",
    "vitamine_a_6_11_mois_site_speciaux_gares",
    "vitamine_a_6_11_mois_site_speciaux_marche",
    "vitamine_a_6_11_mois_site_speciaux_nomad",
    "vitamine_a_6_11_mois_site_speciaux_point_eau",
    "vitamine_a_6_11_mois_site_speciaux_postefron",
    "vitamine_a_6_11_mois_site_speciaux_refugie",
    "vitamine_a_6_11_mois_site_trans_front_cote_front",
    "vitamine_a_6_11_mois_site_trans_front_cote_niger",
] + [
    "depara_12_23_site_ordinaire",
    "depara_24_59_site_ordinaire",
    "depara_24_59_site_speciaux",
    "depara_12_23_site_speciaux_gares",
    "depara_24_59_site_speciaux_gares",
    "depara_12_23_site_speciaux_marche",
    "depara_24_59_site_speciaux_marche",
    "depara_12_23_site_speciaux_point_eau",
    "depara_24_59_site_speciaux_point_eau",
    "depara_12_23_site_speciaux_nomad",
    "depara_24_59_site_speciaux_nomad",
    "depara_12_23_site_speciaux_deplace_int",
    "depara_24_59_site_speciaux_deplace_int",
    "depara_12_23_site_speciaux_refugie",
    "depara_24_59_site_speciaux_refugie",
    "depara_12_23_site_speciaux_autre",
    "depara_24_59_site_speciaux_autre",
    "depara_12_23_site_trans_front_cote_niger",
    "depara_24_59_site_trans_front_cote_niger",
    "depara_12_23_site_trans_front_cote_front",
    "depara_24_59_site_trans_front_cote_front",
    "depara_12_23_site_speciaux_postefron",
    "depara_24_59_site_speciaux_postefron",
]


cvrg_fjaune_cols = (
    [
        "nbre_enfant_vaccine_9_11_mois__fievre_jaune",
        "nbre_enfant_vaccine_12_23_mois_fievre_jaune",
        "nbre_enfant_vaccine_24_59_mois_fievre_jaune",
        "nbre_enfant_vaccine_5_14_ans_fievre_jaune",
        "nbre_enfant_vaccine_15_60_ans_fievre_jaune",
    ]
    + [
        "nbre_enfant_vaccine_9_11_mois__fievre_jaune_site_depla_int",
        "nbre_enfant_vaccine_24_59_mois__fievre_jaune_site_depla_int",
        "nbre_enfant_vaccine_5_14_ans__fievre_jaune_site_depla_int",
        "nbre_enfant_vaccine_15_60_ans__fievre_jaune_site_depla_int",
    ]
    + [
        "nbre_enfant_vaccine_9_11_mois__fievre_jaune_site_speciaux_refugie",
        "nbre_enfant_vaccine_24_59_mois__fievre_jaune_site_speciaux_refugie",
        "nbre_enfant_vaccine_5_14_ans__fievre_jaune_site_speciaux_refugie",
        "nbre_enfant_vaccine_15_60_ans__fievre_jaune_site_speciaux_refugie",
    ]
)

cvrg_rougeole_cols = (
    [
        "nombre_zero_dose_6_fixe",
        "nombre_vacine_6_fixe",
        "nombre_dose_9_fixe",
        "nombre_vacine_9_fixe",
        "nombre_dose_12_fixe",
        "nombre_vacine_12_fixe",
    ]
    + [
        "nombre_zero_dose_6_avance",
        "nombre_vacine_6_avance",
        "nombre_dose_9_avance",
        "nombre_vacine_9_avance",
        "nombre_dose_12_avance",
        "nombre_vacine_12_avance",
    ]
    + [
        "nombre_zero_dose_6_mobile",
        "nombre_vacine_6_mobile",
        "nombre_dose_9_mobile",
        "nombre_vacine_9_mobile",
        "nombre_dose_12_mobile",
        "nombre_vacine_12_mobile",
    ]
)

cvrg_meningite_cols = [
    "men5_men5_1_4_ans",
    "men5_men5_5_14_ans",
    "men5_men5_15_19_ans",
]

cvrg_tcv_cols = [
    "men5_tcv_1_4_ans",
    "men5_tcv_5_14_ans",
    "men5_tcv_15_19_ans",
]

cvrg_campaign_map = {
    "polio": cvrg_polio_cols,
    "jnm": cvrg_jnm_cols,
    "fièvre jaune": cvrg_fjaune_cols,
    "rougeole": cvrg_rougeole_cols,
    "méningite": cvrg_meningite_cols,
    "tcv": cvrg_tcv_cols,
}

cvrg_yellow_fever_age_adjustment = {
    "12-23 mois": "12-59 mois",
    "24-59 mois": "12-59 mois",
    "0-11 mois": "9-11 mois",
}

cvrg_rougeole_age_adjustment = {
    "6-9 mois": "6-11 mois",
    "9-11 mois": "6-11 mois",
}

cvrg_group_by_cols = [
    "year",
    "round",
    "period",
    "age",
    "sexe",
    "org_unit_id",
    "produit",
    "vaccination_status",
    "site",
]

cvrg_district_level_target_keys = [
    "year",
    "round",
    "age",
    "produit",
    "LVL_3_NAME",
]

cvrg_district_level_group_keys = cvrg_district_level_target_keys + [
    "period",
    "order_day",
    "sexe",
    "vaccination_status",
    "site",
]

cvrg_district_level_final_keys = cvrg_district_level_group_keys + [
    "org_unit_id",
    "value",
    "cible",
]

cvrg_district_level_cumsum_keys = list(
    (set(cvrg_district_level_group_keys) | {"org_unit_id"}) - {"period"} - {"order_day"}
)

cvrg_csi_level_target_keys = list(
    (set(cvrg_district_level_target_keys) | {"org_unit_id"}) - {"LVL_3_NAME"}
)

cvrg_csi_level_final_keys = cvrg_district_level_final_keys + ["LVL_6_NAME"]

cvrg_csi_level_cumsum_keys = cvrg_district_level_cumsum_keys + ["LVL_6_NAME"]

# config for the categorizers below
ages_mapping = {
    "0_11_mois": "0-11 mois",
    "12_59_mois": "12-59 mois",
    "6_11_mois": "6-11 mois",
    "12_24_mois": "12-24 mois",
    "12_23": "12-23 mois",
    "24_59": "24-59 mois",
    "e_6_": "6-9 mois",
    "e_9_": "9-11 mois",
    "e_12_": "12-59 mois",
    "9_11_mois": "9-11 mois",
    "5_14_ans": "5-14 ans",
    "15_60_ans": "15-60 ans",
    "1_4_ans": "1-4 ans",
    "15_19_ans": "15-19 ans",
}

sites_mapping = {
    "site_ordinaire": "ordinaire",
    "site_speciaux_nomad": "spécial",
    "site_speciaux_gares": "spécial",
    "site_speciaux_marche": "spécial",
    "site_speciaux_point_eau": "spécial",
    "site_speciaux_postefron": "frontalier",
    "site_trans_front_cote_front": "transfrontalier : étranger",
    "site_trans_front_cote_niger": "transfrontalier : Niger",
    "site_speciaux_deplace_int": "spécial",
    "site_speciaux_refugie": "spécial",
    "site_speciaux": "spécial",
    "site_speciaux_autre": "spécial",
    "fixe": "fixe",
    "avance": "avancé",
    "mobile": "mobile",
}

products_mapping = {
    "vitamine_a": "vitamine A",
    "vit_a": "vitamine A",
    "vpo": "vaccin polio",
    "polio": "vaccin polio",
    "albendazole": "albendazole",
    "depara": "albendazole",
    "nombre_": "rougeole",  # beware
    "fievre_jaune": "fièvre jaune",
    "men5_men5": "méningite",
    "men5_tcv": "tcv",
}

status_mapping = {"zero_dose": "zéro dose", "deja_recu": "déjà reçu"}


def age_categorizer(string):
    """
    Categorizes age groups based on the input string.

    Parameters:
        string (str): The input string containing age information.

    Returns:
        str: The categorized age group, or "N/A" if no specific age group is covered by the form configuration.
    """
    for key in ages_mapping:
        if key in string:
            return ages_mapping[key]

    return "N/A"


def site_categorizer(string: str) -> str:
    """
    Categorizes site types based on the input string.

    Parameters:
        string (str): The input string containing site information.

    Returns:
        str: The categorized site type, or "ordinaire" if no specific type is covered by the form configuration.
    """
    for key in sites_mapping:
        if key in string:
            return sites_mapping[key]

    return "ordinaire"


def produit_categorizer(string: str) -> str:
    """
    Categorizes product types based on the input string.

    Parameters:
        string (str): The input string containing product information.

    Returns:
        str: The categorized product type, or "N/A" if no specific type is covered by the form configuration.
    """
    for key in products_mapping:
        if key in string:
            return products_mapping[key]

    return "N/A"


def vaccination_status_categorizer(string: str) -> str:
    """
    Categorizes vaccination status based on the input string.

    Parameters:
        string (str): The input string containing vaccination information.

    Returns:
        str: The categorized vaccination status, or "zéro dose" if no specific status is covered by the form configuration.
    """
    for key in status_mapping:
        if key in string:
            return status_mapping[key]

    return "zéro dose"


def _warn_missing_targets(
    merged: pd.DataFrame, cvrg_subset: pd.DataFrame, level_label: str
) -> None:
    no_target = merged[merged["cible"].isna()]
    if no_target.empty:
        return
    prop = len(no_target) / len(cvrg_subset)
    warn_msg = f"{len(no_target)} entrée(s) ({prop:.2%}) au niveau {level_label} n'ont pas de cible."
    if level_label == "CSI" and "LVL_6_NAME" in no_target.columns:
        affected = no_target["org_unit_id"].unique().tolist()
        warn_msg += f" CSI affectés: {', '.join(map(str, affected))}"
    current_run.log_warning(warn_msg + " Valeur cible: NaN.")


def _compute_cumulative_value(merged: pd.DataFrame, cumsum_keys: list) -> pd.DataFrame:
    """Fill missing daily values with 0, cumulate them per cumsum_keys, and
    standardize the target column as a nullable integer."""
    merged["value"] = merged["value"].fillna(0)
    merged["value_cum"] = merged.groupby(cumsum_keys, observed=True)["value"].transform(
        "cumsum"
    )
    merged["cible"] = (
        pd.to_numeric(merged["cible"], errors="coerce").round(0).astype("Int64")
    )
    return merged


def _attach_level_names(
    df: pd.DataFrame, name_cols: list, org_unit_tree_df: pd.DataFrame
) -> pd.DataFrame:
    """Attach canonical name columns from the org-unit tree onto `df`, keyed on
    org_unit_id - the reliable, always-present key - rather than trusting
    whatever a later merge happens to carry over. Rows whose org_unit_id doesn't
    resolve in the tree are dropped."""
    df = df.drop(columns=[c for c in name_cols if c in df.columns])
    level_names = org_unit_tree_df[["org_unit_id"] + name_cols].drop_duplicates()
    df = df.merge(level_names, on="org_unit_id", how="left")
    return df.dropna(subset=name_cols)


def _drop_name_cols(df: pd.DataFrame, name_cols: list) -> pd.DataFrame:
    return df.drop(columns=[c for c in name_cols if c in df.columns])


def _attach_csi_names_pre_merge(
    cvrg_subset: pd.DataFrame,
    target_subset: pd.DataFrame,
    org_unit_tree_df: pd.DataFrame,
) -> tuple:
    """CSI join key is org_unit_id (not LVL_3_NAME/LVL_6_NAME), so it's safe - and
    cheaper - to attach the canonical names to the coverage side up front, before
    the merge: every row then has a name whether or not a target matches. Left as
    a post-merge patch-up, a CSI with coverage but no target would get NaN names
    from the merge (target_subset is the only side that carries them), requiring
    a second full-result pass to fill them back in."""
    cvrg_subset = _attach_level_names(
        cvrg_subset, ["LVL_3_NAME", "LVL_6_NAME"], org_unit_tree_df
    )
    target_subset = _drop_name_cols(target_subset, ["LVL_3_NAME", "LVL_6_NAME"])
    return cvrg_subset, target_subset


def _revalidate_district_names_post_merge(
    merged: pd.DataFrame, org_unit_tree_df: pd.DataFrame
) -> pd.DataFrame:
    """Unlike CSI, LVL_3_NAME IS one of the District join keys - re-deriving it
    from the org-unit tree before the merge would risk changing which rows match
    (if the tree's spelling differs from whatever upstream district-anchoring
    already attached to cvrg_subset). So for District, names are (re-)validated
    against the tree AFTER the merge instead."""
    return _attach_level_names(merged, ["LVL_3_NAME"], org_unit_tree_df)


def process_target_level(
    cvrg_subset: pd.DataFrame,
    target_subset: pd.DataFrame,
    join_keys: list,
    final_keys: list,
    cumsum_keys: list,
    level_label: str,
    org_unit_tree_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Helper to merge targets, log warnings, and calculate cumulative values.

    Parameters:
        cvrg_subset (pd.DataFrame): Coverage subset DataFrame.
        target_subset (pd.DataFrame): Target subset DataFrame.
        join_keys (list): List of keys to join on.
        final_keys (list): List of final keys to select after merge.
        cumsum_keys (list): List of keys to calculate cumulative values.
        level_label (str): Label for the level (e.g., "CSI", "District").

    Returns:
        pd.DataFrame: Merged DataFrame with cumulative values.
    """
    try:
        if level_label == "CSI":
            cvrg_subset, target_subset = _attach_csi_names_pre_merge(
                cvrg_subset, target_subset, org_unit_tree_df
            )

        merged = cvrg_subset.merge(target_subset, on=join_keys, how="left")[final_keys]
        _warn_missing_targets(merged, cvrg_subset, level_label)
        merged = _compute_cumulative_value(merged, cumsum_keys)

        if level_label == "District":
            merged = _revalidate_district_names_post_merge(merged, org_unit_tree_df)

        return merged

    except Exception as e:
        current_run.log_error(
            f"Erreur lors du traitement des cibles au niveau {level_label}: {e}"
        )
        raise


# =========================================================================== #
# Coverage                                                                     #
# =========================================================================== #
def create_coverage_dataset(
    iaso_form_data_df: pd.DataFrame,
    expected_structure_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create coverage tables for visualization.

    Args:
        iaso_form_data_df (pd.DataFrame): the dataframe containing the processed data extracted from the IASO multi-campaign form
        expected_structure_df (pd.DataFrame): the dataframe containing the expected structure of the data for each campaign

    Returns:
        cvrg_total (pd.DataFrame): Coverage dataset DataFrame.
        df_final (pd.DataFrame): Final coverage dataset DataFrame after merging with expected
                                 combined campaign data.
    """
    current_run.log_info("Création du tableau de couverture vaccinale...")
    try:
        df = _build_coverage_long(iaso_form_data_df)
        cvrg_total = _aggregate_coverage(df)
        df_final = _merge_coverage_with_expected_structure(
            cvrg_total, expected_structure_df
        )

        current_run.log_info("Tableau de couverture vaccinale créé avec succès.")
        return cvrg_total, df_final

    except Exception as e:
        msg = f"Erreur lors de la création du tableau de couverture vaccinale: {e}"
        current_run.log_error(msg)
        raise


def _build_coverage_long(iaso_form_data_df: pd.DataFrame) -> pd.DataFrame:
    """Melt every campaign's coverage columns into long format, resolve the jnm/polio
    field overlap, categorize, and apply the two campaign-specific age adjustments."""
    id_vars = ["period", "round", "year", "org_unit_id"]
    df = melt_campaign_columns(
        iaso_form_data_df, cvrg_campaign_map, id_vars, tag_col="campaign"
    )

    # drop duplicates in terms of all cols except campaign (keep "jnm" campaign only). This is
    # b/c jnm and polio share the same fields in the form and so they get counted twice.
    dup_cols = df.columns.difference(["campaign"]).tolist()
    df = df.sort_values(by="campaign", key=lambda x: x == "jnm", ascending=False)
    df = df.drop_duplicates(subset=dup_cols, keep="first")

    df = new_cols(
        df,
        "categorizer",
        "category",
        [
            age_categorizer,
            site_categorizer,
            produit_categorizer,
            vaccination_status_categorizer,
        ],
    ).drop(columns=["category"])
    df["sexe"] = "TOUS"  # no gender configuration at the moment

    is_fjaune = df["campaign"] == "fièvre jaune"
    df.loc[is_fjaune, "age"] = df.loc[is_fjaune, "age"].replace(
        cvrg_yellow_fever_age_adjustment
    )
    is_rougeole = df["campaign"] == "rougeole"
    df.loc[is_rougeole, "age"] = df.loc[is_rougeole, "age"].replace(
        cvrg_rougeole_age_adjustment
    )
    return df


def _aggregate_coverage(df: pd.DataFrame) -> pd.DataFrame:
    """Sum to one row per cvrg_group_by_cols combination, dropping zero-value entries."""
    totals = df.groupby(cvrg_group_by_cols, as_index=False, observed=True)[
        "value"
    ].sum()
    return drop_zero_values(totals, "value", "informations du nombre de cas vaccinés")


def _merge_coverage_with_expected_structure(
    cvrg_total: pd.DataFrame, expected_structure_df: pd.DataFrame
) -> pd.DataFrame:
    """Outer-merge with expected_data_structure so every expected combination is
    present, dropping IASO entries whose shape doesn't match the expected structure."""
    cvrg_total["year"] = cvrg_total["year"].astype("Int64")
    cvrg_total["org_unit_id"] = cvrg_total["org_unit_id"].astype("Int64")
    expected_structure = drop_duplicates_low_memory(
        expected_structure_df[cvrg_group_by_cols + ["order_day", "choix_campagne"]]
    )

    # Without this, merging cvrg_total's plain object-dtype columns against
    # expected_structure's category ones would silently fall back to object dtype
    # for the merged result (see align_categories_for_merge) - at this table's
    # ~50M-row scale, that's the difference between this step running in a few
    # hundred MB and it alone re-inflating to several GB.
    category_cols = [
        c for c in EXPECTED_STRUCTURE_CATEGORY_COLS if c in cvrg_group_by_cols
    ]
    cvrg_total, expected_structure = align_categories_for_merge(
        cvrg_total, expected_structure, category_cols
    )

    df_final = cvrg_total.merge(
        expected_structure, on=cvrg_group_by_cols, how="outer", indicator=True
    )

    unmatched_entries_in_iaso = df_final[df_final["_merge"] == "left_only"]
    if not unmatched_entries_in_iaso.empty:
        proportion_unmatched_in_iaso = len(unmatched_entries_in_iaso) / len(df_final)
        current_run.log_warning(
            f"{len(unmatched_entries_in_iaso)} entrées ({proportion_unmatched_in_iaso:.2%}) "
            "n'ont pas le même format que le Dataframe de la structure attendue. Ces "
            "entrées seront supprimées."
        )
    return df_final[df_final["_merge"] != "left_only"].drop(columns=["_merge"])


# =========================================================================== #
# Targets on coverage (CSI + District)                                        #
# =========================================================================== #
def add_target_data(
    cvrg_df: pd.DataFrame,
    target_df: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add District- and CSI-level target data to the coverage dataset to allow computation of
    coverage ratios at the district and CSI level in PBI.

    NB:
    - When targets are defined at CSI-level, the District-level target corresponds to the sum
      of the CSI-level targets for the CSIs in the district.
    - When targets are defined at District-level only, the CSI-level targets are NaN.

    Args:
        cvrg_df (pd.DataFrame): Coverage DataFrame.
        target_df (pd.DataFrame): Target DataFrame.

    Returns:
        cvrg_with_targets (pd.DataFrame): Coverage DataFrame with DS- and CSI-level target data added.

    """
    current_run.log_info(
        "Ajout des données cibles au tableau de couverture vaccinale..."
    )
    try:
        csi_filter_df, ds_filter_df = _split_by_target_reporting_level(target_df)

        cvrg_csi_with_targets = _build_csi_level_targets(
            cvrg_df, target_df, csi_filter_df, iaso_org_unit_tree_raw_df
        )
        cvrg_district_with_targets = _build_district_level_targets(
            cvrg_df,
            target_df,
            ds_filter_df,
            cvrg_csi_with_targets,
            iaso_org_unit_tree_clean_df,
            iaso_org_unit_tree_raw_df,
        )

        cvrg_csi_district = pd.concat(
            [cvrg_csi_with_targets, cvrg_district_with_targets], ignore_index=True
        )
        return _normalize_target_values(cvrg_csi_district)
    except Exception as e:
        current_run.log_error(f"Erreur: {e}")
        raise


def _split_by_target_reporting_level(
    target_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Which (year, produit, round) campaigns report targets at CSI level vs. District-only."""
    csi_filter_df = target_df[target_df["LVL_6_NAME"].notna()][
        ["year", "produit", "round"]
    ].drop_duplicates()
    ds_filter_df = target_df[target_df["LVL_6_NAME"].isna()][
        ["year", "produit", "round"]
    ].drop_duplicates()
    return csi_filter_df, ds_filter_df


def _build_csi_level_targets(
    cvrg_df: pd.DataFrame,
    target_df: pd.DataFrame,
    csi_filter_df: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
) -> pd.DataFrame:
    """Coverage + target merged at CSI level, restricted to campaigns that report at
    that level."""
    target_csi_df = target_df.merge(
        csi_filter_df, on=["year", "produit", "round"], how="inner"
    )
    cvrg_csi_df = cvrg_df.merge(
        csi_filter_df, on=["year", "produit", "round"], how="inner"
    )

    cvrg_csi_with_targets = process_target_level(
        cvrg_csi_df,
        target_csi_df,
        cvrg_csi_level_target_keys,
        cvrg_csi_level_final_keys,
        cvrg_csi_level_cumsum_keys,
        "CSI",
        iaso_org_unit_tree_raw_df,
    )
    cvrg_csi_with_targets["choice_org_unit_level"] = "CSI"
    cvrg_csi_with_targets["org_unit_id"] = cvrg_csi_with_targets["org_unit_id"].astype(
        "Int64"
    )
    cvrg_csi_with_targets["link_key"] = (
        cvrg_csi_with_targets["org_unit_id"].astype(str) + "_CSI"
    )
    return cvrg_csi_with_targets


def _aggregate_csi_to_district(cvrg_csi_with_targets: pd.DataFrame) -> pd.DataFrame:
    """Source 1: CSI-level coverage aggregated up to district level."""
    return cvrg_csi_with_targets.groupby(
        cvrg_district_level_group_keys, as_index=False, observed=True
    ).agg({"value": "sum"})


def _pure_district_coverage(
    cvrg_df: pd.DataFrame,
    ds_filter_df: pd.DataFrame,
    cvrg_csi_with_targets: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
) -> pd.DataFrame:
    """Source 2: coverage for campaigns/districts that ONLY report at District level -
    excluding any district/campaign already covered by source 1 (_aggregate_csi_to_district)."""
    districts_with_csi_reporting = cvrg_csi_with_targets[
        cvrg_csi_level_target_keys
    ].drop_duplicates()
    cvrg_df_pure_district_raw = cvrg_df.merge(
        ds_filter_df, on=["year", "produit", "round"], how="inner"
    )
    cvrg_district_pure = cvrg_df_pure_district_raw.merge(
        districts_with_csi_reporting,
        on=cvrg_csi_level_target_keys,
        how="left",
        indicator=True,
    )
    cvrg_district_pure = cvrg_district_pure[
        cvrg_district_pure["_merge"] == "left_only"
    ].drop(columns=["_merge"])

    lvl_3_name_ds = iaso_org_unit_tree_raw_df[
        ["org_unit_id", "LVL_3_NAME"]
    ].drop_duplicates()
    cvrg_district_pure = cvrg_district_pure.merge(
        lvl_3_name_ds, on="org_unit_id", how="left"
    )
    return cvrg_district_pure.groupby(
        cvrg_district_level_group_keys, as_index=False, observed=True
    ).agg({"value": "sum"})


def _build_district_level_targets(
    cvrg_df: pd.DataFrame,
    target_df: pd.DataFrame,
    ds_filter_df: pd.DataFrame,
    cvrg_csi_with_targets: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Coverage + target merged at District level, from two sources:
    - campaigns that report at CSI level: their CSI coverage aggregated up to District.
    - campaigns that report at District level only: their own coverage directly, minus
      any district/campaign already covered by the first source.
    """
    cvrg_district_from_csi = _aggregate_csi_to_district(cvrg_csi_with_targets)
    cvrg_district_pure = _pure_district_coverage(
        cvrg_df, ds_filter_df, cvrg_csi_with_targets, iaso_org_unit_tree_raw_df
    )
    cvrg_district_df = _anchor_to_representative_org_unit(
        cvrg_district_pure, cvrg_district_from_csi, iaso_org_unit_tree_clean_df
    )
    target_district_df = _sum_targets_to_district(target_df)

    cvrg_district_with_targets = process_target_level(
        cvrg_district_df,
        target_district_df,
        cvrg_district_level_target_keys,
        cvrg_district_level_final_keys,
        cvrg_district_level_cumsum_keys,
        "District",
        iaso_org_unit_tree_raw_df,
    )
    cvrg_district_with_targets["choice_org_unit_level"] = "District"
    cvrg_district_with_targets["org_unit_id"] = cvrg_district_with_targets[
        "org_unit_id"
    ].astype("Int64")
    cvrg_district_with_targets["link_key"] = (
        cvrg_district_with_targets["org_unit_id"].astype(str) + "_District"
    )
    return cvrg_district_with_targets


def _anchor_to_representative_org_unit(
    cvrg_district_pure: pd.DataFrame,
    cvrg_district_from_csi: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """Combine both district-level coverage sources and anchor each district's rows
    to one representative CSI-level org_unit_id (the first, by org_unit_id, in the
    clean tree) - a district-level row needs SOME org_unit_id to plot in PBI, and
    which specific CSI it is doesn't affect the district-level value."""
    rep_ids = (
        iaso_org_unit_tree_clean_df.sort_values(["LVL_3_NAME", "org_unit_id"])
        .groupby("LVL_3_NAME")["org_unit_id"]
        .first()
        .reset_index()
        .rename(columns={"org_unit_id": "rep_id"})
    )
    combined = pd.concat(
        [cvrg_district_pure, cvrg_district_from_csi], ignore_index=True
    )
    combined = combined.merge(rep_ids, on="LVL_3_NAME", how="left")
    combined["org_unit_id"] = combined["rep_id"]
    combined["LVL_6_NAME"] = None
    return combined


def _sum_targets_to_district(target_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate targets up to district level (sum of CSI-level targets, when defined
    at that level)."""
    target_df_unique = target_df[
        cvrg_district_level_target_keys + ["cible"]
    ].drop_duplicates()
    return target_df_unique.groupby(
        cvrg_district_level_target_keys, as_index=False, observed=True
    )["cible"].sum()


def _normalize_target_values(cvrg_csi_district: pd.DataFrame) -> pd.DataFrame:
    """Split each target evenly across however many rows share its (link_key, year,
    round, produit, age, period) - so a target isn't double-counted when several rows
    (e.g. different vaccination statuses) share the same underlying target figure."""
    unique_target_cols = ["link_key", "year", "round", "produit", "age", "period"]
    duplication_counts = (
        cvrg_csi_district.groupby(unique_target_cols, observed=True)
        .size()
        .reset_index(name="row_count")
    )
    cvrg_csi_district = cvrg_csi_district.merge(
        duplication_counts, on=unique_target_cols, how="left"
    )
    cvrg_csi_district["cible_norm"] = (
        cvrg_csi_district["cible"] / cvrg_csi_district["row_count"]
    )
    return cvrg_csi_district.drop(columns=["row_count"])
