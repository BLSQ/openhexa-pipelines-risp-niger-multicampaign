"""
Spatial units theme: the dynamic (District/CSI-switchable) org-unit table for
ner_spatial_units.
"""

import pandas as pd
from openhexa.sdk import current_run


def create_dynamic_org_unit_table(
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create a table that allows to dynamically switch between district-level and CSI-level
    data in Power BI by having the same org unit in different rows with a variable indicating
    the level (district vs CSI) and the corresponding district or CSI name in the same column.

    This is done by:
     - taking the cleaned org unit tree
     - creating two copies of it and keeping only the relevant columns for each copy to
        create a district-level and a CSI-level version of the table
     - concatenating these two tables together.

    Args:
        iaso_org_unit_tree_clean_df (pd.DataFrame): DataFrame containing the cleaned org unit tree.

    Returns:
        spatial_units_combined (pd.DataFrame): DataFrame containing the combined district-level and
                                               CSI-level org unit table with a variable indicating
                                               the level to allow dynamic switching between district
                                               and CSI level in PBI
    """
    current_run.log_info(
        "Création du tableau dynamique des unités organisationnelles..."
    )
    try:
        district_view = _district_level_org_unit_view(iaso_org_unit_tree_clean_df)
        csi_view = _csi_level_org_unit_view(iaso_org_unit_tree_clean_df)
        spatial_units_combined = pd.concat(
            [district_view, csi_view], ignore_index=True
        ).reset_index(drop=True)

        current_run.log_info(
            "Tableau dynamique des unités organisationnelles créé avec succès."
        )
        return spatial_units_combined

    except Exception as e:
        msg = f"Erreur lors de la création du tableau dynamique des unités organisationnelles: {e}"
        current_run.log_error(msg)
        raise


def _district_level_org_unit_view(
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """One row per district (LVL_1 = country, no CSI level)."""
    districts = iaso_org_unit_tree_clean_df.sort_values(["LVL_3_NAME", "org_unit_id"])
    view = districts.groupby("LVL_3_NAME", as_index=False).first()

    view["choice_org_unit_level"] = "District"
    view["LVL_1_NAME"] = "Niger"
    view["LVL_6_NAME"] = None
    view["LVL_6_UID"] = None
    view["link_key"] = (
        view["org_unit_id"].astype(str) + "_" + view["choice_org_unit_level"]
    )
    return view


def _csi_level_org_unit_view(iaso_org_unit_tree_clean_df: pd.DataFrame) -> pd.DataFrame:
    """One row per CSI, with every level shifted up one notch (LVL_1 = region, ...,
    LVL_3 = CSI) so PBI can switch to CSI granularity using the same column names."""
    view = iaso_org_unit_tree_clean_df.copy()
    view["choice_org_unit_level"] = "CSI"
    view["LVL_1_NAME"] = view["LVL_2_NAME"]
    view["LVL_1_UID"] = view["LVL_2_UID"]
    view["LVL_2_NAME"] = view["LVL_3_NAME"]
    view["LVL_2_UID"] = view["LVL_3_UID"]
    view["LVL_3_NAME"] = view["LVL_6_NAME"]
    view["LVL_3_UID"] = view["LVL_6_UID"]
    view["link_key"] = (
        view["org_unit_id"].astype(str) + "_" + view["choice_org_unit_level"]
    )
    return view
