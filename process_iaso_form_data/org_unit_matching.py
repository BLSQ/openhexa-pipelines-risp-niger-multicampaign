"""
Aligning IASO submissions' org_unit_id to the "clean" org-unit tree, via a two-step
LVL_6_UID lookup.
"""

import numpy as np
import pandas as pd
from openhexa.sdk import current_run


def align_to_clean_org_tree(
    iaso_raw_df: pd.DataFrame,
    iaso_org_unit_tree_raw: pd.DataFrame,
    iaso_org_unit_tree_clean: pd.DataFrame,
) -> pd.DataFrame:
    """
    Standardizes org unit IDs in the submission data by mapping them to the 'Clean'
    org unit tree.

    Args:
        iaso_raw_df (pd.DataFrame): The dataframe containing the raw data from the IASO multi-campaign form
        iaso_org_unit_tree_raw (pd.DataFrame): The raw IASO organisation tree DataFrame containing all org units names and IDs
        iaso_org_unit_tree_clean (pd.DataFrame): The clean IASO organisation tree DataFrame containing the clean org units names and IDs.

    Returns:
        iaso_processed_df (pd.DataFrame): The dataframe containing the processed data from the IASO multi-campaign form with org unit IDs retrieved.
    """
    current_run.log_info(
        "Récupération des identifiants des unités organisationnelles..."
    )
    try:
        org_unit_to_final_org_unit_dict = _build_org_unit_resolution_map(
            iaso_org_unit_tree_raw, iaso_org_unit_tree_clean
        )
        iaso_raw_df["org_unit_id"] = iaso_raw_df["org_unit_id"].map(
            org_unit_to_final_org_unit_dict
        )
        iaso_raw_df = _drop_missing_org_units(iaso_raw_df)
        iaso_raw_df.loc[:, "org_unit_id"] = iaso_raw_df["org_unit_id"].astype(np.int64)

        iaso_processed_df = iaso_raw_df.copy()

        current_run.log_info(
            "Récupération des identifiants des unités organisationnelles terminée avec succès."
        )

        return iaso_processed_df
    except Exception as e:
        msg = f"Erreur lors de la récupération des identifiants des unités organisationnelles : {str(e)}"
        current_run.log_error(msg)
        raise


def _build_org_unit_resolution_map(
    iaso_org_unit_tree_raw: pd.DataFrame,
    iaso_org_unit_tree_clean: pd.DataFrame,
) -> dict:
    """Build a {raw org_unit_id -> clean org_unit_id} lookup, via the shared
    LVL_6_UID. Several raw org units can share one physical facility
    (one-to-many), so LVL_6_UID is first made unique per LVL_6_NAME (picking the
    first one) before being used to look up that facility's clean org_unit_id."""
    iaso_org_unit_tree_raw["LVL_6_UID"] = iaso_org_unit_tree_raw.groupby(
        "LVL_6_NAME"
    )["LVL_6_UID"].transform("first")

    uid_to_org_id_dict = iaso_org_unit_tree_clean.set_index("LVL_6_UID").to_dict()[
        "org_unit_id"
    ]
    iaso_org_unit_tree_raw["final_org_unit"] = iaso_org_unit_tree_raw[
        "LVL_6_UID"
    ].map(uid_to_org_id_dict)
    return iaso_org_unit_tree_raw.set_index("org_unit_id").to_dict()["final_org_unit"]


def _drop_missing_org_units(iaso_raw_df: pd.DataFrame) -> pd.DataFrame:
    """Warn about and remove entries whose org_unit_id didn't resolve via the
    mapping built by _build_org_unit_resolution_map."""
    mask_missing_org_unit = iaso_raw_df["org_unit_id"].isna()
    missing_org_unit_entries = iaso_raw_df[mask_missing_org_unit]
    if not missing_org_unit_entries.empty:
        missing_org_unit_proportion = len(missing_org_unit_entries) / len(
            iaso_raw_df
        )
        current_run.log_warning(
            f"{len(missing_org_unit_entries)} entrées ({missing_org_unit_proportion:.2%}) contiennent des org_unit_id manquants. Ces entrées seront supprimées."
        )
        iaso_raw_df = iaso_raw_df[~mask_missing_org_unit].copy()
    return iaso_raw_df
