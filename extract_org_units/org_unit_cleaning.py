"""
Picking the canonical record for an org unit: filtering the raw IASO org-unit tree
down to valid, current SNIS/CSI entries and resolving duplicate records for the
same org unit down to one row each.
"""

import datetime

import numpy as np
import pandas as pd
from openhexa.sdk import current_run


def pyramid_selector(df: pd.DataFrame) -> pd.Series:
    """
    Selects the most recent row, excluding entries from 2023-07-14.

    Parameters:
        df (pd.DataFrame): The input dataframe containing an 'updated_date' column.

    Returns:
        pd.Series: The row with the most recent updated_date, excluding the forbidden date.
    """
    dates = pd.to_datetime(df["updated_date"])
    mask = dates.dt.date != datetime.date(
        2023, 7, 14
    )  # Filter out the "forbidden" date (2023-07-14)
    valid_df_dates = dates[mask]

    if valid_df_dates.empty:
        return pd.Series(dtype="object")

    max_idx = valid_df_dates.idxmax()
    most_recent_row = df.loc[max_idx]
    return most_recent_row


def clean_iaso_org_unit_tree(iaso_org_unit_tree_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the org unit tree data by filtering out rejected entries and selecting relevant records.

    Args:
        iaso_org_unit_tree_df (pd.DataFrame): DataFrame containing the org unit tree data to be cleaned.

    Returns:
        pd.DataFrame: Cleaned DataFrame with relevant org unit tree data.
    """
    current_run.log_info(
        "Nettoyage des données de l'arbre des unités organisationnelles IASO..."
    )
    try:
        iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df[
            iaso_org_unit_tree_df["Validé"] != "REJECTED"  # Keep Valid
        ]
        iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
            iaso_org_unit_tree_df_clean["Source"].isin(
                ["SNIS", "SNIS 2025"]
            )  # keep SNIS only
        ]
        iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
            iaso_org_unit_tree_df_clean["LVL_6_NAME"].str.contains(
                "CSI", case=False, na=False
            )
        ]  # use pre-fix instead

        # Scoped by (district, name), NOT name alone: many CSI names repeat across
        # unrelated districts (e.g. "CSI Sabon Gari" names 5 distinct real
        # facilities). Grouping by name alone merges their LVL_6_UID onto
        # whichever one happens to be "first", silently erasing the others from
        # the tree below.
        iaso_org_unit_tree_df_clean["LVL_6_UID"] = iaso_org_unit_tree_df_clean.groupby(
            ["LVL_3_NAME", "LVL_6_NAME"]
        )["LVL_6_UID"].transform("first")
        iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean.groupby(
            "LVL_6_UID", as_index=False
        ).apply(pyramid_selector, include_groups=False)

        iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
            iaso_org_unit_tree_df_clean["LVL_2_NAME"] != "Niger"
        ]  # delete 2 incoherent entries

        iaso_org_unit_tree_df_clean["org_unit_id"] = iaso_org_unit_tree_df_clean[
            "org_unit_id"
        ].astype(np.int64)

        current_run.log_info(
            "Données de l'arbre des unités organisationnelles IASO nettoyées avec succès."
        )

        return iaso_org_unit_tree_df_clean

    except Exception as e:
        msg = f"Erreur lors du nettoyage des données de l'arbre des unités organisationnelles IASO: {str(e)}"
        current_run.log_error(msg)
        raise
