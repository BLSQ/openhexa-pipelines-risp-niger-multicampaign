from openhexa.sdk import current_run, pipeline
import os
import pandas as pd
import numpy as np
from utils import (
    IASOConnectionHandler,
    pyramid_selector,
)
from config import (
    iaso_connector_slug,
    iaso_form_id,
    OUTPUTS_PATH,
)


@pipeline(
    "extract_org_units",
    name="multi-campagne - Extraction des unités organisationnelles IASO",
)
def extract_org_units():
    """
    This pipeline generates target templates for each campaign type based on the organizational unit tree
    data from IASO. It retrieves the org unit tree data, cleans it, creates template files for each campaign
    type, and saves the cleaned org unit tree data for future use.
    """
    iaso_org_unit_tree_df = get_iaso_org_unit_tree()
    iaso_org_unit_tree_df_clean = clean_iaso_org_unit_tree(iaso_org_unit_tree_df)
    save_file(iaso_org_unit_tree_df, "iaso_org_unit_tree_raw")
    save_file(iaso_org_unit_tree_df_clean, "iaso_org_unit_tree_clean")


def get_iaso_org_unit_tree() -> pd.DataFrame:
    """
    Retrieve organizational unit tree data from IASO based on a specific form ID.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the organizational unit tree data.
    """
    current_run.log_info(
        "Extraction des données de l'arbre des unités organisationnelles IASO..."
    )

    iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
    iaso_org_unit_tree_df = iaso_connector_instance.get_ou_tree_dataframe_from_the_form(
        iaso_form_id
    )

    return iaso_org_unit_tree_df


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

    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df[
        iaso_org_unit_tree_df["Validé"] != "REJECTED"
    ]
    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
        iaso_org_unit_tree_df_clean["Source"].isin(["SNIS", "SNIS 2025"])
    ]
    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
        iaso_org_unit_tree_df_clean["LVL_6_NAME"].str.contains(
            "CSI", case=False, na=False
        )
    ]

    iaso_org_unit_tree_df_clean["LVL_6_UID"] = iaso_org_unit_tree_df_clean.groupby(
        "LVL_6_NAME"
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

    return iaso_org_unit_tree_df_clean


def save_file(df: pd.DataFrame, file_name: str) -> None:
    """
    Save the cleaned org unit tree data to a parquet file.

    Args:
        df (pd.DataFrame): DataFrame containing the cleaned org unit tree data.
        file_name (str): Name of the file to save the DataFrame as.

    Returns:
        None
    """
    current_run.log_info("Enregistrement des données des unités organisationnelles...")
    try:
        if not os.path.exists(OUTPUTS_PATH):
            os.makedirs(OUTPUTS_PATH)
        file_path = os.path.join(
            OUTPUTS_PATH,
            f"{file_name}.parquet",
        )

        df.to_parquet(
            file_path,
            index=False,
        )
        current_run.log_info(
            f"Données des unités organisationnelles enregistrées avec succès: {file_path}"
        )
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'enregistrement du fichier: {e}")
        raise e


if __name__ == "__main__":
    extract_org_units()
