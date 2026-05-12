from openhexa.sdk import current_run, pipeline
import pandas as pd
import numpy as np
from shared_utils import (
    save_file,
    export_to_dataset,
)
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
    This pipeline extracts organizational unit tree data from the IASO multi-campaign form,
    cleans it by filtering out rejected entries and selecting relevant records, and then
    saves both the raw and cleaned data to parquet files in the workspace.
    """
    iaso_org_unit_tree_df = get_iaso_org_unit_tree()
    iaso_org_unit_tree_df_clean = clean_iaso_org_unit_tree(iaso_org_unit_tree_df)
    save_file(iaso_org_unit_tree_df, "iaso_org_unit_tree_raw")
    save_file(iaso_org_unit_tree_df_clean, "iaso_org_unit_tree_clean")
    export_to_dataset(iaso_org_unit_tree_df, OUTPUTS_PATH, "iaso_org_unit_tree_raw")
    export_to_dataset(
        iaso_org_unit_tree_df_clean, OUTPUTS_PATH, "iaso_org_unit_tree_clean"
    )


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
    try:
        iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
        iaso_org_unit_tree_df = (
            iaso_connector_instance.get_ou_tree_dataframe_from_the_form(iaso_form_id)
        )

        current_run.log_info(
            f"Données de l'arbre des unités organisationnelles IASO extraites avec succès. Nombre de lignes extraites: {len(iaso_org_unit_tree_df)}"
        )

        return iaso_org_unit_tree_df
    except Exception as e:
        msg = f"Erreur lors de l'extraction des données de l'arbre des unités organisationnelles IASO: {str(e)}"
        current_run.log_error(msg)
        raise


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

        current_run.log_info(
            "Données de l'arbre des unités organisationnelles IASO nettoyées avec succès."
        )

        return iaso_org_unit_tree_df_clean

    except Exception as e:
        msg = f"Erreur lors du nettoyage des données de l'arbre des unités organisationnelles IASO: {str(e)}"
        current_run.log_error(msg)
        raise


if __name__ == "__main__":
    extract_org_units()
