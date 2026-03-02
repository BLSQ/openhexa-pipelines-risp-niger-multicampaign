from openhexa.sdk import current_run, pipeline, workspace
import os
import pandas as pd
import numpy as np
from pathlib import Path
from utils import (
    IASOConnectionHandler,
    pyramid_selector,
)
from config import (
    iaso_connector_slug,
    iaso_form_id,
    outputs_path,
    templates_path,
)


@pipeline("generate_targets_templates")
def generate_targets_templates():
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    iaso_org_unit_tree_df = get_iaso_org_unit_tree()
    iaso_org_unit_tree_df_clean = clean_iaso_org_unit_tree(iaso_org_unit_tree_df)
    x


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

    # save file to parquet for later use
    file_path = os.path.join(
        workspace.files_path,
        outputs_path,
        "iaso_org_unit_tree_raw.parquet",
    )
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    iaso_org_unit_tree_df.to_parquet(
        file_path,
        index=False,
    )
    iaso_org_unit_tree_df = pd.read_parquet(file_path)
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


def create_template_files(org_unit_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create template files for each organizational unit based on the cleaned org unit tree data.

    Args:
        org_unit_df (pd.DataFrame): DataFrame containing the cleaned org unit tree data.

    Returns:
        pd.DataFrame: DataFrame containing the template files for each organizational unit.
    """


def save_org_unit_tree(iaso_org_unit_tree_df_clean: pd.DataFrame):
    """
    Save the cleaned org unit tree data to a parquet file.

    Args:
        iaso_org_unit_tree_df_clean (pd.DataFrame): DataFrame containing the cleaned org unit tree data.

    Returns:
        None
    """
    current_run.log_info("Enregistrement des données cibles combinées...")
    try:
        folder_path = os.path.join(workspace.files_path, outputs_path)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        file_path = os.path.join(
            folder_path,
            "iaso_org_unit_tree_clean.parquet",
        )

        iaso_org_unit_tree_df_clean.to_parquet(
            file_path,
            index=False,
        )
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'enregistrement du fichier: {e}")
        raise e


if __name__ == "__main__":
    generate_targets_templates()
