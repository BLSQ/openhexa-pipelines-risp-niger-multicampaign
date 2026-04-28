from openhexa.sdk import current_run, pipeline, workspace
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


def save_file(df: pd.DataFrame, file_name: str) -> None:
    """
    Save a dataframe to a parquet file.

    Args:
        df (pd.DataFrame): DataFrame containing the data to be saved.
        file_name (str): Name of the file to save the DataFrame as.

    Returns:
        None
    """
    current_run.log_info("Enregistrement du fichier dans l'espace de travail...")

    if not os.path.exists(OUTPUTS_PATH):
        os.makedirs(OUTPUTS_PATH)
    file_path = os.path.join(
        OUTPUTS_PATH,
        f"{file_name}.parquet",
    )
    try:
        df.to_parquet(
            file_path,
            index=False,
        )
        current_run.log_info(f"Fichier enregistré avec succès: {file_path}")
    except Exception as e:
        msg = f"Erreur lors de l'enregistrement du fichier: {str(e)}"
        current_run.log_error(msg)
        raise


def export_to_dataset(df: pd.DataFrame, df_file_path: str, dataset_name: str) -> None:
    """
    Exports a DataFrame to an OpenHexa dataset in multiple formats (xlsx, parquet, csv).

    Args:
        df (pd.DataFrame): The configuration dataframe to export.
        df_file_path (str): The file path where the dataframe is saved.
        dataset_name (str): The name of the OpenHexa dataset.
    """
    current_run.log_info(
        f"Préparation de l'exportation vers le dataset : {dataset_name}..."
    )
    try:
        dataset_slug = dataset_name.lower().strip().replace(" ", "-").replace("_", "-")

        # check if dataset already exists
        try:
            dataset = workspace.get_dataset(dataset_slug)
            current_run.log_info(f"Dataset existant trouvé : {dataset_slug}")
        except Exception:
            current_run.log_info(
                f"Dataset {dataset_name} non trouvé. Création en cours..."
            )
            dataset = workspace.create_dataset(
                name=dataset_name,
                description="Données de configuration de campagne (multi-formats)",
            )

        # define versioning
        latest_version = dataset.latest_version
        version_number = (
            int(latest_version.name.lstrip("v")) + 1 if latest_version else 1
        )
        new_version_name = f"v{version_number}"

        # create local files
        if not os.path.exists(df_file_path):
            os.makedirs(df_file_path)

        base_path = os.path.join(df_file_path, dataset_name)
        files_to_upload = {
            "parquet": f"{base_path}.parquet",
            "xlsx": f"{base_path}.xlsx",
            "csv": f"{base_path}.csv",
        }

        df.to_parquet(files_to_upload["parquet"], index=False)
        df.to_excel(files_to_upload["xlsx"], index=False)
        df.to_csv(files_to_upload["csv"], index=False)

        # upload to Dataset in OH
        version = dataset.create_version(new_version_name)

        for format_type, file_path in files_to_upload.items():
            version.add_file(file_path, os.path.basename(file_path))
            current_run.log_info(
                f"Fichier {format_type} ajouté à la version {new_version_name}"
            )

        current_run.log_info(
            f"Exportation terminée avec succès pour {dataset_name} ({new_version_name})"
        )
    except Exception as e:
        msg = f"Erreur lors de l'exportation vers le dataset {dataset_name}: {e}"
        current_run.log_error(msg)
        raise


if __name__ == "__main__":
    extract_org_units()
