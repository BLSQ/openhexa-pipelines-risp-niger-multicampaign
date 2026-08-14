"""
GENERATED FILE - do not edit directly.
Source of truth: shared/utils.py (repo root). Regenerate every copy with:
    python scripts/sync_shared_utils.py
"""

import os
from openhexa.sdk import current_run, workspace
import pandas as pd

WORKSPACE_PATH = workspace.files_path
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")


def load_data(file_name: str) -> pd.DataFrame:
    """
    Load data from a parquet file in the OUTPUTS_PATH.

    Args:
        file_name (str): The name of the file to read from.

    Returns:
        df (pd.DataFrame): The dataframe containing the file data.
    """
    current_run.log_info(f"Importation du fichier {file_name}...")
    file_to_import = os.path.join(OUTPUTS_PATH, f"{file_name}.parquet")

    if not os.path.exists(file_to_import):
        msg = f"Le fichier {file_to_import} n'existe pas."
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    try:
        df = pd.read_parquet(file_to_import)
        current_run.log_info(
            f"Données du fichier {file_name} chargées avec succès depuis le fichier {file_to_import}"
        )
        return df
    except Exception as e:
        msg = f"Erreur lors de la lecture du fichier {file_to_import}: {str(e)}"
        current_run.log_error(msg)
        raise


def save_file(df: pd.DataFrame, file_name: str, folder: str = None) -> None:
    """
    Save a dataframe to a parquet file.

    Args:
        df (pd.DataFrame): DataFrame containing the data to be saved.
        file_name (str): Name of the file to save the DataFrame as (no extension).
        folder (str, optional): Directory to save into. Defaults to OUTPUTS_PATH -
            pass a different folder for pipelines that keep per-run output files
            in their own subfolder (e.g. one file per run, compiled from scratch
            later) instead of directly under OUTPUTS_PATH.

    Returns:
        None
    """
    folder = folder or OUTPUTS_PATH
    current_run.log_info(f"Enregistrement du fichier {file_name} dans {folder}...")

    if not os.path.exists(folder):
        os.makedirs(folder)
    file_path = os.path.join(folder, f"{file_name}.parquet")
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


def _get_or_create_dataset(dataset_name: str, dataset_slug: str):
    """Look up the dataset by slug; create it if it doesn't exist yet."""
    try:
        dataset = workspace.get_dataset(dataset_slug)
        current_run.log_info(f"Dataset existant trouvé : {dataset_slug}")
    except Exception:
        current_run.log_info(f"Dataset {dataset_name} non trouvé. Création en cours...")
        dataset = workspace.create_dataset(name=dataset_name, description="")
    return dataset


def _next_version_name(dataset) -> str:
    latest_version = dataset.latest_version
    version_number = int(latest_version.name.lstrip("v")) + 1 if latest_version else 1
    return f"v{version_number}"


def _write_export_files(df: pd.DataFrame, df_file_path: str, dataset_name: str) -> dict:
    """Write df locally as parquet + csv, returning {format: file_path}.

    No xlsx: Excel's 1,048,576-row sheet limit crashes this on any table past
    that size (hit in practice on process_target_data's
    expected_data_structure) - parquet + csv cover the same need without the
    silent row-count ceiling.
    """
    if not os.path.exists(df_file_path):
        os.makedirs(df_file_path)
    base_path = os.path.join(df_file_path, dataset_name)
    files_to_upload = {
        "parquet": f"{base_path}.parquet",
        "csv": f"{base_path}.csv",
    }
    df.to_parquet(files_to_upload["parquet"], index=False)
    df.to_csv(files_to_upload["csv"], index=False)
    return files_to_upload


def _upload_export_files(dataset, version_name: str, files_to_upload: dict) -> None:
    version = dataset.create_version(version_name)
    for format_type, file_path in files_to_upload.items():
        version.add_file(file_path, os.path.basename(file_path))
        current_run.log_info(
            f"Fichier {format_type} ajouté à la version {version_name}"
        )


def export_to_dataset(df: pd.DataFrame, df_file_path: str, dataset_name: str) -> None:
    """
    Exports a DataFrame to an OpenHexa dataset in multiple formats (parquet, csv).

    Args:
        df (pd.DataFrame): The configuration dataframe to export.
        df_file_path (str): The file path where the dataframe is saved.
        dataset_name (str): The name of the OpenHexa dataset.
    """
    current_run.log_info(
        f"Préparation de l'exportation vers le dataset : {dataset_name}..."
    )

    dataset_slug = dataset_name.lower().strip().replace(" ", "-").replace("_", "-")
    dataset = _get_or_create_dataset(dataset_name, dataset_slug)

    try:
        new_version_name = _next_version_name(dataset)
        files_to_upload = _write_export_files(df, df_file_path, dataset_name)
        _upload_export_files(dataset, new_version_name, files_to_upload)
        current_run.log_info(
            f"Exportation terminée avec succès pour {dataset_name} ({new_version_name})"
        )
    except Exception as e:
        msg = f"Erreur lors de l'exportation vers le dataset {dataset_name}: {e}"
        current_run.log_error(msg)
        raise
