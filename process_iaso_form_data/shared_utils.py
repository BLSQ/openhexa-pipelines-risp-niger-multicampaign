import os
from openhexa.sdk import current_run, workspace
import pandas as pd

WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(
#     os.getcwd(), "process_historical_target_data", "workspace"
# )  # local
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

    dataset_slug = dataset_name.lower().strip().replace(" ", "-").replace("_", "-")

    # check if dataset already exists
    try:
        dataset = workspace.get_dataset(dataset_slug)
        current_run.log_info(f"Dataset existant trouvé : {dataset_slug}")
    except Exception:
        current_run.log_info(f"Dataset {dataset_name} non trouvé. Création en cours...")
        dataset = workspace.create_dataset(
            name=dataset_name,
            description="",
        )

    # define versioning
    try:
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
