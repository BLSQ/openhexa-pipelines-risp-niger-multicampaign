import os
from openhexa.sdk import current_run, pipeline, workspace
import pandas as pd
from config import (
    OUTPUTS_PATH,
    CONFIG_PATH,
)


@pipeline(
    "combine_expected_data_structures",
    name="multi-campagne - Etablissement de la structure des données attendues",
)
def combine_expected_data_structures():
    """
    This pipeline combines the expected data structure of historical campaigns with the
    expected data structure of new campaigns.

    Args:
        None

    Returns:
        None
    """
    # load new campaigns config
    expected_data_structure_new_campaigns = (
        generate_expected_data_structure_for_new_campaigns(CONFIG_PATH)
    )

    # combine with historical campaigns config
    expected_data_structure_historical_campaigns = load_data(
        "expected_data_structure_historical_campaigns"
    )
    combined_df = combine(
        expected_data_structure_historical_campaigns,
        expected_data_structure_new_campaigns,
    )

    # save
    save_file(combined_df, "expected_data_structure")


def load_data(name: str) -> pd.DataFrame:
    """
    Import data from a specified file in the outputs directory.
    The file should be in parquet format and the name should be provided without the extension.

    Args:
        name (str): Name of the file to be imported (without extension).

    Returns:
        df (pd.DataFrame): DataFrame containing the imported data.
    """
    current_run.log_info(f"Importation du fichier {name}...")
    try:
        if not os.path.exists(OUTPUTS_PATH):
            os.makedirs(OUTPUTS_PATH)

        file_path = os.path.join(
            OUTPUTS_PATH,
            f"{name}.parquet",
        )
        df = pd.read_parquet(file_path)
        current_run.log_info(f"Fichier importé avec succès: {file_path}")
        return df

    except Exception as e:
        current_run.log_error(f"Erreur lors de l'importation du fichier {name}: {e}")
        raise


def generate_expected_data_structure_for_new_campaigns(
    config_dir_path: str,
) -> pd.DataFrame:
    """
    Import all config files relating to new campaigns and append the corresponding
    configurations to the combined DataFrame of historical campaigns.

    Args:
        config_dir_path (str): Path to the directory containing the new campaign configuration files.

    Returns:
        combined_df (pd.DataFrame): Adjusted DataFrame with new campaign configurations added.
    """
    current_run.log_info(
        "Ajout des configurations des nouvelles campagnes au DataFrame combiné..."
    )
    try:
        config_files = [
            f
            for f in os.listdir(config_dir_path)
            if f.startswith("config_") and f.endswith(".parquet")
        ]
        if not config_files:
            current_run.log_warning(
                f"Aucun fichier de configuration de nouvelle campagne trouvé dans le dossier {CONFIG_PATH}. Aucune configuration de nouvelle campagne ne sera ajoutée aux données combinées."
            )
            return (
                pd.DataFrame()
            )  # Return an empty DataFrame if no config files are found
        else:
            current_run.log_info(
                f"Fichiers de configuration de nouvelle campagne trouvés: {config_files}."
            )
            combined_df = (
                pd.DataFrame()
            )  # Initialize an empty DataFrame to hold the combined data
            for config_file in config_files:
                config_path = os.path.join(config_dir_path, config_file)
                config_df = pd.read_parquet(config_path)
                combined_df = pd.concat([combined_df, config_df], ignore_index=True)

            combined_df = combined_df.drop_duplicates().reset_index(drop=True)

            current_run.log_info(
                "Configurations des nouvelles campagnes ajoutées avec succès au DataFrame combiné."
            )
            return combined_df
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'ajout des configurations des nouvelles campagnes au DataFrame combiné: {e}"
        )
        raise


def combine(
    df_1: pd.DataFrame,
    df_2: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine two dataframes containing the same structure.

    Args:
        df_1 (pd.DataFrame): First dataframe
        df_2 (pd.DataFrame): Second dataframe

    Returns:
        combined_df (pd.DataFrame): Combined DataFrame
    """
    current_run.log_info(
        "Combinaison de la structure des données attendue des campagnes historiques avec celle des nouvelles campagnes..."
    )
    try:
        combined_df = (
            pd.concat(
                [df_1, df_2],
                ignore_index=True,
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

        current_run.log_info("Combinaison des structures de données attendues réussie.")
        return combined_df
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la combinaison des structures de données attendues: {e}"
        )
        raise


def save_file(df: pd.DataFrame, file_name: str) -> None:
    """
    Save the cleaned org unit tree data to a parquet file.

    Args:
        df (pd.DataFrame): DataFrame containing the cleaned org unit tree data.
        file_name (str): Name of the file to save the DataFrame as.

    Returns:
        None
    """
    current_run.log_info("Enregistrement du fichier dans l'espace de travail...")
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
        current_run.log_info(f"Fichier enregistré avec succès: {file_path}")
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'enregistrement du fichier: {e}")
        raise e


if __name__ == "__main__":
    combine_expected_data_structures()
