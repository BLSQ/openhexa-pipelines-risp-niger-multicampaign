"""
Combining every per-month feather extract saved by pipeline.py's monthly extraction
steps into one dataframe: concatenation, uuid-deduplication, and filling in any
column the form structure expects but a given month's extract didn't produce.
"""

import os

import numpy as np
import pandas as pd
from openhexa.sdk import current_run

from config import IASO_EXTRACTION_PATH, iaso_connector_slug, iaso_form_id
from iaso_client import IASOConnectionHandler


def process_historical_and_current_data() -> pd.DataFrame:
    """
    Combine all the historical and current month data extracted from IASO,
    handling duplicates and ensuring alignment with the expected form structure.

    Args:
        None

    Returns:
        pd.DataFrame: Combined DataFrame containing all the extracted data from IASO.
    """
    current_run.log_info("Combinaison des données historiques et du mois en cours...")

    # Checking if the extraction folder exists
    if not os.path.exists(IASO_EXTRACTION_PATH):
        msg = f"Le dossier de données n'existe pas : {IASO_EXTRACTION_PATH}"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    dataframes_list = []
    try:
        # Collecting all feather files in the extraction folder
        for file in os.listdir(IASO_EXTRACTION_PATH):
            if file.endswith(".feather") and not file.startswith("~$"):
                current_run.log_info(f"Lecture du fichier : {file}")
                file_path = os.path.join(IASO_EXTRACTION_PATH, file)
                df = pd.read_feather(file_path)

                if not df.empty:
                    dataframes_list.append(df)
                else:
                    current_run.log_warning(f"Fichier ignoré : {file}")
                    continue

        # Combining all dataframes into one
        if not dataframes_list:
            current_run.log_warning(
                "Aucune donnée trouvée dans les fichiers Feather. Un dataframe vide sera retourné."
            )
            return pd.DataFrame()

        combined_df = pd.concat(dataframes_list, ignore_index=True)

        # Checking for duplicates based on 'uuid' column and keeping the first occurrence
        if "uuid" in combined_df.columns:
            duplicates = combined_df.duplicated(subset=["uuid"], keep="first")
            duplicates_count = duplicates.sum()

            if duplicates_count > 0:
                total = len(combined_df)
                current_run.log_warning(
                    f"{duplicates_count} doublons détectés ({duplicates_count / total:.2%}). "
                    "Suppression en gardant la première occurrence."
                )
                combined_df = combined_df[~duplicates].reset_index(drop=True)
        else:
            msg = "La colonne 'uuid' est absente. Impossible de dédoublonner."
            current_run.log_error(msg)
            raise KeyError(msg)

        # Making sure the combined dataframe has all the expected columns based on the form structure
        iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
        iaso_connector_instance.get_data_structure_from_the_form(iaso_form_id)
        expected_columns = iaso_connector_instance.form_data_structure_df.name.unique()

        missing_cols = [
            col for col in expected_columns if col not in combined_df.columns
        ]

        if missing_cols:
            current_run.log_warning(
                f"{len(missing_cols)} colonnes manquantes ajoutées (NaN)."
            )
            for col in missing_cols:
                combined_df[col] = np.nan

        current_run.log_info(
            f"Combinaison des données réussie. Nombre total de lignes après combinaison : {len(combined_df)}"
        )

        return combined_df
    except KeyError:
        raise
    except Exception as e:
        msg = f"Erreur critique lors de la combinaison des données historiques et du mois en cours : {str(e)}"
        current_run.log_error(msg)
        raise
