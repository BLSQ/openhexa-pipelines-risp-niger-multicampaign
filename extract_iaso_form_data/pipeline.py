import datetime
import os
import pandas as pd
import numpy as np
from openhexa.sdk import current_run, pipeline
from pathlib import Path
from shared_utils import (
    save_file,
    export_to_dataset,
)

from config import (
    OUTPUTS_PATH,
    IASO_EXTRACTION_PATH,
    iaso_connector_slug,
    iaso_form_id,
)
from utils import (
    IASOConnectionHandler,
)


@pipeline(
    "extract_iaso_form_data",
    name="multi-campagne - Extraction des données du formulaire IASO",
)
def extract_iaso_form_data():
    """
    Main pipeline function to extract and process IASO form data
    """
    extract_iaso_data_for_current_month()
    extract_iaso_data_for_other_months()
    combined_df = process_historical_and_current_data()
    save_file(combined_df, "combined_iaso_data_raw")
    export_to_dataset(
        combined_df,
        OUTPUTS_PATH,
        "combined_iaso_data_raw",
    )


def extract_iaso_data_for_current_month() -> None:
    """
    Extracts data from IASO for the current month and saves it as a feather file in the IASO_EXTRACTION_PATH.
    If no data is found, the existing file will not be modified.

    Args:
        None

    Returns:
        None
    """
    now = datetime.datetime.today()
    current_month = now.month
    current_year = now.year
    current_period_str = f"{current_year}-{current_month:02d}"

    current_run.log_info(
        f"Début de l'extraction IASO pour le mois en cours : {current_period_str}..."
    )
    try:
        iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
        iaso_connector_instance.get_data_structure_from_the_form(iaso_form_id)

        current_period_start_date = f"{current_year}-{current_month:02d}-01"

        current_df = iaso_connector_instance.extract_submissions_info(
            form_id=iaso_form_id, dateFrom=current_period_start_date
        )

        if current_df is None or current_df.empty:
            current_run.log_warning(
                f"Aucune soumission trouvée pour {current_period_str}. Le fichier existant ne sera pas modifié."
            )
            return

        file_name = f"multicampaign_df_{current_period_str}_raw.feather"

        file_path = os.path.join(IASO_EXTRACTION_PATH, file_name)
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        current_df.to_feather(file_path)

        current_run.log_info(
            f"Données IASO pour {current_period_str} extraites et sauvegardées avec succès."
        )

    except Exception as e:
        msg = f"Erreur critique lors de l'extraction du mois en cours : {str(e)}"
        current_run.log_error(msg)
        raise


def extract_iaso_data_for_other_months() -> None:
    """
    Extract data from IASO for all months from 2024 up to the current date,
    skipping months that already have a saved file, and saving each month's data as
    a feather file in the IASO_EXTRACTION_PATH.

    Args:
        None

    Returns:
        None
    """
    now = datetime.datetime.today()
    current_month = now.month
    current_year = now.year
    current_period_str = f"{current_year}-{current_month:02d}"

    current_run.log_info(
        f"Extraction des données historiques IASO (Excluant {current_period_str})..."
    )

    try:
        iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
        iaso_connector_instance.get_data_structure_from_the_form(iaso_form_id)

        existing_files = (
            os.listdir(IASO_EXTRACTION_PATH)
            if os.path.exists(IASO_EXTRACTION_PATH)
            else []
        )

        for year in range(2024, current_year + 1):
            for month in range(1, 13):
                if year == current_year and month >= current_month:
                    continue

                period_str = f"{year}-{month:02d}"
                expected_file_name = f"multicampaign_df_{period_str}_raw.feather"

                if expected_file_name in existing_files:
                    continue

                period_start_date = datetime.date(year, month, 1)
                if month == 12:
                    period_end_date = datetime.date(year + 1, 1, 1)
                else:
                    period_end_date = datetime.date(year, month + 1, 1)

                current_run.log_info(
                    f"Extraction des données IASO pour {period_str}..."
                )

                try:
                    month_df = iaso_connector_instance.extract_submissions_info(
                        form_id=iaso_form_id,
                        dateFrom=period_start_date.strftime("%Y-%m-%d"),
                        dateTo=period_end_date.strftime("%Y-%m-%d"),
                    )

                    if month_df is None or month_df.empty:
                        current_run.log_info(
                            f"Aucune donnée trouvée pour {period_str}."
                        )
                        continue

                    # Save the file
                    file_path = os.path.join(IASO_EXTRACTION_PATH, expected_file_name)
                    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

                    month_df.to_feather(file_path)

                    current_run.log_info(
                        f"Extraction des données IASO réussie pour {period_str} : {expected_file_name}"
                    )

                except Exception as e:
                    current_run.log_error(
                        f"Erreur lors de l'extraction de {period_str}: {str(e)}"
                    )
                    continue
    except Exception as e:
        msg = f"Erreur critique lors de l'extraction des données historiques (Excluant {current_period_str}) : {str(e)}"
        current_run.log_error(msg)
        raise


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


if __name__ == "__main__":
    extract_iaso_form_data()
