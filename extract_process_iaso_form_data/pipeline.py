import datetime
import os
import pandas as pd
import numpy as np
from openhexa.sdk import current_run, workspace, pipeline
from pathlib import Path
from config import (
    iaso_connector_slug,
    iaso_form_id,
    iaso_extracted_data_path,
    outputs_path,
    campaign_name_cleaning_dict,
    campaign_name_mapping_dict,
    cols_campaign_map,
)
from utils import (
    IASOConnectionHandler,
    round_assignment,
    year_assignment,
)


@pipeline("extract_process_iaso_form_data")
def extract_process_iaso_form_data():
    """
    Main pipeline function to extract and process IASO form data.
    """
    # data imports
    iaso_org_unit_tree_clean = import_clean_iaso_org_unit_tree()
    iaso_org_unit_tree_raw = import_raw_iaso_org_unit_tree()

    # data extraction
    extract_iaso_data_for_current_month()
    extract_iaso_data_for_other_months()

    # data processing
    combined_df = combine_historical_and_current_data()
    combined_df = retrieve_org_unit_ids(
        combined_df, iaso_org_unit_tree_raw, iaso_org_unit_tree_clean
    )
    combined_df = clean_combined_df(combined_df)

    # save output
    save_output(combined_df)


def extract_iaso_data_for_current_month() -> None:
    """
    Extrait les données IASO pour le mois en cours et met à jour le fichier local.
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
    except Exception as e:
        current_run.log_error(f"Erreur d'initialisation du connecteur : {str(e)}")
        return

    current_period_start_date = f"{current_year}-{current_month:02d}-01"

    try:
        current_df = iaso_connector_instance.extract_submissions_info(
            form_id=iaso_form_id, dateFrom=current_period_start_date
        )

        if current_df is None or current_df.empty:
            current_run.log_warning(
                f"Aucune soumission trouvée pour {current_period_str}. Le fichier existant ne sera pas modifié."
            )
            return

        clean_extract_path = iaso_extracted_data_path.lstrip("/")
        file_name = f"multicampaign_df_{current_period_str}_raw.feather"

        file_path = os.path.join(workspace.files_path, clean_extract_path, file_name)
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        current_df.to_feather(file_path)
        current_run.log_info(
            f"Mise à jour réussie : {file_path} ({len(current_df)} lignes)."
        )

    except Exception as e:
        current_run.log_error(
            f"Erreur critique lors de l'extraction du mois en cours : {str(e)}"
        )


def extract_iaso_data_for_other_months() -> None:
    """
    Extract data from IASO for all months from 2024 up to the current date,
    skipping months that already have a saved file.
    """
    now = datetime.datetime.today()
    current_month = now.month
    current_year = now.year
    current_period_str = f"{current_year}-{current_month:02d}"

    current_run.log_info(
        f"Vérification des données historiques IASO (Excluant {current_period_str})..."
    )

    try:
        iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
        iaso_connector_instance.get_data_structure_from_the_form(iaso_form_id)
    except Exception as e:
        current_run.log_error(f"Échec de l'initialisation du connecteur IASO: {e}")
        return

    clean_extract_path = iaso_extracted_data_path.lstrip("/")
    folder_path = os.path.join(workspace.files_path, clean_extract_path)
    existing_files = os.listdir(folder_path) if os.path.exists(folder_path) else []

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

            current_run.log_info(f"Extraction des données IASO pour {period_str}...")

            try:
                month_df = iaso_connector_instance.extract_submissions_info(
                    form_id=iaso_form_id,
                    dateFrom=period_start_date.strftime("%Y-%m-%d"),
                    dateTo=period_end_date.strftime("%Y-%m-%d"),
                )

                if month_df is None or month_df.empty:
                    current_run.log_info(f"Aucune donnée trouvée pour {period_str}.")
                    continue

                # Save the file
                file_path = os.path.join(folder_path, expected_file_name)
                Path(file_path).parent.mkdir(parents=True, exist_ok=True)

                month_df.to_feather(file_path)
                current_run.log_info(f"Sauvegardé : {expected_file_name}")

            except Exception as e:
                current_run.log_error(
                    f"Erreur lors de l'extraction de {period_str}: {str(e)}"
                )
                continue


def combine_historical_and_current_data() -> pd.DataFrame:
    """
    Combine les données historiques et la période actuelle dans un seul DataFrame
    avec gestion des doublons et validation de la structure.
    """
    clean_path = iaso_extracted_data_path.lstrip("/")
    folder_path = os.path.join(workspace.files_path, clean_path)

    # 1. Vérification de l'existence du dossier
    if not os.path.exists(folder_path):
        current_run.log_error(f"Le dossier de données n'existe pas : {folder_path}")
        return pd.DataFrame()

    dataframes_list = []

    # 2. Collecte efficace des fichiers
    for file in os.listdir(folder_path):
        if file.endswith(".feather") and not file.startswith("~$"):
            file_path = os.path.join(folder_path, file)

            try:
                current_run.log_info(f"Lecture de : {file}")
                df = pd.read_feather(file_path)

                if not df.empty:
                    dataframes_list.append(df)
                else:
                    current_run.log_info(f"Fichier vide ignoré : {file}")

            except Exception as e:
                current_run.log_error(f"Erreur lors de la lecture de {file} : {e}")
                continue

    # 3. Combinaison unique
    if not dataframes_list:
        current_run.log_warning("Aucune donnée trouvée dans les fichiers Feather.")
        return pd.DataFrame()

    combined_df = pd.concat(dataframes_list, ignore_index=True)

    # 4. Gestion robuste des doublons (basée sur UUID)
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
        current_run.log_warning(
            "La colonne 'uuid' est absente. Impossible de dédoublonner."
        )

    # 5. Alignement avec la structure attendue du formulaire IASO
    try:
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
    except Exception as e:
        current_run.log_error(
            f"Impossible de vérifier la structure via l'API IASO : {e}"
        )

    current_run.log_info(f"Combinaison terminée. Taille finale : {combined_df.shape}")

    return combined_df


def import_raw_iaso_org_unit_tree() -> pd.DataFrame:
    """
    Import raw IASO organisation tree from Parquet file.

    Args:
        None
    Returns:
        pd.DataFrame: DataFrame containing the raw IASO organisation tree.
    """
    current_run.log_info("Importing raw IASO organisation tree...")

    file_path = os.path.join(
        workspace.files_path,
        outputs_path,
        "iaso_org_unit_tree_raw.parquet",
    )
    try:
        iaso_org_unit_tree_raw = pd.read_parquet(file_path)
    except FileNotFoundError:
        current_run.log_error(f"Fichier non trouvé : {file_path}")
        return pd.DataFrame()

    return iaso_org_unit_tree_raw


def import_clean_iaso_org_unit_tree() -> pd.DataFrame:
    """
    Import clean IASO organisation tree from Parquet file.

    Args:
        None
    Returns:
        pd.DataFrame: DataFrame containing the clean IASO organisation tree.
    """
    current_run.log_info("Importing clean IASO organisation tree...")

    file_path = os.path.join(
        workspace.files_path,
        outputs_path,
        "iaso_org_unit_tree_clean.parquet",
    )

    try:
        iaso_org_unit_tree_clean = pd.read_parquet(file_path)
    except FileNotFoundError:
        current_run.log_error(f"Fichier non trouvé : {file_path}")
        return pd.DataFrame()

    return iaso_org_unit_tree_clean


def retrieve_org_unit_ids(
    combined_df: pd.DataFrame,
    iaso_org_unit_tree_raw: pd.DataFrame,
    iaso_org_unit_tree_clean: pd.DataFrame,
) -> pd.DataFrame:
    """
    Retrieve organization unit IDs from clean IASO organisation tree.

    Args:
        combined_df (pd.DataFrame): The combined DataFrame to update with org unit IDs.
        iaso_org_unit_tree_raw (pd.DataFrame): The raw IASO organisation tree DataFrame.
        iaso_org_unit_tree_clean (pd.DataFrame): The clean IASO organisation tree DataFrame.

    Returns:
        pd.DataFrame: The updated combined DataFrame with org unit IDs.
    """
    current_run.log_info("Retrieving organization unit IDs...")

    iaso_org_unit_tree_raw["LVL_6_UID"] = iaso_org_unit_tree_raw.groupby("LVL_6_NAME")[
        "LVL_6_UID"
    ].transform("first")

    uid_to_org_id_dict = iaso_org_unit_tree_clean.set_index("LVL_6_UID").to_dict()[
        "org_unit_id"
    ]
    iaso_org_unit_tree_raw["final_org_unit"] = iaso_org_unit_tree_raw["LVL_6_UID"].map(
        uid_to_org_id_dict
    )
    org_unit_to_final_org_unit_dict = iaso_org_unit_tree_raw.set_index(
        "org_unit_id"
    ).to_dict()["final_org_unit"]

    combined_df["org_unit_id"] = combined_df["org_unit_id"].map(
        org_unit_to_final_org_unit_dict
    )
    # remove entries with missing org_unit_id
    mask_missing_org_unit = combined_df["org_unit_id"].isna()
    missing_org_unit_entries = combined_df[mask_missing_org_unit]
    if not missing_org_unit_entries.empty:
        missing_org_unit_proportion = len(missing_org_unit_entries) / len(combined_df)
        current_run.log_warning(
            f"{len(missing_org_unit_entries)} entrées ({missing_org_unit_proportion:.2%}) contiennent des org_unit_id manquants. Ces entrées seront supprimées."
        )
        combined_df = combined_df[~mask_missing_org_unit].copy()

    combined_df.loc[:, "org_unit_id"] = combined_df["org_unit_id"].astype(np.int64)

    return combined_df


def clean_combined_df(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the combined DataFrame by:
        - formatting the period column
        - exploding the data to deal with multi-campaign entries
        - checking and removing entries with no or unknown campaign assigned
        - assigning rounds and checking for entries outside the range of campaign rounds
        - dropping entries whose period is outside the expected campaign periods
    Args:
        combined_df (pd.DataFrame): The combined DataFrame to be cleaned.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    current_run.log_info("Cleaning the combined DataFrame...")

    # format period
    combined_df["period"] = pd.to_datetime(combined_df["period"])

    # explode multi-campaign entries
    combined_df["choix_campagne"] = combined_df["choix_campagne"].replace(
        campaign_name_cleaning_dict
    )
    combined_df["choix_campagne"] = combined_df["choix_campagne"].str.split(" ")
    combined_df = combined_df.explode("choix_campagne").reset_index(drop=True)
    combined_df["choix_campagne"] = combined_df["choix_campagne"].map(
        campaign_name_mapping_dict
    )

    # set values of columns unrelated to a specific campaign to NaN (use cvrg_campaign_map dict to identify columns specific to each campaign)
    for campaign_name, cols in cols_campaign_map.items():
        cols_in_df = [c for c in cols if c in combined_df.columns]
        mask_not_campaign = combined_df["choix_campagne"] != campaign_name
        combined_df.loc[mask_not_campaign, cols_in_df] = np.nan

    # delete entries with no or unknown campaign assigned (i.e. within the mapping dict)
    na_campaigns = combined_df[combined_df["choix_campagne"].isna()]
    invalid_campaigns = combined_df[
        ~combined_df["choix_campagne"].isin(list(campaign_name_mapping_dict.values()))
    ]
    unknown_campaigns = pd.concat([na_campaigns, invalid_campaigns]).drop_duplicates()
    if not unknown_campaigns.empty:
        unknown_campaigns_proportion = len(unknown_campaigns) / len(combined_df)
        current_run.log_warning(
            f"{len(unknown_campaigns)} entrées ({unknown_campaigns_proportion:.2%}) contiennent des noms de campagne manquant ou invalide ({invalid_campaigns['choix_campagne'].unique().tolist()}). Ces entrées seront supprimées."
        )
    combined_df = combined_df[
        combined_df["choix_campagne"].isin(list(campaign_name_mapping_dict.values()))
    ]

    # check duplicates and remove them keeping the last entry
    duplicates_count = combined_df.duplicated(
        subset=["org_unit_id", "period", "choix_campagne"], keep=False
    ).sum()
    if duplicates_count > 0:
        duplicates_proportion = duplicates_count / len(combined_df)
        current_run.log_warning(
            f"{duplicates_count} entrées ({duplicates_proportion:.2%}) dupliquées pour la même UUID, org_unit_id, période et campagne après explosion. Les doublons seront supprimés en gardant la dernière entrée."
        )
        combined_df = combined_df.sort_values(
            ["uuid", "org_unit_id", "period", "choix_campagne"]
        ).drop_duplicates(
            subset=["uuid", "org_unit_id", "period", "choix_campagne"], keep="last"
        )

    # assign rounds and years
    combined_df = round_assignment(combined_df)
    year_assignment(combined_df)

    ## drop entries that are not to be expected in the df:

    # rougeole 2024 --> these entries are artificially created due to the commonality of campaign with polio
    combined_df_updated_0 = combined_df.copy()
    mask_rougeole_2024 = (combined_df_updated_0["year"] == 2024) & (
        combined_df_updated_0["choix_campagne"] == "rougeole"
    )
    combined_df_updated_1 = combined_df_updated_0[~mask_rougeole_2024].copy()

    # date invalide
    mask_date_invalide = combined_df_updated_1["round"] == "date invalide"
    combined_df_updated_2 = combined_df_updated_1[~mask_date_invalide].copy()
    count_invalide = mask_date_invalide.sum()
    if count_invalide > 0:
        proportion_date_invalide = count_invalide / len(combined_df_updated_1)
        current_run.log_warning(
            f"{count_invalide} entrées ({proportion_date_invalide:.2%}) ont été supprimées "
            f"car la période est en dehors de la période de campagne."
        )

    return combined_df_updated_2


def save_output(combined_df: pd.DataFrame):
    """
    Save the combined campaign dataset to a Parquet file.

    Args:
        combined_df (pd.DataFrame): DataFrame containing the combined campaign data.

    Returns:
        None
    """
    current_run.log_info("Saving combined IASO data...")

    output_path = os.path.join(
        workspace.files_path, "niger_june_24", "outputs", "combined_iaso_data.parquet"
    )
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(output_path, index=False)
    current_run.log_info(f"Combined IASO data saved to {output_path}.")


if __name__ == "__main__":
    extract_process_iaso_form_data()
