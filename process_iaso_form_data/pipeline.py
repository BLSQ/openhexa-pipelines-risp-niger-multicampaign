import os
import pandas as pd
import numpy as np
from openhexa.sdk import current_run, pipeline, workspace
from config import (
    OUTPUTS_PATH,
    campaign_name_cleaning_dict,
    campaign_name_mapping_dict,
    campaign_product_name_mapping_dict,
    months_mapping_dict,
    cols_campaign_map,
)


@pipeline(
    "process_iaso_form_data",
    name="multi-campagne - Traitement des données du formulaire IASO",
)
def process_iaso_form_data():
    """
    This pipeline processes the raw combined IASO form data by retrieving org unit IDs,
    cleaning the data, and saving the cleaned DataFrame for future use.
    It performs the following steps:
        1. Imports the necessary data files (raw combined IASO data, clean and raw org unit trees, expected data structure).
        2. Retrieves org unit IDs associated with CSI names and updates the combined DataFrame.
        3. Cleans the combined DataFrame by formatting periods, exploding multi-campaign entries, checking for valid campaign names, removing duplicates, and filtering out entries with invalid periods.
        4. Saves the cleaned combined DataFrame as a parquet file in the outputs directory.
    """
    # data imports
    iaso_org_unit_tree_clean = load_data("iaso_org_unit_tree_clean")
    iaso_org_unit_tree_raw = load_data("iaso_org_unit_tree_raw")
    expected_data_structure = load_data("expected_data_structure")
    iaso_raw_df = load_data("combined_iaso_data_raw")

    # data processing
    iaso_processed_df = align_to_clean_org_tree(
        iaso_raw_df, iaso_org_unit_tree_raw, iaso_org_unit_tree_clean
    )
    iaso_processed_df = clean_combined_df(iaso_processed_df, expected_data_structure)

    # save output
    save_file(iaso_processed_df, "combined_iaso_data")
    export_to_dataset(iaso_processed_df, OUTPUTS_PATH, "combined_iaso_data")


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


def align_to_clean_org_tree(
    iaso_raw_df: pd.DataFrame,
    iaso_org_unit_tree_raw: pd.DataFrame,
    iaso_org_unit_tree_clean: pd.DataFrame,
) -> pd.DataFrame:
    """
    Standardizes org unit IDs in the submission data by mapping them to the 'Clean'
    org unit tree.

    Args:
        iaso_raw_df (pd.DataFrame): The dataframe containing the raw data from the IASO multi-campaign form
        iaso_org_unit_tree_raw (pd.DataFrame): The raw IASO organisation tree DataFrame containing all org units names and IDs
        iaso_org_unit_tree_clean (pd.DataFrame): The clean IASO organisation tree DataFrame containing the clean org units names and IDs.

    Returns:
        iaso_processed_df (pd.DataFrame): The dataframe containing the processed data from the IASO multi-campaign form with org unit IDs retrieved.
    """
    current_run.log_info(
        "Récupération des identifiants des unités organisationnelles..."
    )
    try:
        iaso_org_unit_tree_raw["LVL_6_UID"] = iaso_org_unit_tree_raw.groupby(
            "LVL_6_NAME"
        )["LVL_6_UID"].transform("first")

        uid_to_org_id_dict = iaso_org_unit_tree_clean.set_index("LVL_6_UID").to_dict()[
            "org_unit_id"
        ]
        iaso_org_unit_tree_raw["final_org_unit"] = iaso_org_unit_tree_raw[
            "LVL_6_UID"
        ].map(uid_to_org_id_dict)
        org_unit_to_final_org_unit_dict = iaso_org_unit_tree_raw.set_index(
            "org_unit_id"
        ).to_dict()["final_org_unit"]

        iaso_raw_df["org_unit_id"] = iaso_raw_df["org_unit_id"].map(
            org_unit_to_final_org_unit_dict
        )
        # remove entries with missing org_unit_id
        mask_missing_org_unit = iaso_raw_df["org_unit_id"].isna()
        missing_org_unit_entries = iaso_raw_df[mask_missing_org_unit]
        if not missing_org_unit_entries.empty:
            missing_org_unit_proportion = len(missing_org_unit_entries) / len(
                iaso_raw_df
            )
            current_run.log_warning(
                f"{len(missing_org_unit_entries)} entrées ({missing_org_unit_proportion:.2%}) contiennent des org_unit_id manquants. Ces entrées seront supprimées."
            )
            iaso_raw_df = iaso_raw_df[~mask_missing_org_unit].copy()

        iaso_raw_df.loc[:, "org_unit_id"] = iaso_raw_df["org_unit_id"].astype(np.int64)

        iaso_processed_df = iaso_raw_df.copy()

        current_run.log_info(
            "Récupération des identifiants des unités organisationnelles terminée avec succès."
        )

        return iaso_processed_df
    except Exception as e:
        msg = f"Erreur lors de la récupération des identifiants des unités organisationnelles : {str(e)}"
        current_run.log_error(msg)
        raise


def clean_combined_df(
    iaso_processed_df: pd.DataFrame, expected_data_structure: pd.DataFrame
) -> pd.DataFrame:
    """
    Clean the combined DataFrame by:
        - formatting the period column
        - exploding the data to deal with multi-campaign entries
        - checking and removing entries with no or unknown campaign assigned
        - assigning rounds and checking for entries outside the range of campaign rounds
        - dropping entries whose period is outside the expected campaign periods
        - adding the column 'month' identifying the month when each campaign round starts
    Args:
        iaso_processed_df (pd.DataFrame): The dataframe containing the processed data from the IASO multi-campaign form.
        expected_data_structure (pd.DataFrame): DataFrame containing the expected data structure.

    Returns:
        iaso_processed_df (pd.DataFrame): The cleaned dataframe containing the processed data from the IASO multi-campaign form.
    """
    current_run.log_info("Nettoyage du DataFrame combiné...")
    try:
        # format period
        iaso_processed_df["period"] = pd.to_datetime(iaso_processed_df["period"])

        # explode multi-campaign entries
        iaso_processed_df["choix_campagne"] = iaso_processed_df[
            "choix_campagne"
        ].replace(campaign_name_cleaning_dict)
        iaso_processed_df["choix_campagne"] = iaso_processed_df[
            "choix_campagne"
        ].str.split(" ")
        iaso_processed_df = iaso_processed_df.explode("choix_campagne").reset_index(
            drop=True
        )
        iaso_processed_df["choix_campagne"] = iaso_processed_df["choix_campagne"].map(
            campaign_name_mapping_dict
        )

        # set values of columns unrelated to a specific campaign to NaN
        for campaign_name, cols in cols_campaign_map.items():
            cols_in_df = [c for c in cols if c in iaso_processed_df.columns]
            mask_not_campaign = iaso_processed_df["choix_campagne"] != campaign_name
            iaso_processed_df.loc[mask_not_campaign, cols_in_df] = np.nan

        # delete entries with no or unknown campaign assigned (i.e. within the mapping dict)
        na_campaigns = iaso_processed_df[iaso_processed_df["choix_campagne"].isna()]
        invalid_campaigns = iaso_processed_df[
            ~iaso_processed_df["choix_campagne"].isin(
                list(campaign_name_mapping_dict.values())
            )
        ]
        unknown_campaigns = pd.concat(
            [na_campaigns, invalid_campaigns]
        ).drop_duplicates()
        if not unknown_campaigns.empty:
            unknown_campaigns_proportion = len(unknown_campaigns) / len(
                iaso_processed_df
            )
            current_run.log_warning(
                f"{len(unknown_campaigns)} entrées ({unknown_campaigns_proportion:.2%}) contiennent des noms de campagne manquant ou invalide ({invalid_campaigns['choix_campagne'].unique().tolist()}). Ces entrées seront supprimées."
            )
        iaso_processed_df = iaso_processed_df[
            iaso_processed_df["choix_campagne"].isin(
                list(campaign_name_mapping_dict.values())
            )
        ]

        # check duplicates and remove them keeping the last entry
        duplicates_df = iaso_processed_df.duplicated(
            subset=["uuid", "org_unit_id", "period", "choix_campagne"], keep=False
        )
        duplicates_count = duplicates_df.sum()
        if duplicates_count > 0:
            duplicates_proportion = duplicates_count / len(iaso_processed_df)
            current_run.log_warning(
                f"{duplicates_count} entrées ({duplicates_proportion:.2%}) dupliquées pour la même UUID, org_unit_id, période et campagne. Les doublons seront supprimés en gardant la dernière entrée."
            )
            iaso_processed_df = iaso_processed_df.sort_values(
                ["uuid", "org_unit_id", "period", "choix_campagne"]
            ).drop_duplicates(
                subset=["uuid", "org_unit_id", "period", "choix_campagne"], keep="last"
            )

        # drop entries that are not in the expected campaign periods
        expected_periods = expected_data_structure[
            ["produit", "period", "year", "round"]
        ].drop_duplicates()
        expected_periods["choix_campagne"] = expected_periods["produit"].map(
            campaign_product_name_mapping_dict
        )
        expected_periods = expected_periods.drop(columns=["produit"]).drop_duplicates()
        iaso_processed_df = iaso_processed_df.merge(
            expected_periods[["choix_campagne", "period", "year", "round"]],
            on=["choix_campagne", "period"],
            how="left",
            validate="many_to_one",
            indicator=True,
        )

        # summarize affected entries
        date_invalide_mask = iaso_processed_df["_merge"] == "left_only"
        invalid_count = date_invalide_mask.sum()
        if invalid_count > 0:
            proportion_date_invalide = invalid_count / len(iaso_processed_df)
            current_run.log_warning(
                f"{invalid_count} entrées ({proportion_date_invalide:.2%}) ont été supprimées "
                f"car la période est en dehors de la période de campagne."
            )
            iaso_processed_invalid_df = iaso_processed_df[date_invalide_mask].copy()
            iaso_processed_invalid_df["year"] = iaso_processed_invalid_df[
                "period"
            ].dt.year
            invalid_entries_summary = (
                iaso_processed_invalid_df.groupby(["choix_campagne", "year"])
                .size()
                .reset_index(name="count")
            )
            total_count = iaso_processed_df.shape[0]
            invalid_entries_summary["proportion"] = round(
                (invalid_entries_summary["count"] / total_count) * 100, 1
            )

            current_run.log_warning(
                f"Résumé des entrées avec période invalide par campagne et année :\n{invalid_entries_summary}"
            )
        iaso_processed_df = iaso_processed_df[~date_invalide_mask].drop(
            columns=["_merge"]
        )

        # adding the column 'month' identifying the month when each campaign round starts (expressed in str names for better readability)
        # the month corresponds to the month of the very first date in the period column when the data are grouped by choix_campagne, year and round
        iaso_processed_df["month"] = (
            iaso_processed_df.groupby(["choix_campagne", "year", "round"])["period"]
            .transform("min")
            .dt.month.map(months_mapping_dict)
        )
        current_run.log_info("Nettoyage du DataFrame combiné terminé avec succès.")

        return iaso_processed_df
    except Exception as e:
        msg = f"Erreur lors du nettoyage du DataFrame combiné : {str(e)}"
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
    process_iaso_form_data()
