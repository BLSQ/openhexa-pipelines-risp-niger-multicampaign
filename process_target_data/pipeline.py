import os
import re
from openhexa.sdk import current_run, pipeline, workspace
import pandas as pd
import numpy as np
from utils import (
    org_unit_matching,
)
from config import (
    OUTPUTS_PATH,
    TARGET_OTHER_DATA_PATH,
    TEMP_PATH,
    templates_required_cols_csi,
    templates_required_cols_district,
    campaign_rename_dict,
    cols_for_melting,
    csi_district_rename_dict,
    csi_matching_failed,
)


@pipeline(
    "process_target_data",
    name="multi-campagne - 02 Pipeline d'importation et traitement des données de cibles",
)
def process_target_data():
    """
    Main pipeline function to process target data from various campaigns
    """
    # load org unit tree data
    iaso_org_unit_tree_df = load_data("iaso_org_unit_tree_raw")
    iaso_org_unit_tree_df_clean = load_data("iaso_org_unit_tree_clean")

    # process target data from templates
    all_target_data_csi_combined, all_target_data_district_combined = (
        import_target_data_for_future_campaigns()
    )
    if not all_target_data_csi_combined.empty:
        all_target_data_csi_combined = match_csi_to_org_unit_id(
            all_target_data_csi_combined, iaso_org_unit_tree_df_clean
        )
    if not all_target_data_district_combined.empty:
        all_target_data_district_combined = match_district_to_org_unit_id(
            all_target_data_district_combined, iaso_org_unit_tree_df_clean
        )

    # load historical target data
    historical_target_data = load_data("combined_historical_target_data")

    # combine all target data
    target_data_configured_combined = combine_target_data(
        [
            all_target_data_csi_combined,
            all_target_data_district_combined,
        ]
    )

    target_data_combined = combine_target_data(
        [
            target_data_configured_combined,
            historical_target_data,
        ]
    )

    # add campaign rounds for configured target data
    target_data_combined = add_round_info_to_configured_target_data(
        target_data_combined
    )

    # clean up org unit
    target_data_combined = clean_org_unit_id(
        target_data_combined, iaso_org_unit_tree_df, iaso_org_unit_tree_df_clean
    )

    # save
    save_file(target_data_configured_combined, "combined_configured_target_data")
    save_file(target_data_combined, "combined_target_data")
    export_to_dataset(target_data_combined, "combined_target_data")


def load_data(name: str) -> pd.DataFrame:
    """
    Import data from a specified file in the outputs directory.
    The file should be in parquet format and the name should be provided without the extension.

    Args:
        name (str): Name of the file to be imported (without extension).
    Returns:
        pd.DataFrame: DataFrame containing the imported data.
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


def process_dataframe(df: pd.DataFrame, aggregation_type: str, meta: dict):
    """
    This function processes the input DataFrame by melting it from wide to long format,
    extracting age and site/strategy information, and cleaning up the resulting DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame to be processed.
        aggregation_type (str): The type of aggregation, either "csi" or "district".
        meta (dict): A dictionary containing metadata about the DataFrame, such as the source file name.

    Returns:
        pd.DataFrame: The processed DataFrame in long format with extracted age and site/strategy information.
    """
    id_vars = cols_for_melting.copy()
    if aggregation_type == "csi":
        id_vars.insert(1, "CSI")

    required = (
        templates_required_cols_csi
        if aggregation_type == "csi"
        else templates_required_cols_district
    )
    if not all(col in df.columns for col in required):
        raise ValueError(f"Colonnes manquantes. Attendu: {required}")

    regex_pattern = r"^Cible (\d+-\d+ (?:mois|ans))$"
    extra_cols = [col for col in df.columns if col not in required]
    extra_cols = [col for col in extra_cols if col not in ["year", "produit"]]
    invalid_format_cols = [
        col for col in extra_cols if not re.match(regex_pattern, col)
    ]

    if invalid_format_cols:
        raise ValueError(
            f"Format de colonne invalide détecté : {invalid_format_cols}. "
            "Les colonnes de données doivent suivre le format: 'Cible [age] mois/ans'"
        )

    value_vars = [col for col in df.columns if col.startswith("Cible ")]

    df_melted = pd.melt(
        df,
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="age",
        value_name="cible",
    )

    extracted = df_melted["age"].str.extract(regex_pattern)

    df_melted["age"] = extracted[0]
    df_melted = df_melted.rename(columns=csi_district_rename_dict)

    return df_melted


def import_target_data_for_future_campaigns():
    """
    Placeholder function for importing target data for future campaigns.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for future campaigns.
    """
    current_run.log_info(
        "Importation et traitement des données de cibles générées par les fichiers templates..."
    )
    try:
        if not os.path.exists(TARGET_OTHER_DATA_PATH):
            os.makedirs(TARGET_OTHER_DATA_PATH)
            return pd.DataFrame(), pd.DataFrame()

        all_data = {"csi": [], "district": []}

        file_pattern = re.compile(r"Cibles_([^_]+)_(\d{4})_(.+)\.xlsx")

        for entry in os.scandir(TARGET_OTHER_DATA_PATH):
            if not (
                entry.is_file()
                and entry.name.endswith(".xlsx")
                and not entry.name.startswith("~$")
            ):
                continue

            match = file_pattern.match(entry.name)
            if not match:
                current_run.log_error(f"Format de nommage invalide : '{entry.name}'")
                raise ValueError(f"Format de nommage invalide : '{entry.name}'")

            # Extract groups based on file name convention e.g. Cibles_Rougeole_2026_agadez_diffa_dosso_csi.xlsx
            campaign_raw, year, middle_part = match.groups()
            parts = middle_part.split("_")
            agg = parts[-1].lower()

            if campaign_raw not in campaign_rename_dict:
                current_run.log_error(
                    f"Campagne invalide '{campaign_raw}' dans {entry.name}"
                )
                raise ValueError(
                    f"Campagne invalide '{campaign_raw}' dans {entry.name}"
                )

            # Ensure the aggregation level is one we expect (csi or district)
            if agg not in all_data:
                current_run.log_error(
                    f"Niveau d'agrégation '{agg}' non supporté dans {entry.name}"
                )
                raise ValueError(
                    f"Niveau d'agrégation '{agg}' doit être 'csi' ou 'district'."
                )

            try:
                df = pd.read_excel(entry.path)

                if df.empty:
                    current_run.log_error(f"Le fichier {entry.name} est vide.")
                    raise ValueError(f"Le fichier {entry.name} est vide.")

                target_cols = [col for col in df.columns if col.startswith("Cible ")]

                if not target_cols:
                    current_run.log_error(
                        f"Aucune colonne de cible trouvée dans le fichier {entry.name}."
                    )
                    raise

                # Validate numeric data
                for col in target_cols:
                    non_empty_values = df[col].dropna()
                    check_numeric = pd.to_numeric(non_empty_values, errors="coerce")
                    if check_numeric.isna().any():
                        current_run.log_error(
                            f"Le fichier {entry.name} contient des valeurs non numériques dans la colonne {col}."
                        )
                        raise

                # Metadata assignment
                df["year"] = int(year)
                df["produit"] = campaign_rename_dict.get(campaign_raw, campaign_raw)

                processed_df = process_dataframe(df, agg, {"file": entry.name})
                all_data[agg].append(processed_df)

                current_run.log_info(f"Fichier {entry.name} traité.")

            except Exception as e:
                current_run.log_error(f"Erreur sur {entry.name} : {str(e)}")
                raise ValueError(f"Erreur sur {entry.name} : {str(e)}")

        # Combine results
        df_csi = (
            pd.concat(all_data["csi"], ignore_index=True)
            if all_data["csi"]
            else pd.DataFrame()
        )
        df_dist = (
            pd.concat(all_data["district"], ignore_index=True)
            if all_data["district"]
            else pd.DataFrame()
        )

        current_run.log_info(
            f"Importation terminée: CSI: {len(all_data['csi'])}, District: {len(all_data['district'])}"
        )

        return df_csi, df_dist

    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'importation des données de cibles : {e}"
        )
        raise


def match_csi_to_org_unit_id(
    csi_level_target_df: pd.DataFrame, iaso_org_unit_tree_df_clean: pd.DataFrame
) -> pd.DataFrame:
    """
    Match CSI names in df containing the CSI-level target data to organizational unit IDs using spatial data.

    Args:
        csi_level_target_df (pd.DataFrame): DataFrame containing the target data at CSI level.
        iaso_org_unit_tree_df_clean (pd.DataFrame): DataFrame containing the clean organisational units tree data.

    Returns:
        pd.DataFrame: DataFrame with matched organizational unit IDs.
    """
    current_run.log_info(
        "Appariement des noms CSI aux identifiants des unités organisationnelles..."
    )
    try:
        iaso_org_unit_tree_for_matching = iaso_org_unit_tree_df_clean[
            ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
        ].drop_duplicates()

        target_df_matched, org_unit_tree_check = org_unit_matching(
            csi_level_target_df, iaso_org_unit_tree_for_matching, threshold=50
        )

        # inspect matching results
        target_df_matched_check = target_df_matched[
            [
                "org_unit_id",
                "LVL_3_NAME_original",
                "LVL_6_NAME_original",
                "LVL_3_NAME",
                "LVL_6_NAME",
                "cleansed_target",
                "cleansed_spatial_match",
                "match_score",
            ]
        ]
        target_df_matched_check.drop_duplicates(inplace=True)

        if not os.path.exists(TEMP_PATH):
            os.makedirs(TEMP_PATH)

        target_df_matched_check.to_csv(
            os.path.join(TEMP_PATH, "target_df_matched_check.csv"),
            index=False,
        )
        org_unit_tree_check.drop_duplicates(inplace=True)
        org_unit_tree_check.to_csv(
            os.path.join(TEMP_PATH, "org_unit_tree_check.csv"),
            index=False,
        )

        # manually correct matching failures
        for (
            csi_concat_original,
            csi_concat_correct,
        ) in csi_matching_failed.items():
            if csi_concat_correct is None:
                mask = target_df_matched["cleansed_target"] == csi_concat_original
                target_df_matched.loc[mask, "org_unit_id"] = None
                target_df_matched.loc[mask, "LVL_3_NAME"] = None
                target_df_matched.loc[mask, "LVL_6_NAME"] = None
                continue

            org_unit_tree_row = org_unit_tree_check.loc[
                org_unit_tree_check["cleansed_spatial"] == csi_concat_correct
            ]
            if org_unit_tree_row.empty:
                continue

            lvl_3_name_correct = org_unit_tree_row["LVL_3_NAME"].values[0]
            lvl_6_name_correct = org_unit_tree_row["LVL_6_NAME"].values[0]
            org_unit_id_correct = org_unit_tree_row["org_unit_id"].values[0]
            mask = target_df_matched["cleansed_target"] == csi_concat_original
            target_df_matched.loc[mask, "org_unit_id"] = org_unit_id_correct
            target_df_matched.loc[mask, "LVL_3_NAME"] = lvl_3_name_correct
            target_df_matched.loc[mask, "LVL_6_NAME"] = lvl_6_name_correct

        target_df_matched["LVL_6_NAME"] = np.where(
            target_df_matched["org_unit_id"].isna(),
            target_df_matched["LVL_6_NAME_original"],
            target_df_matched["LVL_6_NAME"],
        )

        unmatched_count = target_df_matched["org_unit_id"].isna().sum()
        total_count = len(target_df_matched)
        if unmatched_count > 0:
            unmatched_csis = target_df_matched[target_df_matched["org_unit_id"].isna()][
                "LVL_6_NAME_original"
            ].unique()
            current_run.log_warning(
                f"{unmatched_count} sur {total_count} entrées n'ont pas pu être appariés à un org_unit_id. "
                f"CSIs non appariés : {', '.join(map(str, unmatched_csis))}. "
                "Un appariement manuel est nécessaire pour ces entrées."
            )

        target_df_matched = target_df_matched.drop(
            columns=[
                "LVL_3_NAME_original",
                "LVL_6_NAME_original",
                "match_score",
                "cleansed_target",
                "cleansed_spatial_match",
            ]
        )
        target_df_matched = target_df_matched.dropna(subset=["org_unit_id"])

        return target_df_matched
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'appariement des noms CSI aux identifiants des unités organisationnelles: {e}"
        )
        raise


def match_district_to_org_unit_id(
    district_level_target_df: pd.DataFrame, iaso_org_unit_tree_df_clean: pd.DataFrame
) -> pd.DataFrame:
    """
    Match district names in df containing the district-level target data to organizational unit IDs using iaso_org_unit_tree data.

    Args:
        district_level_target_df (pd.DataFrame): DataFrame containing the target data at district level.
        iaso_org_unit_tree_df_clean (pd.DataFrame): DataFrame containing the clean organisational units tree data.

    Returns:
        pd.DataFrame: DataFrame with matched organizational unit IDs.
    """
    current_run.log_info("Matching district names to organizational unit IDs...")
    try:
        iaso_org_unit_tree_for_matching = iaso_org_unit_tree_df_clean[
            ["org_unit_id", "LVL_3_NAME"]
        ].drop_duplicates()
        iaso_org_unit_tree_for_matching = iaso_org_unit_tree_for_matching.groupby(
            "LVL_3_NAME", as_index=False
        ).first()

        target_df_matched = district_level_target_df.merge(
            iaso_org_unit_tree_for_matching, on=["LVL_3_NAME"], how="left"
        )

        unmatched_count = target_df_matched["org_unit_id"].isna().sum()
        total_count = len(target_df_matched)
        if unmatched_count > 0:
            unmatched_districts = target_df_matched[
                target_df_matched["org_unit_id"].isna()
            ]["LVL_3_NAME"].unique()
            current_run.log_warning(
                f"{unmatched_count} sur {total_count} entrées n'ont pas pu être appariés à un org_unit_id. "
                f"Districts non appariés : {', '.join(unmatched_districts)}"
                "Ces entrées seront supprimées des données cibles."
            )

        return target_df_matched
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'appariement des noms de districts aux identifiants des unités organisationnelles: {e}"
        )
        raise


def combine_target_data(
    dfs: list[pd.DataFrame],
) -> pd.DataFrame:
    """
    Combine multiple target data DataFrames into a single DataFrame.

    Args:
        dfs (list[pd.DataFrame]): List of DataFrames to be combined.

    Returns:
        pd.DataFrame: Combined DataFrame containing all target data.
    """
    current_run.log_info("Combinaison des différentes données de cibles...")
    try:
        target_data_combined = pd.concat(dfs, ignore_index=True)

        return target_data_combined

    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la combinaison des données de cibles: {e}"
        )
        raise ValueError(f"Erreur lors de la combinaison des données de cibles: {e}")


def clean_org_unit_id(
    target_data_combined: pd.DataFrame,
    iaso_org_unit_tree_df: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Clean the org_unit_id column in the combined target data, by assigning all the org_unit_ids in the raw org unit tree
    to the corresponding LVL_6_UID org_unit_id from the cleaned org unit tree.

    Args:
        target_data_combined (pd.DataFrame): DataFrame containing the combined target data.
        iaso_org_unit_tree_df (pd.DataFrame): DataFrame containing the org unit tree data.
        iaso_org_unit_tree_clean_df (pd.DataFrame): DataFrame containing the cleaned org unit tree data.

    Returns:
        pd.DataFrame: DataFrame with cleaned org_unit_id column.
    """
    current_run.log_info(
        "Récupération des identifiants des unités d'organisation et application de la correspondance un-à-plusieurs..."
    )
    try:
        uid_to_org_id_df_clean = iaso_org_unit_tree_clean_df[
            ["LVL_6_UID", "org_unit_id"]
        ].drop_duplicates()
        uid_to_org_id_df_raw = iaso_org_unit_tree_df.copy()
        uid_to_org_id_df_raw["LVL_6_UID"] = uid_to_org_id_df_raw.groupby("LVL_6_NAME")[
            "LVL_6_UID"
        ].transform("first")
        uid_to_org_id_df_raw = uid_to_org_id_df_raw[
            ["LVL_6_UID", "org_unit_id"]
        ].drop_duplicates()
        uid_to_org_id_df_raw = uid_to_org_id_df_raw.rename(
            columns={"org_unit_id": "final_org_unit_id"}
        )
        mapping_df = uid_to_org_id_df_clean.merge(
            uid_to_org_id_df_raw, on="LVL_6_UID", how="inner"
        )
        mapping_df = mapping_df[["org_unit_id", "final_org_unit_id"]].drop_duplicates()

        target_data_combined = pd.merge(
            target_data_combined,
            mapping_df,
            on="org_unit_id",
            how="left",
            indicator=True,
        )
        target_data_combined["org_unit_id"] = target_data_combined[
            "final_org_unit_id"
        ].fillna(target_data_combined["org_unit_id"])

        target_data_combined.drop(columns=["final_org_unit_id", "_merge"], inplace=True)

        # drop LVL_2_NAME col (not needed)
        target_data_combined = target_data_combined.drop(
            columns=["LVL_2_NAME"], errors="ignore"
        )

        return target_data_combined
    except Exception as e:
        current_run.log_error(
            f"Erreur lors du processus de récupération des identifiants des unités d'organisation: {e}"
        )
        raise


def add_round_info_to_configured_target_data(
    target_data_combined: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add round information to the combined target data for the configured campaigns, assuming that
    the target data applies to all rounds of the campaign, except if there already exist round information
    in the historical data for the campaign.

    Args:
        target_data_combined (pd.DataFrame): DataFrame containing the combined target data.

    Returns:
        pd.DataFrame: DataFrame with added round information for the configured campaigns.
    """
    current_run.log_info(
        "Ajout des informations de rounds pour les campagnes configurées..."
    )
    target_data_combined_historical = target_data_combined[
        target_data_combined["round"].notna()
    ]
    target_data_combined_configured = target_data_combined[
        target_data_combined["round"].isna()
    ]

    # if there already exist round info for the same combination of year-produit in the historical data,
    # create new rounds upto 10 rounds starting from the max round number in the historical data for this
    # combination. Otherwise, assign round 1 to 10 to the target data of the configured campaigns.
    target_data_combined_historical["round_num"] = (
        target_data_combined_historical["round"].str.extract(r"round (\d+)").astype(int)
    )
    max_rounds_historical = (
        target_data_combined_historical.groupby(["year", "produit"])["round_num"]
        .max()
        .reset_index()
    )
    max_rounds_historical["round_start"] = max_rounds_historical["round_num"] + 1
    max_rounds_historical["round_end"] = 10

    target_data_combined_configured = target_data_combined_configured.merge(
        max_rounds_historical[["year", "produit", "round_start", "round_end"]],
        on=["year", "produit"],
        how="left",
    )

    target_data_combined_configured["round"] = target_data_combined_configured.apply(
        lambda row: [
            f"round {i}"
            for i in range(int(row["round_start"]), int(row["round_end"]) + 1)
        ]
        if not pd.isna(row["round_start"])
        else [f"round {i}" for i in range(1, 11)],
        axis=1,
    )
    target_data_combined_configured = target_data_combined_configured.explode(
        "round"
    ).reset_index(drop=True)
    target_data_combined_configured = target_data_combined_configured.drop(
        columns=["round_start", "round_end"]
    )
    target_data_combined = pd.concat(
        [target_data_combined_historical, target_data_combined_configured],
        ignore_index=True,
    )
    target_data_combined = target_data_combined.drop(
        columns=["round_num"], errors="ignore"
    )

    return target_data_combined


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


def export_to_dataset(df: pd.DataFrame, dataset_name: str) -> None:
    """
    Exports a DataFrame to an OpenHexa dataset in multiple formats (xlsx, parquet, csv).

    Args:
        df (pd.DataFrame): The configuration dataframe to export.
        dataset_name (str): The name of the OpenHexa dataset.
    """
    current_run.log_info(
        f"Préparation de l'exportation vers le dataset : {dataset_name}..."
    )
    dataset_slug = dataset_name.lower().strip().replace(" ", "-").replace("_", "-")
    # 1. Manage Dataset Existence
    try:
        dataset = workspace.get_dataset(dataset_slug)
        current_run.log_info(f"Dataset existant trouvé : {dataset_slug}")
    except Exception:
        current_run.log_info(f"Dataset {dataset_name} non trouvé. Création en cours...")
        dataset = workspace.create_dataset(
            name=dataset_name,
            description="Données de configuration de campagne (multi-formats)",
        )

    # 2. Define Versioning
    latest_version = dataset.latest_version
    version_number = int(latest_version.name.lstrip("v")) + 1 if latest_version else 1
    new_version_name = f"v{version_number}"

    # 3. Create Local Files (Temporary Storage)
    # Ensure the config directory exists
    if not os.path.exists(OUTPUTS_PATH):
        os.makedirs(OUTPUTS_PATH)

    # Define the file paths
    base_path = os.path.join(OUTPUTS_PATH, dataset_name)
    files_to_upload = {
        "parquet": f"{base_path}.parquet",
        "xlsx": f"{base_path}.xlsx",
        "csv": f"{base_path}.csv",
    }

    # Save the dataframe in different formats
    df.to_parquet(files_to_upload["parquet"], index=False)
    df.to_excel(files_to_upload["xlsx"], index=False)
    df.to_csv(files_to_upload["csv"], index=False)

    # 4. Upload to Dataset Version
    version = dataset.create_version(new_version_name)

    for format_type, file_path in files_to_upload.items():
        version.add_file(file_path, os.path.basename(file_path))
        current_run.log_info(
            f"Fichier {format_type} ajouté à la version {new_version_name}"
        )

    current_run.log_info(
        f"Exportation terminée avec succès pour {dataset_name} ({new_version_name})"
    )


if __name__ == "__main__":
    process_target_data()
