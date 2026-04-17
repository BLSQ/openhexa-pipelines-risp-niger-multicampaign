import os
from openhexa.sdk import current_run, pipeline, workspace
import pandas as pd
import numpy as np
from utils import (
    validate_campaign_filename,
)
from config import (
    OUTPUTS_PATH,
    TARGET_OTHER_DATA_PATH,
    valid_campaigns,
    valid_scales,
    valid_levels,
    templates_required_cols_csi,
    templates_required_cols_district,
    target_columns_required_dict,
    campaign_rename_dict,
    cols_for_melting,
    csi_district_rename_dict,
)


@pipeline(
    "process_target_data",
    name="multi-campagne - 02 Pipeline d'importation et traitement des données de cibles",
)
def process_target_data():
    """
    This pipeline processes the target data of newly configured campaigns and merge them with the historical target data.
    The target data of the newly configured campaigns is generated based on template Excel files that are uploaded to the
    TARGET_OTHER_DATA_PATH.

    It performs the following steps:
     - imports and processes target data for newly configured campaigns from template Excel files
     - adds historical target data
     - retrieves all org unit IDs from the raw IASO tree associated to the org unit names in the combined target data
     - saves the combined target data and exports it to a dataset.
    """

    # load data
    iaso_org_unit_tree_df = load_data("iaso_org_unit_tree_raw")
    iaso_org_unit_tree_df_clean = load_data("iaso_org_unit_tree_clean")
    historical_target_data = load_data("combined_historical_target_data")

    # process configured target data
    all_target_data_csi_combined, all_target_data_district_combined = (
        import_target_data_for_future_campaigns()
    )
    if not all_target_data_csi_combined.empty:
        all_target_data_csi_combined = add_org_unit_ids(
            all_target_data_csi_combined, iaso_org_unit_tree_df_clean
        )
    if not all_target_data_district_combined.empty:
        all_target_data_district_combined = add_org_unit_ids(
            all_target_data_district_combined, iaso_org_unit_tree_df_clean
        )

    target_data_configured_combined = combine_target_data(
        [
            all_target_data_csi_combined,
            all_target_data_district_combined,
        ]
    )

    target_data_configured_combined = add_round_info_to_configured_target_data(
        target_data_configured_combined, historical_target_data
    )

    target_data_configured_combined = clean_org_unit_id(
        target_data_configured_combined,
        iaso_org_unit_tree_df,
        iaso_org_unit_tree_df_clean,
    )

    # combine with historical target data
    target_data_combined = combine_target_data(
        [
            target_data_configured_combined,
            historical_target_data,
        ]
    )

    # save
    save_file(target_data_configured_combined, "combined_configured_target_data")
    save_file(target_data_combined, "combined_target_data")
    export_to_dataset(target_data_combined, OUTPUTS_PATH, "combined_target_data")


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


def inspect_dataframe(df: pd.DataFrame, aggregation_type: str, meta: dict):
    """
    Performs validation checks on the structure of the configured target data:
        - Verifies presence of required columns based on aggregation type.
        - Verifies that target data columns follow the 'Cible [age] mois/ans' format.

    Args:
        df (pd.DataFrame): The input DataFrame to inspect.
        aggregation_type (str): Either "csi" or "district".
        meta (dict): Metadata containing the file name for logging.
    """
    file_name = meta.get("file", "inconnu")
    current_run.log_info(f"Inspection du fichier de cible configuré {file_name}...")

    try:
        # check for required org unit cols
        required = (
            templates_required_cols_csi
            if aggregation_type == "csi"
            else templates_required_cols_district
        )
        if not all(col in df.columns for col in required):
            missing = [col for col in required if col not in df.columns]
            msg = f"Colonnes manquantes dans {file_name}. Manquantes: {missing}. Attendues: {required}"
            current_run.log_error(msg)
            raise ValueError(msg)

        # check for required target cols
        for campaign, required_target_cols in target_columns_required_dict.items():
            if campaign in file_name:
                missing = list(set(required_target_cols) - set(df.columns))
                if missing:
                    msg = f"Colonnes cibles manquantes dans {file_name}. Manquantes: {missing}. Attendues: {required_target_cols}."
                    current_run.log_error(msg)
                    raise ValueError(msg)

        current_run.log_info(f"Inspection réussie pour {file_name}.")

    except ValueError:
        raise
    except Exception as e:
        msg = f"Erreur lors de l'inspection du fichier {file_name}: {e}"
        current_run.log_error(msg)
        raise


def process_dataframe(df: pd.DataFrame, aggregation_type: str, meta: dict):
    """
    This function processes the input DataFrame by melting it from wide to long format,
    extracting age group information, and cleaning up the resulting DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame to be processed.
        aggregation_type (str): The type of aggregation, either "csi" or "district".
        meta (dict): A dictionary containing metadata about the DataFrame, such as the source file name.

    Returns:
        df_melted (pd.DataFrame): The processed DataFrame in long format with extracted age group information.
    """
    file_name = meta.get("file", "inconnu")
    current_run.log_info(
        f"Traitement du DataFrame pour le fichier {file_name} avec le type d'agrégation '{aggregation_type}'..."
    )
    try:
        id_vars = cols_for_melting.copy()
        if aggregation_type == "csi":
            id_vars.insert(1, "CSI")

        value_vars = [col for col in df.columns if col.startswith("Cible ")]
        regex_pattern = r"^Cible (\d+-\d+ (?:mois|ans))$"

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

        current_run.log_info(
            f"DataFrame traité avec succès pour le fichier {file_name}"
        )

        return df_melted

    except Exception as e:
        msg = f"Erreur lors du traitement du DataFrame pour le fichier {file_name}: {e}"
        current_run.log_error(msg)
        raise


def process_single_target_file(file: os.DirEntry, metadata: dict) -> pd.DataFrame:
    """
    Processes a single target data file by reading it, validating its content, and transforming it into
    the desired format.

    Args:
        file (os.DirEntry): The file entry to be processed, containing the file path and name.
        metadata (dict): A dictionary containing metadata about the file (campaign, year, aggregation level).

    Returns:
        df_processed (pd.DataFrame): The processed DataFrame.
    """
    current_run.log_info(f"Traitement du fichier {file.name}...")
    try:
        df = pd.read_excel(file.path)

        if df.empty:
            msg = f"Le fichier {file.name} est vide."
            current_run.log_error(msg)
            raise ValueError(msg)

        # check content
        target_cols = [col for col in df.columns if col.startswith("Cible ")]
        for col in target_cols:
            # empty cells (NaN/None) or 0 values
            empty_count = df[col].isnull().sum() + (df[col] == 0).sum()
            if empty_count > 0:
                current_run.log_warning(
                    f"Attention: {empty_count} cellule(s) vide(s) ou nulles détectée(s) dans la colonne '{col}' du fichier {file.name}. "
                    "Ces cellules seront traitées comme des valeurs manquantes."
                )
            # non-numeric values
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            if numeric_series.isna().sum() > empty_count:
                msg = f"Données non numériques détectées dans {file.name} (colonne '{col}')"
                current_run.log_error(msg)
                raise ValueError(msg)

        # assign metadata
        df["year"] = metadata["year"]
        df["produit"] = campaign_rename_dict.get(
            metadata["campaign"], metadata["campaign"]
        )

        # inspect and process
        inspect_dataframe(df, metadata["level"], {"file": file.name})
        df_processed = process_dataframe(df, metadata["level"], {"file": file.name})

        current_run.log_info(f"Fichier {file.name} traité avec succès.")

        return df_processed

    except ValueError:
        raise
    except Exception as e:
        msg = f"Erreur sur {file.name} : {str(e)}"
        current_run.log_error(msg)
        raise


def import_target_data_for_future_campaigns():
    """
    Imports target data for future campaigns.

    Args:
        None

    Returns:
        csi_target_df (pd.DataFrame): DataFrame containing the target data at CSI level for future campaigns.
        district_target_df (pd.DataFrame): DataFrame containing the target data at district level for future campaigns.
    """
    current_run.log_info("Démarrage de l'importation...")

    if not os.path.exists(TARGET_OTHER_DATA_PATH):
        raise FileNotFoundError(f"Dossier introuvable: {TARGET_OTHER_DATA_PATH}")

    all_data = {"csi": [], "district": []}

    for file in os.scandir(TARGET_OTHER_DATA_PATH):
        if not (
            file.is_file()
            and file.name.endswith(".xlsx")
            and not file.name.startswith("~$")
        ):
            continue

        metadata = validate_campaign_filename(
            file.name, valid_campaigns, valid_scales, valid_levels
        )

        processed_df = process_single_target_file(file, metadata)
        all_data[metadata["level"]].append(processed_df)
        current_run.log_info(f"Succès: {file.name}")

    csi_target_df = (
        pd.concat(all_data["csi"], ignore_index=True)
        if all_data["csi"]
        else pd.DataFrame()
    )
    district_target_df = (
        pd.concat(all_data["district"], ignore_index=True)
        if all_data["district"]
        else pd.DataFrame()
    )

    return csi_target_df, district_target_df


def add_org_unit_ids(
    target_df: pd.DataFrame,
    iaso_org_unit_tree_df_clean: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enrich the target data with the org unit IDs from the cleansed IASO org unit tree
    using org unit names for matching.

    Args:
        target_df (pd.DataFrame): DataFrame containing the target data with org unit names.
        iaso_org_unit_tree_df_clean (pd.DataFrame): DataFrame containing the clean IASO org unit tree with org unit IDs.

    Returns:
         target_with_org_unit_ids_df (pd.DataFrame): DataFrame containing the target data with matched org unit IDs.
    """
    current_run.log_info(
        "Récupération des identifiants des unités organisationnelles à partir de la pyramide IASO..."
    )
    try:
        district_org_unit_ids_df = iaso_org_unit_tree_df_clean[
            ["org_unit_id", "LVL_3_NAME"]
        ].drop_duplicates()
        csi_org_unit_ids_df = iaso_org_unit_tree_df_clean[
            ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
        ].drop_duplicates()

        # district-level matching
        if target_df["LVL_6_NAME"].isna().all():
            target_with_org_unit_ids_df = target_df.merge(
                district_org_unit_ids_df,
                left_on=["LVL_3_NAME"],
                right_on=["LVL_3_NAME"],
                how="left",
                indicator=True,
            )
            unmatched_count = (
                target_with_org_unit_ids_df["_merge"].value_counts().get("left_only", 0)
            )
            if unmatched_count > 0:
                msg = f"Erreur lors de l'appariement au niveau des districts: {unmatched_count} unités organisationnelles n'ont pas été appariées."
                current_run.log_error(msg)
                raise ValueError(msg)

        # csi-level matching
        else:
            target_with_org_unit_ids_df = target_df.merge(
                csi_org_unit_ids_df,
                left_on=["LVL_3_NAME", "LVL_6_NAME"],
                right_on=["LVL_3_NAME", "LVL_6_NAME"],
                how="left",
                indicator=True,
            )
            unmatched_count = (
                target_with_org_unit_ids_df["_merge"].value_counts().get("left_only", 0)
            )
            if unmatched_count > 0:
                msg = f"Erreur lors de l'appariement au niveau des CSI: {unmatched_count} unités organisationnelles n'ont pas été appariées."
                current_run.log_error(msg)
                raise ValueError(msg)

        target_with_org_unit_ids_df = target_with_org_unit_ids_df.drop(
            columns=["_merge"]
        )

    except ValueError:
        raise
    except Exception as e:
        msg = f"Erreur lors de l'importation des org unit IDs: {e}"
        current_run.log_error(msg)
        raise

    current_run.log_info(
        "Récupération des identifiants des unités organisationnelles terminée avec succès."
    )

    return target_with_org_unit_ids_df


def combine_target_data(
    dfs: list[pd.DataFrame],
) -> pd.DataFrame:
    """
    Combine multiple target data DataFrames into a single DataFrame.

    Args:
        dfs (list[pd.DataFrame]): List of DataFrames to be combined.

    Returns:
        target_data_combined(pd.DataFrame): Combined DataFrame containing all target data.
    """
    current_run.log_info("Combinaison des différentes données de cibles en cours...")
    try:
        valid_dfs = [df for df in dfs if df is not None and not df.empty]
        if not valid_dfs:
            current_run.log_warning(
                "Aucune donnée valide à combiner. Retour d'un DataFrame vide."
            )
            return pd.DataFrame()

        target_data_combined = pd.concat(valid_dfs, ignore_index=True)

        current_run.log_info(
            "Combinaison des différentes données de cibles terminée avec succès."
        )
        return target_data_combined

    except Exception as e:
        msg = f"Erreur lors de la combinaison des données de cibles: {e}"
        current_run.log_error(msg)
        raise


def clean_org_unit_id(
    target_data_combined: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Clean the org_unit_id column in the combined target data, by assigning all the org_unit_ids in the raw org unit tree
    to the corresponding LVL_6_UID org_unit_id from the cleaned org unit tree.

    Args:
        target_data_combined (pd.DataFrame): DataFrame containing the combined configured target data.
        iaso_org_unit_tree_raw_df (pd.DataFrame): DataFrame containing the raw org unit tree data.
        iaso_org_unit_tree_clean_df (pd.DataFrame): DataFrame containing the cleaned org unit tree data.

    Returns:
        target_data_combined(pd.DataFrame): DataFrame with
    """
    current_run.log_info(
        "Récupération des identifiants des unités d'organisation et application de la correspondance un-à-plusieurs..."
    )
    try:
        if target_data_combined.empty:
            current_run.log_info(
                "Aucune donnée de cible configurée à traiter pour le nettoyage des org unit IDs."
            )
            return pd.DataFrame()

        uid_to_org_id_df_clean = iaso_org_unit_tree_clean_df[
            ["LVL_6_UID", "org_unit_id"]
        ].drop_duplicates()
        uid_to_org_id_df_raw = iaso_org_unit_tree_raw_df.copy()
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
        target_data_combined = target_data_combined.copy()
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

        current_run.log_info(
            "Récupération des identifiants des unités d'organisation et application de la correspondance un-à-plusieurs terminée avec succès."
        )

        return target_data_combined
    except Exception as e:
        msg = f"Erreur lors du processus de récupération des identifiants des unités d'organisation: {e}"
        current_run.log_error(msg)
        raise


def add_round_info_to_configured_target_data(
    configured_target_df: pd.DataFrame, historical_target_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Add round information to the combined target data for the configured campaigns, assuming that
    the target data applies to all rounds of the campaign, except if there already exist round information
    in the historical data for the campaign.

    Args:
        configured_target_df (pd.DataFrame): DataFrame containing the configured target data.
        historical_target_df (pd.DataFrame): DataFrame containing the historical target data.

    Returns:
        configured_target_with_rounds_df(pd.DataFrame): DataFrame with added round information for the configured campaigns.
    """
    current_run.log_info(
        "Ajout des informations de rounds pour les campagnes configurées..."
    )
    try:
        if configured_target_df.empty:
            current_run.log_info(
                "Aucune donnée de cible configurée à traiter pour l'ajout des rounds."
            )
            return (
                pd.DataFrame()
            )  # return empty DataFrame if there is no configured target data to process

        configured_target_df["round"] = np.nan

        # if there already exist round info for the same combination of year-produit in the historical data,
        # create new rounds upto 10 rounds starting from the max round number in the historical data for this
        # combination. Otherwise, assign round 1 to 10 to the target data of the configured campaigns.
        historical_target_df_modified = historical_target_df.copy()
        historical_target_df_modified["round_num"] = (
            historical_target_df_modified["round"]
            .str.extract(r"round (\d+)")
            .astype(int)
        )
        max_rounds_historical = (
            historical_target_df_modified.groupby(["year", "produit"])["round_num"]
            .max()
            .reset_index()
        )
        max_rounds_historical["round_start"] = max_rounds_historical["round_num"] + 1
        max_rounds_historical["round_end"] = 10

        configured_target_df = configured_target_df.merge(
            max_rounds_historical[["year", "produit", "round_start", "round_end"]],
            on=["year", "produit"],
            how="left",
        )

        configured_target_df["round"] = configured_target_df.apply(
            lambda row: [
                f"round {i}"
                for i in range(int(row["round_start"]), int(row["round_end"]) + 1)
            ]
            if not pd.isna(row["round_start"])
            else [f"round {i}" for i in range(1, 11)],
            axis=1,
        )
        configured_target_df = configured_target_df.explode("round").reset_index(
            drop=True
        )

        configured_target_with_rounds_df = configured_target_df.drop(
            columns=["round_start", "round_end"]
        )

        current_run.log_info(
            "Ajout des informations de rounds pour les campagnes configurées terminé avec succès."
        )

        return configured_target_with_rounds_df
    except Exception as e:
        msg = f"Erreur lors de l'ajout des informations de rounds: {e}"
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
    process_target_data()
