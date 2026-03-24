import os
import pandas as pd
import numpy as np
from openhexa.sdk import current_run, pipeline
from config import (
    OUTPUTS_PATH,
    campaign_name_cleaning_dict,
    campaign_name_mapping_dict,
    campaign_product_name_mapping_dict,
    cols_campaign_map,
)


@pipeline(
    "process_iaso_form_data",
    name="multi-campagne - Traitement des données du formulaire IASO",
)
def process_iaso_form_data():
    """
    Main pipeline function to extract and process IASO form data
    """
    # data imports
    iaso_org_unit_tree_clean = load_data("iaso_org_unit_tree_clean")
    iaso_org_unit_tree_raw = load_data("iaso_org_unit_tree_raw")
    expected_data_structure = load_data("combined_campaign_data")
    combined_df = load_data("combined_iaso_data_raw")

    # data processing
    combined_df = retrieve_org_unit_ids(
        combined_df, iaso_org_unit_tree_raw, iaso_org_unit_tree_clean
    )
    combined_df = clean_combined_df(combined_df, expected_data_structure)

    # save output
    save_file(combined_df, "combined_iaso_data")


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


def retrieve_org_unit_ids(
    combined_df: pd.DataFrame,
    iaso_org_unit_tree_raw: pd.DataFrame,
    iaso_org_unit_tree_clean: pd.DataFrame,
) -> pd.DataFrame:
    """
    Retrieve all org unit IDs associated with CSI names

    Args:
        combined_df (pd.DataFrame): The combined DataFrame to update with org unit IDs.
        iaso_org_unit_tree_raw (pd.DataFrame): The raw IASO organisation tree DataFrame.
        iaso_org_unit_tree_clean (pd.DataFrame): The clean IASO organisation tree DataFrame.

    Returns:
        pd.DataFrame: The updated combined DataFrame with org unit IDs.
    """
    current_run.log_info("Récupération des identifiants des unités d'organisation...")
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

        combined_df["org_unit_id"] = combined_df["org_unit_id"].map(
            org_unit_to_final_org_unit_dict
        )
        # remove entries with missing org_unit_id
        mask_missing_org_unit = combined_df["org_unit_id"].isna()
        missing_org_unit_entries = combined_df[mask_missing_org_unit]
        if not missing_org_unit_entries.empty:
            missing_org_unit_proportion = len(missing_org_unit_entries) / len(
                combined_df
            )
            current_run.log_warning(
                f"{len(missing_org_unit_entries)} entrées ({missing_org_unit_proportion:.2%}) contiennent des org_unit_id manquants. Ces entrées seront supprimées."
            )
            combined_df = combined_df[~mask_missing_org_unit].copy()

        combined_df.loc[:, "org_unit_id"] = combined_df["org_unit_id"].astype(np.int64)

        return combined_df
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la récupération des identifiants des unités d'organisation : {str(e)}"
        )
        raise


def clean_combined_df(
    combined_df: pd.DataFrame, expected_data_structure: pd.DataFrame
) -> pd.DataFrame:
    """
    Clean the combined DataFrame by:
        - formatting the period column
        - exploding the data to deal with multi-campaign entries
        - checking and removing entries with no or unknown campaign assigned
        - assigning rounds and checking for entries outside the range of campaign rounds
        - dropping entries whose period is outside the expected campaign periods
    Args:
        combined_df (pd.DataFrame): The combined DataFrame to be cleaned.
        expected_data_structure (pd.DataFrame): DataFrame containing the expected data structure.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    current_run.log_info("Nettoyage du DataFrame combiné...")
    try:
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
            ~combined_df["choix_campagne"].isin(
                list(campaign_name_mapping_dict.values())
            )
        ]
        unknown_campaigns = pd.concat(
            [na_campaigns, invalid_campaigns]
        ).drop_duplicates()
        if not unknown_campaigns.empty:
            unknown_campaigns_proportion = len(unknown_campaigns) / len(combined_df)
            current_run.log_warning(
                f"{len(unknown_campaigns)} entrées ({unknown_campaigns_proportion:.2%}) contiennent des noms de campagne manquant ou invalide ({invalid_campaigns['choix_campagne'].unique().tolist()}). Ces entrées seront supprimées."
            )
        combined_df = combined_df[
            combined_df["choix_campagne"].isin(
                list(campaign_name_mapping_dict.values())
            )
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

        # drop entries that are not in the expected campaign periods
        expected_periods = expected_data_structure[
            ["produit", "period", "year", "round"]
        ].drop_duplicates()
        expected_periods["choix_campagne"] = expected_periods["produit"].map(
            campaign_product_name_mapping_dict
        )
        expected_periods = expected_periods.drop(columns=["produit"]).drop_duplicates()
        combined_df = combined_df.merge(
            expected_periods[["choix_campagne", "period", "year", "round"]],
            on=["choix_campagne", "period"],
            how="outer",
            validate="many_to_one",
            indicator=True,
        )

        date_invalide_mask = combined_df["_merge"] == "left_only"
        if date_invalide_mask.sum() > 0:
            proportion_date_invalide = date_invalide_mask.sum() / len(combined_df)
            current_run.log_warning(
                f"{date_invalide_mask.sum()} entrées ({proportion_date_invalide:.2%}) ont été supprimées "
                f"car la période est en dehors de la période de campagne."
            )
        combined_df = combined_df[~date_invalide_mask].drop(columns=["_merge"])

        return combined_df
    except Exception as e:
        current_run.log_error(
            f"Erreur lors du nettoyage du DataFrame combiné : {str(e)}"
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
    process_iaso_form_data()
