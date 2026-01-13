import os
import pandas as pd
import numpy as np
from openhexa.sdk import current_run, workspace, pipeline
from pathlib import Path
from config import (
    iaso_connector_slug,
    iaso_form_id,
    current_period,
    current_period_start_date,
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
    historical_df = import_historical_iaso_data()

    # data extraction
    current_df = extract_iaso_data_for_current_period()

    # data processing
    combined_df = combine_historical_and_current_data(historical_df, current_df)
    combined_df = retrieve_org_unit_ids(
        combined_df, iaso_org_unit_tree_raw, iaso_org_unit_tree_clean
    )
    combined_df = clean_combined_df(combined_df)

    # save output
    save_output(combined_df)


def import_historical_iaso_data() -> pd.DataFrame:
    """
    Import historical data from IASO historical data folder.

    Args:
        None

    Returns:
        pd.DataFrame: Combined historical data from all feather files.
    """
    current_run.log_info("Importing historical data...")
    historical_data_folder = os.path.join(
        workspace.files_path, "niger_june_24", "inputs", "historical_iaso_data"
    )
    historical_df = pd.DataFrame()
    for file in os.listdir(historical_data_folder):
        if file.endswith(".feather"):
            file_path = os.path.join(historical_data_folder, file)
            current_run.log_info(f"Imported {file_path}")
            df = pd.read_feather(file_path)
            historical_df = pd.concat([historical_df, df], ignore_index=True)

    return historical_df


def extract_iaso_data_for_current_period() -> pd.DataFrame:
    """
    Extract data from IASO for the current quarter using the IASO connector.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing data for the current quarter.
    """
    current_run.log_info("Extracting IASO data for the current period...")
    iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
    iaso_connector_instance.get_data_structure_from_the_form(iaso_form_id)
    iaso_connector_instance.form_data_structure_df
    current_df = iaso_connector_instance.extract_submissions_info(
        form_id=iaso_form_id, dateFrom=current_period_start_date
    )

    # save for future use
    file_path = os.path.join(
        workspace.files_path,
        "niger_june_24",
        "temp",
        f"multicampaign_df_{current_period}_raw.feather",
    )
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    current_df.to_feather(file_path)

    # fast import
    current_df = pd.read_feather(file_path)

    return current_df


def combine_historical_and_current_data(
    historical_df: pd.DataFrame, current_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine historical and current period data into a single DataFrame.

    Args:
        historical_df (pd.DataFrame): DataFrame containing historical data.
        current_df (pd.DataFrame): DataFrame containing current period data.

    Returns:
        pd.DataFrame: Combined DataFrame with historical and current data.
    """
    current_run.log_info("Combining historical and current data...")
    combined_df = pd.concat([current_df, historical_df], ignore_index=True)

    duplicates_count = combined_df.duplicated(subset=["uuid"], keep=False).sum()
    if duplicates_count > 0:
        duplicates_proportion = duplicates_count / len(combined_df)
        current_run.log_warning(
            f"{duplicates_count} entrées ({duplicates_proportion:.2%}) dupliquées trouvées pour la même UUID. Les doublons seront supprimés."
        )
        combined_df = combined_df.drop_duplicates(subset="uuid")

    iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
    iaso_connector_instance.get_data_structure_from_the_form(iaso_form_id)
    for col in [
        _
        for _ in iaso_connector_instance.form_data_structure_df.name.unique()
        if _ not in combined_df.columns
    ]:
        current_run.log_warning(
            f"La colonne '{col}' est manquante dans les données combinées. Elle sera ajoutée avec des valeurs NaN."
        )
        combined_df[col] = np.nan

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
        "niger_june_24",
        "outputs",
        "iaso_org_unit_tree_raw.parquet",
    )

    iaso_org_unit_tree_raw = pd.read_parquet(file_path)

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
        "niger_june_24",
        "outputs",
        "iaso_org_unit_tree_clean.parquet",
    )

    iaso_org_unit_tree_clean = pd.read_parquet(file_path)

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
