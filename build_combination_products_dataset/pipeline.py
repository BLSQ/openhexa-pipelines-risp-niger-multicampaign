import os
from openhexa.sdk import current_run, workspace, pipeline
import pandas as pd
import numpy as np
from pathlib import Path
import re
from config import (
    outputs_path,
    product_site_config,
    product_status_config,
    sex_types_config,
    campaign_dates_config,
)


@pipeline("build_combination_products_dataset")
def build_combination_products_dataset():
    """
    Main pipeline function to build combination campaigns dataset.
    """
    org_unit_ids_df = extract_org_unit_id()
    product_site_df = create_product_site_df()
    sex_type_df = create_sex_type_df()
    product_status_df = create_product_status_df()
    target_df = import_target_data()
    age_product_year_round_df = create_age_product_year_round_df(target_df)
    campaign_period_df = create_campaign_period_df()
    combined_df = combine_dfs(
        org_unit_ids_df,
        age_product_year_round_df,
        product_site_df,
        sex_type_df,
        product_status_df,
        campaign_period_df,
    )
    combined_df = adjust_to_specific_campaigns(combined_df)
    save_output(combined_df)


def extract_org_unit_id() -> pd.DataFrame:
    """
    Create a DataFrame containing unique org unit IDs from the spatial DataFrame.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with unique 'org_unit_id's.
    """
    current_run.log_info("Extracting list of org unit IDs...")

    file_path = os.path.join(
        workspace.files_path,
        outputs_path,
        "iaso_org_unit_tree_clean.parquet",
    )

    org_unit_ids_df = pd.read_parquet(file_path)
    org_unit_ids_df = org_unit_ids_df[
        ["org_unit_id", "LVL_2_NAME", "LVL_3_NAME", "LVL_6_NAME"]
    ].drop_duplicates()
    assert org_unit_ids_df["org_unit_id"].is_unique, "Duplicate org_unit_ids found!"

    return org_unit_ids_df


def create_product_site_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all sites.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with all sites.
    """
    current_run.log_info("Creating site DataFrame...")

    combinations = []

    for product, sites in product_site_config.items():
        for site in sites:
            combinations.append((product, site))

    product_site_df = pd.DataFrame(combinations, columns=["produit", "site"])
    product_site_df = product_site_df.sort_values(by=["produit", "site"]).reset_index(
        drop=True
    )

    return product_site_df


def create_sex_type_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all sex types of cases vaccinated.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with all sex types of cases vaccinated.
    """
    current_run.log_info("Creating sex type DataFrame...")

    sex_type_df = pd.DataFrame(sex_types_config, columns=["sexe"])
    return sex_type_df


def import_target_data() -> pd.DataFrame:
    """
    Import target data from Parquet files.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the imported target data.
    """
    current_run.log_info("Importing target data from Parquet files...")

    target_data_path = os.path.join(
        workspace.files_path, outputs_path, "combined_target_data.parquet"
    )
    try:
        target_df = pd.read_parquet(target_data_path)
    except Exception as e:
        current_run.log_error(f"Erreur de lecture des données cibles: {e}")
        raise e

    return target_df


def create_age_product_year_round_df(target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame containing all combinations of age groups, products, years and rounds.

    Args:
        pd.DataFrame: DataFrame containing target data.

    Returns:
        pd.DataFrame: DataFrame with all combinations of age groups, products, years and rounds.
    """
    current_run.log_info("Creating age, product, round, year combinations DataFrame...")

    age_product_year_round_df = target_df[
        ["year", "produit", "round", "age"]
    ].drop_duplicates()

    return age_product_year_round_df


def create_product_status_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all combinations of products and vaccination statuses.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with all combinations of products and vaccination statuses.
    """

    combinations = []

    for product, statuses in product_status_config.items():
        for status in statuses:
            combinations.append((product, status))
    product_status_df = pd.DataFrame(combinations, columns=["produit", "status"])
    product_status_df = product_status_df.sort_values(
        by=["produit", "status"]
    ).reset_index(drop=True)

    return product_status_df


def create_campaign_period_df() -> pd.DataFrame:
    """
    Create a DataFrame containing campaign rounds and periods with their corresponding order days.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with campaign rounds and periods and order days.
    """
    current_run.log_info("Creating campaign period DataFrame...")

    rows = []

    for key, dates in campaign_dates_config.items():
        match = re.match(r"(\d{4})r(\d+)_?(.+)?", key)
        if match:
            year = np.int32(match.group(1))
            round_num = f"round {match.group(2)}"
            raw_campagne = match.group(3) if match.group(3) else "polio"
            product = raw_campagne.replace("__", " ").replace("_", " ")
            date_series = pd.date_range(start=dates["min"], end=dates["max"])
            for i, day in enumerate(date_series, start=1):
                rows.append(
                    {
                        "produit": product,
                        "round": round_num,
                        "year": year,
                        "period": day,
                        "order_day": i,
                    }
                )
    df = pd.DataFrame(rows)
    return df


def combine_dfs(
    org_unit_ids_df,
    age_product_year_round_df,
    product_site_df,
    sex_type_df,
    product_status_df,
    campaign_period_df,
) -> pd.DataFrame:
    """
    Combine all DataFrames into a single DataFrame representing the combination products dataset.

    Args:
        org_unit_ids_df (pd.DataFrame): DataFrame with org unit IDs.
        sites_df (pd.DataFrame): DataFrame with sites.
        strategy_df (pd.DataFrame): DataFrame with strategies.
        campaign_age_product_status_df (pd.DataFrame): DataFrame with campaign, age group, product, and status combinations.
        campaign_period_df (pd.DataFrame): DataFrame with campaign periods.

    Returns:
        pd.DataFrame: Combined DataFrame.
    """
    current_run.log_info("Combining all DataFrames into the final dataset...")

    # cross join org_unit, sex, and combo df
    combined_df = org_unit_ids_df.merge(sex_type_df, how="cross").merge(
        age_product_year_round_df, how="cross"
    )

    # merge with product sites
    combined_df = combined_df.merge(
        product_site_df, on="produit", how="left", indicator=True
    )
    unmatched = combined_df[combined_df["_merge"] == "left_only"]
    if not unmatched.empty:
        current_run.log_error(
            f"Unmatched entries found when merging product and site DataFrames: {unmatched}"
        )
        raise ValueError(
            "Merging product and site DataFrames resulted in unmatched entries."
        )
    combined_df = combined_df.drop(columns=["_merge"])

    # merge with product statuses
    combined_df = combined_df.merge(
        product_status_df, on="produit", how="left", indicator=True
    )
    unmatched = combined_df[combined_df["_merge"] == "left_only"]
    if not unmatched.empty:
        current_run.log_error(
            f"Unmatched entries found when merging product and status DataFrames: {unmatched}"
        )
        raise ValueError(
            "Merging product and status DataFrames resulted in unmatched entries."
        )
    combined_df = combined_df.drop(columns=["_merge"])

    # merge with campaign periods
    combined_df = combined_df.merge(
        campaign_period_df, on=["produit", "year", "round"], how="left", indicator=True
    )
    unmatched = combined_df[combined_df["_merge"] == "left_only"]
    if not unmatched.empty:
        current_run.log_error(
            f"Unmatched entries found when merging campaign period DataFrame: {unmatched}"
        )
        raise ValueError(
            "Merging campaign period DataFrame resulted in unmatched entries."
        )
    combined_df = combined_df.drop(columns=["_merge"])

    # final cleanup
    combined_df = combined_df.drop_duplicates().reset_index(drop=True)

    combined_df = combined_df.rename(
        columns={
            "org_unit_id": "org_unit_id",
            "site": "site",
            "age": "age",
            "sexe": "sexe",
            "product": "produit",
            "status": "vaccination_status",
            "round": "round",
            "year": "year",
            "period": "period",
            "order_day": "order_day",
        }
    )

    return combined_df


def adjust_to_specific_campaigns(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adjust the combined DataFrame for specific campaigns as needed.

    Args:
        combined_df (pd.DataFrame): DataFrame containing the combined campaign data.

    Returns:
        pd.DataFrame: Adjusted DataFrame.
    """
    current_run.log_info("Adjusting combined DataFrame for specific campaigns...")

    # For yellow fever campaigns 2025 2026 round 1, delete all entries outside the regions of Dosso and Tahoua (only these 2 regions have been covered)
    mask_yellow_fever_dosso_tahouha = (
        (combined_df["produit"] == "fièvre jaune")
        & (combined_df["year"].isin([2025, 2026]))
        & (combined_df["round"] == "round 1")
        & (~combined_df["LVL_2_NAME"].isin(["Dosso", "Tahoua"]))
    )
    combined_df = combined_df[~mask_yellow_fever_dosso_tahouha].reset_index(drop=True)
    combined_df = combined_df.drop(columns=["LVL_2_NAME"])

    return combined_df


def save_output(combined_df: pd.DataFrame):
    """
    Save the combined campaign dataset to a Parquet file.

    Args:
        combined_df (pd.DataFrame): DataFrame containing the combined campaign data.

    Returns:
        None
    """
    current_run.log_info("Saving combined campaign data...")

    output_path = os.path.join(
        workspace.files_path,
        outputs_path,
        "combined_campaign_data.parquet",
    )
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(output_path, index=False)
    current_run.log_info(f"Combined campaign data saved to {output_path}.")


if __name__ == "__main__":
    build_combination_products_dataset()
