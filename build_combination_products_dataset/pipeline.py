import os
from openhexa.sdk import current_run, workspace, pipeline
import pandas as pd
import numpy as np
from pathlib import Path
import re
from config import (
    sites_types,
    sex_types,
    campaign_dates,
    campaign_configs,
    sites_types_rougeole_adjustment,
)


@pipeline("build_combination_products_dataset")
def build_combination_products_dataset():
    """
    Main pipeline function to build combination campaigns dataset.
    """
    org_unit_ids_df = extract_org_unit_id()
    sites_df = create_site_df()
    sex_type_df = create_sex_type_df()
    campaign_age_product_status_df = create_campaign_age_product_status_df()
    campaign_period_df = create_campaign_period_df()
    combined_df = combine_dfs(
        org_unit_ids_df,
        sites_df,
        sex_type_df,
        campaign_age_product_status_df,
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
        "process_target_data",
        "output",
        "iaso_org_unit_tree_clean.parquet",
    )

    org_unit_ids_df = pd.read_parquet(file_path)
    org_unit_ids_df = org_unit_ids_df[
        ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
    ].drop_duplicates()
    assert org_unit_ids_df["org_unit_id"].is_unique, "Duplicate org_unit_ids found!"

    return org_unit_ids_df


def create_site_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all sites.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with all sites.
    """
    current_run.log_info("Creating site DataFrame...")

    sites_df = pd.DataFrame(sites_types, columns=["site"])

    return sites_df


def create_sex_type_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all sex types of cases vaccinated.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with all sex types of cases vaccinated.
    """
    current_run.log_info("Creating sex type DataFrame...")

    sex_type_df = pd.DataFrame(sex_types, columns=["sexe"])
    return sex_type_df


def create_campaign_age_product_status_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all combinations of age groups, products, and statuses.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with all combinations of age groups, products, and statuses.
    """
    current_run.log_info(
        "Creating age group, product, and status combinations DataFrame..."
    )

    combinations = []
    for config in campaign_configs.values():
        campaigns = config["choix_campagne"]
        ages = config["ages"]
        products = config["produit"]
        statuses = config["status"]

        for campaign in campaigns:
            for age in ages:
                for product in products:
                    for status in statuses:
                        combinations.append((campaign, age, product, status))

    df = pd.DataFrame(
        combinations, columns=["choix_campagne", "age_group", "product", "status"]
    )
    df = df.sort_values(
        by=["choix_campagne", "age_group", "product", "status"]
    ).reset_index(drop=True)
    return df


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

    for key, dates in campaign_dates.items():
        match = re.match(r"(\d{4})r(\d+)_?(.+)?", key)
        if match:
            year = np.int32(match.group(1))
            round_num = f"round {match.group(2)}"
            raw_campagne = match.group(3) if match.group(3) else "polio"
            choix_campagne = raw_campagne.replace("__", " ").replace("_", " ")
            date_series = pd.date_range(start=dates["min"], end=dates["max"])
            for i, day in enumerate(date_series, start=1):
                rows.append(
                    {
                        "choix_campagne": choix_campagne,
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
    sites_df,
    # strategy_df,
    sex_type_df,
    campaign_age_product_status_df,
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

    combined_df = (
        org_unit_ids_df.merge(sites_df, how="cross")
        # .merge(strategy_df, how="cross")
        .merge(sex_type_df, how="cross")
        .merge(campaign_age_product_status_df, how="cross")
    )

    combined_df = combined_df.merge(
        campaign_period_df, on="choix_campagne", how="inner"
    )
    combined_df = combined_df.drop(columns=["choix_campagne"])
    combined_df = combined_df.drop_duplicates().reset_index(drop=True)

    combined_df = combined_df.rename(
        columns={
            "org_unit_id": "org_unit_id",
            "site": "site",
            # "strategy": "vaccination_strategy",
            "age_group": "age",
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
    Adjust the combined DataFrame to fit specific campaign requirements.

    Args:
        combined_df (pd.DataFrame): The combined DataFrame.

    Returns:
        pd.DataFrame: Adjusted DataFrame.
    """
    current_run.log_info("Adjusting combined DataFrame for specific campaigns...")

    # polio 2024
    mask_polio_2024 = (
        (combined_df["year"] == 2024)
        & (combined_df["produit"].isin(["albendazole", "vitamine A"]))
        & (combined_df["round"] != "round 2")
    )
    combined_df = combined_df[~mask_polio_2024]

    #

    # polio and rougeole 2025 round 1 and 2
    mask_2025_rounds = combined_df["produit"].isin(["rougeole"])
    combined_df.loc[mask_2025_rounds, "site"] = combined_df.loc[
        mask_2025_rounds, "site"
    ].map(sites_types_rougeole_adjustment)
    mask_invalid_sites = (combined_df["produit"] == "rougeole") & (
        ~combined_df["site"].isin(sites_types_rougeole_adjustment.values())
    )
    combined_df = combined_df[~mask_invalid_sites]

    # yellow fever 2025 round 1 and 2
    mask_yellow_fever_sites = (
        (combined_df["year"] == 2025)
        & (combined_df["produit"] == "fièvre jaune")
        & (combined_df["round"].isin(["round 1", "round 2"]))
        & (combined_df["site"] != "ordinaire")
    )
    combined_df = combined_df[~mask_yellow_fever_sites]

    # méningite and tcv 2025 round 1 and 2
    mask_men5_tcv_sites = (
        (combined_df["year"] == 2025)
        & (combined_df["produit"].isin(["méningite", "tcv"]))
        & (combined_df["round"].isin(["round 1", "round 2"]))
        & (combined_df["site"] != "ordinaire")
    )
    combined_df = combined_df[~mask_men5_tcv_sites]

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
        workspace.files_path, "output", "combined_campaign_data.parquet"
    )
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(output_path, index=False)
    current_run.log_info(f"Combined campaign data saved to {output_path}.")


if __name__ == "__main__":
    build_combination_products_dataset()
