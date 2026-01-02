import os
from openhexa.sdk import current_run, workspace, pipeline
import pandas as pd
import numpy as np
from pathlib import Path
import re
# from utils import (

# )
from config import (
    sites_types,
    campaign_dates,
    strategie_types,
    campaign_configs,
)


@pipeline("build_combination_products_dataset")
def build_combination_products_dataset():
    """
    Main pipeline function to build combination products dataset.

    """
    org_unit_ids_df = extract_org_unit_id()
    sites_df = create_site_df()
    # strategy_df = create_strategy_df()
    campaign_age_product_status_df = create_campaign_age_product_status_df()
    campaign_period_df = create_campaign_period_df()
    combined_df = combine_dfs(
        org_unit_ids_df,
        sites_df,
        # strategy_df,
        campaign_age_product_status_df,
        campaign_period_df,
    )
    print(combined_df.shape)
    print(combined_df.head())
    x


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
    org_unit_ids_df = org_unit_ids_df[["org_unit_id"]].drop_duplicates()

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


def create_strategy_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all types of vaccination strategies.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with all types of vaccination strategies.
    """
    current_run.log_info("Creating strategy DataFrame...")

    strategy_df = pd.DataFrame(strategie_types, columns=["strategy"])
    return strategy_df


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
            year = int(match.group(1))
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
            "product": "produit",
            "status": "vaccination_status",
            "round": "round",
            "year": "year",
            "period": "campaign_date",
            "order_day": "order_day",
        }
    )

    return combined_df


if __name__ == "__main__":
    build_combination_products_dataset()
