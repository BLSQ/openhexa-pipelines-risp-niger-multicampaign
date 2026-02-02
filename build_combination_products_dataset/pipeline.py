from datetime import date
import os
from openhexa.sdk import current_run, workspace, pipeline
import pandas as pd
import numpy as np
from pathlib import Path
import re
from config import (
    outputs_path,
    config_path,
    product_site_config,
    product_status_config,
    sex_types_config,
    campaign_names_config,
)


@pipeline("02. Etablissement de la structure des données attendues")
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
    current_run.log_info("Extraction des identifiants des unités d'organisation...")
    try:
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
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'extraction des unités organisationnelles: {e}"
        )
        raise


def create_product_site_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all sites.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with all sites.
    """
    current_run.log_info("Création du DataFrame des sites...")

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
    current_run.log_info("Création du DataFrame des types de sexe...")

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
    current_run.log_info("Importation des données cibles traitées...")
    try:
        target_data_path = os.path.join(
            workspace.files_path, outputs_path, "combined_target_data.parquet"
        )
        target_df = pd.read_parquet(target_data_path)

        return target_df
    except Exception as e:
        current_run.log_error(f"Erreur de lecture des données cibles: {e}")
        raise


def create_age_product_year_round_df(target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame containing all combinations of age groups, products, years and rounds.

    Args:
        pd.DataFrame: DataFrame containing target data.

    Returns:
        pd.DataFrame: DataFrame with all combinations of age groups, products, years and rounds.
    """
    current_run.log_info(
        "Création du DataFrame des combinaisons âge, produit, round, année..."
    )
    try:
        age_product_year_round_df = target_df[
            ["year", "produit", "round", "age"]
        ].drop_duplicates()

        return age_product_year_round_df
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du DataFrame des combinaisons âge, produit, round, année: {e}"
        )
        raise


def create_product_status_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all combinations of products and vaccination statuses.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame with all combinations of products and vaccination statuses.
    """
    current_run.log_info("Création du DataFrame des statuts de vaccination...")
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
    current_run.log_info("Création du DataFrame des périodes de campagne...")
    campaign_round_config_path = os.path.join(
        workspace.files_path, config_path, "campagnes_config.txt"
    )
    if not os.path.exists(campaign_round_config_path):
        current_run.log_error(
            f"Fichier de configuration des campagnes introuvable: {campaign_round_config_path}."
        )
        raise FileNotFoundError
    campaign_round_config = pd.read_json(campaign_round_config_path).to_dict()
    rows = []
    for key, dates in campaign_round_config.items():
        match = re.match(r"(\d{4})r(\d+)_?(.+)?", key)
        if match:
            year = np.int32(match.group(1))
            if year < 2024 or year > date.today().year + 1:
                current_run.log_error(
                    f"Année de campagne non valide dans la configuration: {year}. L'année de campagne doit être comprise entre 2024 et {date.today().year + 1}."
                )
                raise ValueError
            round = int(match.group(2))
            if round < 1 or round > 4:
                current_run.log_warning(
                    f"Numéro de round de campagne anormal dans la configuration: {round}. Veuillez vérifier si ce numéro de round est correct."
                )
            round_num = f"round {match.group(2)}"

            raw_campagne = match.group(3)
            if raw_campagne not in campaign_names_config:
                current_run.log_error(
                    f"Nom de campagne non reconnu dans la configuration: {raw_campagne}. Noms de campagnes acceptés: {campaign_names_config}."
                )
                raise ValueError
            product = raw_campagne.replace("__", " ").replace("_", " ")
            if not (
                re.match(r"\d{4}-\d{2}-\d{2}", dates["début"])
                and re.match(r"\d{4}-\d{2}-\d{2}", dates["fin"])
            ):
                current_run.log_error(
                    f"Format de date invalide pour la campagne {key}: {dates}. Cette campagne sera ignorée."
                )
                continue
            date_series = pd.date_range(start=dates["début"], end=dates["fin"])
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
        else:
            current_run.log_error(
                f"Clé de campagne non conforme dans la configuration: {key}. Cette campagne sera ignorée."
            )
            continue
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
    current_run.log_info(
        "Combinaison de tous les DataFrames en un seul jeu de données final..."
    )

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
            f"Entrées non appariées trouvées lors de la fusion des DataFrames produit et site : {unmatched}"
        )
        raise ValueError()
    combined_df = combined_df.drop(columns=["_merge"])

    # merge with product statuses
    combined_df = combined_df.merge(
        product_status_df, on="produit", how="left", indicator=True
    )
    unmatched = combined_df[combined_df["_merge"] == "left_only"]
    if not unmatched.empty:
        current_run.log_error(
            f"Entrées non appariées trouvées lors de la fusion des DataFrames produit et statut : {unmatched}"
        )
        raise ValueError()
    combined_df = combined_df.drop(columns=["_merge"])

    # merge with campaign periods
    combined_df = combined_df.merge(
        campaign_period_df, on=["produit", "year", "round"], how="left", indicator=True
    )
    unmatched = combined_df[combined_df["_merge"] == "left_only"]
    if not unmatched.empty:
        current_run.log_error(
            f"Entrées non appariées trouvées lors de la fusion du DataFrame des périodes de campagne : {unmatched}"
        )
        raise ValueError()

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
    current_run.log_info(
        "Ajustement du DataFrame combiné pour des campagnes spécifiques..."
    )

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
    current_run.log_info(
        "Enregistrement du Dataframe contenant la structure attendue des campagnes..."
    )
    try:
        output_path = os.path.join(
            workspace.files_path,
            outputs_path,
            "combined_campaign_data.parquet",
        )
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        combined_df.to_parquet(output_path, index=False)
        current_run.log_info(
            f"Dataframe contenant la structure attendue des campagnes enregistré dans {output_path}."
        )
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'enregistrement du DataFrame combiné: {e}"
        )
        raise


if __name__ == "__main__":
    build_combination_products_dataset()
