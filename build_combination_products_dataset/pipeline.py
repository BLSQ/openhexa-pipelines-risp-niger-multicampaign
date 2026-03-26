import os
from openhexa.sdk import current_run, pipeline
import pandas as pd
import numpy as np
from config import (
    OUTPUTS_PATH,
    CONFIG_PATH,
    product_site_config,
    product_status_config,
    sex_types_config,
    historical_campaigns_config,
)


@pipeline(
    "build_combination_products_dataset",
    name="multi-campagne - Etablissement de la structure des données attendues",
)
def build_combination_products_dataset():
    """
    This pipeline builds a dataframe that contains all the combinations of parameter values
    expected based on the configuration of historical and new campaigns. This dataframe
    will be used as a base to combine with the extracted IASO data and create the final
    dataset that will be used for the dashboard.

    The main steps of the pipeline are as follows:
    - create the combinations of parameters (product, site, age group, sex, vaccination status,
      campaign round, campaign year, campaign period) based on the configurations of historical
      campaigns
    - add the configurations of new campaigns to the combined dataframe
    - save the combined dataframe in the outputs folder as a parquet file in the workspace

    """
    # Create combination dataset for historical campaigns
    org_unit_ids_df = load_data("iaso_org_unit_tree_clean")
    target_df = load_data("combined_historical_target_data")

    product_site_df = create_product_site_df()
    sex_type_df = create_sex_type_df()
    product_status_df = create_product_status_df()
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

    # Add new campaign configurations to the combined dataframe
    combined_df = add_new_campaign_configurations(combined_df)

    # Save
    save_file(combined_df, "combined_campaign_data")


def create_product_site_df() -> pd.DataFrame:
    """
    Create a DataFrame containing all sites.

    Args:
        None

    Returns:
        product_site_df (pd.DataFrame): DataFrame with all sites.
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
        sex_type_df (pd.DataFrame): DataFrame with all sex types of cases vaccinated.
    """
    current_run.log_info("Création du DataFrame des types de sexe...")

    sex_type_df = pd.DataFrame(sex_types_config, columns=["sexe"])
    return sex_type_df


def load_data(name: str) -> pd.DataFrame:
    """
    Import data from a specified file in the outputs directory.
    The file should be in parquet format and the name should be provided without the extension.

    Args:
        name (str): Name of the file to be imported (without extension).

    Returns:
        df (pd.DataFrame): DataFrame containing the imported data.
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


def create_age_product_year_round_df(target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame containing all combinations of age groups, products, years and rounds.

    Args:
        pd.DataFrame: DataFrame containing target data.

    Returns:
        age_product_year_round_df (pd.DataFrame): DataFrame with all combinations of age groups, products, years and rounds.
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
        product_status_df (pd.DataFrame): DataFrame with all combinations of products and vaccination statuses.
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
    Create a DataFrame containing all combinations of campaign names, years, rounds, periods
    and order days from historical campaigns.

    Args:
        None

    Returns:
        all_campaigns_df (pd.DataFrame): DataFrame with campaign rounds and periods and order days.
    """
    current_run.log_info("Génération du DataFrame des périodes de campagne...")
    try:
        all_campaigns = []

        for (
            year,
            round_num,
            product_name,
        ), dates in historical_campaigns_config.items():
            date_range = pd.date_range(start=dates["début"], end=dates["fin"])
            temp_df = pd.DataFrame(
                {
                    "produit": product_name,
                    "round": f"round {round_num}",
                    "year": np.int32(year),
                    "period": date_range,
                }
            )
            temp_df["order_day"] = range(1, len(date_range) + 1)

            all_campaigns.append(temp_df)

        if not all_campaigns:
            current_run.log_error(
                "Aucune campagne historique trouvée dans la configuration"
            )
            raise

        all_campaigns_df = pd.concat(all_campaigns, ignore_index=True)

        return all_campaigns_df
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du DataFrame des périodes de campagne: {e}"
        )
        raise


def combine_dfs(
    org_unit_ids_df: pd.DataFrame,
    age_product_year_round_df: pd.DataFrame,
    product_site_df: pd.DataFrame,
    sex_type_df: pd.DataFrame,
    product_status_df: pd.DataFrame,
    campaign_period_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine all DataFrames into a single DataFrame representing the combination products dataset of historical campaigns.

    Args:
        org_unit_ids_df (pd.DataFrame): DataFrame with org unit IDs.
        age_product_year_round_df (pd.DataFrame): DataFrame with age, product, year, and round combinations.
        product_site_df (pd.DataFrame): DataFrame with product and site combinations.
        sex_type_df (pd.DataFrame): DataFrame with sex types.
        product_status_df (pd.DataFrame): DataFrame with product and status combinations.
        campaign_period_df (pd.DataFrame): DataFrame with campaign periods.

    Returns:
        combined_df (pd.DataFrame): The combined DataFrame representing the combination products dataset of historical campaigns.
    """
    current_run.log_info(
        "Combinaison de tous les DataFrames de campagnes historiques en un seul jeu de données..."
    )
    try:
        # cross join org_unit, sex, and combo df
        org_unit_ids_df = org_unit_ids_df[
            ["org_unit_id", "LVL_2_NAME", "LVL_3_NAME", "LVL_6_NAME"]
        ].drop_duplicates()
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
            campaign_period_df,
            on=["produit", "year", "round"],
            how="left",
            indicator=True,
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
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la combinaison des DataFrames de campagnes historiques: {e}"
        )
        raise


def adjust_to_specific_campaigns(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adjust the combined DataFrame for specific campaigns as needed.

    Args:
        combined_df (pd.DataFrame): DataFrame containing the combined campaign data.

    Returns:
        combined_df (pd.DataFrame): Adjusted DataFrame with specific campaign adjustments applied.
    """
    current_run.log_info(
        "Ajustement du DataFrame combiné pour des campagnes spécifiques..."
    )

    # For yellow fever campaigns 2025 2026 round 1, delete all entries outside the
    #  regions of Dosso and Tahoua (only these 2 regions have been covered)
    mask_yellow_fever_dosso_tahouha = (
        (combined_df["produit"] == "fièvre jaune")
        & (combined_df["year"].isin([2025, 2026]))
        & (combined_df["round"] == "round 1")
        & (~combined_df["LVL_2_NAME"].isin(["Dosso", "Tahoua"]))
    )
    combined_df = combined_df[~mask_yellow_fever_dosso_tahouha].reset_index(drop=True)
    combined_df = combined_df.drop(columns=["LVL_2_NAME"])

    return combined_df


def add_new_campaign_configurations(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Import all config files relating to new campaigns and append the corresponding
    configurations to the combined DataFrame of historical campaigns.

    Args:
        combined_df (pd.DataFrame): DataFrame containing the combined campaign data.

    Returns:
        combined_df (pd.DataFrame): Adjusted DataFrame with new campaign configurations added.
    """
    current_run.log_info(
        "Ajout des configurations des nouvelles campagnes au DataFrame combiné..."
    )
    try:
        config_files = [
            f
            for f in os.listdir(CONFIG_PATH)
            if f.startswith("config_") and f.endswith(".parquet")
        ]
        if not config_files:
            current_run.log_warning(
                f"Aucun fichier de configuration de nouvelle campagne trouvé dans le dossier {CONFIG_PATH}. Aucune configuration de nouvelle campagne ne sera ajoutée aux données combinées."
            )
            return combined_df
        else:
            current_run.log_info(
                f"Fichiers de configuration de nouvelle campagne trouvés: {config_files}."
            )
            for config_file in config_files:
                config_path = os.path.join(CONFIG_PATH, config_file)
                config_df = pd.read_parquet(config_path)
                combined_df = pd.concat([combined_df, config_df], ignore_index=True)

            combined_df = combined_df.drop_duplicates().reset_index(drop=True)

            current_run.log_info(
                "Configurations des nouvelles campagnes ajoutées avec succès au DataFrame combiné."
            )
            return combined_df
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'ajout des configurations des nouvelles campagnes au DataFrame combiné: {e}"
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
    build_combination_products_dataset()
