import os
import pandas as pd
import numpy as np
from openhexa.sdk import current_run, workspace, pipeline
from pathlib import Path
from config import (
    iaso_playground_connector_slug,
    iaso_connector_slug,
    iaso_form_id,
    current_period,
    current_period_start_date,
    campaign_name_cleaning_dict,
    campaign_name_mapping_dict,
    cvrg_campaign_map,
    district_level_target_keys,
    district_level_group_keys,
    district_level_final_keys,
    district_level_cumsum_keys,
    csi_level_target_keys,
    csi_level_final_keys,
    csi_level_cumsum_keys,
)
from utils import (
    Conector_from_Dict,
    IASOConnectionHandler,
    round_assignment,
    year_assignment,
    new_cols,
    age_categorizer,
    site_categorizer,
    produit_categorizer,
    vaccination_status_categorizer,
)


@pipeline("build_visualisation_tables")
def build_visualisation_tables():
    """ """
    historical_df = import_historical_iaso_data()
    current_df = extract_iaso_data_for_current_period()
    combined_df = combine_historical_and_current_data(historical_df, current_df)
    combined_df = retrieve_org_unit_ids(combined_df)
    combined_df = clean_combined_df(combined_df)

    target_df = import_target_data()
    combined_campaign_data_df = import_combined_campaign_data()
    coverage_df = create_coverage_dataset(combined_df, combined_campaign_data_df)
    coverage_with_target_df = add_target_data(coverage_df, target_df)
    x


def import_historical_iaso_data() -> pd.DataFrame:
    """
    Import historical data from IASO historical data folder.

    Args:
        None

    Returns:
        pd.DataFrame: Combined historical data from all feather files.
    """
    current_run.log_info("Importing historical data...")
    historical_data_folder = os.path.join(workspace.files_path, "IASO historical data")
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
        "temp",
        f"multicampaign_df_{current_period}_raw.feather",
    )
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    current_df.to_feather(
        os.path.join(
            workspace.files_path,
            "temp",
            f"multicampaign_df_{current_period}_raw.feather",
        )
    )

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


def retrieve_org_unit_ids(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Retrieve organization unit IDs from clean IASO organisation tree.

    Args:
        None

    Returns:
        pd.DataFrame: the combined containing organization unit IDs.
    """
    current_run.log_info("Retrieving organization unit IDs...")

    file_path_clean = os.path.join(
        workspace.files_path,
        "process_target_data",
        "output",
        "iaso_org_unit_tree_clean.parquet",
    )
    file_path_raw = os.path.join(
        workspace.files_path,
        "process_target_data",
        "output",
        "iaso_org_unit_tree_raw.parquet",
    )
    iaso_org_unit_tree_clean = pd.read_parquet(file_path_clean)
    iaso_org_unit_tree_raw = pd.read_parquet(file_path_raw)

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

    return combined_df


def clean_combined_df(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the combined DataFrame by:
        - exploding the data to deal with multi-campaign entries

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

    # assign rounds and check proportion of entries outside the range of campaign rounds
    combined_df = round_assignment(combined_df)
    year_assignment(combined_df)

    invalid_date = combined_df[
        combined_df["round"].isna() | combined_df["round"].isin(["date invalide"])
    ]
    if not invalid_date.empty:
        proportion_invalid = len(invalid_date) / len(combined_df)
        current_run.log_warning(
            f"{len(invalid_date)} entrée(s) ({proportion_invalid:.2%}) contiennent des dates invalides ou hors période de campagne."
        )

    return combined_df


def import_target_data() -> pd.DataFrame:
    """
    Import target data from processed target data folder.

    Args:
        None

    Returns:
        pd.DataFrame: Target data DataFrame.
    """
    current_run.log_info("Importing target data...")
    file_path = os.path.join(
        workspace.files_path,
        "process_target_data",
        "output",
        "combined_target_data.parquet",
    )
    target_df = pd.read_parquet(file_path)
    return target_df


def import_combined_campaign_data() -> pd.DataFrame:
    """
    Import combined campaign data from built combination products pipeline.

    Args:
        None

    Returns:
        pd.DataFrame: Combined campaign data DataFrame.
    """
    current_run.log_info("Importing combined campaign data...")
    file_path = os.path.join(
        workspace.files_path,
        "build_combination_products",
        "output",
        "combined_campaign_data.parquet",
    )
    combined_campaign_data_df = pd.read_parquet(file_path)
    return combined_campaign_data_df


def create_coverage_dataset(
    combined_df: pd.DataFrame,
    combined_campaign_data_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create coverage tables for visualization.

    Args:
        combined_df (pd.DataFrame): Combined campaign data from IASO DataFrame.
        combined_campaign_data_df (pd.DataFrame): Expected combined campaign data DataFrame.

    Returns:
        pd.DataFrame: Coverage dataset DataFrame.
    """
    current_run.log_info("Creating coverage dataset...")

    id_vars = ["period", "round", "year", "org_unit_id"]
    all_campaign_data = []

    for campaign_name, cols in cvrg_campaign_map.items():
        valid_cols = [c for c in cols if c in combined_df.columns]

        if not valid_cols:
            continue

        temp_df = pd.melt(
            combined_df[id_vars + list(set(valid_cols))].fillna(0),
            id_vars=id_vars,
            value_vars=list(set(valid_cols)),
            var_name="category",
            value_name="value",
        )
        temp_df["campaign"] = campaign_name
        all_campaign_data.append(temp_df)

    df = pd.concat(all_campaign_data, ignore_index=True)
    df = new_cols(
        df,
        "categorizer",
        "category",
        [
            age_categorizer,
            site_categorizer,
            produit_categorizer,
            vaccination_status_categorizer,
        ],
    ).drop(columns=["category"])
    df["sexe"] = "TOUS"

    is_fjaune = df["campaign"] == "fievre jaune"
    df.loc[is_fjaune, "site"] = "ordinaire"
    df.loc[is_fjaune, "age"] = df.loc[is_fjaune, "age"].replace(
        {
            "12-23 mois": "12-59 mois",
            "24-59 mois": "12-59 mois",
            "0-11 mois": "9-11 mois",
        }
    )

    is_rougeole = df["campaign"] == "rougeole"
    df.loc[is_rougeole, "produit"] = "rougeole"
    df.loc[is_rougeole, "age"] = df.loc[is_rougeole, "age"].replace(
        {"6-9 mois": "6-11 mois", "9-11 mois": "6-11 mois"}
    )

    is_men_tcv = df["campaign"].isin(["méningite", "tcv"])
    df.loc[is_men_tcv, "vaccination_status"] = "zero dose"
    df.loc[is_men_tcv, "site"] = "ordinaire"

    group_cols = [
        "year",
        "round",
        "period",
        "age",
        "sexe",
        "org_unit_id",
        "produit",
        "vaccination_status",
        "site",
    ]
    df = df.groupby(group_cols, as_index=False)["value"].sum()
    df_final = df

    # # merge with expected combined campaign data to ensure all combinations are present
    # df_final = combined_campaign_data_df.merge(
    #     df,
    #     on=[
    #         "year",
    #         "round",
    #         "period",
    #         "age",
    #         "sexe",
    #         "org_unit_id",
    #         "produit",
    #         "vaccination_status",
    #         "site",
    #     ],
    #     how="left",
    # )

    # # check proportion of entries in combined_campaign_data_df found in coverage dataset
    # unmatched_entries = df_final[df_final["value"].isna()]
    # if not unmatched_entries.empty:
    #     proportion_unmatched = len(unmatched_entries) / len(combined_campaign_data_df)
    #     current_run.log_warning(
    #         f"{len(unmatched_entries)} entrée(s) ({proportion_unmatched:.2%}) du jeu de données combiné de campagne n'ont pas été trouvées dans le jeu de données de couverture."
    #     )
    return df_final


def add_target_data(coverage_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add target data to the coverage dataset.

    Args:
        coverage_df (pd.DataFrame): Coverage dataset DataFrame.
        target_df (pd.DataFrame): Target data DataFrame.

    Returns:
        pd.DataFrame: Coverage dataset with target data added.
    """
    current_run.log_info("Adding target data to coverage dataset...")

    # district-level
    target_district = target_df.groupby(
        district_level_target_keys,
        as_index=False,
    )["cible"].sum()

    district_name_mapping = (
        target_df[["LVL_3_NAME", "org_unit_id"]].copy().drop_duplicates()
    )

    coverage_district = coverage_df.merge(
        district_name_mapping,
        on="org_unit_id",
        how="left",
    )

    no_district_entries = coverage_district[coverage_district["LVL_3_NAME"].isna()]
    if not no_district_entries.empty:
        proportion_no_district = len(no_district_entries) / len(coverage_district)
        current_run.log_warning(
            f"{len(no_district_entries)} entrée(s) ({proportion_no_district:.2%}) du jeu de données de couverture n'ont pas de nom de district associé."
            "Ces entrées auront une valeur de cible NaN."
        )

    coverage_district = coverage_district.groupby(
        district_level_group_keys, as_index=False
    ).agg({"value": "sum", "org_unit_id": "first"})

    coverage_district_with_target_df = coverage_district.merge(
        target_district,
        on=district_level_target_keys,
        how="left",
    )[district_level_final_keys]
    no_target_entries = coverage_district_with_target_df[
        coverage_district_with_target_df["cible"].isna()
    ]
    if not no_target_entries.empty:
        proportion_no_target = len(no_target_entries) / len(
            coverage_district_with_target_df
        )
        current_run.log_warning(
            f"{len(no_target_entries)} entrée(s) ({proportion_no_target:.2%}) du jeu de données de couverture au niveau des districts n'ont pas de cible associée."
            "Ces entrées auront une valeur de cible de 0."
        )

    coverage_district_with_target_df["value_cum"] = (
        coverage_district_with_target_df.groupby(district_level_cumsum_keys)[
            "value"
        ].transform("cumsum")
    )
    coverage_district_with_target_df["cible"] = (
        coverage_district_with_target_df["cible"].fillna(0).astype(int)
    )

    # csi level
    coverage_csi_with_target_df = coverage_df.merge(
        target_df,
        on=csi_level_target_keys,
        how="left",
    )[csi_level_final_keys]

    no_target_entries = coverage_csi_with_target_df[
        coverage_csi_with_target_df["cible"].isna()
    ]

    if not no_target_entries.empty:
        proportion_no_target = len(no_target_entries) / len(coverage_csi_with_target_df)
        current_run.log_warning(
            f"{len(no_target_entries)} entrée(s) ({proportion_no_target:.2%}) du jeu de données de couverture au niveau des CSI n'ont pas de cible associée."
            "Ces entrées auront une valeur de cible de 0."
        )

    coverage_csi_with_target_df["value_cum"] = coverage_csi_with_target_df.groupby(
        csi_level_cumsum_keys
    )["value"].transform("cumsum")
    coverage_csi_with_target_df["cible"] = (
        coverage_csi_with_target_df["cible"].fillna(0).astype(int)
    )

    cvrg_csi_district = pd.concat(
        [coverage_csi_with_target_df, coverage_district_with_target_df],
        ignore_index=True,
    )
    return cvrg_csi_district


if __name__ == "__main__":
    build_visualisation_tables()
