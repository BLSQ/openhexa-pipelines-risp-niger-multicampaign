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
    iaso_df_common_cols,
    cvrg_campaign_map,
    cols_campaign_map,
    district_level_target_keys,
    district_level_group_keys,
    district_level_final_keys,
    district_level_cumsum_keys,
    district_level_config,
    csi_level_target_keys,
    csi_level_final_keys,
    csi_level_cumsum_keys,
    csi_level_config,
    product_campaign_mapping,
    stocks_campaign_map,
    stock_ratios_config,
    surveillance_campaign_map,
    communication_cols,
)
from utils import (
    Conector_from_Dict,
    IASOConnectionHandler,
    all_rows,
    round_assignment,
    year_assignment,
    new_cols,
    age_categorizer,
    site_categorizer,
    produit_categorizer,
    vaccination_status_categorizer,
    product_status_categorizer,
    supervision_categorizer,
)


@pipeline("build_visualisation_tables")
def build_visualisation_tables():
    """ """
    historical_df = import_historical_iaso_data()
    current_df = extract_iaso_data_for_current_period()
    combined_df = combine_historical_and_current_data(historical_df, current_df)
    combined_df = retrieve_org_unit_ids(combined_df)
    combined_df = clean_combined_df(combined_df)

    # target_df = import_target_data()
    # combined_campaign_data_df = import_combined_campaign_data()
    # cvrg_total, cvrg_df = create_coverage_dataset(
    #     combined_df, combined_campaign_data_df
    # )
    # cvrg_csi_district = add_target_data(cvrg_df, target_df)
    # cmpl = create_completeness_dataset(combined_df, combined_campaign_data_df)
    # stock = create_stocks_dataset(combined_df, cvrg_total)
    supervision = create_surveillance_dataset(combined_df)
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
    combined_df["org_unit_id"] = combined_df["org_unit_id"].astype(np.int64)

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

    is_fjaune = df["campaign"] == "fièvre jaune"
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
    df.loc[is_men_tcv, "vaccination_status"] = "zéro dose"
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
    cvrg_total_all_values = df.groupby(group_cols, as_index=False)["value"].sum()

    # remove zero value entries
    mask_value_zero = cvrg_total_all_values["value"] == 0
    cvrg_total = cvrg_total_all_values[~mask_value_zero].copy()

    count_value_zero = mask_value_zero.sum()
    if count_value_zero > 0:
        proportion_value_zero = count_value_zero / len(cvrg_total_all_values)
        current_run.log_warning(
            f"{len(mask_value_zero)} entrées liées aux informations du nombre de cas vaccinés ({proportion_value_zero:.2%}) ont été supprimées car aucune valeur n'a été attribuée."
        )

    # merge with expected combined campaign data to ensure all combinations are present
    df_final = cvrg_total.merge(
        combined_campaign_data_df,
        on=[
            "year",
            "round",
            "period",
            "age",
            "sexe",
            "org_unit_id",
            "produit",
            "vaccination_status",
            "site",
        ],
        how="left",
        indicator=True,
    )
    unmatched_entries_in_iaso = df_final[df_final["_merge"] == "left_only"]
    if not unmatched_entries_in_iaso.empty:
        proportion_unmatched_in_iaso = len(unmatched_entries_in_iaso) / len(df_final)
        current_run.log_warning(
            f"{len(unmatched_entries_in_iaso)} entrées ({proportion_unmatched_in_iaso:.2%}) dans IASO n'ont pas été trouvées dans le expected dataframe. Ces entrées seront supprimées."
        )
    df_final = df_final[df_final["_merge"] == "both"].drop(columns=["_merge"])

    return cvrg_total, df_final


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

    no_district_entries = coverage_df[coverage_df["LVL_3_NAME"].isna()]
    if not no_district_entries.empty:
        proportion_no_district = len(no_district_entries) / len(coverage_df)
        current_run.log_warning(
            f"{len(no_district_entries)} entrée(s) ({proportion_no_district:.2%}) du jeu de données de couverture n'ont pas de nom de district associé."
            "Ces entrées auront une valeur de cible NaN."
        )

    coverage_district = coverage_df.groupby(
        district_level_group_keys, as_index=False
    ).agg({"value": "sum", "org_unit_id": "first"})

    coverage_district_with_target_df = coverage_district.merge(
        target_district,
        on=district_level_target_keys,
        how="left",
    )[district_level_final_keys]
    no_target_entries_ds = coverage_district_with_target_df[
        coverage_district_with_target_df["cible"].isna()
    ]
    if not no_target_entries_ds.empty:
        proportion_no_target_ds = len(no_target_entries_ds) / len(coverage_district)
        current_run.log_warning(
            f"{len(no_target_entries_ds)} entrée(s) ({proportion_no_target_ds:.2%}) du jeu de données de couverture au niveau des districts n'ont pas de cible associée."
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
    config_list = []
    for year, products in csi_level_config.items():
        for prod, rounds in products.items():
            for r in rounds:
                config_list.append({"year": year, "produit": prod, "round": r})
    csi_level_filter = pd.DataFrame(config_list)
    target_csi = target_df.merge(
        csi_level_filter, on=["year", "produit", "round"], how="inner"
    ).drop(columns=["LVL_3_NAME", "LVL_6_NAME"])
    coverage_df_csi = coverage_df.merge(
        csi_level_filter, on=["year", "produit", "round"], how="inner"
    )

    coverage_csi_with_target_df = coverage_df_csi.merge(
        target_csi,
        on=csi_level_target_keys,
        how="left",
    )[csi_level_final_keys]

    no_target_entries_csi = coverage_csi_with_target_df[
        coverage_csi_with_target_df["cible"].isna()
    ]

    if not no_target_entries_csi.empty:
        affected_csi_list = no_target_entries_csi["LVL_6_NAME"].unique().tolist()
        proportion_no_target_csi = len(no_target_entries_csi) / len(coverage_df_csi)
        current_run.log_warning(
            f"{len(no_target_entries_csi)} entrée(s) ({proportion_no_target_csi:.2%}) du jeu de données de couverture au niveau des CSI n'ont pas de cible associée."
            "Ces entrées auront une valeur de cible manquante."
            f"CSI affectés: {', '.join(affected_csi_list)}"
        )

    coverage_csi_with_target_df["value_cum"] = coverage_csi_with_target_df.groupby(
        csi_level_cumsum_keys
    )["value"].transform("cumsum")
    coverage_csi_with_target_df["cible"] = coverage_csi_with_target_df["cible"].astype(
        "Int64"
    )

    cvrg_csi_district = pd.concat(
        [coverage_csi_with_target_df, coverage_district_with_target_df],
        ignore_index=True,
    )
    return cvrg_csi_district


def create_completeness_dataset(
    combined_df: pd.DataFrame, combined_campaign_data_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Create completeness tables for visualization.

    Args:
        combined_df (pd.DataFrame): Combined campaign data from IASO DataFrame.
        combined_campaign_data_df (pd.DataFrame): Expected combined campaign data DataFrame.

    Returns:
        pd.DataFrame: Completeness dataset DataFrame.
    """
    current_run.log_info("Creating completeness dataset...")

    cmpl = combined_df[["period", "org_unit_id", "choix_campagne"]]
    combined_campaign_data_df_adjusted = combined_campaign_data_df[
        ["org_unit_id", "period", "produit"]
    ]
    combined_campaign_data_df_adjusted.loc[:, "produit"] = (
        combined_campaign_data_df_adjusted["produit"].map(product_campaign_mapping)
    )
    combined_campaign_data_df_adjusted = combined_campaign_data_df_adjusted.rename(
        columns={"produit": "choix_campagne"}
    )
    combined_campaign_data_df_adjusted = (
        combined_campaign_data_df_adjusted.drop_duplicates()
    )

    cmpl = (
        all_rows(combined_campaign_data_df_adjusted, cmpl, "org_unit_id", "period")
        .sort_values(by=["period", "org_unit_id"])
        .reset_index(drop=True)
    )
    cmpl = round_assignment(cmpl)
    year_assignment(cmpl)
    cmpl = cmpl.sort_values(
        ["year", "round", "org_unit_id", "choix_campagne", "period"]
    )

    cmpl["presence_equipe_cum"] = cmpl.groupby(
        ["year", "round", "org_unit_id", "choix_campagne"]
    )["presence_equipe"].transform("cummax")

    return cmpl


def create_stocks_dataset(
    combined_df: pd.DataFrame, cvrg_total: pd.DataFrame
) -> pd.DataFrame:
    """
    Create stocks tables for visualization.

    Args:
        combined_df (pd.DataFrame): Combined campaign data from IASO DataFrame.
        cvrg_total (pd.DataFrame): Coverage total DataFrame.

    Returns:
        pd.DataFrame: Completeness dataset DataFrame.
    """
    current_run.log_info("Creating stocks dataset...")

    id_vars = ["period", "round", "year", "org_unit_id"]
    all_campaign_data = []

    for campaign_name, cols in stocks_campaign_map.items():
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
            produit_categorizer,
            product_status_categorizer,
        ],
    ).drop(columns=["category"])

    group_cols = ["year", "round", "period", "org_unit_id", "produit", "product_status"]
    stock_total_all_values = df.groupby(group_cols, as_index=False)["value"].sum()

    # remove zero value entries
    mask_value_zero = stock_total_all_values["value"] == 0
    stock_total = stock_total_all_values[~mask_value_zero].copy()
    count_value_zero = mask_value_zero.sum()
    if count_value_zero > 0:
        proportion_value_zero = count_value_zero / len(stock_total_all_values)
        current_run.log_warning(
            f"{len(mask_value_zero)} entrées liées aux informations des stocks ({proportion_value_zero:.2%}) ont été supprimées car aucune valeur n'a été attribuée."
        )

    # pivot table to have stock, recu, utilise as columns
    stock_total_pivot = pd.pivot_table(
        stock_total,
        index=["year", "round", "period", "org_unit_id", "produit"],
        columns=["product_status"],
        values="value",
    ).reset_index()

    # compute stock metrics
    stock_total_pivot["box_ratio"] = stock_total_pivot["produit"].map(
        stock_ratios_config
    )
    stock_total_pivot["stock"] = stock_total_pivot["stock"].fillna(0)
    stock_total_pivot["reçu"] = stock_total_pivot["reçu"].fillna(0)
    stock_total_pivot["utilisé"] = stock_total_pivot["utilisé"].fillna(0)

    stock_total_pivot["total"] = stock_total_pivot["stock"] + stock_total_pivot["reçu"]
    stock_total_pivot["restant"] = (
        stock_total_pivot["total"] - stock_total_pivot["utilisé"]
    )
    stock_total_pivot["restant"] = np.where(
        stock_total_pivot["restant"] < 0, 0, stock_total_pivot["restant"]
    )

    # add number of cases vaccinated from coverage data
    cvrg_stock = (
        cvrg_total.groupby(
            ["year", "round", "period", "org_unit_id", "produit"], as_index=False
        )["value"]
        .sum()
        .rename(columns={"value": "enfants_vaccines"})
    )

    # cvrg_stock["produit"] = np.where(
    #     cvrg_stock["produit"] == "vaccin polio", "flacons polio", cvrg_stock["produit"]
    # )

    stock = stock_total_pivot.merge(cvrg_stock, how="left").fillna(0)

    return stock


def create_surveillance_dataset(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create surveillance tables for visualization.

    Args:
        combined_df (pd.DataFrame): Combined campaign data from IASO DataFrame.
        combined_campaign_data_df (pd.DataFrame): Expected combined campaign data DataFrame.

    Returns:
        pd.DataFrame: Surveillance dataset DataFrame.
    """
    current_run.log_info("Creating surveillance dataset...")

    id_vars = ["period", "round", "year", "org_unit_id"]
    all_campaign_data = []

    for campaign_name, cols in surveillance_campaign_map.items():
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
        temp_df["choix_campagne"] = campaign_name
        all_campaign_data.append(temp_df)

    supervision_all_values = pd.concat(all_campaign_data, ignore_index=True)

    # remove zero value entries
    mask_value_zero = supervision_all_values["value"] == 0
    supervision = supervision_all_values[~mask_value_zero].copy()
    count_value_zero = mask_value_zero.sum()
    if count_value_zero > 0:
        proportion_value_zero = count_value_zero / len(supervision_all_values)
        current_run.log_warning(
            f"{len(mask_value_zero)} entrées ({proportion_value_zero:.2%}) liées aux informations de surveillance ont été supprimées car aucune valeur n'a été attribuée."
        )

    supervision = new_cols(
        supervision,
        "categorizer",
        "category",
        [
            supervision_categorizer,
        ],
    ).drop(columns=["category"])

    supervision_total = (
        supervision.groupby(
            ["year", "round", "period", "org_unit_id", "choix_campagne", "supervision"],
            as_index=False,
        )["value"]
        .sum()
        .fillna(0)
    )

    supervision_pivot = pd.pivot_table(
        supervision_total,
        index=["year", "round", "period", "org_unit_id", "choix_campagne"],
        columns=["supervision"],
        values="value",
        fill_value=0,
    ).reset_index()

    return supervision_pivot


if __name__ == "__main__":
    build_visualisation_tables()
