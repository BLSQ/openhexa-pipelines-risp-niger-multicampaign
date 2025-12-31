import os
from openhexa.sdk import current_run, workspace, pipeline
import pandas as pd
import numpy as np
from pathlib import Path
from utils import (
    IASOConnectionHandler,
    org_unit_matching,
    pyramid_selector,
)
from config import (
    iaso_connector_slug,
    iaso_form_id,
    target_polio_2024_cols,
    polio_2024_dict_districts_cibles_iaso,
    target_polio_rougeole_2025_columns,
    target_yellow_fever_2025_columns,
    target_yellow_fever_2025_age_ranges,
    target_men5_tcv_2025_columns_dict,
    target_polio_2025_r3_columns,
    csi_matching_failed,
)


@pipeline("process_target_data")
def process_target_data():
    """ """
    spatial_df = get_spatial_data()
    spatial_df_clean = clean_spatial_data(spatial_df)

    # district-level target data
    targets_polio_2024_r1_r4 = import_target_data_for_polio_2024_r1_r4()
    targets_polio_2024_r1_r4 = match_district_to_org_unit_id(
        targets_polio_2024_r1_r4, spatial_df_clean
    )
    targets_polio_2024_r1_r4 = add_rounds_and_products(targets_polio_2024_r1_r4)

    target_polio_rougeole_2025_r1_r2 = (
        import_target_data_for_polio_and_rougeole_2025_r1_r2()
    )
    target_polio_rougeole_2025_r1_r2 = match_district_to_org_unit_id(
        target_polio_rougeole_2025_r1_r2, spatial_df_clean
    )
    target_polio_rougeole_2025_r1_r2 = add_rounds_and_products(
        target_polio_rougeole_2025_r1_r2
    )

    # csi-level target data
    target_yellow_fever_2025_r1_r2 = import_target_data_for_yellow_fever_2025_r1_r2()
    target_yellow_fever_2025_r1_r2 = match_csi_to_org_unit_id(
        target_yellow_fever_2025_r1_r2, spatial_df_clean
    )
    target_yellow_fever_2025_r1_r2 = add_rounds_and_products(
        target_yellow_fever_2025_r1_r2
    )

    target_men5_tcv_2025_r1_r2 = import_target_data_for_men5_and_tcv_2025_r1_r2()
    target_men5_tcv_2025_r1_r2 = match_csi_to_org_unit_id(
        target_men5_tcv_2025_r1_r2, spatial_df_clean
    )
    target_men5_tcv_2025_r1_r2 = add_rounds_and_products(target_men5_tcv_2025_r1_r2)

    target_polio_2025_r3 = import_target_data_for_polio_2025_r3()
    target_polio_2025_r3 = match_csi_to_org_unit_id(
        target_polio_2025_r3, spatial_df_clean
    )
    target_polio_2025_r3 = add_rounds_and_products(target_polio_2025_r3)

    # combine all target data
    target_data_combined = combine_target_data(
        [
            targets_polio_2024_r1_r4,
            target_polio_rougeole_2025_r1_r2,
            target_yellow_fever_2025_r1_r2,
            target_men5_tcv_2025_r1_r2,
            target_polio_2025_r3,
        ]
    )

    # save
    save_output(target_data_combined)


def get_spatial_data() -> pd.DataFrame:
    """
    Retrieve organizational unit tree data from IASO based on a specific form ID.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the organizational unit tree data.
    """
    current_run.log_info("Retrieving spatial data...")

    # iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
    # spatial_df = iaso_connector_instance.get_ou_tree_dataframe_from_the_form(
    #     iaso_form_id
    # )

    spatial_df = pd.read_parquet(
        os.path.join(
            workspace.files_path,
            "temp",
            "spatial_units_raw.parquet",
        )
    )

    return spatial_df


def clean_spatial_data(spatial_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the spatial data by filtering out rejected entries and selecting relevant records.

    Args:
        spatial_df (pd.DataFrame): DataFrame containing the spatial data to be cleaned.

    Returns:
        pd.DataFrame: Cleaned DataFrame with relevant spatial data.
    """
    current_run.log_info("Cleaning spatial data...")

    spatial_df_clean = spatial_df[spatial_df["Validé"] != "REJECTED"]
    spatial_df_clean = spatial_df_clean[spatial_df_clean["Source"] == "SNIS"]

    spatial_df_clean = spatial_df_clean.groupby("LVL_6_UID", as_index=False).apply(
        pyramid_selector, include_groups=False
    )

    return spatial_df_clean


def import_target_data_for_polio_2024_r1_r4() -> pd.DataFrame:
    """
    Import target data for Polio 2024 rounds 1 to 4

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for Polio 2024 rounds 1 to 4
    """
    current_run.log_info("Importing target data for Polio 2024 rounds 1 to 4...")

    file_path = os.path.join(
        workspace.files_path, "cibles", "Population JNV JNM ET DEPRARASITAGE.xlsx"
    )
    target_polio_2024 = pd.read_excel(
        file_path, skiprows=6, header=None, usecols=[1, 2, 3, 6, 7, 9, 10]
    )

    target_polio_2024.columns = target_polio_2024_cols

    target_polio_2024 = target_polio_2024[
        ~target_polio_2024["LVL_3_NAME"]
        .str.contains("Région|TOTAL", case=False)
        .fillna(True)
    ]

    target_polio_2024["LVL_3_NAME"] = target_polio_2024["LVL_3_NAME"].map(
        polio_2024_dict_districts_cibles_iaso
    )
    target_polio_2024 = pd.melt(
        target_polio_2024,
        id_vars="LVL_3_NAME",
        var_name="full_name",
        value_name="cible",
    )

    target_polio_2024["age"] = target_polio_2024["full_name"].str.split(
        "_", expand=True
    )[1]
    target_polio_2024["cible"] = target_polio_2024["cible"].astype(int)
    target_polio_2024["year"] = 2024
    target_polio_2024["campaign"] = "polio"

    return target_polio_2024


def import_target_data_for_polio_and_rougeole_2025_r1_r2() -> pd.DataFrame:
    """
    Import target data for polio and rougeole campaigns for year 2025 rounds 1 and 2

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for polio and rougeole campaigns year 2025 rounds 1 and 2
    """
    current_run.log_info("Importing target data for Polio and Rougeole 2025...")

    file_path = os.path.join(
        workspace.files_path, "cibles", "cible_niger_et_refugies_2025.xlsx"
    )

    target_polio_rougeole_2025 = pd.read_excel(
        file_path, header=[0], skiprows=1, usecols=[0, 9, 10]
    )

    target_polio_rougeole_2025.columns = target_polio_rougeole_2025_columns

    target_polio_rougeole_2025 = target_polio_rougeole_2025.dropna(
        subset=["LVL_3_NAME"]
    )
    target_polio_rougeole_2025 = target_polio_rougeole_2025[
        ~target_polio_rougeole_2025["LVL_3_NAME"]
        .str.contains("Région|TOTAL|Refugie", case=False)
        .fillna(True)
    ]
    target_polio_rougeole_2025 = pd.melt(
        target_polio_rougeole_2025,
        id_vars="LVL_3_NAME",
        var_name="age",
        value_name="cible",
    ).fillna(0)

    target_polio_rougeole_2025["LVL_3_NAME"] = target_polio_rougeole_2025[
        "LVL_3_NAME"
    ].map(polio_2024_dict_districts_cibles_iaso)
    target_polio_rougeole_2025["cible"] = target_polio_rougeole_2025["cible"].astype(
        int
    )
    target_polio_rougeole_2025["year"] = 2025
    target_polio_rougeole_2025["campaign"] = "polio_rougeole"

    return target_polio_rougeole_2025


def import_target_data_for_yellow_fever_2025_r1_r2() -> pd.DataFrame:
    """
    Import target data for yellow fever campaign for year 2025 rounds 1 and 2

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for yellow fever campaign year 2025 rounds 1 and 2
    """
    current_run.log_info("Importing target data for Yellow Fever 2025...")

    file_path = os.path.join(
        workspace.files_path, "cibles", "cible_csi_fj_dosso_tahoua.xlsx"
    )

    target_yellow_fever_2025 = pd.read_excel(
        file_path,
        header=[0],
        skiprows=10,
        usecols=[2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 14, 15, 16, 17],
    )

    target_yellow_fever_2025.columns = target_yellow_fever_2025_columns
    target_yellow_fever_2025 = target_yellow_fever_2025[
        ~target_yellow_fever_2025["LVL_3_NAME"].str.contains("Total")
    ]
    target_yellow_fever_2025 = target_yellow_fever_2025[
        ~(target_yellow_fever_2025["LVL_6_NAME"] == "DS")
    ]

    for age in target_yellow_fever_2025_age_ranges:
        target_yellow_fever_2025[age] = (
            target_yellow_fever_2025[age + "_urban"]
            + target_yellow_fever_2025[age + "_avancee"]
            + target_yellow_fever_2025[age + "_mobile"]
        )
        target_yellow_fever_2025[age] = target_yellow_fever_2025[age].astype(int)

    target_yellow_fever_2025_clean = target_yellow_fever_2025[
        ["LVL_3_NAME", "LVL_6_NAME"] + target_yellow_fever_2025_age_ranges
    ]
    target_yellow_fever_2025_clean = pd.melt(
        target_yellow_fever_2025_clean,
        id_vars=["LVL_3_NAME", "LVL_6_NAME"],
        var_name="age",
        value_name="cible",
    ).fillna(0)

    target_yellow_fever_2025_clean["cible"] = target_yellow_fever_2025_clean[
        "cible"
    ].astype(int)
    target_yellow_fever_2025_clean["year"] = 2025
    target_yellow_fever_2025_clean["campaign"] = "fievre jaune"

    return target_yellow_fever_2025_clean


def import_target_data_for_men5_and_tcv_2025_r1_r2() -> pd.DataFrame:
    """
    Import target data for men5 and tcv campaigns for year 2025 rounds 1 and 2

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for yellow fever campaign year 2025 rounds 1 and 2
    """
    current_run.log_info("Importing target data for Men5 and TCV campaign 2025...")

    file_path = os.path.join(workspace.files_path, "cibles", "Cible Men5-TCV CSI.xlsx")

    target_men5_tcv_2025 = pd.read_excel(
        file_path,
        skiprows=3,
        usecols=[1, 2, 4, 5, 6],
    )

    target_men5_tcv_2025.columns = [
        target_men5_tcv_2025_columns_dict[col]
        for col in target_men5_tcv_2025_columns_dict
    ]

    target_men5_tcv_2025_clean = pd.melt(
        target_men5_tcv_2025,
        id_vars=["LVL_3_NAME", "LVL_6_NAME"],
        value_vars=["1-4 ans", "5-14 ans", "15-19 ans"],
        var_name="age",
        value_name="cible",
    )

    target_men5_tcv_2025_clean["cible"] = target_men5_tcv_2025_clean["cible"].astype(
        int
    )
    target_men5_tcv_2025_clean["year"] = 2025
    target_men5_tcv_2025_clean = target_men5_tcv_2025_clean[
        ["LVL_3_NAME", "LVL_6_NAME", "age", "cible", "year"]
    ].drop_duplicates()
    target_men5_tcv_2025_clean["campaign"] = "men5_tcv"

    return target_men5_tcv_2025_clean


def import_target_data_for_polio_2025_r3() -> pd.DataFrame:
    """
    Import target data for polio campaign for year 2025 round 3

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for polio campaign year 2025 round 3
    """
    current_run.log_info("Importing target data for Polio 2025 campaign round 3...")

    file_path = os.path.join(
        workspace.files_path, "cibles", "cible_jnv_polio_2025.xlsx"
    )

    target_polio_2025_r3 = pd.read_excel(
        file_path, header=[0], skiprows=9, usecols=[1, 2, 3, 7]
    )

    target_polio_2025_r3.columns = target_polio_2025_r3_columns

    target_polio_2025_r3 = target_polio_2025_r3.dropna(subset=["LVL_3_NAME"])
    target_polio_2025_r3 = target_polio_2025_r3.dropna(subset=["cible"])

    target_polio_2025_r3 = target_polio_2025_r3[
        ~target_polio_2025_r3["LVL_3_NAME"].str.contains("Total")
    ]
    target_polio_2025_r3 = target_polio_2025_r3[
        ~(target_polio_2025_r3["LVL_6_NAME"] == "DS")
    ]

    target_polio_2025_r3["cible"] = target_polio_2025_r3["cible"].astype(int)
    target_polio_2025_r3["0-11 mois"] = (
        target_polio_2025_r3["cible"] * 0.119140832
    ).round(0)
    target_polio_2025_r3["12-59 mois"] = (
        target_polio_2025_r3["cible"] * 0.880859168
    ).round(0)
    target_polio_2025_r3 = target_polio_2025_r3.dropna(subset=["LVL_6_NAME"])
    target_polio_2025_r3.drop(["cible", "LVL_2_NAME"], axis=1, inplace=True)

    target_polio_2025_r3_clean = pd.melt(
        target_polio_2025_r3,
        id_vars=["LVL_3_NAME", "LVL_6_NAME"],
        var_name="age",
        value_name="cible",
    ).fillna(0)

    target_polio_2025_r3_clean["cible"] = target_polio_2025_r3_clean["cible"].astype(
        int
    )
    target_polio_2025_r3_clean["year"] = 2025
    target_polio_2025_r3_clean["campaign"] = "polio"

    return target_polio_2025_r3_clean


def match_csi_to_org_unit_id(
    csi_level_target_df: pd.DataFrame, spatial_df_clean: pd.DataFrame
) -> pd.DataFrame:
    """
    Match CSI names in df containing the CSI-level target data to organizational unit IDs using spatial data.

    Args:
        df (pd.DataFrame): DataFrame containing the target data at CSI level.
        spatial_df_clean (pd.DataFrame): Cleaned DataFrame containing the spatial data.

    Returns:
        pd.DataFrame: DataFrame with matched organizational unit IDs.
    """
    current_run.log_info("Matching CSI names to organizational unit IDs...")

    spatial_units_for_matching = spatial_df_clean[
        ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
    ].drop_duplicates()

    target_df_matched, spatial_check = org_unit_matching(
        csi_level_target_df, spatial_units_for_matching, threshold=50
    )

    # # save file to parquet for later use
    # cols_to_fix = [
    #     "LVL_3_NAME_original",
    #     "LVL_6_NAME_original",
    #     "LVL_3_NAME",
    #     "LVL_6_NAME",
    #     "cleansed_target",
    #     "cleansed_spatial_match",
    # ]
    # for col in cols_to_fix:
    #     if col in target_df_matched.columns:
    #         target_df_matched[col] = (
    #             target_df_matched[col].astype(str).replace("nan", "")
    #         )
    #     if col in spatial_check.columns:
    #         spatial_check[col] = spatial_check[col].astype(str).replace("nan", "")
    # target_df_matched.to_parquet(
    #     os.path.join(workspace.files_path, "temp", "target_df_matched.parquet"),
    #     index=False,
    # )
    # spatial_check.to_parquet(
    #     os.path.join(workspace.files_path, "temp", "spatial_check.parquet"), index=False
    # )
    # target_df_matched = pd.read_parquet(
    #     os.path.join(workspace.files_path, "temp", "target_df_matched.parquet")
    # )
    # spatial_check = pd.read_parquet(
    #     os.path.join(workspace.files_path, "temp", "spatial_check.parquet")
    # )

    # # inspect matching results
    # target_df_matched_check = target_df_matched[
    #     [
    #         "org_unit_id",
    #         "LVL_3_NAME_original",
    #         "LVL_6_NAME_original",
    #         "LVL_3_NAME",
    #         "LVL_6_NAME",
    #         "cleansed_target",
    #         "cleansed_spatial_match",
    #         "match_score",
    #     ]
    # ]
    # target_df_matched_check.to_csv(
    #     os.path.join(workspace.files_path, "temp", "target_df_matched_check.csv"),
    #     index=False,
    # )
    # spatial_check.to_csv(
    #     os.path.join(workspace.files_path, "temp", "spatial_check.csv"), index=False
    # )

    for (
        csi_concat_original,
        csi_concat_correct,
    ) in csi_matching_failed.items():
        if csi_concat_correct is None:
            mask = target_df_matched["cleansed_target"] == csi_concat_original
            target_df_matched.loc[mask, "org_unit_id"] = None
            target_df_matched.loc[mask, "LVL_3_NAME"] = None
            target_df_matched.loc[mask, "LVL_6_NAME"] = None
            continue

        spatial_row = spatial_check.loc[
            spatial_check["cleansed_spatial"] == csi_concat_correct
        ]
        if spatial_row.empty:
            continue

        lvl_3_name_correct = spatial_row["LVL_3_NAME"].values[0]
        lvl_6_name_correct = spatial_row["LVL_6_NAME"].values[0]
        org_unit_id_correct = spatial_row["org_unit_id"].values[0]
        mask = target_df_matched["cleansed_target"] == csi_concat_original
        target_df_matched.loc[mask, "org_unit_id"] = org_unit_id_correct
        target_df_matched.loc[mask, "LVL_3_NAME"] = lvl_3_name_correct
        target_df_matched.loc[mask, "LVL_6_NAME"] = lvl_6_name_correct

    target_df_matched["LVL_6_NAME"] = np.where(
        target_df_matched["org_unit_id"].isna(),
        target_df_matched["LVL_6_NAME_original"],
        target_df_matched["LVL_6_NAME"],
    )

    unmatched_count = target_df_matched["org_unit_id"].isna().sum()
    total_count = len(target_df_matched)
    if unmatched_count > 0:
        unmatched_csis = target_df_matched[target_df_matched["org_unit_id"].isna()][
            "LVL_6_NAME_original"
        ].unique()
        current_run.log_warning(
            f"{unmatched_count} out of {total_count} records could not be matched to an org_unit_id. "
            f"Unmatched CSIs: {', '.join(map(str, unmatched_csis))}"
            "These entries will be dropped from the target data."
        )

    target_df_matched = target_df_matched.drop(
        columns=[
            "LVL_3_NAME_original",
            "LVL_6_NAME_original",
            "match_score",
            "cleansed_target",
            "cleansed_spatial_match",
        ]
    )
    target_df_matched = target_df_matched.dropna(subset=["org_unit_id"])

    return target_df_matched


def match_district_to_org_unit_id(
    district_level_target_df: pd.DataFrame, spatial_df_clean: pd.DataFrame
) -> pd.DataFrame:
    """
    Match district names in df containing the district-level target data to organizational unit IDs using spatial data.

    Args:
        df (pd.DataFrame): DataFrame containing the target data at district level.
        spatial_df_clean (pd.DataFrame): Cleaned DataFrame containing the spatial data.

    Returns:
        pd.DataFrame: DataFrame with matched organizational unit IDs.
    """
    current_run.log_info("Matching district names to organizational unit IDs...")

    spatial_units_for_matching = spatial_df_clean[
        ["org_unit_id", "LVL_3_NAME"]
    ].drop_duplicates()
    spatial_units_for_matching = spatial_units_for_matching.groupby(
        "LVL_3_NAME", as_index=False
    ).first()

    target_df_matched = district_level_target_df.merge(
        spatial_units_for_matching, on=["LVL_3_NAME"], how="left"
    )

    unmatched_count = target_df_matched["org_unit_id"].isna().sum()
    total_count = len(target_df_matched)
    if unmatched_count > 0:
        unmatched_districts = target_df_matched[
            target_df_matched["org_unit_id"].isna()
        ]["LVL_3_NAME"].unique()
        current_run.log_warning(
            f"{unmatched_count} out of {total_count} records could not be matched to an org_unit_id. "
            f"Unmatched districts: {', '.join(unmatched_districts)}"
            "These entries will be dropped from the target data."
        )

    return target_df_matched


def add_rounds_and_products(target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create rounds for the target data.

    Args:
        target_df (pd.DataFrame): DataFrame containing the target data.

    Returns:
        pd.DataFrame: DataFrame with rounds added.
    """
    current_run.log_info("Adding rounds and products to target data...")

    # polio 2024
    if target_df["campaign"].iloc[0] == "polio" and target_df["year"].iloc[0] == 2024:
        rounds = ["round 1", "round 2", "round 3", "round 4"]
        target_df_expanded = pd.DataFrame(
            np.repeat(target_df.values, len(rounds), axis=0),
            columns=target_df.columns,
        )
        target_df_expanded["round"] = rounds * (len(target_df))
        target_df_expanded["produit"] = np.where(
            target_df_expanded["full_name"].str.contains("VPO"),
            "vaccin polio",
            np.where(
                target_df_expanded["full_name"].str.contains("VA"),
                "vitamine A",
                np.where(
                    target_df_expanded["full_name"].str.contains("AL"),
                    "albendazole",
                    "produit inconnu",
                ),
            ),
        )
        target_df_expanded = target_df_expanded.drop(["full_name", "campaign"], axis=1)

    # rougeole and polio 2025
    elif (
        target_df["campaign"].iloc[0] == "polio_rougeole"
        and target_df["year"].iloc[0] == 2025
    ):
        rounds = ["round 1", "round 2"]
        target_df_expanded = pd.DataFrame(
            np.repeat(target_df.values, len(rounds), axis=0),
            columns=target_df.columns,
        )
        target_df_expanded["round"] = rounds * (len(target_df))
        target_df_expanded_rougeole = target_df_expanded.copy()
        target_df_expanded_rougeole["produit"] = "rougeole"
        target_df_expanded_polio = target_df_expanded.copy()
        target_df_expanded_polio["produit"] = "vaccin polio"
        target_df_expanded = pd.concat(
            [target_df_expanded_rougeole, target_df_expanded_polio],
            ignore_index=True,
        )
        target_df_expanded = target_df_expanded.drop("campaign", axis=1)

    # yellow fever 2025
    elif (
        target_df["campaign"].iloc[0] == "fievre jaune"
        and target_df["year"].iloc[0] == 2025
    ):
        rounds = ["round 1", "round 2"]
        target_df_expanded = pd.DataFrame(
            np.repeat(target_df.values, len(rounds), axis=0),
            columns=target_df.columns,
        )
        target_df_expanded["round"] = rounds * (len(target_df))
        target_df_expanded["produit"] = "fievre jaune"
        target_df_expanded = target_df_expanded.drop("campaign", axis=1)

    # men5 and tcv 2025
    elif (
        target_df["campaign"].iloc[0] == "men5_tcv"
        and target_df["year"].iloc[0] == 2025
    ):
        rounds = ["round 1", "round 2"]
        target_df_expanded = pd.DataFrame(
            np.repeat(target_df.values, len(rounds), axis=0),
            columns=target_df.columns,
        )
        target_df_expanded["round"] = rounds * (len(target_df))
        target_df_expanded_men5 = target_df_expanded.copy()
        target_df_expanded_men5["produit"] = "méningite"
        target_df_expanded_tcv = target_df_expanded.copy()
        target_df_expanded_tcv["produit"] = "tcv"
        target_df_expanded = pd.concat(
            [target_df_expanded_men5, target_df_expanded_tcv],
            ignore_index=True,
        )
        target_df_expanded = target_df_expanded.drop("campaign", axis=1)

    # polio 2025 round 3
    elif target_df["campaign"].iloc[0] == "polio" and target_df["year"].iloc[0] == 2025:
        target_df_expanded = target_df.copy()
        target_df_expanded["round"] = "round 3"
        target_df_expanded["produit"] = "vaccin polio"
        target_df_expanded = target_df_expanded.drop("campaign", axis=1)

    else:
        current_run.log_error(
            "Unknown campaign and year combination. Cannot add rounds and products."
        )
        return target_df

    return target_df_expanded


def combine_target_data(
    dfs: list[pd.DataFrame],
) -> pd.DataFrame:
    """
    Combine multiple target data DataFrames into a single DataFrame.

    Args:
        dfs (list[pd.DataFrame]): List of DataFrames to be combined.

    Returns:
        pd.DataFrame: Combined DataFrame containing all target data.
    """
    current_run.log_info("Combining target data...")

    target_data_combined = pd.concat(dfs, ignore_index=True)

    return target_data_combined


def save_output(target_data_combined: pd.DataFrame):
    """
    Save the combined target data to a parquet file.

    Args:
        target_data_combined (pd.DataFrame): DataFrame containing the combined target data.

    Returns:
        None
    """
    current_run.log_info("Saving combined target data...")

    output_path = os.path.join(
        workspace.files_path, "output", "combined_target_data.parquet"
    )
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    target_data_combined.to_parquet(output_path, index=False)
    current_run.log_info(f"Combined target data saved to {output_path}.")


if __name__ == "__main__":
    process_target_data()
