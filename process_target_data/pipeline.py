import os
from openhexa.sdk import current_run, workspace, pipeline
import pandas as pd
import numpy as np
from pathlib import Path
from utils import (
    IASOConnectionHandler,
    org_unit_matching,
    pyramid_selector,
    strip_accents,
)
from config import (
    iaso_connector_slug,
    iaso_form_id,
    target_polio_2024_cols,
    polio_2024_dict_districts_cibles_iaso,
    target_polio_rougeole_2025_columns,
    age_adjustment_rougeole,
    age_adjustment_albendazole,
    age_adjustment_vitA,
    target_yellow_fever_2025_columns,
    target_yellow_fever_2025_age_ranges,
    target_men5_tcv_2025_columns_dict,
    target_polio_2025_r3_columns,
    csi_matching_failed,
)


@pipeline("process_target_data")
def process_target_data():
    """
    Main pipeline function to process target data from various campaigns.
    """
    iaso_org_unit_tree_df = get_iaso_org_unit_tree()
    iaso_org_unit_tree_df_clean = clean_iaso_org_unit_tree(iaso_org_unit_tree_df)

    # district-level target data
    targets_polio_2024_r1_r4 = import_target_data_for_polio_2024_r1_r4()
    targets_polio_2024_r1_r4 = match_district_to_org_unit_id(
        targets_polio_2024_r1_r4, iaso_org_unit_tree_df_clean
    )
    targets_polio_2024_r1_r4 = add_rounds_and_products(targets_polio_2024_r1_r4)

    target_polio_rougeole_2025_r1_r2 = (
        import_target_data_for_polio_and_rougeole_2025_r1_r2()
    )
    target_polio_rougeole_2025_r1_r2 = match_district_to_org_unit_id(
        target_polio_rougeole_2025_r1_r2, iaso_org_unit_tree_df_clean
    )
    target_polio_rougeole_2025_r1_r2 = add_rounds_and_products(
        target_polio_rougeole_2025_r1_r2
    )

    # csi-level target data
    target_yellow_fever_2025_r1_r2 = import_target_data_for_yellow_fever_2025_r1_r2()
    target_yellow_fever_2025_r1_r2 = match_csi_to_org_unit_id(
        target_yellow_fever_2025_r1_r2, iaso_org_unit_tree_df_clean
    )
    target_yellow_fever_2025_r1_r2 = add_rounds_and_products(
        target_yellow_fever_2025_r1_r2
    )

    target_men5_tcv_2025_r1_r2 = import_target_data_for_men5_and_tcv_2025_r1_r2()
    target_men5_tcv_2025_r1_r2 = match_csi_to_org_unit_id(
        target_men5_tcv_2025_r1_r2, iaso_org_unit_tree_df_clean
    )
    target_men5_tcv_2025_r1_r2 = add_rounds_and_products(target_men5_tcv_2025_r1_r2)

    target_polio_2025_r3 = import_target_data_for_polio_2025_r3()
    target_polio_2025_r3 = match_csi_to_org_unit_id(
        target_polio_2025_r3, iaso_org_unit_tree_df_clean
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

    # clean up org unit
    target_data_combined = clean_org_unit_id(
        target_data_combined, iaso_org_unit_tree_df, iaso_org_unit_tree_df_clean
    )

    # save
    save_output(target_data_combined)


def get_iaso_org_unit_tree() -> pd.DataFrame:
    """
    Retrieve organizational unit tree data from IASO based on a specific form ID.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the organizational unit tree data.
    """
    current_run.log_info("Retrieving org unit tree data from IASO...")

    iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
    iaso_org_unit_tree_df = iaso_connector_instance.get_ou_tree_dataframe_from_the_form(
        iaso_form_id
    )

    # save file to parquet for later use
    file_path = os.path.join(
        workspace.files_path,
        "niger_june_2024",
        "process_target_data",
        "output",
        "iaso_org_unit_tree_raw.parquet",
    )
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    iaso_org_unit_tree_df.to_parquet(
        file_path,
        index=False,
    )
    iaso_org_unit_tree_df = pd.read_parquet(file_path)

    return iaso_org_unit_tree_df


def clean_iaso_org_unit_tree(iaso_org_unit_tree_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the org unit tree data by filtering out rejected entries and selecting relevant records.

    Args:
        iaso_org_unit_tree_df (pd.DataFrame): DataFrame containing the org unit tree data to be cleaned.

    Returns:
        pd.DataFrame: Cleaned DataFrame with relevant org unit tree data.
    """
    current_run.log_info("Cleaning org unit tree data...")

    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df[
        iaso_org_unit_tree_df["Validé"] != "REJECTED"
    ]
    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
        iaso_org_unit_tree_df_clean["Source"].isin(["SNIS", "SNIS 2025"])
    ]
    # # remove all CSI marked as closed
    # cloture_filter = (
    #     iaso_org_unit_tree_df_clean["LVL_6_NAME"]
    #     .apply(strip_accents)
    #     .str.contains("cloture", case=False, na=False)
    # )
    # iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[~cloture_filter]
    # if cloture_filter.sum() > 0:
    #     proportion_closed_csi = (
    #         cloture_filter.sum() / len(iaso_org_unit_tree_df_clean)
    #     ) * 100
    #     current_run.log_info(
    #         f"Removed {cloture_filter.sum()} closed CSI ({proportion_closed_csi:.2f}%) from the org unit tree."
    #     )
    iaso_org_unit_tree_df_clean["LVL_6_UID"] = iaso_org_unit_tree_df_clean.groupby(
        "LVL_6_NAME"
    )["LVL_6_UID"].transform("first")
    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean.groupby(
        "LVL_6_UID", as_index=False
    ).apply(pyramid_selector, include_groups=False)

    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
        iaso_org_unit_tree_df_clean["LVL_2_NAME"] != "Niger"
    ]  # delete 2 incoherent entries

    iaso_org_unit_tree_df_clean["org_unit_id"] = iaso_org_unit_tree_df_clean[
        "org_unit_id"
    ].astype(np.int64)

    # save file
    file_path = os.path.join(
        workspace.files_path, "output", "iaso_org_unit_tree_clean.parquet"
    )
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    iaso_org_unit_tree_df_clean.to_parquet(
        file_path,
        index=False,
    )

    return iaso_org_unit_tree_df_clean


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

    # adjust age category 12-59 mois to 12-24 mois for Vit A (this is b/c this age category is found in the IASO data instead of 12-59 mois)
    target_polio_2024["age"] = np.where(
        target_polio_2024["full_name"].str.contains("VA_12-59 mois"),
        "12-24 mois",
        target_polio_2024["age"],
    )

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
    target_yellow_fever_2025_clean["campaign"] = "fièvre jaune"

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
    csi_level_target_df: pd.DataFrame, iaso_org_unit_tree_df_clean: pd.DataFrame
) -> pd.DataFrame:
    """
    Match CSI names in df containing the CSI-level target data to organizational unit IDs using spatial data.

    Args:
        csi_level_target_df (pd.DataFrame): DataFrame containing the target data at CSI level.
        iaso_org_unit_tree_df_clean (pd.DataFrame): DataFrame containing the clean organisational units tree data.

    Returns:
        pd.DataFrame: DataFrame with matched organizational unit IDs.
    """
    current_run.log_info("Matching CSI names to organizational unit IDs...")

    iaso_org_unit_tree_for_matching = iaso_org_unit_tree_df_clean[
        ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
    ].drop_duplicates()

    target_df_matched, org_unit_tree_check = org_unit_matching(
        csi_level_target_df, iaso_org_unit_tree_for_matching, threshold=50
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
    #     if col in org_unit_tree_check.columns:
    #         org_unit_tree_check[col] = (
    #             org_unit_tree_check[col].astype(str).replace("nan", "")
    #         )
    # target_df_matched.to_parquet(
    #     os.path.join(workspace.files_path, "temp", "target_df_matched.parquet"),
    #     index=False,
    # )
    # org_unit_tree_check.to_parquet(
    #     os.path.join(workspace.files_path, "temp", "org_unit_tree_check.parquet"),
    #     index=False,
    # )
    # target_df_matched = pd.read_parquet(
    #     os.path.join(workspace.files_path, "temp", "target_df_matched.parquet")
    # )
    # org_unit_tree_check = pd.read_parquet(
    #     os.path.join(workspace.files_path, "temp", "org_unit_tree_check.parquet")
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
    # org_unit_tree_check.drop_duplicates(inplace=True)
    # org_unit_tree_check.to_csv(
    #     os.path.join(workspace.files_path, "temp", "org_unit_tree_check.csv"),
    #     index=False,
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

        org_unit_tree_row = org_unit_tree_check.loc[
            org_unit_tree_check["cleansed_spatial"] == csi_concat_correct
        ]
        if org_unit_tree_row.empty:
            continue

        lvl_3_name_correct = org_unit_tree_row["LVL_3_NAME"].values[0]
        lvl_6_name_correct = org_unit_tree_row["LVL_6_NAME"].values[0]
        org_unit_id_correct = org_unit_tree_row["org_unit_id"].values[0]
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
    district_level_target_df: pd.DataFrame, iaso_org_unit_tree_df_clean: pd.DataFrame
) -> pd.DataFrame:
    """
    Match district names in df containing the district-level target data to organizational unit IDs using iaso_org_unit_tree data.

    Args:
        district_level_target_df (pd.DataFrame): DataFrame containing the target data at district level.
        iaso_org_unit_tree_df_clean (pd.DataFrame): DataFrame containing the clean organisational units tree data.

    Returns:
        pd.DataFrame: DataFrame with matched organizational unit IDs.
    """
    current_run.log_info("Matching district names to organizational unit IDs...")

    iaso_org_unit_tree_for_matching = iaso_org_unit_tree_df_clean[
        ["org_unit_id", "LVL_3_NAME"]
    ].drop_duplicates()
    iaso_org_unit_tree_for_matching = iaso_org_unit_tree_for_matching.groupby(
        "LVL_3_NAME", as_index=False
    ).first()

    target_df_matched = district_level_target_df.merge(
        iaso_org_unit_tree_for_matching, on=["LVL_3_NAME"], how="left"
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
        target_df_expanded_rougeole["age"] = target_df_expanded_rougeole["age"].replace(
            age_adjustment_rougeole
        )

        target_df_expanded_polio = target_df_expanded.copy()
        target_df_expanded_polio["produit"] = "vaccin polio"

        target_df_expanded_albendazole = target_df_expanded.copy()
        target_df_expanded_albendazole["produit"] = "albendazole"
        target_df_expanded_albendazole["age"] = target_df_expanded_albendazole[
            "age"
        ].replace(age_adjustment_albendazole)

        target_df_expanded_vitA = target_df_expanded.copy()
        target_df_expanded_vitA["produit"] = "vitamine A"
        target_df_expanded_vitA["age"] = target_df_expanded_vitA["age"].replace(
            age_adjustment_vitA
        )

        target_df_expanded = pd.concat(
            [
                target_df_expanded_rougeole,
                target_df_expanded_polio,
                target_df_expanded_albendazole,
                target_df_expanded_vitA,
            ],
            ignore_index=True,
        )
        target_df_expanded = target_df_expanded.drop("campaign", axis=1)

    # yellow fever 2025
    elif (
        target_df["campaign"].iloc[0] == "fièvre jaune"
        and target_df["year"].iloc[0] == 2025
    ):
        rounds = ["round 1", "round 2"]
        target_df_expanded = pd.DataFrame(
            np.repeat(target_df.values, len(rounds), axis=0),
            columns=target_df.columns,
        )
        target_df_expanded["round"] = rounds * (len(target_df))
        target_df_expanded["produit"] = "fièvre jaune"
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


def clean_org_unit_id(
    target_data_combined: pd.DataFrame,
    iaso_org_unit_tree_df: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Clean the org_unit_id column in the combined target data, by assigning all the org_unit_ids in the raw org unit tree
    to the corresponding LVL_6_UID org_unit_id from the cleaned org unit tree.

    Args:
        target_data_combined (pd.DataFrame): DataFrame containing the combined target data.
        iaso_org_unit_tree_df (pd.DataFrame): DataFrame containing the org unit tree data.
        iaso_org_unit_tree_clean_df (pd.DataFrame): DataFrame containing the cleaned org unit tree data.

    Returns:
        pd.DataFrame: DataFrame with cleaned org_unit_id column.
    """
    current_run.log_info(
        "Retrieving organization unit IDs and applying one-to-many mapping..."
    )

    uid_to_org_id_df_clean = iaso_org_unit_tree_clean_df[
        ["LVL_6_UID", "org_unit_id"]
    ].drop_duplicates()
    uid_to_org_id_df_raw = iaso_org_unit_tree_df.copy()
    uid_to_org_id_df_raw["LVL_6_UID"] = uid_to_org_id_df_raw.groupby("LVL_6_NAME")[
        "LVL_6_UID"
    ].transform("first")
    uid_to_org_id_df_raw = uid_to_org_id_df_raw[
        ["LVL_6_UID", "org_unit_id"]
    ].drop_duplicates()
    uid_to_org_id_df_raw = uid_to_org_id_df_raw.rename(
        columns={"org_unit_id": "final_org_unit_id"}
    )
    mapping_df = uid_to_org_id_df_clean.merge(
        uid_to_org_id_df_raw, on="LVL_6_UID", how="inner"
    )
    mapping_df = mapping_df[["org_unit_id", "final_org_unit_id"]].drop_duplicates()

    target_data_combined = pd.merge(
        target_data_combined, mapping_df, on="org_unit_id", how="left", indicator=True
    )
    target_data_combined["org_unit_id"] = target_data_combined[
        "final_org_unit_id"
    ].fillna(target_data_combined["org_unit_id"])

    target_data_combined.drop(columns=["final_org_unit_id", "_merge"], inplace=True)

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
