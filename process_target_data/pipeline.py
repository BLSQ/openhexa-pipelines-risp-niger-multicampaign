import os
import re
from openhexa.sdk import current_run, pipeline
import pandas as pd
import numpy as np
from utils import (
    org_unit_matching,
)
from config import (
    OUTPUTS_PATH,
    TARGETS_HISTORICAL_PATH,
    TARGET_OTHER_DATA_PATH,
    TEMP_PATH,
    target_polio_2024_cols,
    polio_2024_dict_districts_cibles_iaso,
    target_polio_rougeole_2025_columns,
    age_adjustment_rougeole,
    age_adjustment_albendazole,
    age_adjustment_vitA,
    target_yellow_fever_2025_2026_columns,
    target_yellow_fever_2025_2026_age_ranges,
    target_men5_tcv_2025_columns_dict,
    target_polio_2026_r1_columns,
    csi_matching_failed,
    templates_required_cols_csi,
    templates_required_cols_district,
    campaign_rename_dict,
    site_strategy_types_dict,
    cols_for_melting,
    csi_district_rename_dict,
)


@pipeline(
    "process_target_data",
    name="multi-campagne - Import et traitement des données de cibles",
)
def process_target_data():
    """
    Main pipeline function to process target data from various campaigns.
    """
    iaso_org_unit_tree_df = load_data("iaso_org_unit_tree_raw")
    iaso_org_unit_tree_df_clean = load_data("iaso_org_unit_tree_clean")

    # district-level historical target data
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

    # csi-level historical target data
    target_yellow_fever_2025_2026_r1 = (
        import_target_data_for_yellow_fever_2025_2026_r1()
    )
    target_yellow_fever_2025_2026_r1 = match_csi_to_org_unit_id(
        target_yellow_fever_2025_2026_r1, iaso_org_unit_tree_df_clean
    )
    target_yellow_fever_2025_2026_r1 = add_rounds_and_products(
        target_yellow_fever_2025_2026_r1
    )

    target_men5_tcv_2025_r1_r2 = import_target_data_for_men5_and_tcv_2025_r1_r2()
    target_men5_tcv_2025_r1_r2 = match_csi_to_org_unit_id(
        target_men5_tcv_2025_r1_r2, iaso_org_unit_tree_df_clean
    )
    target_men5_tcv_2025_r1_r2 = add_rounds_and_products(target_men5_tcv_2025_r1_r2)

    target_polio_2026_r1 = import_target_data_for_polio_2026_r1()
    target_polio_2026_r1 = match_csi_to_org_unit_id(
        target_polio_2026_r1, iaso_org_unit_tree_df_clean
    )
    target_polio_2026_r1 = add_rounds_and_products(target_polio_2026_r1)

    # future target data
    all_target_data_csi_combined, all_target_data_district_combined = (
        import_target_data_for_future_campaigns()
    )
    if not all_target_data_csi_combined.empty:
        all_target_data_csi_combined = match_csi_to_org_unit_id(
            all_target_data_csi_combined, iaso_org_unit_tree_df_clean
        )
    if not all_target_data_district_combined.empty:
        all_target_data_district_combined = match_district_to_org_unit_id(
            all_target_data_district_combined, iaso_org_unit_tree_df_clean
        )

    # combine all target data
    target_data_combined = combine_target_data(
        [
            targets_polio_2024_r1_r4,
            target_polio_rougeole_2025_r1_r2,
            target_yellow_fever_2025_2026_r1,
            target_men5_tcv_2025_r1_r2,
            target_polio_2026_r1,
            all_target_data_csi_combined,
            all_target_data_district_combined,
        ]
    )

    # clean up org unit
    target_data_combined = clean_org_unit_id(
        target_data_combined, iaso_org_unit_tree_df, iaso_org_unit_tree_df_clean
    )

    # save
    save_output(target_data_combined)


def load_data(name: str) -> pd.DataFrame:
    """
    Import data from a specified file in the outputs directory.
    The file should be in parquet format and the name should be provided without the extension.

    Args:
        name (str): Name of the file to be imported (without extension).
    Returns:
        pd.DataFrame: DataFrame containing the imported data.
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

        return df

    except Exception as e:
        current_run.log_error(f"Erreur lors de l'importation du fichier {name}: {e}")
        raise


def import_target_data_for_polio_2024_r1_r4() -> pd.DataFrame:
    """
    Import target data for Polio 2024 rounds 1 to 4

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for Polio 2024 rounds 1 to 4
    """
    current_run.log_info(
        "Importation des données de cibles pour la polio 2024 rounds 1 à 4..."
    )
    try:
        if not os.path.exists(TARGETS_HISTORICAL_PATH):
            os.makedirs(TARGETS_HISTORICAL_PATH)

        file_path = os.path.join(
            TARGETS_HISTORICAL_PATH,
            "Population JNV JNM ET DEPRARASITAGE.xlsx",
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
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'importation des données de cibles pour la polio 2024: {e}"
        )
        raise


def import_target_data_for_polio_and_rougeole_2025_r1_r2() -> pd.DataFrame:
    """
    Import target data for polio and rougeole campaigns for year 2025 rounds 1 and 2

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for polio and rougeole campaigns year 2025 rounds 1 and 2
    """
    current_run.log_info(
        "Importation des données de cibles pour la polio et rougeole 2025..."
    )

    try:
        file_path = os.path.join(
            TARGETS_HISTORICAL_PATH,
            "cible_niger_et_refugies_2025.xlsx",
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
        target_polio_rougeole_2025["cible"] = target_polio_rougeole_2025[
            "cible"
        ].astype(int)
        target_polio_rougeole_2025["year"] = 2025
        target_polio_rougeole_2025["campaign"] = "polio_rougeole"

        return target_polio_rougeole_2025
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'importation des données historiques de cibles pour les campagnes polio 2025 rounds 1-2 et rougeole 2025 round 1: {e}"
        )
        raise


def import_target_data_for_yellow_fever_2025_2026_r1() -> pd.DataFrame:
    """
    Import target data for yellow fever campaign for year 2025 and 2026 rounds 1 for the regions of Dosso and Tahoua

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for yellow fever campaign year 2025 and 2026 rounds 1 for the regions of Dosso and Tahoua
    """
    current_run.log_info(
        "Importation des données de cibles historiques pour la campagne fièvre jaune 2025/2026 rounds 1..."
    )
    try:
        file_path = os.path.join(
            TARGETS_HISTORICAL_PATH,
            "cible_csi_fj_dosso_tahoua.xlsx",
        )

        target_yellow_fever_2025_r1 = pd.read_excel(
            file_path,
            header=[0],
            skiprows=10,
            usecols=[2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 14, 15, 16, 17],
        )

        target_yellow_fever_2025_r1.columns = target_yellow_fever_2025_2026_columns
        target_yellow_fever_2025_r1 = target_yellow_fever_2025_r1[
            ~target_yellow_fever_2025_r1["LVL_3_NAME"].str.contains("Total")
        ]
        target_yellow_fever_2025_r1 = target_yellow_fever_2025_r1[
            ~(target_yellow_fever_2025_r1["LVL_6_NAME"] == "DS")
        ]

        for age in target_yellow_fever_2025_2026_age_ranges:
            target_yellow_fever_2025_r1[age] = (
                target_yellow_fever_2025_r1[age + "_urban"]
                + target_yellow_fever_2025_r1[age + "_avancee"]
                + target_yellow_fever_2025_r1[age + "_mobile"]
            )
            target_yellow_fever_2025_r1[age] = target_yellow_fever_2025_r1[age].astype(
                int
            )
        target_yellow_fever_2025_r1_clean = target_yellow_fever_2025_r1[
            ["LVL_3_NAME", "LVL_6_NAME"] + target_yellow_fever_2025_2026_age_ranges
        ]
        target_yellow_fever_2025_r1_clean = pd.melt(
            target_yellow_fever_2025_r1_clean,
            id_vars=["LVL_3_NAME", "LVL_6_NAME"],
            var_name="age",
            value_name="cible",
        ).fillna(0)

        target_yellow_fever_2025_r1_clean["cible"] = target_yellow_fever_2025_r1_clean[
            "cible"
        ].astype(int)
        target_yellow_fever_2025_r1_clean["year"] = 2025
        target_yellow_fever_2025_r1_clean["campaign"] = "fièvre jaune"

        target_yellow_fever_2026_r1_clean = target_yellow_fever_2025_r1_clean.copy()
        target_yellow_fever_2026_r1_clean["year"] = 2026

        target_yellow_fever_2025_2026_r1 = pd.concat(
            [target_yellow_fever_2025_r1_clean, target_yellow_fever_2026_r1_clean],
            ignore_index=True,
        )

        return target_yellow_fever_2025_2026_r1
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'importation des données de cibles pour la fièvre jaune 2025 et 2026 rounds 1: {e}"
        )
        raise


def import_target_data_for_men5_and_tcv_2025_r1_r2() -> pd.DataFrame:
    """
    Import target data for men5 and tcv campaigns for year 2025 rounds 1 and 2

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for yellow fever campaign year 2025 rounds 1 and 2
    """
    current_run.log_info(
        "Importation des données de cibles pour la campagne Méningite et TCV 2025..."
    )

    try:
        file_path = os.path.join(
            TARGETS_HISTORICAL_PATH,
            "Cible Men5-TCV CSI.xlsx",
        )

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

        target_men5_tcv_2025_clean["cible"] = target_men5_tcv_2025_clean[
            "cible"
        ].astype(int)
        target_men5_tcv_2025_clean["year"] = 2025
        target_men5_tcv_2025_clean = target_men5_tcv_2025_clean[
            ["LVL_3_NAME", "LVL_6_NAME", "age", "cible", "year"]
        ].drop_duplicates()
        target_men5_tcv_2025_clean["campaign"] = "men5_tcv"

        return target_men5_tcv_2025_clean

    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'importation des données de cibles historiques pour la campagne Méningite et TCV 2025 rounds 1 et 2: {e}"
        )
        raise


def import_target_data_for_polio_2026_r1() -> pd.DataFrame:
    """
    Import target data for polio campaign for year 2026 round 1

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for polio campaign year 2026 round 1
    """
    current_run.log_info(
        "Importation des données de cibles historiques pour la campagne de polio 2026 round 1..."
    )
    try:
        file_path = os.path.join(
            TARGETS_HISTORICAL_PATH,
            "cible_jnv_polio_2025.xlsx",
        )

        target_polio_2026_r1 = pd.read_excel(
            file_path, header=[0], skiprows=9, usecols=[1, 2, 3, 7]
        )

        target_polio_2026_r1.columns = target_polio_2026_r1_columns
        target_polio_2026_r1 = target_polio_2026_r1.dropna(subset=["LVL_3_NAME"])
        target_polio_2026_r1 = target_polio_2026_r1.dropna(subset=["cible"])

        target_polio_2026_r1 = target_polio_2026_r1[
            ~target_polio_2026_r1["LVL_3_NAME"].str.contains("Total")
        ]
        target_polio_2026_r1 = target_polio_2026_r1[
            ~(target_polio_2026_r1["LVL_6_NAME"] == "DS")
        ]

        target_polio_2026_r1["cible"] = target_polio_2026_r1["cible"].astype(int)
        target_polio_2026_r1["0-11 mois"] = (
            target_polio_2026_r1["cible"] * 0.119140832
        ).round(0)
        target_polio_2026_r1["12-59 mois"] = (
            target_polio_2026_r1["cible"] * 0.880859168
        ).round(0)
        target_polio_2026_r1 = target_polio_2026_r1.dropna(subset=["LVL_6_NAME"])
        target_polio_2026_r1.drop(["cible", "LVL_2_NAME"], axis=1, inplace=True)

        target_polio_2026_r1_clean = pd.melt(
            target_polio_2026_r1,
            id_vars=["LVL_3_NAME", "LVL_6_NAME"],
            var_name="age",
            value_name="cible",
        ).fillna(0)

        target_polio_2026_r1_clean["cible"] = target_polio_2026_r1_clean[
            "cible"
        ].astype(int)
        target_polio_2026_r1_clean["year"] = 2026
        target_polio_2026_r1_clean["campaign"] = "polio"
        return target_polio_2026_r1_clean
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'importation des données de cibles historiques pour la campagne de polio 2026 round 1: {e}"
        )
        raise


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
    current_run.log_info(
        "Appariement des noms CSI aux identifiants des unités organisationnelles..."
    )
    try:
        iaso_org_unit_tree_for_matching = iaso_org_unit_tree_df_clean[
            ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
        ].drop_duplicates()

        target_df_matched, org_unit_tree_check = org_unit_matching(
            csi_level_target_df, iaso_org_unit_tree_for_matching, threshold=50
        )

        # inspect matching results
        target_df_matched_check = target_df_matched[
            [
                "org_unit_id",
                "LVL_3_NAME_original",
                "LVL_6_NAME_original",
                "LVL_3_NAME",
                "LVL_6_NAME",
                "cleansed_target",
                "cleansed_spatial_match",
                "match_score",
            ]
        ]
        target_df_matched_check.drop_duplicates(inplace=True)

        if not os.path.exists(TEMP_PATH):
            os.makedirs(TEMP_PATH)

        target_df_matched_check.to_csv(
            os.path.join(TEMP_PATH, "target_df_matched_check.csv"),
            index=False,
        )
        org_unit_tree_check.drop_duplicates(inplace=True)
        org_unit_tree_check.to_csv(
            os.path.join(TEMP_PATH, "org_unit_tree_check.csv"),
            index=False,
        )

        # manually correct matching failures
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
                f"{unmatched_count} sur {total_count} entrées n'ont pas pu être appariés à un org_unit_id. "
                f"CSIs non appariés : {', '.join(map(str, unmatched_csis))}. "
                "Un appariement manuel est nécessaire pour ces entrées."
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
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'appariement des noms CSI aux identifiants des unités organisationnelles: {e}"
        )
        raise


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
    try:
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
                f"{unmatched_count} sur {total_count} entrées n'ont pas pu être appariés à un org_unit_id. "
                f"Districts non appariés : {', '.join(unmatched_districts)}"
                "Ces entrées seront supprimées des données cibles."
            )

        return target_df_matched
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'appariement des noms de districts aux identifiants des unités organisationnelles: {e}"
        )
        raise


def add_rounds_and_products(target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create rounds for the target data.

    Args:
        target_df (pd.DataFrame): DataFrame containing the target data.

    Returns:
        pd.DataFrame: DataFrame with rounds added.
    """
    current_run.log_info("Ajout des rounds et des produits aux données de cibles...")

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
        target_df_expanded_rougeole = target_df_expanded_rougeole[
            target_df_expanded_rougeole["round"] == "round 1"
        ]

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

    # yellow fever 2025/2026
    elif (
        target_df["campaign"].iloc[0] == "fièvre jaune"
        and target_df["year"].iloc[0] == 2025
    ):
        rounds = ["round 1"]
        target_df_expanded = pd.DataFrame(
            np.repeat(target_df.values, len(rounds), axis=0),
            columns=target_df.columns,
        )
        target_df_expanded["round"] = rounds * (len(target_df))
        target_df_expanded["produit"] = "fièvre jaune"
        target_df_expanded = target_df_expanded.drop("campaign", axis=1)

    elif (
        target_df["campaign"].iloc[0] == "fièvre jaune"
        and target_df["year"].iloc[0] == 2026
    ):
        rounds = ["round 1"]
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

    # polio 2026 round 1
    elif target_df["campaign"].iloc[0] == "polio" and target_df["year"].iloc[0] == 2026:
        target_df_expanded = target_df.copy()
        target_df_expanded["round"] = "round 1"
        target_df_expanded["produit"] = "vaccin polio"
        target_df_expanded = target_df_expanded.drop("campaign", axis=1)

    else:
        current_run.log_error(
            "Combinaison campagne et année inconnue. Impossible d'ajouter les rounds et les produits."
        )
        return target_df

    return target_df_expanded


def process_dataframe(df: pd.DataFrame, aggregation_type: str, meta: dict):
    """
    This function processes the input DataFrame by melting it from wide to long format,
    extracting age and site/strategy information, and cleaning up the resulting DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame to be processed.
        aggregation_type (str): The type of aggregation, either "csi" or "district".
        meta (dict): A dictionary containing metadata about the DataFrame, such as the source file name.

    Returns:
        pd.DataFrame: The processed DataFrame in long format with extracted age and site/strategy information.
    """
    id_vars = cols_for_melting.copy()
    if aggregation_type == "csi":
        id_vars.insert(1, "CSI")

    required = (
        templates_required_cols_csi
        if aggregation_type == "csi"
        else templates_required_cols_district
    )
    if not all(col in df.columns for col in required):
        raise ValueError(f"Colonnes manquantes. Attendu: {required}")

    value_vars = [col for col in df.columns if col.startswith("Cible ")]

    df_melted = pd.melt(
        df,
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="age_site_strategy",
        value_name="cible",
    )

    regex_extract = r"Cible (\d+-\d+ (?:mois|ans))(?:_(.+))?"
    extracted = df_melted["age_site_strategy"].str.extract(regex_extract)

    df_melted["age"] = extracted[0]
    df_melted["site_strategy"] = extracted[1].replace(site_strategy_types_dict)
    df_melted.drop(columns=["age_site_strategy"], inplace=True)
    df_melted = df_melted.rename(columns=csi_district_rename_dict)

    return df_melted


def import_target_data_for_future_campaigns():
    """
    Placeholder function for importing target data for future campaigns.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for future campaigns.
    """
    current_run.log_info("Importation et traitement des données non-historiques...")

    if not os.path.exists(TARGET_OTHER_DATA_PATH):
        os.makedirs(TARGET_OTHER_DATA_PATH)
        return pd.DataFrame(), pd.DataFrame()

    all_data = {"csi": [], "district": []}

    file_pattern = re.compile(r"Cibles_(.+)_(\d{4})_(r\d+)_(csi|district)\.csv")

    current_run.log_info(
        "Importation et traitement des données non-historiques de cibles..."
    )

    # loop through each file
    for entry in os.scandir(TARGET_OTHER_DATA_PATH):
        if not (
            entry.is_file()
            and entry.name.endswith(".csv")
            and not entry.name.startswith("~$")
        ):
            continue

        match = file_pattern.match(entry.name)
        if not match:
            current_run.log_warning(f"Format de nommage invalide : '{entry.name}'")
            continue

        campaign, year, round_code, agg = match.groups()

        if campaign not in campaign_rename_dict:
            current_run.log_warning(f"Campagne invalide '{campaign}' dans {entry.name}")
            continue

        try:
            df = pd.read_csv(entry.path)
            if df.empty:
                continue

            df["year"] = int(year)
            df["produit"] = campaign_rename_dict.get(campaign, campaign)
            df["round"] = round_code.replace("r", "round ")

            processed_df = process_dataframe(df, agg, {"file": entry.name})
            all_data[agg].append(processed_df)

            current_run.log_info(f"Fichier {entry.name} traité.")

        except Exception as e:
            current_run.log_error(f"Erreur sur {entry.name} : {str(e)}")

    # combine results
    df_csi = (
        pd.concat(all_data["csi"], ignore_index=True)
        if all_data["csi"]
        else pd.DataFrame()
    )
    df_dist = (
        pd.concat(all_data["district"], ignore_index=True)
        if all_data["district"]
        else pd.DataFrame()
    )

    current_run.log_info(
        f"Importation des fichiers de cibles non-historiques terminée: CSI: {len(all_data['csi'])}, District: {len(all_data['district'])}"
    )

    return df_csi, df_dist


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
    current_run.log_info("Combinaison des différentes données de cibles...")
    try:
        target_data_combined = pd.concat(dfs, ignore_index=True)

        return target_data_combined

    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la combinaison des données de cibles: {e}"
        )
        raise


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
        "Récupération des identifiants des unités d'organisation et application de la correspondance un-à-plusieurs..."
    )
    try:
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
            target_data_combined,
            mapping_df,
            on="org_unit_id",
            how="left",
            indicator=True,
        )
        target_data_combined["org_unit_id"] = target_data_combined[
            "final_org_unit_id"
        ].fillna(target_data_combined["org_unit_id"])

        target_data_combined.drop(columns=["final_org_unit_id", "_merge"], inplace=True)

        return target_data_combined
    except Exception as e:
        current_run.log_error(
            f"Erreur lors du processus de récupération des identifiants des unités d'organisation: {e}"
        )
        raise


def save_output(target_data_combined: pd.DataFrame):
    """
    Save the combined target data to a parquet file.

    Args:
        target_data_combined (pd.DataFrame): DataFrame containing the combined target data.

    Returns:
        None
    """
    current_run.log_info("Enregistrement des données cibles combinées...")

    if not os.path.exists(OUTPUTS_PATH):
        os.makedirs(OUTPUTS_PATH)

    file_path = os.path.join(
        OUTPUTS_PATH,
        "combined_target_data.parquet",
    )
    target_data_combined.to_parquet(file_path, index=False)
    current_run.log_info(f"Données cibles combinées enregistrées dans {file_path}.")


if __name__ == "__main__":
    process_target_data()
