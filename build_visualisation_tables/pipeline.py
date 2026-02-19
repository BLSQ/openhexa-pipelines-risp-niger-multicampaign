import os
import pandas as pd
import numpy as np
import sqlalchemy as sa
from openhexa.sdk import current_run, workspace, pipeline
from config import (
    outputs_path,
    cvrg_campaign_map,
    district_level_target_keys,
    district_level_group_keys,
    district_level_final_keys,
    district_level_cumsum_keys,
    csi_level_target_keys,
    csi_level_final_keys,
    csi_level_cumsum_keys,
    csi_level_config,
    product_campaign_mapping,
    stocks_campaign_map,
    stock_ratios_config,
    surveillance_campaign_map,
    communication_campaign_map,
    communication_category_groups,
)
from utils import (
    new_cols,
    age_categorizer,
    site_categorizer,
    produit_categorizer,
    produit_categorizer_stocks,
    vaccination_status_categorizer,
    product_status_categorizer,
    supervision_categorizer,
    communication_categorizer,
    get_communication_category_type,
)


@pipeline("04. Construction des tableaux pour la visualisation")
def build_visualisation_tables():
    """
    Main pipeline function to build visualisation tables.
    """
    # data imports
    combined_df = import_iaso_combined_data()
    target_df = import_target_data()
    combined_campaign_data_df = import_combined_campaign_data()

    # create datasets
    cvrg_total, cvrg_df = create_coverage_dataset(
        combined_df, combined_campaign_data_df
    )
    cvrg_csi_district = add_target_data(cvrg_df, target_df)
    cmpl = create_completeness_dataset(combined_df, combined_campaign_data_df)
    stock = create_stocks_dataset(combined_df, cvrg_total)
    supervision = create_surveillance_dataset(combined_df)
    communication_long, communication = create_communication_dataset(combined_df)
    (
        campaign_filter_table,
        round_filter_table,
        year_filter_table,
        products_filter_table,
        combination_filter_table,
    ) = create_filter_tables(combined_df, combined_campaign_data_df)
    spatial_units_combined = create_dynamic_org_unit_table()

    # write to db
    write_to_db(cvrg_total, "ner_vaccination_couverture")
    write_to_db(cvrg_csi_district, "ner_vaccination_couverture_csi_district_cibled")
    write_to_db(cmpl, "ner_vaccination_completude")
    write_to_db(stock, "ner_vaccination_stock")
    write_to_db(supervision, "ner_vaccination_supervision")
    write_to_db(communication_long, "ner_vaccination_communications_long")
    write_to_db(communication, "ner_vaccination_communications")
    write_to_db(target_df, "ner_vaccination_cibles_district")
    write_to_db(campaign_filter_table, "ner_vaccination_campaign_filter_table")
    write_to_db(round_filter_table, "ner_vaccination_round_filter_table")
    write_to_db(year_filter_table, "ner_vaccination_year_filter_table")
    write_to_db(products_filter_table, "ner_vaccination_products_filter_table")
    write_to_db(combination_filter_table, "ner_vaccination_combination_filter_table")
    write_to_db(spatial_units_combined, "ner_spatial_units")


def import_iaso_combined_data() -> pd.DataFrame:
    """
    Import historical data from IASO historical data folder.

    Args:
        None

    Returns:
        pd.DataFrame: Combined historical data from all feather files.
    """
    current_run.log_info("Importation des données combinées du formulaire IASO...")
    try:
        file_path = os.path.join(
            workspace.files_path,
            outputs_path,
            "combined_iaso_data.parquet",
        )
        combined_df = pd.read_parquet(file_path)
        return combined_df
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'importation des données combinées du formulaire IASO: {e}"
        )
        raise


def import_target_data() -> pd.DataFrame:
    """
    Import target data from processed target data folder.

    Args:
        None

    Returns:
        pd.DataFrame: Target data DataFrame.
    """
    current_run.log_info("Importation des données cibles...")
    try:
        file_path = os.path.join(
            workspace.files_path,
            outputs_path,
            "combined_target_data.parquet",
        )
        target_df = pd.read_parquet(file_path)
        return target_df
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'importation des données cibles: {e}")
        raise


def import_combined_campaign_data() -> pd.DataFrame:
    """
    Import combined campaign data from built combination products pipeline.

    Args:
        None

    Returns:
        pd.DataFrame: Combined campaign data DataFrame.
    """
    current_run.log_info(
        "Importation du Dataframe contenant la structure attendue des données de campagne..."
    )
    try:
        file_path = os.path.join(
            workspace.files_path,
            outputs_path,
            "combined_campaign_data.parquet",
        )
        combined_campaign_data_df = pd.read_parquet(file_path)
        return combined_campaign_data_df
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'importation du Dataframe contenant la structure attendue des données de campagne: {e}"
        )
        raise


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
    current_run.log_info("Création du tableau de couverture vaccinale...")
    combined_df = combined_df
    try:
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
        df_final = combined_campaign_data_df.merge(
            cvrg_total,
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
        unmatched_entries_in_iaso = df_final[df_final["_merge"] == "right_only"]
        if not unmatched_entries_in_iaso.empty:
            proportion_unmatched_in_iaso = len(unmatched_entries_in_iaso) / len(
                df_final
            )
            current_run.log_warning(
                f"{len(unmatched_entries_in_iaso)} entrées ({proportion_unmatched_in_iaso:.2%}) n'ont pas le même format que le Dataframe de la structure attendue. Ces entrées seront supprimées."
            )
        df_final = df_final[df_final["_merge"] != "right_only"].drop(columns=["_merge"])

        return cvrg_total, df_final
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du tableau de couverture vaccinale: {e}"
        )
        raise


def add_target_data(coverage_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add target data to the coverage dataset.

    Args:
        coverage_df (pd.DataFrame): Coverage dataset DataFrame.
        target_df (pd.DataFrame): Target data DataFrame.

    Returns:
        pd.DataFrame: Coverage dataset with target data added.
    """
    current_run.log_info(
        "Ajout des données cibles au tableau de couverture vaccinale..."
    )
    try:
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

        coverage_csi_with_target_df["value"] = coverage_csi_with_target_df[
            "value"
        ].fillna(0)
        coverage_csi_with_target_df["value_cum"] = coverage_csi_with_target_df.groupby(
            csi_level_cumsum_keys
        )["value"].transform("cumsum")
        coverage_csi_with_target_df["cible"] = coverage_csi_with_target_df[
            "cible"
        ].astype("Int64")

        # combine csi and district level
        cvrg_csi_district = pd.concat(
            [coverage_csi_with_target_df, coverage_district_with_target_df],
            ignore_index=True,
        )

        return cvrg_csi_district
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'ajout des données cibles au tableau de couverture vaccinale: {e}"
        )
        raise


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
    current_run.log_info("Création du tableau de complétude vaccinale...")
    try:
        reality = combined_df[
            ["period", "org_unit_id", "choix_campagne", "round", "year"]
        ].copy()
        reality["presence_equipe"] = 1

        expectation = combined_campaign_data_df[
            ["period", "org_unit_id", "produit", "round", "year"]
        ].copy()
        expectation["choix_campagne"] = expectation["produit"].map(
            product_campaign_mapping
        )
        expectation = expectation.drop(columns=["produit"]).drop_duplicates()

        cmpl = pd.merge(
            expectation,
            reality,
            on=["period", "org_unit_id", "choix_campagne", "round", "year"],
            how="left",
        )

        cmpl["presence_equipe"] = cmpl["presence_equipe"].fillna(0).astype(int)
        cmpl = cmpl.sort_values(
            ["year", "round", "org_unit_id", "choix_campagne", "period"]
        )
        cmpl["presence_equipe_cum"] = cmpl.groupby(
            ["year", "round", "org_unit_id", "choix_campagne"]
        )["presence_equipe"].transform("cummax")

        cmpl = cmpl.reset_index(drop=True)

        return cmpl
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du tableau de complétude vaccinale: {e}"
        )
        raise


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
    current_run.log_info("Création du tableau des stocks...")
    try:
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

        df = (
            new_cols(
                df,
                "categorizer",
                "category",
                [
                    produit_categorizer_stocks,
                    product_status_categorizer,
                ],
            )
            .drop(columns=["category"])
            .rename(columns={"produit_categorizer": "produit"})
        )

        group_cols = [
            "year",
            "round",
            "period",
            "org_unit_id",
            "produit",
            "product_status",
        ]
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

        stock_total_pivot["total"] = (
            stock_total_pivot["stock"] + stock_total_pivot["reçu"]
        )
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
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du tableau de suivi des stocks: {e}"
        )
        raise


def create_surveillance_dataset(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create surveillance tables for visualization.

    Args:
        combined_df (pd.DataFrame): Combined campaign data from IASO DataFrame.
        combined_campaign_data_df (pd.DataFrame): Expected combined campaign data DataFrame.

    Returns:
        pd.DataFrame: Surveillance dataset DataFrame.
    """
    current_run.log_info("Création du tableau de surveillance...")
    try:
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
                [
                    "year",
                    "round",
                    "period",
                    "org_unit_id",
                    "choix_campagne",
                    "supervision",
                ],
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
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du tableau de surveillance: {e}"
        )
        raise


def create_communication_dataset(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create communication tables for visualization.

    Args:
        combined_df (pd.DataFrame): Combined campaign data from IASO DataFrame.

    Returns:
        pd.DataFrame: Communication dataset DataFrame.
    """
    current_run.log_info("Création du tableau de communication...")
    try:
        id_vars = ["period", "round", "year", "org_unit_id"]

        all_campaign_data = []

        for campaign_name, cols in communication_campaign_map.items():
            valid_cols = [c for c in cols if c in combined_df.columns]
            if not valid_cols:
                current_run.log_warning(
                    f"Aucune colonne valide trouvée pour la campagne de communication '{campaign_name}'. Cette campagne sera ignorée."
                )
                continue
            temp_df = pd.melt(
                combined_df[id_vars + list(set(valid_cols))].fillna(0),
                id_vars=id_vars,
                value_vars=list(set(valid_cols)),
                var_name="raw_indicator",
                value_name="value",
            )
            temp_df["choix_campagne"] = campaign_name
            temp_df["category"] = temp_df["raw_indicator"].apply(
                lambda x: get_communication_category_type(
                    x, communication_category_groups
                )
            )
            all_campaign_data.append(temp_df)

        communication_all_values = pd.concat(all_campaign_data, ignore_index=True)

        # remove zero value entries
        mask_value_zero = communication_all_values["value"] == 0
        communication = communication_all_values[~mask_value_zero].copy()
        count_value_zero = mask_value_zero.sum()
        if count_value_zero > 0:
            proportion_value_zero = count_value_zero / len(communication_all_values)
            current_run.log_warning(
                f"{len(mask_value_zero)} entrées ({proportion_value_zero:.2%}) liées aux informations de communication ont été supprimées car aucune valeur n'a été attribuée."
            )

        communication = new_cols(
            communication,
            "categorizer",
            "raw_indicator",
            [
                communication_categorizer,
            ],
        ).rename(columns={"communication": "variable"})

        # raise a warning if the communication category contains empty values after categorization and if so, display its rwa indicator names
        mask_communication_empty = communication["variable"] == ""
        if mask_communication_empty.any():
            count_communication_empty = mask_communication_empty.sum()
            proportion_communication_empty = count_communication_empty / len(
                communication
            )
            raw_indicators_empty = communication.loc[
                mask_communication_empty, "raw_indicator"
            ].unique()
            current_run.log_warning(
                f"{count_communication_empty} entrées ({proportion_communication_empty:.2%}) liées aux informations de communication n'ont pas pu être catégorisées. Raw indicators: {', '.join(raw_indicators_empty)}"
            )

        communication_long = (
            communication.groupby(
                [
                    "year",
                    "round",
                    "period",
                    "org_unit_id",
                    "choix_campagne",
                    "category",
                    "variable",
                ],
                as_index=False,
            )["value"]
            .sum()
            .fillna(0)
        )

        communication_wide = pd.pivot_table(
            communication_long,
            index=["year", "round", "period", "org_unit_id", "choix_campagne"],
            columns=["variable"],
            values="value",
            fill_value=0,
        ).reset_index()

        return communication_long, communication_wide
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du tableau des stratégies de communication: {e}"
        )
        raise


def create_filter_tables(
    combined_df: pd.DataFrame, combined_campaign_data_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Create filter tables for visualization.

    Args:
        combined_df (pd.DataFrame): Combined campaign data from IASO DataFrame.
        combined_campaign_data_df (pd.DataFrame): Expected combined campaign data DataFrame.

    Returns:
        pd.DataFrame: Filter tables DataFrames.
    """
    current_run.log_info("Création de filtres nécessaires à la visualisation...")
    try:
        campaign_filter_table = (
            combined_df[["choix_campagne"]]
            .drop_duplicates()
            .dropna()
            .reset_index(drop=True)
        )
        round_filter_table = (
            combined_campaign_data_df[["round"]]
            .drop_duplicates()
            .dropna()
            .reset_index(drop=True)
        )
        year_filter_table = (
            combined_campaign_data_df[["year"]]
            .drop_duplicates()
            .dropna()
            .reset_index(drop=True)
        )
        products_filter_table = (
            combined_campaign_data_df[["produit"]]
            .drop_duplicates()
            .dropna()
            .reset_index(drop=True)
        )

        choice_filter_table = pd.DataFrame(
            {"choice_org_unit_level": ["District", "CSI"]}
        )

        combination_filter_table = pd.MultiIndex.from_product(
            [
                campaign_filter_table["choix_campagne"],
                round_filter_table["round"],
                year_filter_table["year"],
                products_filter_table["produit"],
                choice_filter_table["choice_org_unit_level"],
            ],
            names=[
                "choix_campagne",
                "round",
                "year",
                "produit",
                "choice_org_unit_level",
            ],
        ).to_frame(index=False)

        return (
            campaign_filter_table,
            round_filter_table,
            year_filter_table,
            products_filter_table,
            combination_filter_table,
        )
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création des filtres pour la visualisation: {e}"
        )
        raise


def create_dynamic_org_unit_table() -> pd.DataFrame:
    """
    Create dynamic organization unit table for visualization.

    Args:
        None

    Returns:
        pd.DataFrame: Dynamic organization unit table DataFrame.
    """
    current_run.log_info(
        "Création du tableau dynamique des unités organisationnelles..."
    )
    try:
        file_path = os.path.join(
            workspace.files_path,
            outputs_path,
            "iaso_org_unit_tree_clean.parquet",
        )

        iaso_org_unit_tree_clean_df = pd.read_parquet(file_path)

        spatial_units_choice_0 = iaso_org_unit_tree_clean_df.copy()

        spatial_units_choice_0["choice_org_unit_level"] = "District"
        spatial_units_choice_0["LVL_1_NAME"] = "Niger"

        spatial_units_choice_1 = iaso_org_unit_tree_clean_df.copy()
        spatial_units_choice_1["choice_org_unit_level"] = "CSI"
        spatial_units_choice_1["LVL_1_NAME"] = spatial_units_choice_1["LVL_2_NAME"]
        spatial_units_choice_1["LVL_1_UID"] = spatial_units_choice_1["LVL_2_UID"]

        spatial_units_choice_1["LVL_2_NAME"] = spatial_units_choice_1["LVL_3_NAME"]
        spatial_units_choice_1["LVL_2_UID"] = spatial_units_choice_1["LVL_3_UID"]

        spatial_units_choice_1["LVL_3_NAME"] = spatial_units_choice_1["LVL_6_NAME"]
        spatial_units_choice_1["LVL_3_UID"] = spatial_units_choice_1["LVL_6_UID"]

        spatial_units_choice_1["LVL_6_NAME"] = None
        spatial_units_choice_1["LVL_6_UID"] = None

        spatial_units_combined = pd.concat(
            [spatial_units_choice_0, spatial_units_choice_1], ignore_index=True
        ).reset_index(drop=True)

        return spatial_units_combined
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du tableau dynamique des unités organisationnelles: {e}"
        )
        raise


def write_to_db(df: pd.DataFrame, table_name: str) -> None:
    """Write the dataframe to a DB table.

    Parameters
    ----------
    df : pl.DataFrame
        The dataframe to write.
    table_name : str
        The name of the table to write to.
    """
    current_run.log_info(f"Écriture des données dans la table DB {table_name}...")
    try:
        engine = sa.create_engine(workspace.database_url)
        connection = engine.connect()
        df.to_sql(
            name=table_name,
            con=connection,
            if_exists="replace",
            index=False,
        )
        current_run.log_info(f"Données écrites dans la table DB {table_name}")
        connection.close()
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'écriture des données dans la table DB {table_name}: {e}"
        )
        raise


if __name__ == "__main__":
    build_visualisation_tables()
