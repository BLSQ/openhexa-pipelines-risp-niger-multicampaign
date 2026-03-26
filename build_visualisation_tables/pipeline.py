import os
import pandas as pd
import numpy as np
import sqlalchemy as sa
from openhexa.sdk import current_run, workspace, pipeline
from config import (
    OUTPUTS_PATH,
    cvrg_campaign_map,
    cvrg_yellow_fever_age_adjustment,
    cvrg_rougeole_age_adjustment,
    cvrg_group_by_cols,
    cvrg_district_level_target_keys,
    cvrg_district_level_group_keys,
    cvrg_district_level_final_keys,
    cvrg_district_level_cumsum_keys,
    cvrg_csi_level_target_keys,
    cvrg_csi_level_final_keys,
    cvrg_csi_level_cumsum_keys,
    cmpl_cols_selection_1,
    cmpl_cols_selection_2,
    cmpl_cols_selection_3,
    cmpl_product_campaign_mapping,
    stocks_campaign_map,
    stocks_cols_selection_1,
    stocks_cols_selection_2,
    stocks_cols_selection_3,
    stock_ratios_config,
    supervision_campaign_map,
    supervision_cols_selection_1,
    supervision_cols_selection_2,
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
    process_target_level,
)


@pipeline(
    "build_visualisation_tables",
    name="multi-campagne - Construction des tableaux pour la visualisation",
)
def build_visualisation_tables():
    """
    This pipeline creates different tables for vizualization in Power BI and sends them to the database in the OH workspace.

    The tables created are:
    - ner_vaccination_couverture: contains coverage data for all campaigns at the org unit level with categorization variables to allow for flexible visualizations in PBI
    - ner_vaccination_couverture_csi_district_cibled: contains coverage data for all campaigns at the district and CSI level with target data to allow for flexible visualizations in PBI
    - ner_vaccination_completude: contains completeness data for all campaigns at the org unit level
    - ner_vaccination_stock: contains stock data for all campaigns at the org unit level with the number of children vaccinated to allow computation of stock ratios in PBI
    - ner_vaccination_supervision: contains supervision data for all campaigns at the org unit level
    - ner_vaccination_communications_long: contains communication data for all campaigns at the org unit level in long format with categorization variables to allow for flexible visualizations in PBI
    - ner_vaccination_communications: contains communication data for all campaigns at the org unit level in wide format
    - ner_vaccination_cibles_district: contains target data at the district level
    - ner_vaccination_campaign_filter_table: contains the list of campaigns to be used as filter in PBI
    - ner_vaccination_round_filter_table: contains the list of rounds to be used as filter in PBI
    - ner_vaccination_year_filter_table: contains the list of years to be used as filter in PBI
    - ner_vaccination_products_filter_table: contains the list of products to be used as filter in PBI
    - ner_vaccination_combination_filter_table: contains the list of combinations to be used as filter in PBI
    - ner_spatial_units: contains the list of spatial units to be used as filter in PBI
    """
    # data imports
    combined_df = load_data("combined_iaso_data")
    target_df = load_data("combined_target_data")
    combined_campaign_data_df = load_data("combined_campaign_data")

    # create datasets
    cvrg_total, cvrg_df = create_coverage_dataset(
        combined_df, combined_campaign_data_df
    )
    cvrg_csi_district = add_target_data(cvrg_df, target_df)
    cmpl = create_completeness_dataset(combined_df, combined_campaign_data_df)
    stock = create_stocks_dataset(combined_df, cvrg_total)
    supervision = create_supervision_dataset(combined_df)
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


def create_coverage_dataset(
    iaso_form_data_df: pd.DataFrame,
    expected_structure_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create coverage tables for visualization.

    Args:
        iaso_form_data_df (pd.DataFrame): the dataframe containing the processed data extracted from the IASO multi-campaign form
        expected_structure_df (pd.DataFrame): the dataframe containing the expected structure of the data for each campaign

    Returns:
        cvrg_total (pd.DataFrame): Coverage dataset DataFrame.
        df_final (pd.DataFrame): Final coverage dataset DataFrame after merging with expected
                                 combined campaign data.
    """
    current_run.log_info("Création du tableau de couverture vaccinale...")
    try:
        id_vars = ["period", "round", "year", "org_unit_id"]
        all_campaign_data = []

        for campaign_name, cols in cvrg_campaign_map.items():
            valid_cols = [c for c in cols if c in iaso_form_data_df.columns]

            if not valid_cols:
                continue

            temp_df = pd.melt(
                iaso_form_data_df[id_vars + list(set(valid_cols))].fillna(0),
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
        df["sexe"] = "TOUS"  # no gender configuration at the moment

        # adjust age categories for yellow fever
        is_fjaune = df["campaign"] == "fièvre jaune"
        df.loc[is_fjaune, "age"] = df.loc[is_fjaune, "age"].replace(
            cvrg_yellow_fever_age_adjustment
        )

        # adjust age categories for rougeole campaign
        is_rougeole = df["campaign"] == "rougeole"
        df.loc[is_rougeole, "age"] = df.loc[is_rougeole, "age"].replace(
            cvrg_rougeole_age_adjustment
        )

        # remove zero value entries
        cvrg_total_all_values = df.groupby(cvrg_group_by_cols, as_index=False)[
            "value"
        ].sum()
        mask_value_zero = cvrg_total_all_values["value"] == 0
        cvrg_total = cvrg_total_all_values[~mask_value_zero].copy()

        count_value_zero = mask_value_zero.sum()
        if count_value_zero > 0:
            proportion_value_zero = count_value_zero / len(cvrg_total_all_values)
            current_run.log_warning(
                f"{len(mask_value_zero)} entrées liées aux informations du nombre de cas vaccinés ({proportion_value_zero:.2%}) ont été supprimées car aucune valeur n'a été attribuée."
            )

        # merge with expected combined campaign data to ensure all combinations are present
        df_final = expected_structure_df.merge(
            cvrg_total,
            on=cvrg_group_by_cols,
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


def add_target_data(cvrg_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add District- and CSI-level target data to the coverage dataset to allow computation of
    coverage ratios at the district and CSI level in PBI.

    NB:
    - When targets are defined at CSI-level, the District-level target corresponds to the sum
      of the CSI-level targets for the CSIs in the district.
    - When targets are defined at District-level only, the CSI-level targets are NaN.

    Args:
        cvrg_df (pd.DataFrame): Coverage DataFrame.
        target_df (pd.DataFrame): Target DataFrame.

    Returns:
        cvrg_with_targets (pd.DataFrame): Coverage DataFrame with DS- and CSI-level target data added.

    """
    current_run.log_info(
        "Ajout des données cibles au tableau de couverture vaccinale..."
    )

    try:
        # Add district-level targets
        target_district_df = target_df.groupby(
            cvrg_district_level_target_keys, as_index=False
        )["cible"].sum()

        cvrg_district_df = cvrg_df.groupby(
            cvrg_district_level_group_keys, as_index=False
        ).agg({"value": "sum", "org_unit_id": "first"})

        cvrg_district_with_targets = process_target_level(
            cvrg_district_df,
            target_district_df,
            cvrg_district_level_target_keys,
            cvrg_district_level_final_keys,
            cvrg_district_level_cumsum_keys,
            "District",
        )

        # Add CSI-level targets
        csi_filter_df = target_df[target_df["LVL_6_NAME"].notna()]
        csi_filter_df = csi_filter_df[["year", "produit", "round"]].drop_duplicates()

        target_csi_df = target_df.merge(
            csi_filter_df, on=["year", "produit", "round"], how="inner"
        )
        target_csi_df = target_csi_df.drop(columns=["LVL_3_NAME", "LVL_6_NAME"])

        cvrg_csi_df = cvrg_df.merge(
            csi_filter_df, on=["year", "produit", "round"], how="inner"
        )

        cvrg_csi_with_targets = process_target_level(
            cvrg_csi_df,
            target_csi_df,
            cvrg_csi_level_target_keys,
            cvrg_csi_level_final_keys,
            cvrg_csi_level_cumsum_keys,
            "CSI",
        )

        # combine
        cvrg_with_targets = pd.concat(
            [cvrg_csi_with_targets, cvrg_district_with_targets], ignore_index=True
        )

        return cvrg_with_targets

    except Exception as e:
        current_run.log_error(f"Erreur lors de l'ajout des données cibles: {e}")
        raise


def create_completeness_dataset(
    iaso_form_data_df: pd.DataFrame, expected_structure_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Create table to calculate the completeness of campaigns (i.e. whether an
    immunization team was present in a given CSI). This is done by calculting 2 indicators:

    - 'presence_equipe': this indicates whether a vaccine was administered in a given CSI
      (presence_equipe = 1) or not (presence_equipe = 0)
    - 'presence_equipe_cum': this indicates whether a vaccine was administered in a given
        CSI at any point during the campaign (presence_equipe_cum = 1) or not (presence_equipe_cum = 0)
        by doing a cumulative sum of the 'presence_equipe' indicator across the different periods of
        the campaign

    These two indicators are then used in the Power BI dashboard to calculate the completeness ratio at the
    district and CSI level by campaign, round, year and product.

    Args:
        iaso_form_data_df (pd.DataFrame): the dataframe containing the processed data extracted from the IASO multi-campaign form
        expected_structure_df (pd.DataFrame): the dataframe containing the expected structure of the data for each campaign

    Returns:
        cmpl (pd.DataFrame): Completeness dataset DataFrame.
    """
    current_run.log_info("Création du tableau de complétude vaccinale...")
    try:
        actual = iaso_form_data_df[cmpl_cols_selection_1].copy()
        actual["presence_equipe"] = 1

        expected = expected_structure_df[cmpl_cols_selection_2].copy()
        expected["choix_campagne"] = expected["produit"].map(
            cmpl_product_campaign_mapping
        )
        expected = expected.drop(columns=["produit"]).drop_duplicates()

        cmpl = pd.merge(
            expected,
            actual,
            on=cmpl_cols_selection_1,
            how="left",
        )

        cmpl["presence_equipe"] = cmpl["presence_equipe"].fillna(0).astype(int)
        cmpl = cmpl.sort_values(cmpl_cols_selection_1)
        cmpl["presence_equipe_cum"] = cmpl.groupby(cmpl_cols_selection_3)[
            "presence_equipe"
        ].transform("cummax")

        cmpl = cmpl.reset_index(drop=True)

        return cmpl
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du tableau de complétude vaccinale: {e}"
        )
        raise


def create_stocks_dataset(
    iaso_form_data_df: pd.DataFrame, cvrg_total: pd.DataFrame
) -> pd.DataFrame:
    """
    Create table to track stocks during the campaign. This is done by creating the following indicators:
    - 'stock': this indicates the number of vaccines in stock at the beginning of a given period
    - 'reçu': this indicates the number of vaccines received during a given period
    - 'utilisé': this indicates the number of vaccines used during a given period
    - 'total': this indicates the total number of vaccines available during a given period (stock + reçu)
    - 'restant': this indicates the number of vaccines remaining at the end of a given period (total - utilisé)
    - 'box_ratio': this indicates the number of units contained in each vaccine box for a given campaign
      (e.g. 50 for polio, 1 for vitamine A, etc.) to allow the conversion of boxes to number of doses in PBI
    - 'enfants_vaccines': this indicates the number of children vaccinated during a given period (imported
       from the coverage dataset to allow the calculation of stock loss ratio in PBI as
       1 - (enfants_vaccines / (utilisé * box_ratio)))

    Args:
        iaso_form_data_df (pd.DataFrame): the dataframe containing the processed data extracted from the
        IASO multi-campaign form
        cvrg_total (pd.DataFrame): Coverage total DataFrame.

    Returns:
        pd.DataFrame: Stocks dataset DataFrame.
    """
    current_run.log_info("Création du tableau des stocks...")
    try:
        id_vars = stocks_cols_selection_1
        all_campaign_data = []

        for campaign_name, cols in stocks_campaign_map.items():
            valid_cols = [c for c in cols if c in iaso_form_data_df.columns]

            if not valid_cols:
                continue

            temp_df = pd.melt(
                iaso_form_data_df[id_vars + list(set(valid_cols))].fillna(0),
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

        stock_total_all_values = df.groupby(stocks_cols_selection_2, as_index=False)[
            "value"
        ].sum()

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
            index=stocks_cols_selection_3,
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
            cvrg_total.groupby(stocks_cols_selection_3, as_index=False)["value"]
            .sum()
            .rename(columns={"value": "enfants_vaccines"})
        )

        # merge stock data with coverage data to have the number of children vaccinated
        # alongside the stock data to allow to compute a ratio of remaining stock per children
        #  vaccinated in PBI
        stock = stock_total_pivot.merge(cvrg_stock, how="left").fillna(0)

        return stock
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la création du tableau de suivi des stocks: {e}"
        )
        raise


def create_supervision_dataset(iaso_form_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a table to track the number of notified cases of different types during each campaign.
    The following indicators are calculated:
    - 'pfa': this indicates the number of cases of acute flaccid paralysis (AFP) notified during a given period
    - 'mapi_mineur': this indicates the number of minor cases of MAPI (Manifestation Adverse Post-Immunisation)
                     notified during a given period
    - 'mapi_majeur': this indicates the number of major cases of MAPI (Manifestation Adverse Post-Immunisation)
                     notified during a given period
    - 'fievre_jaune_notifie': this indicates the number of cases of yellow fever notified during a given period

    Args:
        iaso_form_data_df (pd.DataFrame): the dataframe containing the processed data extracted from the IASO
        multi-campaign form

    Returns:
        supervision_pivot(pd.DataFrame): Supervision dataset DataFrame with the number of cases notified for
                                        each type of case as columns and the different campaign, round, year,
                                        org unit combinations as rows.
    """
    current_run.log_info("Création du tableau de surveillance...")
    try:
        id_vars = supervision_cols_selection_1
        all_campaign_data = []

        for campaign_name, cols in supervision_campaign_map.items():
            valid_cols = [c for c in cols if c in iaso_form_data_df.columns]

            if not valid_cols:
                continue

            temp_df = pd.melt(
                iaso_form_data_df[id_vars + list(set(valid_cols))].fillna(0),
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
                supervision_cols_selection_2 + ["supervision"],
                as_index=False,
            )["value"]
            .sum()
            .fillna(0)
        )

        supervision_pivot = pd.pivot_table(
            supervision_total,
            index=supervision_cols_selection_2,
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


def create_communication_dataset(
    iaso_form_data_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create two tables (one in long format and one in wide format) to track the different
    communication strategies implemented during each campaign.

    Args:
        iaso_form_data_df (pd.DataFrame): the dataframe containing the processed data extracted
        from the IASO multi-campaign form

    Returns:
        communication_long (pd.DataFrame): Communication dataset DataFrame in long format
        communication_wide (pd.DataFrame): Communication dataset DataFrame in wide format
    """
    current_run.log_info("Création du tableau de communication...")
    try:
        id_vars = ["period", "round", "year", "org_unit_id"]

        all_campaign_data = []

        for campaign_name, cols in communication_campaign_map.items():
            valid_cols = [c for c in cols if c in iaso_form_data_df.columns]
            if not valid_cols:
                current_run.log_warning(
                    f"Aucune colonne valide trouvée pour la campagne de communication '{campaign_name}'. Cette campagne sera ignorée."
                )
                continue
            temp_df = pd.melt(
                iaso_form_data_df[id_vars + list(set(valid_cols))].fillna(0),
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
    iaso_form_data_df: pd.DataFrame, expected_structure_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Create filter tables for visualization in Power BI

    Args:
        iaso_form_data_df (pd.DataFrame): The dataframe containing the processed data extracted from the IASO multi-campaign form.
        expected_structure_df (pd.DataFrame): The dataframe containing the expected structure of the IASO multi-campaign form.

    Returns:
        campaign_filter_table (pd.DataFrame): DataFrame containing the list of campaigns to be used as filter in PBI
        round_filter_table (pd.DataFrame): DataFrame containing the list of rounds to be used as filter in PBI
        year_filter_table (pd.DataFrame): DataFrame containing the list of years to be used as filter in PBI
        products_filter_table (pd.DataFrame): DataFrame containing the list of products to be used as filter in PBI
        combination_filter_table (pd.DataFrame): DataFrame containing the list of combinations of campaign, round, year, product and aggregation level (district vs CSI) to allow flexible filtering in PBI
    """
    current_run.log_info("Création de filtres nécessaires à la visualisation...")
    try:
        campaign_filter_table = (
            iaso_form_data_df[["choix_campagne"]]
            .drop_duplicates()
            .dropna()
            .reset_index(drop=True)
        )
        round_filter_table = (
            expected_structure_df[["round"]]
            .drop_duplicates()
            .dropna()
            .reset_index(drop=True)
        )
        year_filter_table = (
            expected_structure_df[["year"]]
            .drop_duplicates()
            .dropna()
            .reset_index(drop=True)
        )
        products_filter_table = (
            expected_structure_df[["produit"]]
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
    Create a table that allows to dynamically switch between district-level and CSI-level
    data in Power BI by having the same org unit in different rows with a variable indicating
    the level (district vs CSI) and the corresponding district or CSI name in the same column.

    This is done by:
     - taking the cleaned org unit tree
     - creating two copies of it and keeping only the relevant columns for each copy to
        create a district-level and a CSI-level version of the table
     - concatenating these two tables together.

    Args:
        None

    Returns:
        spatial_units_combined (pd.DataFrame): DataFrame containing the combined district-level and
                                               CSI-level org unit table with a variable indicating
                                               the level to allow dynamic switching between district
                                               and CSI level in PBI
    """
    current_run.log_info(
        "Création du tableau dynamique des unités organisationnelles..."
    )
    try:
        file_path = os.path.join(
            OUTPUTS_PATH,
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
    """
    Write the dataframe to a DB table with a given name. If the table already exists, it will be replaced.

    Args:
        df (pd.DataFrame): The dataframe to write.
        table_name (str): The name of the table to write to.

    Returns:
        None
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
