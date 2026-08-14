import pandas as pd
import numpy as np
from openhexa.sdk import current_run, pipeline
from shared_utils import (
    load_data,
    save_file,
    export_to_dataset,
)

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
    cmpl_cols_selection,
    cmpl_cols_selection_2,
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
    months_mapping_dict,
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
    melt_campaign_columns,
    drop_zero_values,
)


@pipeline(
    "build_visualisation_tables",
    name="multi-campagne - Construction des tableaux pour la visualisation",
)
def build_visualisation_tables():
    """
    This pipeline creates the different tables for vizualization in Power BI and saves/exports
    them as this pipeline's output (the Transform stage of the v2 architecture, per
    docs/ARCHITECTURE.md §2/§3). The database push happens separately, in
    load_visualisation_tables (the Load stage), which reads these same outputs back.

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

    Kept as one linear orchestrator despite its length (§7's "no function over ~50 lines
    without a stated reason"): every step below is a single call into an already-factored,
    independently readable function - splitting further would just relocate the same flat
    sequence, not clarify it. Most of this function's line count is its docstring above.
    """
    (
        combined_df,
        target_df,
        expected_structure_df,
        iaso_org_unit_tree_clean_df,
        iaso_org_unit_tree_raw_df,
    ) = _load_inputs()

    # create datasets
    cvrg_total, cvrg_df = create_coverage_dataset(combined_df, expected_structure_df)
    cvrg_csi_district = add_target_data(
        cvrg_df, target_df, iaso_org_unit_tree_clean_df, iaso_org_unit_tree_raw_df
    )
    cmpl = create_completeness_dataset(
        combined_df, expected_structure_df, iaso_org_unit_tree_clean_df
    )
    stock = create_stocks_dataset(combined_df, cvrg_total)
    supervision = create_supervision_dataset(combined_df)
    communication_long, communication = create_communication_dataset(combined_df)
    (
        campaign_filter_table,
        month_filter_table,
        round_filter_table,
        year_filter_table,
        products_filter_table,
        combination_filter_table,
    ) = create_filter_tables(combined_df, expected_structure_df)
    spatial_units_combined = create_dynamic_org_unit_table(iaso_org_unit_tree_clean_df)
    campaign_round_summary = create_campaign_round_summary_table(cvrg_total)

    _add_month_columns(
        [
            cvrg_total,
            cvrg_csi_district,
            cmpl,
            stock,
            supervision,
            communication_long,
            communication,
        ]
    )

    _save_and_export_outputs(
        {
            "ner_vaccination_couverture": cvrg_total,
            "ner_vaccination_couverture_csi_district_cibled": cvrg_csi_district,
            "ner_vaccination_completude": cmpl,
            "ner_vaccination_stock": stock,
            "ner_vaccination_supervision": supervision,
            "ner_vaccination_communications_long": communication_long,
            "ner_vaccination_communications": communication,
            "ner_vaccination_cibles_district": target_df,
            "ner_vaccination_campaign_filter_table": campaign_filter_table,
            "ner_vaccination_month_filter_table": month_filter_table,
            "ner_vaccination_round_filter_table": round_filter_table,
            "ner_vaccination_year_filter_table": year_filter_table,
            "ner_vaccination_products_filter_table": products_filter_table,
            "ner_vaccination_combination_filter_table": combination_filter_table,
            "ner_spatial_units": spatial_units_combined,
            "ner_spatial_units_non_dynamic": iaso_org_unit_tree_clean_df,
            "ner_vaccination_campaign_round_summary": campaign_round_summary,
        }
    )


def _load_inputs() -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """The 5 datasets every table in this pipeline is built from."""
    return (
        load_data("combined_iaso_data"),
        load_data("combined_target_data"),
        load_data("expected_data_structure"),
        load_data("iaso_org_unit_tree_clean"),
        load_data("iaso_org_unit_tree_raw"),
    )


def _add_month_columns(dfs: list) -> None:
    """add_month_column mutates its argument in place (assigns a new column onto the
    dataframe it's given), so this only needs the side effect, not a reassignment."""
    for df in dfs:
        add_month_column(df)


def _save_and_export_outputs(outputs_dict: dict) -> None:
    """Save + export every table this pipeline produces (the DB push happens
    separately, in load_visualisation_tables)."""
    for table_name, df in outputs_dict.items():
        save_file(df, table_name)
        export_to_dataset(df, OUTPUTS_PATH, table_name)


# =========================================================================== #
# Coverage                                                                     #
# =========================================================================== #
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
        df = _build_coverage_long(iaso_form_data_df)
        cvrg_total = _aggregate_coverage(df)
        df_final = _merge_coverage_with_expected_structure(
            cvrg_total, expected_structure_df
        )

        current_run.log_info("Tableau de couverture vaccinale créé avec succès.")
        return cvrg_total, df_final

    except Exception as e:
        msg = f"Erreur lors de la création du tableau de couverture vaccinale: {e}"
        current_run.log_error(msg)
        raise


def _build_coverage_long(iaso_form_data_df: pd.DataFrame) -> pd.DataFrame:
    """Melt every campaign's coverage columns into long format, resolve the jnm/polio
    field overlap, categorize, and apply the two campaign-specific age adjustments."""
    id_vars = ["period", "round", "year", "org_unit_id"]
    df = melt_campaign_columns(
        iaso_form_data_df, cvrg_campaign_map, id_vars, tag_col="campaign"
    )

    # drop duplicates in terms of all cols except campaign (keep "jnm" campaign only). This is
    # b/c jnm and polio share the same fields in the form and so they get counted twice.
    dup_cols = df.columns.difference(["campaign"]).tolist()
    df = df.sort_values(by="campaign", key=lambda x: x == "jnm", ascending=False)
    df = df.drop_duplicates(subset=dup_cols, keep="first")

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

    is_fjaune = df["campaign"] == "fièvre jaune"
    df.loc[is_fjaune, "age"] = df.loc[is_fjaune, "age"].replace(
        cvrg_yellow_fever_age_adjustment
    )
    is_rougeole = df["campaign"] == "rougeole"
    df.loc[is_rougeole, "age"] = df.loc[is_rougeole, "age"].replace(
        cvrg_rougeole_age_adjustment
    )
    return df


def _aggregate_coverage(df: pd.DataFrame) -> pd.DataFrame:
    """Sum to one row per cvrg_group_by_cols combination, dropping zero-value entries."""
    totals = df.groupby(cvrg_group_by_cols, as_index=False)["value"].sum()
    return drop_zero_values(totals, "value", "informations du nombre de cas vaccinés")


def _merge_coverage_with_expected_structure(
    cvrg_total: pd.DataFrame, expected_structure_df: pd.DataFrame
) -> pd.DataFrame:
    """Outer-merge with expected_data_structure so every expected combination is
    present, dropping IASO entries whose shape doesn't match the expected structure."""
    cvrg_total["year"] = cvrg_total["year"].astype("Int64")
    cvrg_total["org_unit_id"] = cvrg_total["org_unit_id"].astype("Int64")
    expected_structure = expected_structure_df[
        cvrg_group_by_cols + ["order_day", "choix_campagne"]
    ].drop_duplicates()
    df_final = cvrg_total.merge(
        expected_structure, on=cvrg_group_by_cols, how="outer", indicator=True
    )

    unmatched_entries_in_iaso = df_final[df_final["_merge"] == "left_only"]
    if not unmatched_entries_in_iaso.empty:
        proportion_unmatched_in_iaso = len(unmatched_entries_in_iaso) / len(df_final)
        current_run.log_warning(
            f"{len(unmatched_entries_in_iaso)} entrées ({proportion_unmatched_in_iaso:.2%}) "
            "n'ont pas le même format que le Dataframe de la structure attendue. Ces "
            "entrées seront supprimées."
        )
    return df_final[df_final["_merge"] != "left_only"].drop(columns=["_merge"])


# =========================================================================== #
# Targets on coverage (CSI + District)                                        #
# =========================================================================== #
def add_target_data(
    cvrg_df: pd.DataFrame,
    target_df: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
) -> pd.DataFrame:
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
        csi_filter_df, ds_filter_df = _split_by_target_reporting_level(target_df)

        cvrg_csi_with_targets = _build_csi_level_targets(
            cvrg_df, target_df, csi_filter_df, iaso_org_unit_tree_raw_df
        )
        cvrg_district_with_targets = _build_district_level_targets(
            cvrg_df,
            target_df,
            ds_filter_df,
            cvrg_csi_with_targets,
            iaso_org_unit_tree_clean_df,
            iaso_org_unit_tree_raw_df,
        )

        cvrg_csi_district = pd.concat(
            [cvrg_csi_with_targets, cvrg_district_with_targets], ignore_index=True
        )
        return _normalize_target_values(cvrg_csi_district)
    except Exception as e:
        current_run.log_error(f"Erreur: {e}")
        raise


def _split_by_target_reporting_level(
    target_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Which (year, produit, round) campaigns report targets at CSI level vs. District-only."""
    csi_filter_df = target_df[target_df["LVL_6_NAME"].notna()][
        ["year", "produit", "round"]
    ].drop_duplicates()
    ds_filter_df = target_df[target_df["LVL_6_NAME"].isna()][
        ["year", "produit", "round"]
    ].drop_duplicates()
    return csi_filter_df, ds_filter_df


def _build_csi_level_targets(
    cvrg_df: pd.DataFrame,
    target_df: pd.DataFrame,
    csi_filter_df: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
) -> pd.DataFrame:
    """Coverage + target merged at CSI level, restricted to campaigns that report at
    that level."""
    target_csi_df = target_df.merge(
        csi_filter_df, on=["year", "produit", "round"], how="inner"
    )
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
        iaso_org_unit_tree_raw_df,
    )
    cvrg_csi_with_targets["choice_org_unit_level"] = "CSI"
    cvrg_csi_with_targets["org_unit_id"] = cvrg_csi_with_targets["org_unit_id"].astype(
        "Int64"
    )
    cvrg_csi_with_targets["link_key"] = (
        cvrg_csi_with_targets["org_unit_id"].astype(str) + "_CSI"
    )
    return cvrg_csi_with_targets


def _aggregate_csi_to_district(cvrg_csi_with_targets: pd.DataFrame) -> pd.DataFrame:
    """Source 1: CSI-level coverage aggregated up to district level."""
    return cvrg_csi_with_targets.groupby(
        cvrg_district_level_group_keys, as_index=False
    ).agg({"value": "sum"})


def _pure_district_coverage(
    cvrg_df: pd.DataFrame,
    ds_filter_df: pd.DataFrame,
    cvrg_csi_with_targets: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
) -> pd.DataFrame:
    """Source 2: coverage for campaigns/districts that ONLY report at District level -
    excluding any district/campaign already covered by source 1 (_aggregate_csi_to_district)."""
    districts_with_csi_reporting = cvrg_csi_with_targets[
        cvrg_csi_level_target_keys
    ].drop_duplicates()
    cvrg_df_pure_district_raw = cvrg_df.merge(
        ds_filter_df, on=["year", "produit", "round"], how="inner"
    )
    cvrg_district_pure = cvrg_df_pure_district_raw.merge(
        districts_with_csi_reporting,
        on=cvrg_csi_level_target_keys,
        how="left",
        indicator=True,
    )
    cvrg_district_pure = cvrg_district_pure[
        cvrg_district_pure["_merge"] == "left_only"
    ].drop(columns=["_merge"])

    lvl_3_name_ds = iaso_org_unit_tree_raw_df[
        ["org_unit_id", "LVL_3_NAME"]
    ].drop_duplicates()
    cvrg_district_pure = cvrg_district_pure.merge(
        lvl_3_name_ds, on="org_unit_id", how="left"
    )
    return cvrg_district_pure.groupby(
        cvrg_district_level_group_keys, as_index=False
    ).agg({"value": "sum"})


def _build_district_level_targets(
    cvrg_df: pd.DataFrame,
    target_df: pd.DataFrame,
    ds_filter_df: pd.DataFrame,
    cvrg_csi_with_targets: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Coverage + target merged at District level, from two sources:
    - campaigns that report at CSI level: their CSI coverage aggregated up to District.
    - campaigns that report at District level only: their own coverage directly, minus
      any district/campaign already covered by the first source.
    """
    cvrg_district_from_csi = _aggregate_csi_to_district(cvrg_csi_with_targets)
    cvrg_district_pure = _pure_district_coverage(
        cvrg_df, ds_filter_df, cvrg_csi_with_targets, iaso_org_unit_tree_raw_df
    )
    cvrg_district_df = _anchor_to_representative_org_unit(
        cvrg_district_pure, cvrg_district_from_csi, iaso_org_unit_tree_clean_df
    )
    target_district_df = _sum_targets_to_district(target_df)

    cvrg_district_with_targets = process_target_level(
        cvrg_district_df,
        target_district_df,
        cvrg_district_level_target_keys,
        cvrg_district_level_final_keys,
        cvrg_district_level_cumsum_keys,
        "District",
        iaso_org_unit_tree_raw_df,
    )
    cvrg_district_with_targets["choice_org_unit_level"] = "District"
    cvrg_district_with_targets["org_unit_id"] = cvrg_district_with_targets[
        "org_unit_id"
    ].astype("Int64")
    cvrg_district_with_targets["link_key"] = (
        cvrg_district_with_targets["org_unit_id"].astype(str) + "_District"
    )
    return cvrg_district_with_targets


def _anchor_to_representative_org_unit(
    cvrg_district_pure: pd.DataFrame,
    cvrg_district_from_csi: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """Combine both district-level coverage sources and anchor each district's rows
    to one representative CSI-level org_unit_id (the first, by org_unit_id, in the
    clean tree) - a district-level row needs SOME org_unit_id to plot in PBI, and
    which specific CSI it is doesn't affect the district-level value."""
    rep_ids = (
        iaso_org_unit_tree_clean_df.sort_values(["LVL_3_NAME", "org_unit_id"])
        .groupby("LVL_3_NAME")["org_unit_id"]
        .first()
        .reset_index()
        .rename(columns={"org_unit_id": "rep_id"})
    )
    combined = pd.concat(
        [cvrg_district_pure, cvrg_district_from_csi], ignore_index=True
    )
    combined = combined.merge(rep_ids, on="LVL_3_NAME", how="left")
    combined["org_unit_id"] = combined["rep_id"]
    combined["LVL_6_NAME"] = None
    return combined


def _sum_targets_to_district(target_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate targets up to district level (sum of CSI-level targets, when defined
    at that level)."""
    target_df_unique = target_df[
        cvrg_district_level_target_keys + ["cible"]
    ].drop_duplicates()
    return target_df_unique.groupby(cvrg_district_level_target_keys, as_index=False)[
        "cible"
    ].sum()


def _normalize_target_values(cvrg_csi_district: pd.DataFrame) -> pd.DataFrame:
    """Split each target evenly across however many rows share its (link_key, year,
    round, produit, age, period) - so a target isn't double-counted when several rows
    (e.g. different vaccination statuses) share the same underlying target figure."""
    unique_target_cols = ["link_key", "year", "round", "produit", "age", "period"]
    duplication_counts = (
        cvrg_csi_district.groupby(unique_target_cols)
        .size()
        .reset_index(name="row_count")
    )
    cvrg_csi_district = cvrg_csi_district.merge(
        duplication_counts, on=unique_target_cols, how="left"
    )
    cvrg_csi_district["cible_norm"] = (
        cvrg_csi_district["cible"] / cvrg_csi_district["row_count"]
    )
    return cvrg_csi_district.drop(columns=["row_count"])


# =========================================================================== #
# Completeness                                                                #
# =========================================================================== #
def create_completeness_dataset(
    iaso_form_data_df: pd.DataFrame,
    expected_structure_df: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
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
        iaso_org_unit_tree_clean_df (pd.DataFrame): the dataframe containing the cleaned organizational unit tree
    Returns:
        cmpl (pd.DataFrame): Completeness dataset DataFrame.
    """
    current_run.log_info("Création du tableau de complétude vaccinale...")
    try:
        actual = iaso_form_data_df[cmpl_cols_selection].copy()
        actual["presence_equipe"] = 1

        clean_org_unit_ids = iaso_org_unit_tree_clean_df["org_unit_id"].unique()
        expected = expected_structure_df[cmpl_cols_selection].copy()
        expected = expected[expected["org_unit_id"].isin(clean_org_unit_ids)]
        expected = expected.drop_duplicates()

        cmpl = pd.merge(expected, actual, on=cmpl_cols_selection, how="left")
        cmpl = _add_cumulative_presence(cmpl)

        current_run.log_info("Tableau de complétude vaccinale créé avec succès.")
        return cmpl
    except Exception as e:
        msg = f"Erreur lors de la création du tableau de complétude vaccinale: {e}"
        current_run.log_error(msg)
        raise


def _add_cumulative_presence(cmpl: pd.DataFrame) -> pd.DataFrame:
    """Flag, per (CSI, campaign/round/...), the first period a team was present, and
    mark every period from then on as cumulatively "complete"."""
    cmpl = cmpl.sort_values(cmpl_cols_selection)
    is_visited = cmpl["presence_equipe"] == 1
    # groupby/transform on the visited-only subset returns a Series indexed by that
    # subset, not by cmpl - reindex back onto cmpl's full index (NaN for non-visited
    # rows) before comparing element-wise, or pandas raises on the mismatched index.
    first_visit_period = (
        cmpl[is_visited]
        .groupby(cmpl_cols_selection_2)["period"]
        .transform("min")
        .reindex(cmpl.index)
    )
    cmpl["presence_equipe_cum"] = (
        (cmpl["period"] == first_visit_period) & is_visited
    ).astype(int)
    return cmpl.drop_duplicates().reset_index(drop=True)


# =========================================================================== #
# Stocks                                                                       #
# =========================================================================== #
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
        stock_totals = _build_stock_totals(iaso_form_data_df)
        stock = _compute_stock_metrics(stock_totals)
        stock = _add_children_vaccinated(stock, cvrg_total)

        current_run.log_info("Tableau des stocks créé avec succès.")
        return stock

    except Exception as e:
        msg = f"Erreur lors de la création du tableau des stocks: {e}"
        current_run.log_error(msg)
        raise


def _build_stock_totals(iaso_form_data_df: pd.DataFrame) -> pd.DataFrame:
    """Melt every campaign's stock columns, categorize by product/status, and sum to
    one row per stocks_cols_selection_2 combination (dropping zero-value entries)."""
    df = melt_campaign_columns(
        iaso_form_data_df,
        stocks_campaign_map,
        stocks_cols_selection_1,
        tag_col="campaign",
    )
    df = (
        new_cols(
            df,
            "categorizer",
            "category",
            [produit_categorizer_stocks, product_status_categorizer],
        )
        .drop(columns=["category"])
        .rename(columns={"produit_categorizer": "produit"})
    )
    totals = df.groupby(stocks_cols_selection_2, as_index=False)["value"].sum()
    return drop_zero_values(totals, "value", "informations des stocks")


def _compute_stock_metrics(stock_totals: pd.DataFrame) -> pd.DataFrame:
    """Pivot stock/reçu/utilisé into columns and derive total/restant/box_ratio."""
    pivot = pd.pivot_table(
        stock_totals,
        index=stocks_cols_selection_3,
        columns=["product_status"],
        values="value",
    ).reset_index()

    pivot["box_ratio"] = pivot["produit"].map(stock_ratios_config)
    pivot["stock"] = pivot["stock"].fillna(0)
    pivot["reçu"] = pivot["reçu"].fillna(0)
    pivot["utilisé"] = pivot["utilisé"].fillna(0)

    pivot["total"] = pivot["stock"] + pivot["reçu"]
    pivot["restant"] = pivot["total"] - pivot["utilisé"]
    pivot["restant"] = np.where(pivot["restant"] < 0, 0, pivot["restant"])
    return pivot


def _add_children_vaccinated(
    stock: pd.DataFrame, cvrg_total: pd.DataFrame
) -> pd.DataFrame:
    """Attach the number of children vaccinated (from coverage) so PBI can compute a
    stock-loss ratio."""
    cvrg_stock = (
        cvrg_total.groupby(stocks_cols_selection_3, as_index=False)["value"]
        .sum()
        .rename(columns={"value": "enfants_vaccines"})
    )
    return stock.merge(cvrg_stock, how="left").fillna(0)


# =========================================================================== #
# Supervision                                                                  #
# =========================================================================== #
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
        supervision_total = _build_supervision_totals(iaso_form_data_df)
        supervision_pivot = pd.pivot_table(
            supervision_total,
            index=supervision_cols_selection_2,
            columns=["supervision"],
            values="value",
            fill_value=0,
        ).reset_index()

        current_run.log_info("Tableau de surveillance créé avec succès.")
        return supervision_pivot
    except Exception as e:
        msg = f"Erreur lors de la création du tableau de surveillance: {e}"
        current_run.log_error(msg)
        raise


def _build_supervision_totals(iaso_form_data_df: pd.DataFrame) -> pd.DataFrame:
    """Melt every campaign's supervision columns, drop zero-value entries, categorize,
    and sum to one row per (supervision_cols_selection_2, supervision category)."""
    df = melt_campaign_columns(
        iaso_form_data_df,
        supervision_campaign_map,
        supervision_cols_selection_1,
        tag_col="choix_campagne",
    )
    df = drop_zero_values(df, "value", "informations de surveillance")
    supervision = new_cols(
        df, "categorizer", "category", [supervision_categorizer]
    ).drop(columns=["category"])
    return (
        supervision.groupby(
            supervision_cols_selection_2 + ["supervision"], as_index=False
        )["value"]
        .sum()
        .fillna(0)
    )


# =========================================================================== #
# Communications                                                               #
# =========================================================================== #
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
    current_run.log_info("Création des tableaux de stratégies de communication...")
    try:
        communication_long = _build_communication_long(iaso_form_data_df)
        communication_wide = pd.pivot_table(
            communication_long,
            index=["year", "round", "period", "org_unit_id", "choix_campagne"],
            columns=["variable"],
            values="value",
            fill_value=0,
        ).reset_index()

        current_run.log_info(
            "Tableaux de stratégies de communication créés avec succès."
        )
        return communication_long, communication_wide

    except Exception as e:
        msg = f"Erreur lors de la création des tableaux de stratégies de communication: {e}"
        current_run.log_error(msg)
        raise


def _build_communication_long(iaso_form_data_df: pd.DataFrame) -> pd.DataFrame:
    """Melt every campaign's communication columns, categorize each raw indicator into
    its group (Déploiement/Portée/...) and its normalized variable name, and aggregate."""
    id_vars = ["period", "round", "year", "org_unit_id"]
    df = melt_campaign_columns(
        iaso_form_data_df,
        communication_campaign_map,
        id_vars,
        tag_col="choix_campagne",
        var_name="raw_indicator",
        warn_on_missing=True,
    )
    df["category"] = df["raw_indicator"].apply(
        lambda x: get_communication_category_type(x, communication_category_groups)
    )

    df = drop_zero_values(df, "value", "informations de communication")

    df = new_cols(
        df, "categorizer", "raw_indicator", [communication_categorizer]
    ).rename(columns={"communication": "variable"})

    mask_communication_empty = df["variable"] == ""
    if mask_communication_empty.any():
        count_communication_empty = int(mask_communication_empty.sum())
        proportion_communication_empty = count_communication_empty / len(df)
        raw_indicators_empty = df.loc[
            mask_communication_empty, "raw_indicator"
        ].unique()
        current_run.log_warning(
            f"{count_communication_empty} entrées ({proportion_communication_empty:.2%}) "
            "liées aux informations de communication n'ont pas pu être catégorisées. Raw "
            f"indicators: {', '.join(raw_indicators_empty)}"
        )

    return (
        df.groupby(
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


# =========================================================================== #
# Filter tables                                                                #
# =========================================================================== #
def create_filter_tables(
    iaso_form_data_df: pd.DataFrame, expected_structure_df: pd.DataFrame
) -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """
    Create filter tables for visualization in Power BI

    Args:
        iaso_form_data_df (pd.DataFrame): The dataframe containing the processed data extracted from the IASO multi-campaign form.
        expected_structure_df (pd.DataFrame): The dataframe containing the expected structure of the IASO multi-campaign form.

    Returns:
        campaign_filter_table (pd.DataFrame): DataFrame containing the list of campaigns to be used as filter in PBI
        month_filter_table (pd.DataFrame): DataFrame containing the list of months to be used as filter in PBI
        round_filter_table (pd.DataFrame): DataFrame containing the list of rounds to be used as filter in PBI
        year_filter_table (pd.DataFrame): DataFrame containing the list of years to be used as filter in PBI
        products_filter_table (pd.DataFrame): DataFrame containing the list of products to be used as filter in PBI
        combination_filter_table (pd.DataFrame): DataFrame containing the list of combinations of campaign, round, year, product and aggregation level (district vs CSI) to allow flexible filtering in PBI
    """
    current_run.log_info("Création de filtres nécessaires à la visualisation...")
    try:
        campaign_filter_table = _distinct_values(iaso_form_data_df, "choix_campagne")
        month_filter_table = _distinct_values(iaso_form_data_df, "month")
        round_filter_table = _distinct_values(expected_structure_df, "round")
        year_filter_table = _distinct_values(expected_structure_df, "year")
        products_filter_table = _distinct_values(expected_structure_df, "produit")

        combination_filter_table = _build_combination_filter_table(
            campaign_filter_table,
            month_filter_table,
            round_filter_table,
            year_filter_table,
            products_filter_table,
        )

        current_run.log_info("Filtres pour la visualisation créés avec succès.")

        return (
            campaign_filter_table,
            month_filter_table,
            round_filter_table,
            year_filter_table,
            products_filter_table,
            combination_filter_table,
        )
    except Exception as e:
        msg = f"Erreur lors de la création des filtres pour la visualisation: {e}"
        current_run.log_error(msg)
        raise


def _build_combination_filter_table(
    campaign_filter_table: pd.DataFrame,
    month_filter_table: pd.DataFrame,
    round_filter_table: pd.DataFrame,
    year_filter_table: pd.DataFrame,
    products_filter_table: pd.DataFrame,
) -> pd.DataFrame:
    """Every combination of campaign/month/round/year/product/org-unit-level, for
    flexible cross-filtering in PBI."""
    choice_filter_table = pd.DataFrame({"choice_org_unit_level": ["District", "CSI"]})
    names = [
        "choix_campagne",
        "month",
        "round",
        "year",
        "produit",
        "choice_org_unit_level",
    ]
    return pd.MultiIndex.from_product(
        [
            campaign_filter_table["choix_campagne"],
            month_filter_table["month"],
            round_filter_table["round"],
            year_filter_table["year"],
            products_filter_table["produit"],
            choice_filter_table["choice_org_unit_level"],
        ],
        names=names,
    ).to_frame(index=False)


def _distinct_values(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """The distinct, non-null values of one column, as a single-column filter table."""
    return df[[col]].drop_duplicates().dropna().reset_index(drop=True)


# =========================================================================== #
# Spatial units                                                                #
# =========================================================================== #
def create_dynamic_org_unit_table(
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
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
        iaso_org_unit_tree_clean_df (pd.DataFrame): DataFrame containing the cleaned org unit tree.

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
        district_view = _district_level_org_unit_view(iaso_org_unit_tree_clean_df)
        csi_view = _csi_level_org_unit_view(iaso_org_unit_tree_clean_df)
        spatial_units_combined = pd.concat(
            [district_view, csi_view], ignore_index=True
        ).reset_index(drop=True)

        current_run.log_info(
            "Tableau dynamique des unités organisationnelles créé avec succès."
        )
        return spatial_units_combined

    except Exception as e:
        msg = f"Erreur lors de la création du tableau dynamique des unités organisationnelles: {e}"
        current_run.log_error(msg)
        raise


def _district_level_org_unit_view(
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """One row per district (LVL_1 = country, no CSI level)."""
    districts = iaso_org_unit_tree_clean_df.sort_values(["LVL_3_NAME", "org_unit_id"])
    view = districts.groupby("LVL_3_NAME", as_index=False).first()

    view["choice_org_unit_level"] = "District"
    view["LVL_1_NAME"] = "Niger"
    view["LVL_6_NAME"] = None
    view["LVL_6_UID"] = None
    view["link_key"] = (
        view["org_unit_id"].astype(str) + "_" + view["choice_org_unit_level"]
    )
    return view


def _csi_level_org_unit_view(iaso_org_unit_tree_clean_df: pd.DataFrame) -> pd.DataFrame:
    """One row per CSI, with every level shifted up one notch (LVL_1 = region, ...,
    LVL_3 = CSI) so PBI can switch to CSI granularity using the same column names."""
    view = iaso_org_unit_tree_clean_df.copy()
    view["choice_org_unit_level"] = "CSI"
    view["LVL_1_NAME"] = view["LVL_2_NAME"]
    view["LVL_1_UID"] = view["LVL_2_UID"]
    view["LVL_2_NAME"] = view["LVL_3_NAME"]
    view["LVL_2_UID"] = view["LVL_3_UID"]
    view["LVL_3_NAME"] = view["LVL_6_NAME"]
    view["LVL_3_UID"] = view["LVL_6_UID"]
    view["link_key"] = (
        view["org_unit_id"].astype(str) + "_" + view["choice_org_unit_level"]
    )
    return view


# =========================================================================== #
# Campaign/round summary                                                       #
# =========================================================================== #
def create_campaign_round_summary_table(
    cvrg_total: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create a summary table of the different campaigns, rounds, years, periods and products present in the combined IASO data to be used as a visual in PBI.

    Args:
        cvrg_total (pd.DataFrame): the dataframe containing the coverage data for all campaigns, rounds, years, periods and products present in the combined IASO data.

    Returns:
        campaign_round_summary_df (pd.DataFrame): summary table of the different campaigns, rounds, years and products present in the combined IASO data.
    """
    current_run.log_info(
        "Création du tableau de résumé des campagnes, produits, années, rounds, et périodes..."
    )
    try:
        summary = (
            cvrg_total[["produit", "year", "round", "period"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        group_keys = ["produit", "year", "round"]
        summary["round_start"] = summary.groupby(group_keys)["period"].transform("min")
        summary["round_end"] = summary.groupby(group_keys)["period"].transform("max")
        summary = (
            summary[group_keys + ["round_start", "round_end"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        current_run.log_info(
            "Tableau de résumé des campagnes, rounds, années et produits créé avec succès."
        )
        return summary

    except Exception as e:
        msg = f"Erreur lors de la création du tableau de résumé des campagnes, rounds, années et produits: {e}"
        current_run.log_error(msg)
        raise


def add_month_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'month' column to the dataframe based on the columns 'choix_campagne'/'produit', 'year', 'round' and 'period',
    where it corresponds to the month of the very first period for each unique combination of 'choix_campagne'/'produit',
      'year' and 'round' to allow filtering by month in PBI.

    Args:
        df (pd.DataFrame): DataFrame containing a 'period' column in datetime format.

    Returns:
        df(pd.DataFrame): DataFrame with an added 'month' column representing the month of the 'period'.
    """
    current_run.log_info("Ajout de la colonne 'Mois de la campagne'...")
    try:
        group_cols = (
            ["choix_campagne", "year", "round"]
            if "choix_campagne" in df.columns
            else ["produit", "year", "round"]
        )
        df["month"] = (
            df.groupby(group_cols)["period"]
            .transform("min")
            .dt.month.map(months_mapping_dict)
        )

        current_run.log_info("Colonne 'Mois de la campagne' ajoutée avec succès.")
        return df
    except Exception as e:
        msg = f"Erreur lors de l'ajout de la colonne 'Mois de la campagne': {e}"
        current_run.log_error(msg)
        raise


if __name__ == "__main__":
    build_visualisation_tables()
