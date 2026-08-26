import pandas as pd
from openhexa.sdk import pipeline
from shared_utils import (
    load_data,
    save_file,
    export_to_dataset,
)

from config import OUTPUTS_PATH
from data_cleaning import (
    EXPECTED_STRUCTURE_COLS,
    EXPECTED_STRUCTURE_CATEGORY_COLS,
    add_month_column,
)
from coverage_tables import create_coverage_dataset, add_target_data
from completeness_table import create_completeness_dataset
from stocks_table import create_stocks_dataset
from supervision_table import create_supervision_dataset
from communications_tables import create_communication_dataset
from filter_tables import create_filter_tables, create_campaign_round_summary_table
from spatial_units_table import create_dynamic_org_unit_table


@pipeline(
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
    Each step's own logic lives in its theme's module (coverage.py, stocks.py, etc. - see
    docs/ARCHITECTURE.md §14.4); this file only sequences the calls between them.
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
        _load_expected_structure(),
        load_data("iaso_org_unit_tree_clean"),
        load_data("iaso_org_unit_tree_raw"),
    )


def _load_expected_structure() -> pd.DataFrame:
    """expected_data_structure can run to ~50M rows - load only the columns this
    pipeline actually uses (see EXPECTED_STRUCTURE_COLS in data_cleaning.py) and
    decode the low-cardinality ones straight into category dtype at read time (via
    load_data's categories= - see shared/utils.py), or the plain object-dtype
    string columns alone (even just transiently, during the parquet->pandas
    conversion itself, before any post-load .astype("category") could run) inflate
    to tens of GB in memory and OOM-kill the run."""
    return load_data(
        "expected_data_structure",
        columns=EXPECTED_STRUCTURE_COLS,
        categories=EXPECTED_STRUCTURE_CATEGORY_COLS,
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


if __name__ == "__main__":
    build_visualisation_tables()
