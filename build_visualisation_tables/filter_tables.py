"""
PBI filter/summary lookups: the campaign/round/year/product/combination filter
tables, and the campaign-round period summary. Cross-theme by nature (each pulls
from whichever theme's output happens to carry the relevant column), so no config
constants of its own.
"""

import pandas as pd
from openhexa.sdk import current_run


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
        summary["round_start"] = summary.groupby(group_keys, observed=True)[
            "period"
        ].transform("min")
        summary["round_end"] = summary.groupby(group_keys, observed=True)[
            "period"
        ].transform("max")
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
