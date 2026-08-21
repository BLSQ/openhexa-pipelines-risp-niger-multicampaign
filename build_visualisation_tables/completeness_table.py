"""
Completeness theme: whether an immunization team was present in a given CSI
during a given period, and cumulatively across the campaign.
"""

import pandas as pd
from openhexa.sdk import current_run

from data_cleaning import align_categories_for_merge, drop_duplicates_low_memory, EXPECTED_STRUCTURE_CATEGORY_COLS

# completeness table
cmpl_cols_selection = ["choix_campagne", "org_unit_id", "year", "round", "period"]
cmpl_cols_selection_2 = ["choix_campagne", "org_unit_id", "year", "round"]


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
        expected = drop_duplicates_low_memory(expected)

        # Same reasoning as the coverage merge (coverage._merge_coverage_with_expected_structure):
        # align category dtype on both sides first, or the merge falls back to
        # object dtype for these columns across ~50M rows.
        category_cols = [
            c for c in EXPECTED_STRUCTURE_CATEGORY_COLS if c in cmpl_cols_selection
        ]
        expected, actual = align_categories_for_merge(expected, actual, category_cols)

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
        .groupby(cmpl_cols_selection_2, observed=True)["period"]
        .transform("min")
        .reindex(cmpl.index)
    )
    cmpl["presence_equipe_cum"] = (
        (cmpl["period"] == first_visit_period) & is_visited
    ).astype(int)
    return cmpl.drop_duplicates().reset_index(drop=True)
