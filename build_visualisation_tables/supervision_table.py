"""
Supervision theme: notified case counts (AFP, MAPI, yellow fever) for
ner_vaccination_supervision.
"""

import pandas as pd
from openhexa.sdk import current_run

from data_cleaning import drop_zero_values, melt_campaign_columns, new_cols

# supervision table
supervision_polio_cols = [
    "nbre_cas_pfa_notifie",
    "nbre_cas_mapi_notifie_mapi",
    "nbre_cas_mapi_majeur_notifie_mapi",
]
supervision_rougeole_cols = [
    "nombre_MAPI_non_grave",
    "nombre_MAPI_graves",
]
supervision_fjaune_cols = [
    "nbre_cas_pfa_notifie_fievre_jaune",
    "nbre_cas_fievre_jaune_notifie_fievre_jaune",
    "nbre_cas_mapi_notifie_mapi_fievre_jaune",
    "nbre_cas_mapi_majeur_notifie_mapi_fievre_jaune",
]
supervision_men5_cols = [
    "men5_total_de_cas_de_pfa_signales",
    "men5_mapi_mineurs",
    "men5_mapi_graves",
]

supervision_campaign_map = {
    "polio": supervision_polio_cols,
    "rougeole": supervision_rougeole_cols,
    "fièvre jaune": supervision_fjaune_cols,
    "méningite": supervision_men5_cols,
}

supervision_cols_selection_1 = ["period", "round", "year", "org_unit_id"]
supervision_cols_selection_2 = supervision_cols_selection_1 + ["choix_campagne"]

surveillance_category_mapping = {
    "pfa": "pfa",
    "nbre_cas_fievre_jaune_notifie": "fievre_jaune_notifie",
    "mapi_notifie_mapi": "mapi_mineur",
    "MAPI_non_grave": "mapi_mineur",
    "mapi_mineur": "mapi_mineur",
    "mapi_majeur": "mapi_majeur",
    "MAPI_grave": "mapi_majeur",
    "mapi_grave": "mapi_majeur",
}


def supervision_categorizer(string: str) -> str:
    """
    Categorizes supervision types based on the input string.

    Parameters:
        string (str): The input string containing supervision information.

    Returns:
        str: The categorized supervision type, or "N/A" if no specific type is covered by the form configuration.
    """
    for key in surveillance_category_mapping:
        if key in string:
            return surveillance_category_mapping[key]

    return "N/A"


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
            supervision_cols_selection_2 + ["supervision"],
            as_index=False,
            observed=True,
        )["value"]
        .sum()
        .fillna(0)
    )
