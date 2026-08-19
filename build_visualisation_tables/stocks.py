"""
Stocks theme: vaccine/consumable stock movements (received/used/remaining) and the
derived box-ratio and stock-loss inputs for ner_vaccination_stock.
"""

import numpy as np
import pandas as pd
from openhexa.sdk import current_run

from data_cleaning import drop_zero_values, melt_campaign_columns, new_cols

# stocks table
stock_polio_cols = (
    ["stock_polio", "nbre_flacons_polio_recus", "nbre_flacons_polio_utilises"]
    + ["stock_vitamine_a", "nbre_vit_a_recu", "nbre_vit_a_utilise"]
    + ["stock_albendazole", "nbre_albendazole_recu", "nbre_albendazole_utilise"]
)

stock_rougeole_cols = [
    "nombre_vaccin_recu",
    "nombre_vaccin_utilise",
]

stock_fjaune_cols = [
    "stock_fievre_jaune",
    "nbre_flacons_recus_fievre_jaune",
    "nbre_flacons_utilises_fievre_jaune",
]

stock_men5_cols = (
    [
        "men5_flacons_diluant_recus",
        "men5_flacons_diluant_utilises",
        "men5_flacons_diluant_restants",
    ]
    + [
        "men5_seringues_auto_bloquantes_recus",
        "men5_seringues_auto_bloquantes_utilises",
        "men5_seringues_auto_bloquantes_restants",
    ]
    + [
        "men5_seringues_dilution_recus",
        "men5_seringues_dilution_utilises",
        "men5_seringues_dilution_restants",
    ]
    + [
        "men5_boites_securite_recus",
        "men5_boites_securite_utilises",
        "men5_boites_securite_restants",
    ]
)

stock_tcv_cols = (
    [
        "tcv_flacons_vaccin_recus",
        "tcv_flacons_vaccin_utilises",
        "tcv_flacons_vaccin_restants",
    ]
    + [
        "tcv_seringues_auto_bloquantes_recus",
        "tcv_seringues_auto_bloquantes_utilises",
        "tcv_seringues_auto_bloquantes_restants",
    ]
    + [
        "tcv_boites_securite_recus",
        "tcv_boites_securite_utilises",
        "tcv_boites_securite_restants",
    ]
)

stocks_cols_selection_1 = ["period", "round", "year", "org_unit_id"]
stocks_cols_selection_2 = stocks_cols_selection_1 + ["produit", "product_status"]
stocks_cols_selection_3 = stocks_cols_selection_1 + ["produit"]

stocks_campaign_map = {
    "polio": stock_polio_cols,
    "fièvre jaune": stock_fjaune_cols,
    "rougeole": stock_rougeole_cols,
    "méningite": stock_men5_cols,
    "tcv": stock_tcv_cols,
}

stock_ratios_config = {
    "vaccin polio": 50,
    "vitamine A": 1,
    "albendazole": 1,
    "rougeole": 10,
    "fièvre jaune": 1,
    "méningite": 1,
    "tcv": 1,
}

# config for the categorizers below
products_mapping_stocks = {
    "vitamine_a": "vitamine A",
    "vit_a": "vitamine A",
    "vpo": "vaccin polio",
    "polio": "vaccin polio",
    "albendazole": "albendazole",
    "depara": "albendazole",
    "nombre_": "rougeole",  # beware
    "fievre_jaune": "fièvre jaune",
    "men5": "méningite",
    "tcv": "tcv",
}

stock_status_mapping = {
    "stock": "stock",
    "restants": "stock",
    "recu": "reçu",
    "utilise": "utilisé",
}


def produit_categorizer_stocks(string: str) -> str:
    """
    Categorizes product types based on the input string.

    Parameters:
        string (str): The input string containing product information.

    Returns:
        str: The categorized product type, or "N/A" if no specific product type is covered by the form configuration.
    """
    for key in products_mapping_stocks:
        if key in string:
            return products_mapping_stocks[key]

    return "N/A"


def product_status_categorizer(string: str) -> str:
    """
    Categorizes product status based on the input string.

    Parameters:
        string (str): The input string containing product status information.

    Returns:
        str: The categorized product status, or "N/A" if no specific status is covered by the form configuration.
    """
    for key in stock_status_mapping:
        if key in string:
            return stock_status_mapping[key]

    return "N/A"


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
    totals = df.groupby(stocks_cols_selection_2, as_index=False, observed=True)[
        "value"
    ].sum()
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
        cvrg_total.groupby(stocks_cols_selection_3, as_index=False, observed=True)[
            "value"
        ]
        .sum()
        .rename(columns={"value": "enfants_vaccines"})
    )
    return stock.merge(cvrg_stock, how="left").fillna(0)
