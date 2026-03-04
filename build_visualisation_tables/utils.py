import numpy as np
import pandas as pd


def age_categorizer(string):
    """
    Categorizes age groups based on the input string.

    Parameters:
        string (str): The input string containing age information.

    Returns:
        str: The categorized age group.
    """
    ages = {
        "0_11_mois": "0-11 mois",
        "12_59_mois": "12-59 mois",
        "6_11_mois": "6-11 mois",
        "12_24_mois": "12-24 mois",
        "12_23": "12-23 mois",
        "24_59": "24-59 mois",
        "e_6_": "6-9 mois",
        "e_9_": "9-11 mois",
        "e_12_": "12-59 mois",
        "9_11_mois": "9-11 mois",
        "12_24_mois": "12-24 mois",
        "5_14_ans": "5-14 ans",
        "15_60_ans": "15-60 ans",
        "1_4_ans": "1-4 ans",
        "15_19_ans": "15-19 ans",
    }

    for key in ages:
        if key in string:
            return ages[key]

    return "TOUS"


def site_categorizer(string):
    """
    Categorizes site types based on the input string.

    Parameters:
        string (str): The input string containing site information.

    Returns:
        str: The categorized site type.
    """
    sites = {
        "site_ordinaire": "ordinaire",
        "site_speciaux_nomad": "spécial",
        "site_speciaux_gares": "spécial",
        "site_speciaux_marche": "spécial",
        "site_speciaux_point_eau": "spécial",
        "site_speciaux_postefron": "frontalier",
        "site_trans_front_cote_front": "transfrontalier : étranger",
        "site_trans_front_cote_niger": "transfrontalier : Niger",
        "site_speciaux_deplace_int": "déplacés internes",
        "site_speciaux_refugie": "réfugiés",
        "site_speciaux": "spécial",
        "site_speciaux_autre": "spécial",
        "fixe": "fixe",
        "avance": "avancé",
        "mobile": "mobile",
    }

    for key in sites:
        if key in string:
            return sites[key]

    return "TOUS"


def produit_categorizer(string):
    """
    Categorizes product types based on the input string.

    Parameters:
        string (str): The input string containing product information.

    Returns:
        str: The categorized product type.
    """
    products = {
        "vitamine_a": "vitamine A",
        "vit_a": "vitamine A",
        "vpo": "vaccin polio",
        "polio": "vaccin polio",
        "albendazole": "albendazole",
        "depara": "albendazole",
        "nombre_": "rougeole",  # beware
        "fievre_jaune": "fièvre jaune",
        "men5_men5": "méningite",
        "men5_tcv": "tcv",
    }

    for key in products:
        if key in string:
            return products[key]

    return "TOUS"

def produit_categorizer_stocks(string):
    """
    Categorizes product types based on the input string.

    Parameters:
        string (str): The input string containing product information.

    Returns:
        str: The categorized product type.
    """
    products = {
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

    for key in products:
        if key in string:
            return products[key]

    return "TOUS"


def vaccination_status_categorizer(string):
    """
    Categorizes vaccination status based on the input string.

    Parameters:
        string (str): The input string containing vaccination information.

    Returns:
        str: The categorized vaccination status.
    """
    status = {"zero_dose": "zéro dose", "deja_recu": "déjà reçu"}

    for key in status:
        if key in string:
            return status[key]

    return "zéro dose"


def product_status_categorizer(string):
    """
    Categorizes product status based on the input string.

    Parameters:
        string (str): The input string containing product status information.

    Returns:
        str: The categorized product status.
    """
    products = {
        "stock": "stock",
        "restants": "stock",
        "recu": "reçu",
        "utilise": "utilisé",
    }

    for key in products:
        if key in string:
            return products[key]

    return "TOUS"


def supervision_categorizer(string):
    """
    Categorizes supervision types based on the input string.

    Parameters:
        string (str): The input string containing supervision information.

    Returns:
        str: The categorized supervision type.
    """
    supervision_cat = {
        "pfa": "pfa",
        "nbre_cas_fievre_jaune_notifie": "fievre_jaune_notifie",
        "mapi_notifie_mapi": "mapi_mineur",
        "MAPI_non_grave": "mapi_mineur",
        "mapi_mineur": "mapi_mineur",
        "mapi_majeur": "mapi_majeur",
        "MAPI_grave": "mapi_majeur",
        "mapi_grave": "mapi_majeur",
    }

    for key in supervision_cat:
        if key in string:
            return supervision_cat[key]

    return ""


def communication_categorizer(string):
    """
    Categorizes communication types based on the input string.

    Parameters:
        string (str): The input string containing communication information.

    Returns:
        str: The categorized communication type.
    """
    communication_cat = {
        "appel_leader": "appel_leader",
        "appels_de_leaders": "appel_leader",
        "rumeur": "cas_notifies",
        "cas_fievre_jaune_signale": "cas_notifies",
        "pfa": "cas_pfa_notifies",
        "nbre_causerie": "causerie_ordinaire",
        "causeries_au_niveau_des_sites_ordinaires": "causerie_ordinaire",
        "causeri_marche": "causerie_marche",
        "causeries_au_niveau_des_marches": "causerie_marche",
        "causerie_refugie": "causerie_refugie",
        "causeries_au_niveau_des_camps_des_refugies": "causerie_refugie",
        "causerie_nomade": "causerie_nomade",
        "causeries_au_niveau_des_camps_des_nomades": "causerie_nomade",
        "causerie_fontalier": "causerie_frontalier",
        "causeries_au_niveau_des_villages_frontaliers": "causerie_frontalier",
        "concession_visite": "concessions_visitees",
        "concessions_visitees": "concessions_visitees",
        "concession_non_favorable": "concessions_non_favorables",
        "concessions_non_favorables": "concessions_non_favorables",
        "debats": "debats",
        "debats_organises": "debats",
        "dialoge_commutaire": "dialogue_communautaire",
        "dialogues_communautaires_tenus": "dialogue_communautaire",
        "emission_publique": "emission_publique",
        "emissions_publiques_diffusees": "emission_publique",
        "enfant_0_5_ans_demonbre": "denombrement_0_5_ans",
        "enfants_de_0_5_ans_dans_les_concessions_visitees": "denombrement_0_5_ans",
        "personnes_5_14_ans_demonbre": "denombrement_5_14_ans",
        "personnes_15_60_ans_demonbre": "denombrement_15_60_ans",
        "enfant_9_11_mois_demonbre": "denombrement_9_11_mois",
        "enfant_12_23_mois_demonbre": "denombrement_12_23_mois",
        "enfant_24_59_mois_demonbre": "denombrement_24_59_mois",
        "interviews": "interviews",
        "leader_engage": "leader_engage",
        "leaders_engages": "leader_engage",
        "personne_touche_nomande": "personnes_touchees_nomades",
        "personnes_touchees_au_niveau_des_camps_des_nomades": "personnes_touchees_nomades",
        "personne_touche_par_refugie": "personnes_touchees_refugies",
        "personnes_touchees_au_niveau_des_camps_des_refugies": "personnes_touchees_refugies",
        "personne_touche_zone_frontaliere": "personnes_touchees_frontaliers",
        "personnes_touchees_au_niveau_des_villages_sites_frontaliers": "personnes_touchees_frontaliers",
        "personne_touche_par_relais": "personnes_touchees_relais",
        "personnes_touchees_par_le_relais": "personnes_touchees_relais",
        "radio": "radios_impliquees",
        "nbre_relais": "nbre_relais_mobilises",
        "relais_mobilises": "nbre_relais_mobilises",
        "plaidoyer": "reunions_plaidoyer",
        "spots": "spots_diffuses",
        "population_expose": "population_exposee",
    }

    for key in communication_cat:
        if key in string:
            return communication_cat[key]

    return ""


def get_communication_category_type(col_name, master_groups):
    """
    Identifies which group (Deployment, Reach, etc.) a column belongs to.
    """
    for group_name, list_of_cols in master_groups.items():
        if col_name in list_of_cols:
            return group_name
    return "Unknown"


def new_cols(
    df: pd.DataFrame, pattern: str, value_col: str, function_list=None
) -> pd.DataFrame:
    """
    Adds new columns to the dataframe based on functions whose names contain a specific pattern.

    Parameters:
        df (pd.DataFrame): The input dataframe to modify.
        pattern (str): A common string in function names to identify relevant functions.
        value_col (str): The name of the column in the dataframe to provide values for the functions.
        function_list (list, optional): A list of functions to use for creating new columns.
                                         If None, functions will be identified based on the pattern.

    Returns:
        pd.DataFrame: The modified dataframe with new columns added.
    """
    if not function_list:
        function_list = [
            f
            for f in globals().values()
            if type(f) == type(lambda *args: None) and pattern in f.__name__
        ]

    for fun in function_list:
        new_colname = fun.__name__.rsplit("_", maxsplit=1)[0]
        df.loc[:, new_colname] = df.loc[:, value_col].map(fun)
    return df
