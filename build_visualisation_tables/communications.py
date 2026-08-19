"""
Communications theme: outreach/deployment/causeries/cases/activities indicators
for ner_vaccination_communications(_long).
"""

import pandas as pd
from openhexa.sdk import current_run

from data_cleaning import drop_zero_values, melt_campaign_columns, new_cols

# communication table
communication_deployment_polio = [
    "nbre_relais",
    "nbre_concession_visite",
    "nbre_concession_non_favorable_vaccination",
]

communication_deployment_rougeole = [
    "nbre_relais_car",
    "nbre_concession_visite_car",
    "nbre_concession_non_favorable_vaccination_car",
]

communication_deployment_fjaune = [
    "nbre_relais_fievre_jaune",
    "nbre_concession_visite_fievre_jaune",
    "nbre_concession_non_favorable_vaccination_fievre_jaune",
]

communication_deployment_men5_cols = [
    "men5_nombre_total_de_relais_mobilises",
    "men5_nombre_total_de_concessions_visitees",
    "men5_nombre_de_concessions_non_favorables_a_la_vaccination",
]


communication_reach_polio = [
    "nbre_personne_touche_par_relais",
    "nbre_personne_touche_par_refugie_deplace_mi",
    "nbre_personne_touche_nomande_trans_puits",
    "nbre_personne_touche_zone_frontaliere",
]
communication_reach_rougeole = [
    "nbre_personne_touche_par_relais_car",
    "nbre_personne_touche_par_refugie_deplace_mi_car",
    "nbre_personne_touche_nomande_trans_puits_car",
    "nbre_personne_touche_zone_frontaliere_car",
]

communication_reach_fjaune = [
    "nbre_personne_touche_par_relais_fievre_jaune",
    "nbre_personne_touche_par_refugie_deplace_mi_fievre_jaune",
    "nbre_personne_touche_nomande_trans_puits_fievre_jaune",
    "nbre_personne_touche_zone_frontaliere_fievre_jaune",
]

communication_reach_men5_cols = [
    "men5_nombre_de_personnes_touchees_par_le_relais",
    "men5_nombre_de_personnes_touchees_au_niveau_des_camps_des_refugies_deplaces_migrants",
    "men5_nombre_de_personnes_touchees_au_niveau_des_camps_des_nomades_transhumants_puits",
    "men5_nombre_de_personnes_touchees_au_niveau_des_villages_sites_frontaliers",
]


communication_denombrement_polio = [
    "nbre_enfant_0_5_ans_demonbre",
]

communication_denombrement_rougeole = [
    "nbre_enfant_0_5_ans_demonbre_car",
]

communication_denombrement_fjaune = [
    "nbre_enfant_9_11_mois_demonbre_fievre_jaune",
    "nbre_enfant_12_23_mois_demonbre_fievre_jaune",
    "nbre_enfant_24_59_mois_demonbre_fievre_jaune",
    "nbre_personnes_5_14_ans_demonbre_fievre_jaune",
    "nbre_personnes_15_60_ans_demonbre_fievre_jaune",
]

communication_denombrement_men5_cols = [
    "men5_nombre_d_enfants_de_0_5_ans_dans_les_concessions_visitees",
]

communication_causeries_polio = [
    "nbre_causerie",
    "nbre_causeri_marche",
    "nbre_causerie_refugie_deplace_migrant",
    "nbre_causerie_nomade_trans_puit",
    "nbre_causerie_fontalier",
]

communication_causeries_rougeole = [
    "nbre_causerie_car",
    "nbre_causeri_marche_car",
    "nbre_causerie_refugie_deplace_migrant_car",
    "nbre_causerie_nomade_trans_puit_car",
    "nbre_causerie_fontalier_car",
]

communication_causeries_fjaune = [
    "nbre_causerie_fievre_jaune",
    "nbre_causeri_marche_fievre_jaune",
    "nbre_causerie_refugie_deplace_migrant_fievre_jaune",
    "nbre_causerie_nomade_trans_puit_fievre_jaune",
    "nbre_causerie_fontalier_fievre_jaune",
]

communication_causeries_men5_cols = [
    "men5_nombre_de_causeries_au_niveau_des_sites_ordinaires",
    "men5_nombre_de_causeries_au_niveau_des_marches",
    "men5_nombre_de_causeries_au_niveau_des_camps_des_refugies_deplaces_migrants",
    "men5_nombre_de_causeries_au_niveau_des_camps_des_nomades_transhumants_puits",
    "men5_nombre_de_causeries_au_niveau_des_villages_frontaliers",
]

communication_cas_polio = [
    "nbre_total_cas_pfa_signale",
    "nbre_cas_rumeur_notifier",
]

communication_cas_rougeole = [
    "nbre_total_cas_pfa_signale_car",
    "nbre_cas_rumeur_notifier_car",
]

communication_cas_fjaune = [
    "nbre_total_cas_pfa_signale_fievre_jaune",
    "nbre_total_cas_fievre_jaune_signale_fievre_jaune",
]

communication_cas_men5_cols = [
    "men5_nombre_total_de_cas_pfa_signales",  # to check with Issa/Fernando if these are the right columns
    "men5_nombre_de_cas_de_rumeur_notifies_par_le_comite_de_veille",
]

communication_activities_polio = [
    "nbre_reunion_plaidoyer_tenue",
    "nbre_leader_engage",
    "nbre_radio_implique",
    "nbre_spots",
    "nbre_appel_leader",
    "nbre_debats",
    "nbre_interviews",
    "nbre_emission_publique",
    "nbre_dialoge_commutaire_tenu",
    "estimation_population_expose",
]

communication_activities_rougeole = [
    "nbre_reunion_plaidoyer_tenue_car",
    "nbre_leader_engage_car",
    "nbre_radio_implique_car",
    "nbre_spots_car",
    "nbre_appel_leader_car",
    "nbre_debats_car",
    "nbre_interviews_car",
    "nbre_emission_publique_car",
    "nbre_dialoge_commutaire_tenu_car",
    "estimation_population_expose_car",
]

communication_activities_fjaune = [
    "nbre_reunion_plaidoyer_tenue_fievre_jaune",
    "nbre_leader_engage_fievre_jaune",
    "nbre_radio_implique_fievre_jaune",
    "nbre_spots_fievre_jaune",
    "nbre_appel_leader_fievre_jaune",
    "nbre_debats_fievre_jaune",
    "nbre_interviews_fievre_jaune",
    "nbre_emission_publique_fievre_jaune",
    "nbre_dialoge_commutaire_tenu_fievre_jaune",
    "estimation_population_expose_fievre_jaune",
]

communication_activities_men5_cols = [
    "men5_nombre_de_reunions_de_plaidoyer_tenues",
    "men5_nombre_de_leaders_engages",
    "men5_nombre_de_radios_impliques",
    "men5_nombre_de_spots_diffuses",
    "men5_nombre_d_appels_de_leaders",
    "men5_nombre_de_debats_organises",
    "men5_nombre_d_interviews_realises",
    "men5_nombre_d_emissions_publiques_diffusees",
    "men5_nombre_de_dialogues_communautaires_tenus",
    "men5_nombre_estime_de_la_population_exposee_aux_messages",
]

communication_campaign_map = {
    "polio": communication_deployment_polio
    + communication_denombrement_polio
    + communication_reach_polio
    + communication_causeries_polio
    + communication_cas_polio
    + communication_activities_polio,
    "rougeole": communication_deployment_rougeole
    + communication_denombrement_rougeole
    + communication_reach_rougeole
    + communication_causeries_rougeole
    + communication_cas_rougeole
    + communication_activities_rougeole,
    "fièvre jaune": communication_deployment_fjaune
    + communication_denombrement_fjaune
    + communication_reach_fjaune
    + communication_causeries_fjaune
    + communication_cas_fjaune
    + communication_activities_fjaune,
    "méningite": communication_deployment_men5_cols
    + communication_denombrement_men5_cols
    + communication_reach_men5_cols
    + communication_causeries_men5_cols
    + communication_cas_men5_cols
    + communication_activities_men5_cols,
    "tcv": communication_deployment_men5_cols
    + communication_denombrement_men5_cols
    + communication_reach_men5_cols
    + communication_causeries_men5_cols
    + communication_cas_men5_cols
    + communication_activities_men5_cols,
}

communication_category_groups = {
    "Deploiement": (
        communication_deployment_polio
        + communication_deployment_rougeole
        + communication_deployment_fjaune
        + communication_deployment_men5_cols
    ),
    "Portée": (
        communication_reach_polio
        + communication_reach_rougeole
        + communication_reach_fjaune
        + communication_reach_men5_cols
    ),
    "Dénombrement": (
        communication_denombrement_polio
        + communication_denombrement_rougeole
        + communication_denombrement_fjaune
        + communication_denombrement_men5_cols
    ),
    "Causeries": (
        communication_causeries_polio
        + communication_causeries_rougeole
        + communication_causeries_fjaune
        + communication_causeries_men5_cols
    ),
    "Cas Notifiés": (
        communication_cas_polio
        + communication_cas_rougeole
        + communication_cas_fjaune
        + communication_cas_men5_cols
    ),
    "Activitiés": (
        communication_activities_polio
        + communication_activities_rougeole
        + communication_activities_fjaune
        + communication_activities_men5_cols
    ),
}

communication_category_mapping = {
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


def communication_categorizer(string: str) -> str:
    """
    Categorizes communication types based on the input string.

    Parameters:
        string (str): The input string containing communication information.

    Returns:
        str: The categorized communication type, or "N/A" if no specific type is covered by the form configuration.
    """
    for key in communication_category_mapping:
        if key in string:
            return communication_category_mapping[key]

    return "N/A"


def get_communication_category_type(col_name: str, master_groups: dict) -> str:
    """
    Identifies which group (Deployment, Reach, etc.) a column belongs to.

    Parameters:
        col_name (str): The name of the column to categorize.
        master_groups (dict): A dictionary where keys are group names and values are sets of column names.

    Returns:
        str: The name of the group the column belongs to, or "N/A" if it doesn't belong to any group.
    """
    for group_name, list_of_cols in master_groups.items():
        if col_name in list_of_cols:
            return group_name

    return "N/A"


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
            observed=True,
        )["value"]
        .sum()
        .fillna(0)
    )
