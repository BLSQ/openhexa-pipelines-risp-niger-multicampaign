# IASO Connector Instances
iaso_playground_connector_slug = {
    "url": "https://iaso-playground.bluesquare.org",
    "username": "fernando_di_demo",
    "password": "13.5	19.5	10.5",
}

iaso_connector_slug = {
    "url": "https://iaso.bluesquare.org",
    "username": "fernando_diniger",
    "password": "hbe8quh1hjm*cyx6AQH",
}

iaso_form_id = 1186
current_period = "2026Q1"
current_period_start_date = "2026-01-01"

# campaign name cleaning and mapping
campaign_name_cleaning_dict = {
    "men5_tcv": "men5 tcv",
}

campaign_name_mapping_dict = {
    "POLIOMYELITE": "polio",
    "rougeole": "rougeole",
    "Fievre_Jaune": "fièvre jaune",
    "men5": "méningite",
    "tcv": "tcv",
}

# iaso df
iaso_df_common_cols = [
    "uuid",
    "form_id",
    "created_at",
    "period",
    "statuschoix_campagne",
    "org_unit_id",
]

# coverage table
cvrg_polio_cols = (
    [
        "vitamine_a_12_24_mois_site_ordinaire",
        "vitamine_a_12_24_mois_site_speciaux",
        "vitamine_a_12_24_mois_site_speciaux_autre",
        "vitamine_a_12_24_mois_site_speciaux_deplace_int",
        "vitamine_a_12_24_mois_site_speciaux_gares",
        "vitamine_a_12_24_mois_site_speciaux_marche",
        "vitamine_a_12_24_mois_site_speciaux_nomad",
        "vitamine_a_12_24_mois_site_speciaux_point_eau",
        "vitamine_a_12_24_mois_site_speciaux_postefron",
        "vitamine_a_12_24_mois_site_speciaux_refugie",
        "vitamine_a_12_24_mois_site_trans_front_cote_front",
        "vitamine_a_12_24_mois_site_trans_front_cote_niger",
        "vitamine_a_6_11_mois_site_ordinaire",
        "vitamine_a_6_11_mois_site_speciaux",
        "vitamine_a_6_11_mois_site_speciaux_autre",
        "vitamine_a_6_11_mois_site_speciaux_deplace_int",
        "vitamine_a_6_11_mois_site_speciaux_gares",
        "vitamine_a_6_11_mois_site_speciaux_marche",
        "vitamine_a_6_11_mois_site_speciaux_nomad",
        "vitamine_a_6_11_mois_site_speciaux_point_eau",
        "vitamine_a_6_11_mois_site_speciaux_postefron",
        "vitamine_a_6_11_mois_site_speciaux_refugie",
        "vitamine_a_6_11_mois_site_trans_front_cote_front",
        "vitamine_a_6_11_mois_site_trans_front_cote_niger",
    ]
    + [
        "zero_dose_vpo_0_11_mois_fois_site_ordinaire",
        "zero_dose_vpo_0_11_mois_fois_site_speciaux",
        "zero_dose_vpo_0_11_mois_fois_site_speciaux_postefron",
        "zero_dose_vpo_0_11_mois_fois_site_trans_front_cote_front",
        "zero_dose_vpo_0_11_mois_fois_site_trans_front_cote_niger",
        "zero_dose_vpo_12_59_mois_fois_site_ordinaire",
        "zero_dose_vpo_12_59_mois_fois_site_speciaux",
        "zero_dose_vpo_12_59_mois_fois_site_speciaux_postefron",
        "zero_dose_vpo_12_59_mois_fois_site_trans_front_cote_front",
        "zero_dose_vpo_12_59_mois_fois_site_trans_front_cote_niger",
    ]
    + [
        "deja_recu_vpo_0_11_mois_site_ordinaire",
        "deja_recu_vpo_0_11_mois_site_speciaux",
        "deja_recu_vpo_0_11_mois_site_speciaux_postefron",
        "deja_recu_vpo_0_11_mois_site_trans_front_cote_front",
        "deja_recu_vpo_0_11_mois_site_trans_front_cote_niger",
        "deja_recu_vpo_12_59_mois_site_ordinaire",
        "deja_recu_vpo_12_59_mois_site_speciaux",
        "deja_recu_vpo_12_59_mois_site_speciaux_postefron",
        "deja_recu_vpo_12_59_mois_site_trans_front_cote_front",
        "deja_recu_vpo_12_59_mois_site_trans_front_cote_niger",
    ]
    + [
        "depara_12_23_site_ordinaire",
        "depara_24_59_site_ordinaire",
        "depara_24_59_site_speciaux",
        "depara_12_23_site_speciaux_gares",
        "depara_24_59_site_speciaux_gares",
        "depara_12_23_site_speciaux_marche",
        "depara_24_59_site_speciaux_marche",
        "depara_12_23_site_speciaux_point_eau",
        "depara_24_59_site_speciaux_point_eau",
        "depara_12_23_site_speciaux_nomad",
        "depara_24_59_site_speciaux_nomad",
        "depara_12_23_site_speciaux_deplace_int",
        "depara_24_59_site_speciaux_deplace_int",
        "depara_12_23_site_speciaux_refugie",
        "depara_24_59_site_speciaux_refugie",
        "depara_12_23_site_speciaux_autre",
        "depara_24_59_site_speciaux_autre",
        "depara_12_23_site_trans_front_cote_niger",
        "depara_24_59_site_trans_front_cote_niger",
        "depara_12_23_site_trans_front_cote_front",
        "depara_24_59_site_trans_front_cote_front",
        "depara_12_23_site_speciaux_postefron",
        "depara_24_59_site_speciaux_postefron",
    ]
)

cvrg_fjaune_cols = (
    [
        "nbre_enfant_vaccine_9_11_mois__fievre_jaune",
        "nbre_enfant_vaccine_12_23_mois_fievre_jaune",
        "nbre_enfant_vaccine_24_59_mois_fievre_jaune",
        "nbre_enfant_vaccine_5_14_ans_fievre_jaune",
        "nbre_enfant_vaccine_15_60_ans_fievre_jaune",
    ]
    + [
        "nbre_enfant_vaccine_9_11_mois__fievre_jaune_site_depla_int",
        "nbre_enfant_vaccine_24_59_mois__fievre_jaune_site_depla_int",
        "nbre_enfant_vaccine_5_14_ans__fievre_jaune_site_depla_int",
        "nbre_enfant_vaccine_15_60_ans__fievre_jaune_site_depla_int",
    ]
    + [
        "nbre_enfant_vaccine_9_11_mois__fievre_jaune_site_speciaux_refugie",
        "nbre_enfant_vaccine_24_59_mois__fievre_jaune_site_speciaux_refugie",
        "nbre_enfant_vaccine_5_14_ans__fievre_jaune_site_speciaux_refugie",
        "nbre_enfant_vaccine_15_60_ans__fievre_jaune_site_speciaux_refugie",
    ]
)

cvrg_rougeole_cols = (
    [
        "nombre_zero_dose_6_fixe",
        "nombre_vacine_6_fixe",
        "nombre_dose_9_fixe",
        "nombre_vacine_9_fixe",
        "nombre_dose_12_fixe",
        "nombre_vacine_12_fixe",
    ]
    # + [  # Lionel: why do we add these columns for rougeole coverage (they are specific to Polio)? Check with Fernando
    #     "zero_dose_vpo_0_11_mois_fois_site_ordinaire",
    #     "zero_dose_vpo_0_11_mois_fois_site_speciaux",
    #     "zero_dose_vpo_0_11_mois_fois_site_speciaux_postefron",
    #     "zero_dose_vpo_0_11_mois_fois_site_trans_front_cote_front",
    #     "zero_dose_vpo_0_11_mois_fois_site_trans_front_cote_niger",
    #     "zero_dose_vpo_12_59_mois_fois_site_ordinaire",
    #     "zero_dose_vpo_12_59_mois_fois_site_speciaux",
    #     "zero_dose_vpo_12_59_mois_fois_site_speciaux_postefron",
    #     "zero_dose_vpo_12_59_mois_fois_site_trans_front_cote_front",
    #     "zero_dose_vpo_12_59_mois_fois_site_trans_front_cote_niger",
    # ]
    + [
        "nombre_zero_dose_6_avance",
        "nombre_vacine_6_avance",
        "nombre_dose_9_avance",
        "nombre_vacine_9_avance",
        "nombre_dose_12_avance",
        "nombre_vacine_12_avance",
    ]
    + [
        "nombre_zero_dose_6_mobile",
        "nombre_vacine_6_mobile",
        "nombre_dose_9_mobile",
        "nombre_vacine_9_mobile",
        "nombre_dose_12_mobile",
        "nombre_vacine_12_mobile",
    ]
)

cvrg_meningite_cols = [
    "men5_men5_1_4_ans",
    "men5_men5_5_14_ans",
    "men5_men5_15_19_ans",
]

cvrg_tcv_cols = [
    "men5_tcv_1_4_ans",
    "men5_tcv_5_14_ans",
    "men5_tcv_15_19_ans",
]

# completeness table
product_campaign_mapping = {
    "vaccin polio": "polio",
    "vitamine A": "polio",
    "albendazole": "polio",
    "rougeole": "rougeole",
    "fièvre jaune": "fièvre jaune",
    "méningite": "méningite",
    "tcv": "tcv",
}

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

stocks_campaign_map = {
    "polio": stock_polio_cols,
    "fièvre jaune": stock_fjaune_cols,
    "rougeole": stock_rougeole_cols,
    "méningite": stock_men5_cols,
    "tcv": stock_tcv_cols,
}

# surveillance table
surveillance_polio_cols = [
    "nbre_cas_pfa_notifie",
    "nbre_cas_mapi_notifie_mapi",
    "nbre_cas_mapi_majeur_notifie_mapi",
]
surveillance_rougeole_cols = [
    "nombre_MAPI_non_grave",
    "nombre_MAPI_graves",
]
surveillance_fjaune_cols = [
    "nbre_cas_pfa_notifie_fievre_jaune",
    "nbre_cas_fievre_jaune_notifie_fievre_jaune",
    "nbre_cas_mapi_notifie_mapi_fievre_jaune",
    "nbre_cas_mapi_majeur_notifie_mapi_fievre_jaune",
]
surveillance_men5_cols = [
    "men5_total_de_cas_de_pfa_signales",
    "men5_mapi_mineurs",
    "men5_mapi_graves",
]


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
    "men5_nombre_total_de_cas_pfa_signales",
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

# all columns
cols_campaign_map = {
    "polio": cvrg_polio_cols
    + stock_polio_cols
    + surveillance_polio_cols
    + communication_deployment_polio
    + communication_denombrement_polio
    + communication_reach_polio
    + communication_causeries_polio
    + communication_cas_polio
    + communication_activities_polio,
    "fièvre jaune": cvrg_fjaune_cols
    + stock_fjaune_cols
    + surveillance_fjaune_cols
    + communication_deployment_fjaune
    + communication_denombrement_fjaune
    + communication_reach_fjaune
    + communication_causeries_fjaune
    + communication_cas_fjaune
    + communication_activities_fjaune,
    "rougeole": cvrg_rougeole_cols
    + stock_rougeole_cols
    + surveillance_rougeole_cols
    + communication_deployment_rougeole
    + communication_denombrement_rougeole
    + communication_reach_rougeole
    + communication_causeries_rougeole
    + communication_cas_rougeole
    + communication_activities_rougeole,
    "méningite": cvrg_meningite_cols
    + stock_men5_cols
    + surveillance_men5_cols
    + communication_deployment_men5_cols
    + communication_denombrement_men5_cols
    + communication_reach_men5_cols
    + communication_causeries_men5_cols
    + communication_cas_men5_cols
    + communication_activities_men5_cols,
    "tcv": cvrg_tcv_cols + stock_tcv_cols,
}
