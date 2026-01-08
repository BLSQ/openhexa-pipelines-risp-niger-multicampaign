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

cvrg_campaign_map = {
    "polio": cvrg_polio_cols,
    "fièvre jaune": cvrg_fjaune_cols,
    "rougeole": cvrg_rougeole_cols,
    "méningite": cvrg_meningite_cols,
    "tcv": cvrg_tcv_cols,
}

district_level_target_keys = [
    "year",
    "round",
    "age",
    "produit",
    "LVL_3_NAME",
]

district_level_group_keys = district_level_target_keys + [
    "period",
    "sexe",
    "vaccination_status",
    "site",
]

district_level_final_keys = district_level_group_keys + [
    "org_unit_id",
    "value",
    "cible",
]

district_level_cumsum_keys = list(
    (set(district_level_group_keys) | {"org_unit_id"}) - {"period"}
)

district_level_config = {
    2024: {
        "vaccin polio": ["round 1", "round 2", "round 3", "round 4"],
        "albendazole": ["round 1", "round 2", "round 3", "round 4"],
        "vitamine A": ["round 1", "round 2", "round 3", "round 4"],
    },
    2025: {
        "vaccin polio": ["round 1", "round 2"],
        "albendazole": ["round 1", "round 2"],
        "vitamine A": ["round 1", "round 2"],
        "rougeole": ["round 1", "round 2"],
    },
}

csi_level_target_keys = list(
    (set(district_level_target_keys) | {"org_unit_id"}) - {"LVL_3_NAME"}
)

csi_level_final_keys = district_level_final_keys + ["LVL_6_NAME"]

csi_level_cumsum_keys = district_level_cumsum_keys + ["LVL_6_NAME"]

csi_level_config = {
    2025: {
        "fièvre jaune": ["round 1", "round 2"],
        "méningite": ["round 1", "round 2"],
        "tcv": ["round 1", "round 2"],
        "vaccin polio": ["round 3"],
    },
}

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

stock_ratios_config = {
    "vaccin polio": 50,
    "vitamine A": 1,
    "albendazole": 1,
    "rougeole": 10,
    "fièvre jaune": 1,
    "méningite": 1,
    "tcv": 1,
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

surveillance_campaign_map = {
    "polio": surveillance_polio_cols,
    "rougeole": surveillance_rougeole_cols,
    "fièvre jaune": surveillance_fjaune_cols,
    "méningite": surveillance_men5_cols,
}

# communication table
communication_deployment = [
    "nbre_relais",
    "nbre_concession_visite",
    "nbre_concession_non_favorable_vaccination",
]

communication_deployment_men5_cols = [
    "men5_nombre_total_de_relais_mobilises",
    "men5_nombre_total_de_concessions_visitees",
    "men5_nombre_de_concessions_non_favorables_a_la_vaccination",
]

communication_reach = [
    "nbre_personne_touche_par_relais",
    "nbre_personne_touche_par_refugie_deplace_mi",
    "nbre_personne_touche_nomande_trans_puits",
    "nbre_personne_touche_zone_frontaliere",
    "nbre_enfant_0_5_ans_demonbre",
]

communication_reach_men5_cols = [
    "men5_nombre_de_personnes_touchees_par_le_relais",
    "men5_nombre_d_enfants_de_0_5_ans_dans_les_concessions_visitees",
    "men5_nombre_de_personnes_touchees_au_niveau_des_camps_des_refugies_deplaces_migrants",
    "men5_nombre_de_personnes_touchees_au_niveau_des_camps_des_nomades_transhumants_puits",
    "men5_nombre_de_personnes_touchees_au_niveau_des_villages_sites_frontaliers",
]
communication_causeries = [
    "nbre_causerie",
    "nbre_causeri_marche",
    "nbre_causerie_refugie_deplace_migrant",
    "nbre_causerie_nomade_trans_puit",
    "nbre_causerie_fontalier",
]

communication_causeries_men5_cols = [
    "men5_nombre_de_causeries_au_niveau_des_sites_ordinaires",
    "men5_nombre_de_causeries_au_niveau_des_marches",
    "men5_nombre_de_causeries_au_niveau_des_camps_des_refugies_deplaces_migrants",
    "men5_nombre_de_causeries_au_niveau_des_camps_des_nomades_transhumants_puits",
    "men5_nombre_de_causeries_au_niveau_des_villages_frontaliers",
]
communication_cas = [
    "nbre_total_cas_pfa_signale",
    "nbre_cas_rumeur_notifier",
]

communication_cas_men5_cols = [
    "men5_nombre_total_de_cas_pfa_signales",  # to check with Issa/Fernando if these are the right columns
    "men5_nombre_de_cas_de_rumeur_notifies_par_le_comite_de_veille",
]
communication_activities = [
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

communication_activities_men5_cols = [
    "men5_nombre_de_reunions_de_plaidoyer_tenues",  # to check with Issa/Fernando if these are the right columns
    "men5_nombre_de_leaders_engages",
    "men5_nombre_de_radios_impliques",
    "men5_nombre_de_spots_diffuses",
    "men5_nombre_d_appels_de_leaders",
    "men5_nombre_de_debats_organises",
    "men5_nombre_d_interviews_realises",
    "men5_nombre_d_emissions_publiques_diffusees",
    "men5_nombre_estime_de_la_population_exposee_aux_messages",
    "men5_nombre_de_dialogues_communautaires_tenus",
]


communication_denombrement_enfants_fjaune = [
    "nbre_enfant_9_11_mois_demonbre_fievre_jaune",
    "nbre_enfant_12_23_mois_demonbre_fievre_jaune",
    "nbre_enfant_24_59_mois_demonbre_fievre_jaune",
]
communication_denombrement_adultes_fjaune = [
    "nbre_personnes_5_14_ans_demonbre_fievre_jaune",
    "nbre_personnes_15_60_ans_demonbre_fievre_jaune",
]

communication_cols = (
    communication_deployment
    + communication_deployment_men5_cols
    + communication_reach
    + communication_reach_men5_cols
    + communication_causeries
    + communication_causeries_men5_cols
    + communication_cas
    + communication_cas_men5_cols
    + communication_activities
    + communication_activities_men5_cols
    + communication_denombrement_enfants_fjaune
    + communication_denombrement_adultes_fjaune
)


# all columns
cols_campaign_map = {
    "polio": cvrg_polio_cols
    + stock_polio_cols
    + surveillance_polio_cols
    + communication_deployment
    + communication_reach
    + communication_causeries
    + communication_cas
    + communication_activities,
    "fièvre jaune": cvrg_fjaune_cols
    + stock_fjaune_cols
    + surveillance_fjaune_cols
    + communication_denombrement_enfants_fjaune
    + communication_denombrement_adultes_fjaune,
    "rougeole": cvrg_rougeole_cols
    + stock_rougeole_cols
    + surveillance_rougeole_cols
    + communication_deployment
    + communication_reach
    + communication_causeries
    + communication_cas
    + communication_activities,
    "méningite": cvrg_meningite_cols
    + stock_men5_cols
    + surveillance_men5_cols
    + communication_deployment_men5_cols
    + communication_reach_men5_cols
    + communication_causeries_men5_cols
    + communication_cas_men5_cols
    + communication_activities_men5_cols,
    "tcv": cvrg_tcv_cols + stock_tcv_cols,
}
