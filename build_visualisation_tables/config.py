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
