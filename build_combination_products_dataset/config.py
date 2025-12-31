import pandas as pd

# Definition of date ranges for all campaign rounds
campaign_dates = {
    # 2024
    "2024r1_polio": {
        "min": pd.to_datetime("2024-07-10"),
        "max": pd.to_datetime("2024-07-24"),
    },
    "2024r2_polio": {
        "min": pd.to_datetime("2024-09-28"),
        "max": pd.to_datetime("2024-10-06"),
    },
    "2024r3_polio": {
        "min": pd.to_datetime("2024-10-25"),
        "max": pd.to_datetime("2024-11-01"),
    },
    "2024r4_polio": {
        "min": pd.to_datetime("2024-12-01"),
        "max": pd.to_datetime("2024-12-12"),
    },
    # 2025
    "2025r1_rougeole": {
        "min": pd.to_datetime("2025-04-18"),
        "max": pd.to_datetime("2025-04-24"),
    },
    "2025r1_polio": {
        "min": pd.to_datetime("2025-05-04"),
        "max": pd.to_datetime("2025-05-08"),
    },
    "2025r1_fièvre__jaune": {
        "min": pd.to_datetime("2025-10-27"),
        "max": pd.to_datetime("2025-11-04"),
    },
    "2025r1_méningite": {
        "min": pd.to_datetime("2025-11-24"),
        "max": pd.to_datetime("2025-12-02"),
    },
    "2025r1_tcv": {
        "min": pd.to_datetime("2025-11-24"),
        "max": pd.to_datetime("2025-12-02"),
    },
    "2025r2_fièvre__jaune": {
        "min": pd.to_datetime("2025-12-10"),
        "max": pd.to_datetime("2025-12-17"),
    },
    "2025r2_vita": {
        "min": pd.to_datetime("2025-06-14"),
        "max": pd.to_datetime("2025-06-21"),
    },
    "2025r2_polio": {
        "min": pd.to_datetime("2025-06-14"),
        "max": pd.to_datetime("2025-06-21"),
    },
    "2025r2_méningite": {
        "min": pd.to_datetime("2025-12-15"),
        "max": pd.to_datetime("2025-12-22"),
    },
    "2025r2_tcv": {
        "min": pd.to_datetime("2025-12-15"),
        "max": pd.to_datetime("2025-12-22"),
    },
    "2025r3_polio": {
        "min": pd.to_datetime("2025-12-05"),
        "max": pd.to_datetime("2025-12-08"),
    },
}

sites_types = [
    "ordinaire",
    "special",
    "frontalier",
    "transfrontalier_etranger",
    "transfrontarlier_Niger",
]


strategie_types = ["fixe", "avance", "mobile"]

campaign_configs = {
    "vpo": {
        "choix_campagne": ["polio"],
        "ages": ["0-11 mois", "12-59 mois"],
        "produit": ["vaccin polio"],
        "status": ["déjà reçu", "zéro dose"],
    },
    "vita": {
        "choix_campagne": ["polio"],
        "ages": ["6-11 mois", "12-59 mois"],
        "produit": ["vitamine A"],
        "status": ["zéro dose"],
    },
    "albendazole": {
        "choix_campagne": ["polio"],
        "ages": ["12-23 mois", "24-59 mois"],
        "produit": ["albendazole"],
        "status": ["zéro dose"],
    },
    "rougeole": {
        "choix_campagne": ["rougeole"],
        "ages": ["0-11 mois", "12-59 mois"],
        "produit": ["rougeole"],
        "status": ["zéro dose"],
    },
    "fjaune": {
        "choix_campagne": ["fièvre jaune"],
        "ages": ["9-11 mois", "12-23 mois", "24-59 mois", "5-14 ans", "15-60 ans"],
        "produit": ["fièvre jaune"],
        "status": ["zéro dose"],
    },
    "meningite": {
        "choix_campagne": ["méningite"],
        "ages": ["1-4 ans", "5-14 ans", "15-19 ans"],
        "produit": ["méningite"],
        "status": ["zéro dose"],
    },
    "tcv": {
        "choix_campagne": ["tcv"],
        "ages": ["1-4 ans", "5-14 ans", "15-19 ans"],
        "produit": ["tcv"],
        "status": ["zéro dose"],
    },
}
