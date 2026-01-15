import pandas as pd

# paths
outputs_path = "niger_june_24/outputs/"

campaign_dates_config = {
    # 2024
    "2024r1_vaccin__polio": {
        "min": pd.to_datetime("2024-07-10"),
        "max": pd.to_datetime("2024-07-24"),
    },
    "2024r1_vitamine__A": {
        "min": pd.to_datetime("2024-07-10"),
        "max": pd.to_datetime("2024-07-24"),
    },
    "2024r1_albendazole": {
        "min": pd.to_datetime("2024-07-10"),
        "max": pd.to_datetime("2024-07-24"),
    },
    "2024r2_vaccin__polio": {
        "min": pd.to_datetime("2024-09-28"),
        "max": pd.to_datetime("2024-10-06"),
    },
    "2024r2_vitamine__A": {
        "min": pd.to_datetime("2024-09-28"),
        "max": pd.to_datetime("2024-10-06"),
    },
    "2024r2_albendazole": {
        "min": pd.to_datetime("2024-09-28"),
        "max": pd.to_datetime("2024-10-06"),
    },
    "2024r3_vaccin__polio": {
        "min": pd.to_datetime("2024-10-25"),
        "max": pd.to_datetime("2024-11-01"),
    },
    "2024r3_vitamine__A": {
        "min": pd.to_datetime("2024-10-25"),
        "max": pd.to_datetime("2024-11-01"),
    },
    "2024r3_albendazole": {
        "min": pd.to_datetime("2024-10-25"),
        "max": pd.to_datetime("2024-11-01"),
    },
    "2024r4_vaccin__polio": {
        "min": pd.to_datetime("2024-12-01"),
        "max": pd.to_datetime("2024-12-12"),
    },
    "2024r4_vitamine__A": {
        "min": pd.to_datetime("2024-12-01"),
        "max": pd.to_datetime("2024-12-12"),
    },
    "2024r4_albendazole": {
        "min": pd.to_datetime("2024-12-01"),
        "max": pd.to_datetime("2024-12-12"),
    },
    # 2025
    "2025r1_vaccin__polio": {
        "min": pd.to_datetime("2025-05-04"),
        "max": pd.to_datetime("2025-05-08"),
    },
    "2025r1_vitamine__A": {
        "min": pd.to_datetime("2025-05-04"),
        "max": pd.to_datetime("2025-05-08"),
    },
    "2025r1_albendazole": {
        "min": pd.to_datetime("2025-05-04"),
        "max": pd.to_datetime("2025-05-08"),
    },
    "2025r1_rougeole": {
        "min": pd.to_datetime("2025-04-18"),
        "max": pd.to_datetime("2025-04-24"),
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
    "2025r2_vaccin__polio": {
        "min": pd.to_datetime("2025-06-14"),
        "max": pd.to_datetime("2025-06-21"),
    },
    "2025r2_vitamine__A": {
        "min": pd.to_datetime("2025-06-14"),
        "max": pd.to_datetime("2025-06-21"),
    },
    "2025r2_albendazole": {
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
    # 2026
    "2026r1_vaccin__polio": {
        "min": pd.to_datetime("2026-01-11"),
        "max": pd.to_datetime("2026-01-15"),
    },
    "2026r1_vitamine__A": {
        "min": pd.to_datetime("2026-01-11"),
        "max": pd.to_datetime("2026-01-15"),
    },
    "2026r1_albendazole": {
        "min": pd.to_datetime("2026-01-11"),
        "max": pd.to_datetime("2026-01-15"),
    },
    "2026r1_fièvre__jaune": {
        "min": pd.to_datetime("2026-01-20"),
        "max": pd.to_datetime("2026-01-26"),
    },
}

product_site_config = {
    "vaccin polio": {
        "ordinaire",
        "spécial",
        "frontalier",
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "vitamine A": {
        "ordinaire",
        "spécial",
        "frontalier",
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "albendazole": {
        "ordinaire",
        "spécial",
        "frontalier",
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "rougeole": {"fixe", "avancé", "mobile"},
    "fièvre jaune": {"ordinaire"},
    "méningite": {"ordinaire"},
    "tcv": {"ordinaire"},
}

sex_types_config = ["TOUS"]

product_status_config = {
    "vaccin polio": {
        "déjà reçu",
        "zéro dose",
    },
    "vitamine A": {
        "zéro dose",
    },
    "albendazole": {
        "zéro dose",
    },
    "rougeole": {
        "zéro dose",
    },
    "fièvre jaune": {
        "zéro dose",
    },
    "méningite": {
        "zéro dose",
    },
    "tcv": {
        "zéro dose",
    },
}
