from openhexa.sdk import workspace
import os

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(
#     os.getcwd(), "build_combination_products_dataset", "workspace"
# )
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
CONFIG_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "config")

# configs
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
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "albendazole": {
        "ordinaire",
        "spécial",
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

campaign_names_config = [
    "albendazole",
    "fièvre__jaune",
    "méningite",
    "rougeole",
    "tcv",
    "vaccin__polio",
    "vitamine__A",
]

historical_campaigns_config = {
    "2024r1_vaccin__polio": {"début": "2024-07-10", "fin": "2024-07-24"},
    "2024r1_vitamine__A": {"début": "2024-07-10", "fin": "2024-07-24"},
    "2024r1_albendazole": {"début": "2024-07-10", "fin": "2024-07-24"},
    "2024r2_vaccin__polio": {"début": "2024-09-28", "fin": "2024-10-06"},
    "2024r2_vitamine__A": {"début": "2024-09-28", "fin": "2024-10-06"},
    "2024r2_albendazole": {"début": "2024-09-28", "fin": "2024-10-06"},
    "2024r3_vaccin__polio": {"début": "2024-10-25", "fin": "2024-11-01"},
    "2024r3_vitamine__A": {"début": "2024-10-25", "fin": "2024-11-01"},
    "2024r3_albendazole": {"début": "2024-10-25", "fin": "2024-11-01"},
    "2024r4_vaccin__polio": {"début": "2024-12-01", "fin": "2024-12-12"},
    "2024r4_vitamine__A": {"début": "2024-12-01", "fin": "2024-12-12"},
    "2024r4_albendazole": {"début": "2024-12-01", "fin": "2024-12-12"},
    "2025r1_vaccin__polio": {"début": "2025-05-04", "fin": "2025-05-08"},
    "2025r1_vitamine__A": {"début": "2025-05-04", "fin": "2025-05-08"},
    "2025r1_albendazole": {"début": "2025-05-04", "fin": "2025-05-08"},
    "2025r1_rougeole": {"début": "2025-04-18", "fin": "2025-04-24"},
    "2025r1_fièvre__jaune": {"début": "2025-10-27", "fin": "2025-11-04"},
    "2025r1_méningite": {"début": "2025-11-24", "fin": "2025-12-02"},
    "2025r1_tcv": {"début": "2025-11-24", "fin": "2025-12-02"},
    "2025r2_vaccin__polio": {"début": "2025-06-14", "fin": "2025-06-21"},
    "2025r2_vitamine__A": {"début": "2025-06-14", "fin": "2025-06-21"},
    "2025r2_albendazole": {"début": "2025-06-14", "fin": "2025-06-21"},
    "2025r2_méningite": {"début": "2025-12-15", "fin": "2025-12-22"},
    "2025r2_tcv": {"début": "2025-12-15", "fin": "2025-12-22"},
    "2026r1_vaccin__polio": {"début": "2026-01-11", "fin": "2026-01-15"},
    "2026r1_vitamine__A": {"début": "2026-01-11", "fin": "2026-01-15"},
    "2026r1_albendazole": {"début": "2026-01-11", "fin": "2026-01-15"},
    "2026r1_fièvre__jaune": {"début": "2026-01-20", "fin": "2026-01-26"},
}
