from openhexa.sdk import workspace
import os

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
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

historical_campaigns_config = {
    (2024, 1, "vaccin polio"): {"début": "2024-07-10", "fin": "2024-07-24"},
    (2024, 1, "vitamine A"): {"début": "2024-07-10", "fin": "2024-07-24"},
    (2024, 1, "albendazole"): {"début": "2024-07-10", "fin": "2024-07-24"},
    (2024, 2, "vaccin polio"): {"début": "2024-09-28", "fin": "2024-10-06"},
    (2024, 2, "vitamine A"): {"début": "2024-09-28", "fin": "2024-10-06"},
    (2024, 2, "albendazole"): {"début": "2024-09-28", "fin": "2024-10-06"},
    (2024, 3, "vaccin polio"): {"début": "2024-10-25", "fin": "2024-11-01"},
    (2024, 3, "vitamine A"): {"début": "2024-10-25", "fin": "2024-11-01"},
    (2024, 3, "albendazole"): {"début": "2024-10-25", "fin": "2024-11-01"},
    (2024, 4, "vaccin polio"): {"début": "2024-12-01", "fin": "2024-12-12"},
    (2024, 4, "vitamine A"): {"début": "2024-12-01", "fin": "2024-12-12"},
    (2024, 4, "albendazole"): {"début": "2024-12-01", "fin": "2024-12-12"},
    (2025, 1, "vaccin polio"): {"début": "2025-05-04", "fin": "2025-05-08"},
    (2025, 1, "vitamine A"): {"début": "2025-05-04", "fin": "2025-05-08"},
    (2025, 1, "albendazole"): {"début": "2025-05-04", "fin": "2025-05-08"},
    (2025, 1, "rougeole"): {"début": "2025-04-18", "fin": "2025-04-24"},
    (2025, 1, "fièvre jaune"): {"début": "2025-10-27", "fin": "2025-11-04"},
    (2025, 1, "méningite"): {"début": "2025-11-24", "fin": "2025-12-02"},
    (2025, 1, "tcv"): {"début": "2025-11-24", "fin": "2025-12-02"},
    (2025, 2, "vaccin polio"): {"début": "2025-06-14", "fin": "2025-06-21"},
    (2025, 2, "vitamine A"): {"début": "2025-06-14", "fin": "2025-06-21"},
    (2025, 2, "albendazole"): {"début": "2025-06-14", "fin": "2025-06-21"},
    (2025, 2, "méningite"): {"début": "2025-12-15", "fin": "2025-12-22"},
    (2025, 2, "tcv"): {"début": "2025-12-15", "fin": "2025-12-22"},
    (2026, 1, "vaccin polio"): {"début": "2026-01-11", "fin": "2026-01-15"},
    (2026, 1, "vitamine A"): {"début": "2026-01-11", "fin": "2026-01-15"},
    (2026, 1, "albendazole"): {"début": "2026-01-11", "fin": "2026-01-15"},
    (2026, 1, "fièvre jaune"): {"début": "2026-01-20", "fin": "2026-01-26"},
}
