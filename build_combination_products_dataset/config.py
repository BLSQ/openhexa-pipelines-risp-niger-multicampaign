from openhexa.sdk import workspace
import os

# paths
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(
#     os.getcwd(), "build_combination_products_dataset", "workspace"
# )  # local only
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, "niger_june_24", "outputs")
CONFIG_PATH = os.path.join(WORKSPACE_PATH, "niger_june_24", "config")

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

campaign_names_config = [
    "albendazole",
    "fièvre__jaune",
    "méningite",
    "rougeole",
    "tcv",
    "vaccin__polio",
    "vitamine__A",
]
