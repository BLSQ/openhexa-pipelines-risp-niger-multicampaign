import pandas as pd

# paths
outputs_path = "niger_june_24/outputs/"
config_path = "niger_june_24/config/"

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
