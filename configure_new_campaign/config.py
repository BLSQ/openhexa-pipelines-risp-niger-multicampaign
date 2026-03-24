from openhexa.sdk import workspace
import os

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(os.getcwd(), "configure_new_campaign", "workspace") # local
CONFIG_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "config")
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")

# configs
campaign_name_dict = {
    "Polio": "vaccin polio",
    "Vitamine A": "vitamine A",
    "Albendazole": "albendazole",
    "Rougeole": "rougeole",
    "Fièvre jaune": "fièvre jaune",
    "Méningite": "méningite",
    "TCV": "tcv",
}

required_regions = [
    "Agadez",
    "Diffa",
    "Dosso",
    "Maradi",
    "Niamey",
    "Tahoua",
    "Tillaberi",
    "Zinder",
]

campaign_config_dict = {
    "Polio": {
        "age": ["0-11 mois", "12-59 mois"],
        "site": [
            "ordinaire",
            "spécial",
            "frontalier",
            "transfrontalier : étranger",
            "transfrontalier : Niger",
        ],
        "vaccination_status": [
            "déjà reçu",
            "zéro dose",
        ],
        "sexe": ["TOUS"],
    },
    "Vitamine A": {
        "age": ["6-11 mois", "12-24 mois"],
        "site": [
            "ordinaire",
            "spécial",
            "transfrontalier : étranger",
            "transfrontalier : Niger",
        ],
        "vaccination_status": [
            "zéro dose",
        ],
        "sexe": ["TOUS"],
    },
    "Albendazole": {
        "age": ["12-23 mois", "24-59 mois"],
        "site": [
            "ordinaire",
            "spécial",
            "transfrontalier : étranger",
            "transfrontalier : Niger",
        ],
        "vaccination_status": [
            "zéro dose",
        ],
        "sexe": ["TOUS"],
    },
    "Rougeole": {
        "age": ["6-9 mois", "9-11 mois", "12-59 mois"],
        "site": ["fixe", "avancé", "mobile"],
        "vaccination_status": [
            "zéro dose",
        ],
        "sexe": ["TOUS"],
    },
    "Fièvre jaune": {
        "age": ["9-11 mois", "12-23 mois", "24-59 mois", "5-14 ans", "15-60 ans"],
        "site": ["ordinaire", "spécial"],
        "vaccination_status": [
            "zéro dose",
        ],
        "sexe": ["TOUS"],
    },
    "Méningite": {
        "age": ["1-4 ans", "5-14 ans", "15-19 ans"],
        "site": ["ordinaire"],
        "vaccination_status": [
            "zéro dose",
        ],
        "sexe": ["TOUS"],
    },
    "TCV": {
        "age": ["1-4 ans", "5-14 ans", "15-19 ans"],
        "site": ["ordinaire"],
        "vaccination_status": [
            "zéro dose",
        ],
        "sexe": ["TOUS"],
    },
}
