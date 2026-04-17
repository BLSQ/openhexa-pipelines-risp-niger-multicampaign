from openhexa.sdk import workspace
import os

# paths
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(os.getcwd(), "process_target_data", "workspace")  # local
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
TARGET_OTHER_DATA_PATH = os.path.join(
    WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles", "autres"
)

# configs
valid_campaigns = [
    "Polio",
    "Rougeole",
    "Méningite",
    "TCV",
    "Fièvre jaune",
    "Albendazole",
    "Vitamine A",
]
valid_scales = [
    "nationale",
    "agadez",
    "diffa",
    "dosso",
    "maradi",
    "niamey",
    "tahoua",
    "tillaberi",
    "zinder",
]
valid_levels = ["csi", "district"]

campaign_rename_dict = {
    "Polio": "vaccin polio",
    "Rougeole": "rougeole",
    "Fièvre jaune": "fièvre jaune",
    "Méningite": "méningite",
    "TCV": "tcv",
    "Albendazole": "albendazole",
    "Vitamine A": "vitamine A",
}

templates_required_cols_district = [
    "Pays",
    "Région",
    "District Sanitaire",
]

templates_required_cols_csi = [
    *templates_required_cols_district,
    "Commune",
    "CSI",
]

target_columns_required_dict = {
    "Polio": ["Cible 0-11 mois", "Cible 12-59 mois"],
    "Vitamine A": ["Cible 6-11 mois", "Cible 12-24 mois"],
    "Albendazole": ["Cible 12-23 mois", "Cible 24-59 mois"],
    "Rougeole": ["Cible 6-9 mois", "Cible 9-11 mois", "Cible 12-59 mois"],
    "Fièvre jaune": [
        "Cible 9-11 mois",
        "Cible 12-23 mois",
        "Cible 24-59 mois",
        "Cible 5-14 ans",
        "Cible 15-60 ans",
    ],
    "Méningite": ["Cible 1-4 ans", "Cible 5-14 ans", "Cible 15-19 ans"],
    "TCV": ["Cible 1-4 ans", "Cible 5-14 ans", "Cible 15-19 ans"],
}

site_strategy_types_dict = {
    "Ordinaire": "ordinaire",
    "Spécial": "spécial",
    "Frontalier": "frontalier",
    "Transfrontalier : étranger": "transfrontalier : étranger",
    "Transfrontalier : Niger": "transfrontalier : Niger",
    "Fixe": "fixe",
    "Avancée": "avancé",
    "Mobile": "mobile",
}

cols_for_melting = ["Région", "District Sanitaire", "year", "produit"]

csi_district_rename_dict = {
    "Région": "LVL_2_NAME",
    "District Sanitaire": "LVL_3_NAME",
    "CSI": "LVL_6_NAME",
}
