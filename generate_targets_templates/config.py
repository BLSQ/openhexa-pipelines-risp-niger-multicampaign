from openhexa.sdk import workspace
import os

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(
#     os.getcwd(), "generate_targets_templates", "workspace"
# )  # local
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
TEMPLATES_PATH = os.path.join(
    WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles", "autres", "templates"
)

# configs
rename_dict = {
    "LVL_2_NAME": "Région",
    "LVL_3_NAME": "District Sanitaire",
    "LVL_4_NAME": "Commune",
    "LVL_6_NAME": "CSI",
}

age_group_campaign_dict = {
    "Polio": ["0-11 mois", "12-59 mois"],
    "Vitamine A": ["6-11 mois", "12-24 mois"],
    "Albendazole": ["12-23 mois", "24-59 mois"],
    "Rougeole": ["6-9 mois", "9-11 mois", "12-59 mois"],
    "Fièvre jaune": ["9-11 mois", "12-23 mois", "24-59 mois", "5-14 ans", "15-60 ans"],
    "Méningite": ["1-4 ans", "5-14 ans", "15-19 ans"],
    "TCV": ["1-4 ans", "5-14 ans", "15-19 ans"],
}

campaign_names_mapping = {
    "Polio": "vaccin polio",
    "Vitamine A": "vitamine A",
    "Albendazole": "albendazole",
    "Rougeole": "rougeole",
    "Fièvre jaune": "fièvre jaune",
    "Méningite": "méningite",
    "TCV": "tcv",
}

regions_list = [
    "Agadez",
    "Diffa",
    "Dosso",
    "Maradi",
    "Niamey",
    "Tahoua",
    "Tillaberi",
    "Zinder",
]
