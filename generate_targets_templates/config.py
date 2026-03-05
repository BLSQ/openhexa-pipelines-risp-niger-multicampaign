from openhexa.sdk import workspace
import os

# configs
iaso_connector_slug = {
    "url": "https://iaso.bluesquare.org",
    "username": "fernando_diniger",
    "password": "hbe8quh1hjm*cyx6AQH",
}

iaso_form_id = 1186

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(
#     os.getcwd(), "generate_targets_templates", "workspace"
# )  # local only
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
TEMPLATES_PATH = os.path.join(
    WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles", "autres", "templates"
)

# cols
rename_dict = {
    "LVL_2_NAME": "Région",
    "LVL_3_NAME": "District Sanitaire",
    "LVL_4_NAME": "Commune",
    "LVL_6_NAME": "CSI",
}

campaigns_config_dict = {
    "albendazole": ["12-23 mois", "24-59 mois"],
    "vitamine A": ["6-11 mois", "12-24 mois"],
    "vaccin polio": ["0-11 mois", "12-59 mois"],
    "rougeole": ["6-11 mois", "12-59 mois"],
    "fièvre jaune": ["9-11 mois", "12-23 mois", "24-59 mois", "5-14 ans", "5-60 ans"],
    "méningite": ["1-4 ans", "5-14 ans", "15-19 ans"],
    "tcv": ["1-4 ans", "5-14 ans", "15-19 ans"],
}
