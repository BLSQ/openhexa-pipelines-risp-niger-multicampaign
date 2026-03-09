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
