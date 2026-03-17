from openhexa.sdk import workspace
import os

# configs
connection = workspace.get_connection("iaso-pev-niger")
iaso_connector_slug = {
    "url": connection.url,
    "username": connection.username,
    "password": connection.password,
}

iaso_form_id = 1186

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
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
