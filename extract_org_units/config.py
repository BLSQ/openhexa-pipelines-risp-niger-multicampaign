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
WORKSPACE_PATH = os.path.join(
    os.getcwd(), "extract_org_units", "workspace"
)  # local only
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
