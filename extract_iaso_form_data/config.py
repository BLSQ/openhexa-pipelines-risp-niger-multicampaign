from openhexa.sdk import workspace
import os

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(
#     os.getcwd(), "extract_process_iaso_form_data", "workspace"
# )  # local only
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
IASO_EXTRACTION_PATH = os.path.join(OUTPUTS_PATH, "iaso_données_extraites")

# IASO Connector Instances
connection = workspace.get_connection("iaso-pev-niger")
iaso_connector_slug = {
    "url": connection.url,
    "username": connection.username,
    "password": connection.password,
}

iaso_form_id = 1186
