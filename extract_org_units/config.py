from openhexa.sdk import workspace
import os
from pathlib import Path
from dotenv import load_dotenv

# # set up env (for local testing only)
# dotenv_path = Path(__file__).parent / ".env"
# if dotenv_path.exists():
#     load_dotenv(dotenv_path)
# os.environ["HEXA_WORKSPACE"] = os.getenv("HEXA_WORKSPACE")
# os.environ["HEXA_SERVER_URL"] = os.getenv("HEXA_SERVER_URL")
# os.environ["HEXA_TOKEN"] = os.getenv("HEXA_TOKEN")

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
# WORKSPACE_PATH = os.path.join(
#     os.getcwd(), "extract_org_units", "workspace"
# )  # local only
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
