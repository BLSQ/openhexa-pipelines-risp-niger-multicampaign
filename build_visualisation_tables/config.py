import os
from openhexa.sdk import workspace

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
WORKSPACE_PATH = os.path.join(os.getcwd(), "build_visualisation_tables", "workspace")
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
