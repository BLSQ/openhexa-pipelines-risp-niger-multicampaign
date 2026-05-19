from openhexa.sdk import workspace
import os

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(
#     os.getcwd(), "combine_expected_data_structures", "workspace"
# )  # local
CONFIG_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "config")
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
