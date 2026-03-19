from openhexa.sdk import workspace
import os

# configs
org_unit_DB_name = "org_units_pyramid"

# paths
PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = workspace.files_path
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
TEMPLATES_PATH = os.path.join(
    WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles", "autres", "templates"
)

# cols
rename_dict = {
    "level_4_name": "Région",
    "level_3_name": "District Sanitaire",
    "level_2_name": "Commune",
    "name": "CSI",
}
