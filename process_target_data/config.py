"""
Workspace paths only. This pipeline reads the per-run target files extract_target_data has
already produced and compiles them into combined_target_data, then builds expected_data_structure
whole from combined_target_data itself - so it needs no per-run expected-structure folder path at
all, and none of extract_target_data's structure-detection or org-unit-matching config.
"""

import os
from openhexa.sdk import workspace

# Paths
WORKSPACE_PATH = workspace.files_path
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
PROCESSED_TARGETS_PATH = os.path.join(OUTPUTS_PATH, "historical targets processed")
