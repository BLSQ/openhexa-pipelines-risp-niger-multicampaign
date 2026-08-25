"""
Workspace paths only. This pipeline only reads the per-run files extract_target_data has
already produced and compiles them - it has no structure-detection, org-unit-matching, or
expected-structure logic of its own, so it needs none of the config those carry.
"""

import os
from openhexa.sdk import workspace

# Paths
WORKSPACE_PATH = workspace.files_path
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
PROCESSED_TARGETS_PATH = os.path.join(OUTPUTS_PATH, "historical targets processed")
EXPECTED_STRUCTURE_PROCESSED_PATH = os.path.join(
    OUTPUTS_PATH, "expected data structure processed"
)
