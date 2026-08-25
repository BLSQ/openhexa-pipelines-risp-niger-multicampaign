"""
Workspace paths only. Structure detection and campaign semantics are handled by
target_import.py / layouts.py. District name reconciliation is handled by
geo_match.py (fuzzy). CSI name reconciliation (org_unit_matching, the manual
csi_matching_failed corrections) lives in utils.py. Expected-data-structure config
(SEX_TYPE/PRODUCT_STATUS/SITE_TYPE/HISTORICAL_CAMPAIGNS_CONFIG) lives in
expected_structure.py, alongside the functions that consume it.
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
TARGETS_INPUT_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles")
TEMP_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "temp")
