"""
Workspace paths only. Structure detection and campaign semantics are handled by
target_import.py / layouts.py. District name reconciliation is handled by
geo_match.py (fuzzy). CSI name reconciliation (org_unit_matching, the manual
csi_matching_failed corrections) lives in utils.py. Campaign-period config
(HISTORICAL_CAMPAIGNS_CONFIG) lives in expected_structure.py, alongside the functions
that consume it.

No EXPECTED_STRUCTURE_PROCESSED_PATH here: this pipeline saves no per-run expected-data-structure
file - process_target_data builds expected_data_structure whole from combined_target_data.
"""

import os
from openhexa.sdk import workspace

# Paths
WORKSPACE_PATH = workspace.files_path
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
PROCESSED_TARGETS_PATH = os.path.join(OUTPUTS_PATH, "historical targets processed")
TARGETS_INPUT_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles")
TEMP_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "temp")
