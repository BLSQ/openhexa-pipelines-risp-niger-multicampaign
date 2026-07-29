"""
Paths and matching-stage configuration.

Structure detection and campaign semantics are handled by target_import.py /
layouts.py. District name reconciliation is handled by geo_match.py (fuzzy).
This file keeps only the workspace paths and the known CSI fuzzy-match
corrections.
"""

import os

from openhexa.sdk import workspace

# --------------------------------------------------------------------------- #
# Paths                                                                        #
# --------------------------------------------------------------------------- #
WORKSPACE_PATH = workspace.files_path
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
# Each per-run processed target file is saved here; the combined historical
# dataset is rebuilt by concatenating every file in this folder.
PROCESSED_TARGETS_PATH = os.path.join(OUTPUTS_PATH, "historical targets processed")
TARGETS_HISTORICAL_PATH = os.path.join(
    WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles", "historique"
)
TEMP_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "temp")

# --------------------------------------------------------------------------- #
# CSI fuzzy-match corrections (used by the CSI matcher).                        #
# --------------------------------------------------------------------------- #
csi_matching_failed = {
    "agadez sabon gari agadez": "agadez sabongari",
    "boboye birni i": "boboye birni ngaoure",
    "boboye birni ii": "boboye birni 2",
    "dosso bella1": "dosso bella i",
    "dosso bellaii": "dosso bella ii",
    "guidan roumdji g roumdji": "guidan roumdji guidan roumdji 1",
    "kollo lakabia": "kollo latakabia sonrai",
    "madarounfa harounawa": "madarounfa harounaoua",
    "madarounfa madarounfa": "madarounfa madarounfa 1",
    "maradi sabongari": "maradi sabongari maradi",
    "maradi zariai": "maradi zaria i",
    "maradi zariaii": "maradi zaria ii",
    "maradi zariaiii": "maradi zaria iii",
    "matameye danbarto": "matameye dan barto",
    "matameye matameye 1": "matameye matameye",
    "zinder sabongarizinder": "zinder sabon gari",
    "zinder sabongari zinder": "zinder sabon gari",
}
