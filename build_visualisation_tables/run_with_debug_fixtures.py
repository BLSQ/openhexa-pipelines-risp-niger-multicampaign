"""
Launch build_visualisation_tables against the small local fixture set (see
make_debug_fixtures.py) instead of the full ~50M-row real data - meant to be
run directly under the debugger (VS Code "launch", not "attach"), so
breakpoints inside pipeline.py/utils.py work normally and every intermediate
DataFrame is small enough for the Variables pane / Data Viewer to inspect
instantly.

Not for validating real output - use the real workspace/multi-campagne/outputs
for that (non-interactively, no debugger attached).
"""

import os
import sys

sys.path.insert(0, ".")

PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = os.path.join(os.getcwd(), "build_visualisation_tables", "workspace")


import shared_utils

shared_utils.OUTPUTS_PATH = f"{WORKSPACE_PATH}/{PROJECT_FOLDER}/outputs_debug_fixtures"

import pipeline

pipeline.OUTPUTS_PATH = shared_utils.OUTPUTS_PATH
pipeline.export_to_dataset = (
    lambda *a, **k: None
)  # no real OH dataset creds needed locally

pipeline.build_visualisation_tables.function()
