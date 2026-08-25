"""
Build a small local fixture set for interactive debugging - a real slice of
data (one campaign/year/round), not synthetic, but small enough that VS Code's
Variables pane / Data Viewer can inspect any intermediate DataFrame instantly.

Run once from build_visualisation_tables/:
    python make_debug_fixtures.py

Then point OUTPUTS_PATH at the fixtures folder it creates (see the launch.json
config below) instead of the real workspace/multi-campagne/outputs/.
"""

import os
import pandas as pd

PROJECT_FOLDER = "multi-campagne"
WORKSPACE_PATH = os.path.join(os.getcwd(), "build_visualisation_tables", "workspace")

REAL_OUTPUTS = f"{WORKSPACE_PATH}/{PROJECT_FOLDER}/outputs"
FIXTURES = f"{WORKSPACE_PATH}/{PROJECT_FOLDER}/outputs_debug_fixtures"

# Pick one real, small-ish slice - adjust to whatever you're actually debugging.
YEAR = 2026
PRODUIT = "albendazole"
ROUND = "round 1"

os.makedirs(FIXTURES, exist_ok=True)

FILTER = [("year", "==", YEAR), ("produit", "==", PRODUIT), ("round", "==", ROUND)]

# filters= pushes the predicate down into the parquet read itself (row-group
# pruning + per-row filtering before materialization) - this is NOT the same
# as read_parquet(...) followed by [...] filtering, which loads the entire
# ~30GB file into memory first and OOM-kills the process before the filter
# ever runs.
expected = pd.read_parquet(
    f"{REAL_OUTPUTS}/expected_data_structure.parquet", filters=FILTER
)
expected.to_parquet(f"{FIXTURES}/expected_data_structure.parquet", index=False)
print(f"expected_data_structure: {len(expected):,} rows")

target = pd.read_parquet(f"{REAL_OUTPUTS}/combined_target_data.parquet", filters=FILTER)
target.to_parquet(f"{FIXTURES}/combined_target_data.parquet", index=False)
print(f"combined_target_data: {len(target):,} rows")

iaso = pd.read_parquet(
    f"{REAL_OUTPUTS}/combined_iaso_data.parquet", filters=[("year", "==", YEAR)]
)
iaso.to_parquet(f"{FIXTURES}/combined_iaso_data.parquet", index=False)
print(f"combined_iaso_data: {len(iaso):,} rows")

# Org-unit trees aren't campaign-scoped - just copy them as-is (already small).
for name in ["iaso_org_unit_tree_clean", "iaso_org_unit_tree_raw"]:
    df = pd.read_parquet(f"{REAL_OUTPUTS}/{name}.parquet")
    df.to_parquet(f"{FIXTURES}/{name}.parquet", index=False)
    print(f"{name}: {len(df):,} rows (unfiltered)")

print(f"\nFixtures written to {FIXTURES}/")
