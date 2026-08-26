"""
Updates combined_target_data.parquet and expected_data_structure.parquet from every per-run
file extract_target_data has produced so far, under "historical targets processed" and
"expected data structure processed".

Incremental, not a from-scratch recompile every time (see run_persistence.compile_processed_files
for the actual mechanics): only per-run files containing a combination not yet reflected in the
combined output, or that changed since the combined output was last written, are read and merged
in; a run with nothing new to add skips the read/write entirely. This pipeline is kept separate
from extract_target_data (the manual, per-file step) and runs automatically instead, since
deciding what's new/changed against the combined output is its own concern, distinct from
importing one file. No parameters - it always updates from whatever per-run files currently
exist.

Runs automatically as the first step of orchestrate_pipelines_flow's chain (before
extract_org_units), since build_visualisation_tables/process_iaso_form_data downstream both
expect combined_target_data / expected_data_structure to already reflect every extraction done
so far. extract_target_data remains the only manual step in this whole flow.
"""

from openhexa.sdk import current_run, pipeline

from config import EXPECTED_STRUCTURE_PROCESSED_PATH, PROCESSED_TARGETS_PATH
from run_persistence import compile_processed_files

EXPECTED_STRUCTURE_CATEGORY_COLS = [
    "round",
    "age",
    "sexe",
    "produit",
    "vaccination_status",
    "site",
    "choix_campagne",
]


@pipeline(
    name="multi-campagne - Compilation des cibles et de la structure attendue",
)
def process_target_data():
    """Compile every per-run target / expected-structure file extract_target_data
    has produced into the two combined datasets downstream pipelines read."""
    target_row_count = compile_processed_files(
        PROCESSED_TARGETS_PATH, "combined_target_data", "de cibles traité(s)"
    )
    expected_row_count = compile_processed_files(
        EXPECTED_STRUCTURE_PROCESSED_PATH,
        "expected_data_structure",
        "de structure attendue traité(s)",
        category_columns=EXPECTED_STRUCTURE_CATEGORY_COLS,
    )
    current_run.log_info(f"combined_target_data compilé: {target_row_count} ligne(s).")
    current_run.log_info(
        f"expected_data_structure compilé: {expected_row_count} ligne(s)."
    )


if __name__ == "__main__":
    process_target_data()
