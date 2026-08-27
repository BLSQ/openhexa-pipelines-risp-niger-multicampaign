"""
Updates combined_target_data.parquet from every per-run target file extract_target_data has
produced so far (under "historical targets processed"), then builds expected_data_structure.parquet
whole from combined_target_data (see docs/ARCHITECTURE.md for the full design).

This pipeline is kept separate from extract_target_data (the manual, per-file step) and runs
automatically instead, since deciding what's new/changed is its own concern, distinct from
importing one file. No parameters - it always updates from whatever per-run files currently
exist.

Runs automatically as the first step of orchestrate_pipelines_flow's chain (before
extract_org_units), since build_visualisation_tables/process_iaso_form_data downstream both
expect combined_target_data / expected_data_structure to already reflect every extraction done
so far. extract_target_data remains the only manual step in this whole flow.
"""

import os

from openhexa.sdk import current_run, pipeline

from config import PROCESSED_TARGETS_PATH
from expected_structure import generate_and_save_expected_data_structure
from run_persistence import compile_combined_target_data, output_path


@pipeline(
    name="multi-campagne - Compilation des cibles et de la structure attendue",
)
def process_target_data():
    """Compile every per-run target file into combined_target_data, then build
    expected_data_structure from it whole - skipped entirely when combined_target_data
    didn't change and expected_data_structure already exists."""
    target_row_count, changed = compile_combined_target_data(
        PROCESSED_TARGETS_PATH, "combined_target_data", "de cibles traité(s)"
    )
    current_run.log_info(f"combined_target_data compilé: {target_row_count} ligne(s).")

    if not changed and os.path.exists(output_path("expected_data_structure")):
        current_run.log_info(
            "expected_data_structure inchangé: combined_target_data n'a pas changé."
        )
        return

    expected_row_count = generate_and_save_expected_data_structure()
    current_run.log_info(
        f"expected_data_structure généré: {expected_row_count} ligne(s)."
    )


if __name__ == "__main__":
    process_target_data()
