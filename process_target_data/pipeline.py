"""
Compiles combined_target_data.parquet and expected_data_structure.parquet from every per-run
file extract_target_data has produced so far, under "historical targets processed" and
"expected data structure processed".

Recompiling both multi-million-row combined datasets from scratch is the expensive part of this
flow, so it's kept separate from extract_target_data (the manual, per-file step) and runs
automatically instead. This pipeline has no parameters - it always recompiles from whatever
per-run files currently exist.

Runs automatically as the first step of orchestrate_pipelines_flow's chain (before
extract_org_units), since build_visualisation_tables/process_iaso_form_data downstream both
expect combined_target_data / expected_data_structure to already reflect every extraction done
so far. extract_target_data remains the only manual step in this whole flow.
"""

from openhexa.sdk import current_run, pipeline

from config import EXPECTED_STRUCTURE_PROCESSED_PATH, PROCESSED_TARGETS_PATH
from run_persistence import compile_processed_files


@pipeline(
    "process_target_data",
    name="multi-campagne - Compilation des cibles et de la structure attendue",
)
def process_target_data():
    """Compile every per-run target / expected-structure file extract_target_data
    has produced into the two combined datasets downstream pipelines read."""
    combined_targets = compile_processed_files(
        PROCESSED_TARGETS_PATH, "combined_target_data", "de cibles traité(s)"
    )
    combined_expected = compile_processed_files(
        EXPECTED_STRUCTURE_PROCESSED_PATH,
        "expected_data_structure",
        "de structure attendue traité(s)",
    )
    current_run.log_info(
        f"combined_target_data compilé: {len(combined_targets)} ligne(s)."
    )
    current_run.log_info(
        f"expected_data_structure compilé: {len(combined_expected)} ligne(s)."
    )


if __name__ == "__main__":
    process_target_data()
