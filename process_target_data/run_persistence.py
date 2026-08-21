"""
Compiling the per-run files extract_target_data has produced into one combined dataset.

Duplicate detection, superseded-slice removal, and the run-slug naming scheme belong to
extract_target_data (see that pipeline's own run_persistence.py) - this pipeline never saves a
per-run file itself, so it has no use for them.
"""

import glob
import os

import pandas as pd
from openhexa.sdk import current_run

from config import OUTPUTS_PATH
from shared_utils import export_to_dataset, save_file


def compile_processed_files(folder: str, output_name: str, description: str) -> pd.DataFrame:
    """
    Build `output_name` by concatenating EVERY processed file in `folder` -
    historical and new campaigns alike go through extract_target_data, so both
    land in the same folder. Exact duplicate rows (a slice covered by more than
    one run configuration) are collapsed; distinct runs are all preserved.

    One shared function for both combined datasets (combined_target_data,
    expected_data_structure) - they only differ in which folder/name they
    compile, not in the compile-from-scratch mechanic itself.
    """
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"Aucun fichier {description} trouvé dans {folder}.")
    current_run.log_info(
        f"Compilation de '{output_name}' à partir de {len(files)} fichier(s) "
        f"{description}..."
    )
    frames = [pd.read_parquet(f) for f in files]
    combined = (
        pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
    )

    save_file(combined, output_name)
    export_to_dataset(combined, OUTPUTS_PATH, output_name)
    return combined
