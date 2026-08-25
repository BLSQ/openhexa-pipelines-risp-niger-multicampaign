from openhexa.sdk import pipeline
from shared_utils import (
    load_data,
    save_file,
    export_to_dataset,
)

from config import OUTPUTS_PATH
from campaign_cleaning import clean_combined_df
from org_unit_matching import align_to_clean_org_tree


@pipeline(
    "process_iaso_form_data",
    name="multi-campagne - Traitement des données du formulaire IASO",
)
def process_iaso_form_data():
    """
    This pipeline processes the raw combined IASO form data by retrieving org unit IDs,
    cleaning the data, and saving the cleaned DataFrame for future use.
    It performs the following steps:
        1. Imports the necessary data files (raw combined IASO data, clean and raw org unit trees, expected data structure).
        2. Retrieves org unit IDs associated with CSI names and updates the combined DataFrame.
        3. Cleans the combined DataFrame by formatting periods, exploding multi-campaign entries, checking for valid campaign names, removing duplicates, and filtering out entries with invalid periods.
        4. Saves the cleaned combined DataFrame as a parquet file in the outputs directory.
    """
    # data imports
    iaso_org_unit_tree_clean = load_data("iaso_org_unit_tree_clean")
    iaso_org_unit_tree_raw = load_data("iaso_org_unit_tree_raw")
    expected_data_structure = load_data("expected_data_structure")
    iaso_raw_df = load_data("combined_iaso_data_raw")

    # data processing
    iaso_processed_df = align_to_clean_org_tree(
        iaso_raw_df, iaso_org_unit_tree_raw, iaso_org_unit_tree_clean
    )
    iaso_processed_df = clean_combined_df(iaso_processed_df, expected_data_structure)

    # save output
    save_file(iaso_processed_df, "combined_iaso_data")
    export_to_dataset(iaso_processed_df, OUTPUTS_PATH, "combined_iaso_data")


if __name__ == "__main__":
    process_iaso_form_data()
