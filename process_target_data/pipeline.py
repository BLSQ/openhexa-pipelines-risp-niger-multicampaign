"""Template for newly generated pipelines."""

import os
from openhexa.sdk import current_run, workspace, pipeline
import pandas as pd
import numpy as np
from utils import (
    IASOConnectionHandler,
    pyramid_selector,
)
from config import (
    iaso_connector_slug,
    iaso_form_id,
    dict_districts_cibles_iaso,
)


@pipeline("process_target_data")
def process_target_data():
    """ """
    spatial_df = get_spatial_data()
    spatial_df_clean = clean_spatial_data(spatial_df)

    targets_polio_2024_r4 = import_target_data_for_polio_2024_r4()


def get_spatial_data() -> pd.DataFrame:
    """
    Retrieve organizational unit tree data from IASO based on a specific form ID.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the organizational unit tree data.
    """
    current_run.log_info("Retrieving spatial data...")

    # iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
    # spatial_df = iaso_connector_instance.get_ou_tree_dataframe_from_the_form(
    #     iaso_form_id
    # )

    spatial_df = pd.read_parquet(
        os.path.join(
            workspace.files_path,
            "temp",
            "spatial_units_raw.parquet",
        )
    )

    return spatial_df


def clean_spatial_data(spatial_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the spatial data by filtering out rejected entries and selecting relevant records.

    Args:
        spatial_df (pd.DataFrame): DataFrame containing the spatial data to be cleaned.

    Returns:
        pd.DataFrame: Cleaned DataFrame with relevant spatial data.
    """
    current_run.log_info("Cleaning spatial data...")

    spatial_df_clean = spatial_df[spatial_df.Validé != "REJECTED"]
    spatial_df_clean = spatial_df_clean[spatial_df_clean.Source == "SNIS"]

    spatial_df_clean = spatial_df_clean.groupby("LVL_6_UID", as_index=False).apply(
        pyramid_selector
    )

    return spatial_df_clean


def import_target_data_for_polio_2024_r4():
    """
    Import target data for Polio 2024 R4 campaign.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the target data for Polio 2024 R4
    """
    current_run.log_info("Importing target data for Polio 2024 R4...")

    file_path = os.path.join(
        workspace.files_path, "cibles", "Population JNV JNM ET DEPRARASITAGE.xlsx"
    )
    cibles_2024_r4 = pd.read_excel(file_path, skiprows=4, header=[0, 1]).iloc[
        :, [0, 1, 2, 3, 4]
    ]

    cibles_2024_r4.columns = [
        "LVL_2_NAME",
        "LVL_3_NAME",
        "VPO_0-11 mois",
        "VPO_12-59 mois",
        "VPO_0-59 mois",
    ]
    cibles_2024_r4 = cibles_2024_r4[
        ["LVL_3_NAME", "VPO_0-11 mois", "VPO_12-59 mois", "VPO_0-59 mois"]
    ]

    cibles_2024_r4 = cibles_2024_r4[
        ~cibles_2024_r4["LVL_3_NAME"].str.contains("Région").fillna(True)
    ]
    cibles_2024_r4 = cibles_2024_r4[
        ~cibles_2024_r4["LVL_3_NAME"].str.contains("TOTAL").fillna(True)
    ]
    cibles_2024_r4 = cibles_2024_r4[
        ~cibles_2024_r4["LVL_3_NAME"].str.contains("Total").fillna(True)
    ]

    cibles_2024_r4["LVL_3_NAME"] = cibles_2024_r4["LVL_3_NAME"].map(
        dict_districts_cibles_iaso
    )
    cibles_2024_r4 = pd.melt(
        cibles_2024_r4, id_vars="LVL_3_NAME", var_name="full_name", value_name="cible"
    )

    cibles_2024_r4["produit"] = np.where(
        cibles_2024_r4.full_name.str.contains("VPO"),
        "vaccin polio",
        np.where(
            cibles_2024_r4.full_name.str.contains("VA"), "vitamine A", "albendazole"
        ),
    )
    cibles_2024_r4["age"] = cibles_2024_r4["full_name"].str.split("_", expand=True)[1]
    cibles_2024_r4["round"] = "round 4"
    cibles_2024_r4["year"] = 2024

    cibles_2024_r4 = cibles_2024_r4.drop("full_name", axis=1)

    return cibles_2024_r4


if __name__ == "__main__":
    process_target_data()
