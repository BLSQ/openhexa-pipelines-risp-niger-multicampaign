from openhexa.sdk import current_run, pipeline
import os
import pandas as pd
import numpy as np
from utils import (
    IASOConnectionHandler,
    pyramid_selector,
)
from config import (
    iaso_connector_slug,
    iaso_form_id,
    OUTPUTS_PATH,
    TEMPLATES_PATH,
    rename_dict,
    campaigns_config_dict,
)


@pipeline(
    "generate_targets_templates",
    name="01. Création des fichiers templates pour les cibles",
)
def generate_targets_templates():
    """
    This pipeline generates target templates for each campaign type based on the organizational unit tree
    data from IASO. It retrieves the org unit tree data, cleans it, creates template files for each campaign
    type, and saves the cleaned org unit tree data for future use.
    """
    iaso_org_unit_tree_df = get_iaso_org_unit_tree()
    iaso_org_unit_tree_df_clean = clean_iaso_org_unit_tree(iaso_org_unit_tree_df)
    create_template_files(iaso_org_unit_tree_df_clean)
    save_org_unit_tree(iaso_org_unit_tree_df_clean)


def get_iaso_org_unit_tree() -> pd.DataFrame:
    """
    Retrieve organizational unit tree data from IASO based on a specific form ID.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame containing the organizational unit tree data.
    """
    current_run.log_info(
        "Extraction des données de l'arbre des unités organisationnelles IASO..."
    )

    iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
    iaso_org_unit_tree_df = iaso_connector_instance.get_ou_tree_dataframe_from_the_form(
        iaso_form_id
    )

    # save file to parquet for later use
    if not os.path.exists(OUTPUTS_PATH):
        os.makedirs(OUTPUTS_PATH)

    iaso_org_unit_tree_df.to_parquet(
        os.path.join(OUTPUTS_PATH, "iaso_org_unit_tree_raw.parquet"),
        index=False,
    )
    iaso_org_unit_tree_df = pd.read_parquet(
        os.path.join(OUTPUTS_PATH, "iaso_org_unit_tree_raw.parquet")
    )
    return iaso_org_unit_tree_df


def clean_iaso_org_unit_tree(iaso_org_unit_tree_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the org unit tree data by filtering out rejected entries and selecting relevant records.

    Args:
        iaso_org_unit_tree_df (pd.DataFrame): DataFrame containing the org unit tree data to be cleaned.

    Returns:
        pd.DataFrame: Cleaned DataFrame with relevant org unit tree data.
    """
    current_run.log_info(
        "Nettoyage des données de l'arbre des unités organisationnelles IASO..."
    )

    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df[
        iaso_org_unit_tree_df["Validé"] != "REJECTED"
    ]
    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
        iaso_org_unit_tree_df_clean["Source"].isin(["SNIS", "SNIS 2025"])
    ]
    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
        iaso_org_unit_tree_df_clean["LVL_6_NAME"].str.contains(
            "CSI", case=False, na=False
        )
    ]

    iaso_org_unit_tree_df_clean["LVL_6_UID"] = iaso_org_unit_tree_df_clean.groupby(
        "LVL_6_NAME"
    )["LVL_6_UID"].transform("first")
    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean.groupby(
        "LVL_6_UID", as_index=False
    ).apply(pyramid_selector, include_groups=False)

    iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
        iaso_org_unit_tree_df_clean["LVL_2_NAME"] != "Niger"
    ]  # delete 2 incoherent entries

    iaso_org_unit_tree_df_clean["org_unit_id"] = iaso_org_unit_tree_df_clean[
        "org_unit_id"
    ].astype(np.int64)

    return iaso_org_unit_tree_df_clean


def create_template_files(org_unit_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create template files for each organizational unit based on the cleaned org unit tree data.

    Args:
        org_unit_df (pd.DataFrame): DataFrame containing the cleaned org unit tree data.

    Returns:
        pd.DataFrame: DataFrame containing the template files for each organizational unit.
    """
    current_run.log_info(
        "Création des fichiers templates pour chaque unité organisationnelle..."
    )
    try:
        org_unit_df_restricted = org_unit_df[
            ["LVL_2_NAME", "LVL_3_NAME", "LVL_4_NAME", "LVL_6_NAME"]
        ].copy()
        org_unit_df_restricted["Pays"] = "Niger"
        org_unit_df_restricted = org_unit_df_restricted[
            ["Pays", "LVL_2_NAME", "LVL_3_NAME", "LVL_4_NAME", "LVL_6_NAME"]
        ]
        org_unit_df_restricted = org_unit_df_restricted.rename(columns=rename_dict)
        org_unit_df_restricted = org_unit_df_restricted.sort_values(
            by=["Pays", "Région", "District Sanitaire", "Commune", "CSI"]
        )

        if not os.path.exists(TEMPLATES_PATH):
            os.makedirs(TEMPLATES_PATH)

        for campaign_type, age_groups in campaigns_config_dict.items():
            df = org_unit_df_restricted.copy()
            for age_group in age_groups:
                df[f"Cible {age_group}"] = ""
            df.to_csv(
                os.path.join(TEMPLATES_PATH, f"Cibles_{campaign_type}_template.csv"),
                index=False,
                encoding="utf-8-sig",
            )
        current_run.log_info(
            f"Fichiers templates créés avec succès dans le dossier {TEMPLATES_PATH}"
        )
    except Exception as e:
        current_run.log_error(f"Erreur lors de la création des fichiers templates: {e}")
        raise


def save_org_unit_tree(iaso_org_unit_tree_df_clean: pd.DataFrame):
    """
    Save the cleaned org unit tree data to a parquet file.

    Args:
        iaso_org_unit_tree_df_clean (pd.DataFrame): DataFrame containing the cleaned org unit tree data.

    Returns:
        None
    """
    current_run.log_info("Enregistrement des données cibles combinées...")
    try:
        if not os.path.exists(OUTPUTS_PATH):
            os.makedirs(OUTPUTS_PATH)
        file_path = os.path.join(
            OUTPUTS_PATH,
            "iaso_org_unit_tree_clean.parquet",
        )

        iaso_org_unit_tree_df_clean.to_parquet(
            file_path,
            index=False,
        )
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'enregistrement du fichier: {e}")
        raise e


if __name__ == "__main__":
    generate_targets_templates()
