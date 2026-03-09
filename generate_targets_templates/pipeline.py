from openhexa.sdk import current_run, parameter, pipeline
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
)


@pipeline(
    "generate_targets_templates",
    name="multi-campagne - 01. Création des fichiers templates pour les cibles",
)
@parameter(
    "campaign",
    name="Campagne",
    help="Sélectionnez le type de campagne",
    type=str,
    required=True,
    choices=["Polio", "Rougeole", "Méningite", "TCV", "Fièvre jaune"],
    default="Polio",
)
@parameter(
    "year",
    name="Année",
    help="Veuillez entrer l'année de la campagne (2026, 2027, etc.)",
    type=int,
    required=True,
    default=2026,
)
@parameter(
    "round",
    name="Round",
    help="Veuillez entrer le round de la campagne (1, 2, etc.)",
    type=int,
    required=True,
    default=2,
)
@parameter(
    "aggregation_level",
    name="Niveau d'agrégation",
    help="Niveau d'agrégation pour les cibles",
    type=str,
    required=True,
    choices=["CSI", "District"],
    default="CSI",
)
@parameter(
    "age_range",
    name="Tranches d'âges",
    help="Veuillez entrer les tranches d'âges applicables aux cibles",
    type=str,
    multiple=True,
    required=True,
    choices=[
        "0-11 mois",
        "6-11 mois",
        "9-11 mois",
        "12-23 mois",
        "12-24 mois",
        "12-59 mois",
        "24-59 mois",
        "1-4 ans",
        "5-14 ans",
        "15-19 ans",
        "15-60 ans",
    ],
    default=["0-11 mois", "12-59 mois"],
)
@parameter(
    "site_type",
    name="Type de site",
    help="Veuillez entrer le(s) type(s) de site applicable aux cibles",
    type=str,
    multiple=True,
    required=False,
    choices=[
        "Ordinaire",
        "Spécial",
        "Frontalier",
        "Transfrontalier : étranger",
        "Transfrontalier : Niger",
    ],
    default=["Ordinaire"],
)
@parameter(
    "strategy_type",
    name="Type de stratégie",
    help="Veuillez entrer le(s) type(s) de stratégie applicable aux cibles",
    type=str,
    multiple=True,
    required=False,
    choices=["Fixe", "Avancée", "Mobile"],
)
def generate_targets_templates(
    campaign: str,
    year: int,
    round: int,
    aggregation_level: str,
    age_range: list,
    site_type: list = None,
    strategy_type: list = None,
):
    """
    This pipeline generates target templates for each campaign type based on the organizational unit tree
    data from IASO. It retrieves the org unit tree data, cleans it, creates template files for each campaign
    type, and saves the cleaned org unit tree data for future use.
    """
    iaso_org_unit_tree_df = get_iaso_org_unit_tree()
    iaso_org_unit_tree_df_clean = clean_iaso_org_unit_tree(iaso_org_unit_tree_df)
    create_template_files(
        iaso_org_unit_tree_df_clean,
        campaign,
        year,
        round,
        aggregation_level,
        age_range,
        site_type,
        strategy_type,
    )
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

    # iaso_connector_instance = IASOConnectionHandler(iaso_connector_slug)
    # iaso_org_unit_tree_df = iaso_connector_instance.get_ou_tree_dataframe_from_the_form(
    #     iaso_form_id
    # )

    # # save file to parquet for later use
    # if not os.path.exists(OUTPUTS_PATH):
    #     os.makedirs(OUTPUTS_PATH)

    # iaso_org_unit_tree_df.to_parquet(
    #     os.path.join(OUTPUTS_PATH, "iaso_org_unit_tree_raw.parquet"),
    #     index=False,
    # )
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


def create_template_files(
    org_unit_df: pd.DataFrame,
    campaign: str,
    year: int,
    round: int,
    aggregation_level: str,
    age_range: list,
    site_type: list = None,
    strategy_type: list = None,
) -> pd.DataFrame:
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
        cols = ["LVL_2_NAME", "LVL_3_NAME", "LVL_4_NAME", "LVL_6_NAME"]
        df = org_unit_df[cols].copy()
        df.insert(0, "Pays", "Niger")
        df = df.rename(columns=rename_dict)

        if aggregation_level == "District":
            if "CSI" in df.columns:
                df = df.drop(columns=["CSI"])
            df = df.drop_duplicates()

        df = df.sort_values(by=list(df.columns))

        suffixes = (site_type or []) + (strategy_type or [])
        if not suffixes:
            suffixes = [""]

        new_cols_list = []
        for age in age_range:
            for suffix in suffixes:
                col_name = f"Cible {age}_{suffix}".strip("_ ")
                if col_name not in df.columns and col_name not in new_cols_list:
                    new_cols_list.append(col_name)
        new_columns_df = pd.DataFrame(
            {col: "" for col in new_cols_list}, index=df.index
        )
        df = pd.concat([df, new_columns_df], axis=1)

        os.makedirs(TEMPLATES_PATH, exist_ok=True)
        filename = f"Cibles_{campaign}_{year}_round {round}_{aggregation_level.lower()}_template.csv"
        file_path = os.path.join(TEMPLATES_PATH, filename)

        df.to_csv(file_path, index=False, encoding="utf-8-sig")

        current_run.log_info(f"Fichier template généré avec succès: {file_path}")
        return df

    except Exception as e:
        current_run.log_error(f"Erreur lors de la création du fichier template: {e}")
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
