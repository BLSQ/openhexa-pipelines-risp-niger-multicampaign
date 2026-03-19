from openhexa.sdk import current_run, parameter, pipeline, workspace
import os
import pandas as pd
import numpy as np
from itertools import combinations
import sqlalchemy as sa
from openpyxl.styles import PatternFill
from openpyxl.utils import get_column_letter
from utils import (
    pyramid_selector,
    parse_age_to_months,
)
from config import (
    OUTPUTS_PATH,
    TEMPLATES_PATH,
    rename_dict,
    org_unit_DB_name,
)


@pipeline(
    "generate_targets_templates",
    name="multi-campagne - 01 - Pipeline de création de fichier template pour les cibles",
)
@parameter(
    "campaign",
    name="Campagne",
    help="Sélectionnez le type de campagne",
    type=str,
    required=True,
    choices=[
        "Polio",
        "Rougeole",
        "Méningite",
        "TCV",
        "Fièvre jaune",
        "Albendazole",
        "Vitamine A",
    ],
)
@parameter(
    "campaign_scale",
    name="Échelle de la campagne",
    help="Sélectionnez l'échelle de la campagne",
    type=str,
    required=True,
    choices=[
        "Nationale",
        "Agadez",
        "Diffa",
        "Dosso",
        "Maradi",
        "Niamey",
        "Tahoua",
        "Tillaberi",
        "Zinder",
    ],
    multiple=True,
)
@parameter(
    "year",
    name="Année",
    help="Veuillez entrer l'année de la campagne (2026, 2027, etc.)",
    type=int,
    required=True,
    choices=[
        2026,
        2027,
        2028,
        2029,
        2030,
        2031,
        2032,
        2033,
        2034,
        2035,
        2036,
        2037,
        2038,
        2039,
        2040,
        2041,
        2042,
        2043,
        2044,
        2045,
        2046,
        2047,
        2048,
        2049,
        2050,
    ],
)
@parameter(
    "aggregation_level",
    name="Niveau d'agrégation",
    help="Niveau d'agrégation pour les cibles",
    type=str,
    required=True,
    choices=["CSI", "District"],
)
@parameter(
    "age_range",
    name="Tranches d'âges",
    help="Veuillez entrer les tranches d'âges applicables aux cibles",
    type=str,
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
    multiple=True,
)
@parameter(
    "site_type",
    name="Type de site",
    help="Veuillez entrer le(s) type(s) de site applicable aux cibles",
    type=str,
    required=False,
    choices=[
        "Ordinaire",
        "Spécial",
        "Frontalier",
        "Transfrontalier : étranger",
        "Transfrontalier : Niger",
    ],
    multiple=True,
)
@parameter(
    "strategy_type",
    name="Type de stratégie",
    help="Veuillez entrer le(s) type(s) de stratégie applicable aux cibles",
    type=str,
    required=False,
    choices=["Fixe", "Avancée", "Mobile"],
    multiple=True,
)
def generate_targets_templates(
    campaign: str,
    campaign_scale: list,
    year: int,
    aggregation_level: str,
    age_range: list,
    site_type: list = None,
    strategy_type: list = None,
):
    """
    This pipeline generates target templates for each campaign type based on the organizational unit tree
    data from IASO. It retrieves the org unit tree data, cleans it, creates template files for each campaign
    type, and saves the cleaned org unit tree data for future use
    """
    inspect_params(campaign_scale, year, age_range, site_type, strategy_type)
    iaso_org_unit_tree_df = load_from_db(org_unit_DB_name)
    iaso_org_unit_tree_df_clean = clean_iaso_org_unit_tree(iaso_org_unit_tree_df)
    create_template_files(
        iaso_org_unit_tree_df_clean,
        campaign,
        campaign_scale,
        year,
        aggregation_level,
        age_range,
        site_type,
        strategy_type,
    )
    save_file(iaso_org_unit_tree_df_clean)


def inspect_params(
    campaign_scale,
    year,
    age_range,
    site_type,
    strategy_type,
):
    """
    This function runs checks on the parmater choices for year, round, age, site, and strategy
    """
    current_run.log_info("Vérification des choix des paramètres...")

    # for campaign_scale, if 'Nationale' is present, it cannot coexist with any other choice
    if "Nationale" in campaign_scale and len(campaign_scale) > 1:
        current_run.log_error(
            "Le choix 'Nationale' pour le paramètre 'Échelle de la campagne' ne peut pas être sélectionné avec d'autres choix."
        )
        raise

    # year must be a reasonable integer between 2026 and 2050
    if not isinstance(year, int) or year < 2026 or year > 2050:
        current_run.log_error(
            "Le paramètre 'Année' doit être un entier entre 2026 et 2100."
        )
        raise

    # age groups cannot overlap
    ranges = {c: parse_age_to_months(c) for c in age_range}
    overlaps = []
    for (name1, range1), (name2, range2) in combinations(ranges.items(), 2):
        if range1[0] < range2[1] and range1[1] > range2[0]:
            overlaps.append((name1, name2))
    if overlaps:
        current_run.log_error("Chevauchement(s) de catégories d'âge détecté(s):")
        for a, b in overlaps:
            current_run.log_error(f" - '{a}' chevauche avec '{b}'")
        raise

    # site and strategy cannot coexist
    if site_type and strategy_type:
        current_run.log_error(
            "Les paramètres 'Type de site' et 'Type de stratégie' ne peuvent pas être utilisés simultanément."
        )
        raise

    current_run.log_info("Choix des paramètres vérifiés: il n'y a pas d'erreur.")


def load_from_db(table_name: str) -> pd.DataFrame:
    """Read a table from the workspace database and return it as a dataframe.

    Parameters
    ----------
    table_name : str
        The name of the table to read from.

    Returns
    -------
    pd.DataFrame
        The dataframe containing the table data.
    """
    current_run.log_info(
        f"Lecture des données de la pyramide des unités organisationnelles depuis la table DB {table_name}..."
    )
    try:
        engine = sa.create_engine(workspace.database_url)
        with engine.connect() as connection:
            df = pd.read_sql(sql=table_name, con=connection)

        current_run.log_info(
            f"Données de la pyramide des unités organisationnelles chargées avec succès depuis {table_name} ({len(df)} lignes)."
        )
        return df

    except Exception as e:
        current_run.log_error(
            f"Erreur lors de la lecture des données de la pyramide des unités organisationnelles depuis la table DB {table_name}: {e}"
        )
        raise


def clean_iaso_org_unit_tree(iaso_org_unit_tree_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the org unit tree data by filtering out rejected entries and selecting relevant records.

    Args:
        iaso_org_unit_tree_df (pd.DataFrame): DataFrame containing the org unit tree data to be cleaned.

    Returns:
        pd.DataFrame: Cleaned DataFrame with relevant org unit tree data.
    """
    current_run.log_info(
        "Nettoyage des données de la pyramide des unités organisationnelles..."
    )
    try:
        iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df[
            iaso_org_unit_tree_df["validation_status"] != "REJECTED"
        ]
        iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
            iaso_org_unit_tree_df_clean["source"].isin(["SNIS", "SNIS 2025"])
        ]
        iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean[
            iaso_org_unit_tree_df_clean["org_unit_type"] == "CSI"
        ]

        iaso_org_unit_tree_df_clean["id"] = iaso_org_unit_tree_df_clean.groupby("name")[
            "id"
        ].transform("first")
        iaso_org_unit_tree_df_clean = iaso_org_unit_tree_df_clean.groupby(
            "id", as_index=False
        ).apply(pyramid_selector, include_groups=False)
        iaso_org_unit_tree_df_clean["org_unit_id"] = iaso_org_unit_tree_df_clean[
            "id"
        ].astype(np.int64)

        return iaso_org_unit_tree_df_clean
    except Exception as e:
        current_run.log_error(
            f"Erreur lors du nettoyage des données de la pyramide des unités organisationnelles: {e}"
        )
        raise


def create_template_files(
    org_unit_df: pd.DataFrame,
    campaign: str,
    campaign_scale: list,
    year: int,
    aggregation_level: str,
    age_range: list,
    site_type: list = None,
    strategy_type: list = None,
) -> pd.DataFrame:
    """
    Create template files for each organizational unit based on the cleaned org unit tree data.

    Args:
        org_unit_df (pd.DataFrame): DataFrame containing the cleaned org unit tree data.
        campaign (str): The name of the campaign.
        campaign_scale (list): The scale of the campaign.
        year (int): The year of the campaign.
        aggregation_level (str): The level of aggregation for the campaign.
        age_range (list): The age range for the campaign.
        site_type (list, optional): The type of site for the campaign. Defaults to None.
        strategy_type (list, optional): The type of strategy for the campaign. Defaults to None.


    Returns:
        pd.DataFrame: DataFrame containing the template files for each organizational unit.
    """
    current_run.log_info(
        "Création des fichiers templates pour chaque unité organisationnelle..."
    )
    try:
        # select and rename cols
        cols = rename_dict.keys()
        df = org_unit_df[cols].copy()
        df.insert(0, "Pays", "Niger")
        df = df.rename(columns=rename_dict)

        if aggregation_level == "District":
            if "CSI" in df.columns:
                df = df.drop(columns=["CSI", "Commune"])
            df = df.drop_duplicates()

        df = df.sort_values(by=list(df.columns))

        # create new cols for targets based on age range, site type, and strategy type
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

        # filter by campaign scale if not national
        if "Nationale" not in campaign_scale:
            df = df[df["Région"].isin(campaign_scale)]

        # define file path and name
        os.makedirs(TEMPLATES_PATH, exist_ok=True)
        filename = f"Cibles_{campaign}_{year}_{'_'.join([s.lower() for s in campaign_scale])}_{aggregation_level.lower()}_template.xlsx"
        file_path = os.path.join(TEMPLATES_PATH, filename)

        # Use ExcelWriter to apply formatting
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Template")
            workbook = writer.book
            worksheet = writer.sheets["Template"]

            # set color patterns
            gray_fill = PatternFill(
                start_color="E0E0E0", end_color="E0E0E0", fill_type="solid"
            )
            yellow_fill = PatternFill(
                start_color="FFFF00", end_color="FFFF00", fill_type="solid"
            )

            # apply auto-filter and freeze top row
            total_cols = len(df.columns)
            total_rows = len(df) + 1
            worksheet.auto_filter.ref = (
                f"A1:{get_column_letter(total_cols)}{total_rows}"
            )
            worksheet.freeze_panes = "A2"
            num_untouchable_cols = (
                df.columns.get_loc(new_cols_list[0]) if new_cols_list else total_cols
            )
            for col_idx in range(1, total_cols + 1):
                col_letter = get_column_letter(col_idx)
                max_length = 0
                column_cells = worksheet[col_letter]

                for cell in column_cells:
                    try:
                        if cell.value:
                            cell_len = len(str(cell.value))
                            if cell_len > max_length:
                                max_length = cell_len
                    except:
                        pass
                adjusted_width = min(max_length * 1.1, 50) + 2
                worksheet.column_dimensions[col_letter].width = adjusted_width
                if col_idx <= num_untouchable_cols:
                    for cell in column_cells:
                        cell.fill = gray_fill
                else:
                    for row_idx, cell in enumerate(column_cells, start=1):
                        cell.fill = gray_fill if row_idx == 1 else yellow_fill

        current_run.add_file_output(file_path)

        current_run.log_info(f"Fichier template généré avec succès: {file_path}")
        return df

    except Exception as e:
        current_run.log_error(f"Erreur lors de la création du fichier template: {e}")
        raise


def save_file(iaso_org_unit_tree_df_clean: pd.DataFrame):
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
