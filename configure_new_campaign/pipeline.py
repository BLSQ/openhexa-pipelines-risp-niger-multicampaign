from openhexa.sdk import current_run, parameter, pipeline, workspace
import os
import pandas as pd
from config import (
    CONFIG_PATH,
    OUTPUTS_PATH,
    required_regions,
    campaign_config_dict,
    campaign_name_dict,
)


@pipeline(
    "configure_new_campaign",
    name="multi-campagne - 03 - Pipeline de configuration d'une nouvelle campagne",
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
    # default="Polio",
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
    # default=2026,
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
    # default=["Nationale"],
)
@parameter(
    "campaign_round_start_date",
    name="Date de début du round de la campagne",
    help="Sélectionnez la date de début du round de la campagne. La date doit être au format AAAA-MM-JJ.",
    type=str,
    required=True,
    # default="2026-05-01",
)
@parameter(
    "campaign_round_end_date",
    name="Date de fin du round de la campagne",
    help="Sélectionnez la date de fin du round de la campagne. La date doit être au format AAAA-MM-JJ.",
    type=str,
    required=True,
    # default="2026-05-20",
)
@parameter(
    "overwrite_existing_round",
    name="Effacer toute configuration précédente en cas de chevauchement de période",
    help="Indiquez si les configurations de rounds précédents doivent être effacées en cas de chevauchement de période avec la nouvelle campagne. Si non, une erreur sera levée en cas de chevauchement de période.",
    type=bool,
    required=True,
    default=False,
)
def configure_new_campaign(
    campaign: str,
    year: int,
    campaign_scale: list,
    campaign_round_start_date: str,
    campaign_round_end_date: str,
    overwrite_existing_round: bool,
):
    """
    This pipeline generates a dataframe containing the configuration for a new vaccination
    campaign based on the parameters provided by the user. It checks the validity of the
    parameters, loads necessary data, creates a configuration dataframe, and saves it for
    future use.
    """
    inspect_params(
        year,
        campaign_scale,
        campaign_round_start_date,
        campaign_round_end_date,
    )
    configured_target_data = load_data("combined_configured_target_data")
    combined_campaign_data = load_data("combined_campaign_data")
    overlap_exists = validate_coherence_of_params(
        configured_target_data,
        combined_campaign_data,
        campaign,
        campaign_scale,
        year,
        campaign_round_start_date,
        campaign_round_end_date,
        overwrite_existing_round,
    )
    config_df = create_configuration_df(
        combined_campaign_data,
        campaign,
        year,
        campaign_round_start_date,
        campaign_round_end_date,
        overlap_exists,
    )
    campaign_round = config_df["round"].iloc[0]
    org_unit_tree = load_data("iaso_org_unit_tree_clean")
    config_df = add_org_unit_info(config_df, org_unit_tree, campaign_scale)
    save_file(config_df, f"config_{campaign}_{year}_{campaign_round.replace(' ', '_')}")
    export_to_dataset(
        config_df,
        CONFIG_PATH,
        f"config_{campaign}_{year}_{campaign_round.replace(' ', '_')}",
    )


def inspect_params(
    year: int,
    campaign_scale: list,
    campaign_round_start_date: str,
    campaign_round_end_date: str,
) -> None:
    """
    Runs checks on the parameter choices to ensure they are valid and coherent before running the rest
      of the pipeline.

    Args:
        year (int): The year of the campaign.
        campaign_scale (list): The scale of the campaign (e.g., "Nationale", "Agadez", etc.).
        campaign_round_start_date (str): The start date of the campaign round in the format YYYY-MM-DD.
        campaign_round_end_date (str): The end date of the campaign round in the format YYYY-MM-DD.

    Returns:
        None
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

    # campaign_round_start_date and campaign_round_end_date must:
    # - be in the format YYYY-MM-DD
    # - campaign_round_start_date must be in the same year as the 'year' parameter
    # - campaign_round_end_date must be in the same year as the 'year' or the year after
    # - campaign_round_end_date must be after campaign_round_start_date
    try:
        start_date = pd.to_datetime(campaign_round_start_date, format="%Y-%m-%d")
        end_date = pd.to_datetime(campaign_round_end_date, format="%Y-%m-%d")
        if start_date.year != year:
            current_run.log_error(
                "La date de début du round de la campagne doit être dans la même année que le paramètre 'Année'."
            )
            raise
        if end_date.year != year and end_date.year != year + 1:
            current_run.log_error(
                "La date de fin du round de la campagne doit être dans la même année que le paramètre 'Année' ou l'année suivante."
            )
            raise
        if end_date <= start_date:
            current_run.log_error(
                "La date de fin du round de la campagne doit être après la date de début."
            )
            raise
    except ValueError:
        current_run.log_error(
            "Les paramètres 'Date de début du round de la campagne' et 'Date de fin du round de la campagne' doivent être au format AAAA-MM-JJ."
        )
        raise

    current_run.log_info("Choix des paramètres vérifiés: il n'y a pas d'erreur.")


def validate_coherence_of_params(
    target_df: pd.DataFrame,
    expected_structure_df: pd.DataFrame,
    campaign: str,
    campaign_scale: list,
    year: int,
    campaign_round_start_date: str,
    campaign_round_end_date: str,
    overwrite_existing_round: bool,
) -> bool:
    """
    Validates the coherence of the parameters against the configured target data.

    Args:
        target_df (pd.DataFrame): The dataframe containing the configured target data.
        expected_structure_df (pd.DataFrame): the dataframe containing the expected structure of the data for each campaign
        campaign (str): The campaign for which the configuration is being created.
        campaign_scale (list): The scale of the campaign (e.g., "Nationale", "Agadez", etc.).
        year (int): The year of the campaign.
        campaign_round_start_date (str): The start date of the campaign round in the format YYYY-MM-DD.
        campaign_round_end_date (str): The end date of the campaign round in the format YYYY-MM-DD.
        overwrite_existing_round (bool): Indicates whether to overwrite existing round configurations
                                         in case of overlapping periods.

    Returns:
        overlap_exists (bool): True if there is an overlap with existing campaign rounds, False otherwise.
                                This information is used later on to determine the round number for the
                                new campaign configuration.
    """
    current_run.log_info(
        "Validation de la cohérence des paramètres avec les données de cibles configurées..."
    )
    try:
        # Checking for overlapping periods with existing campaign rounds for the same product and year in
        # the expected structure dataframe
        campaign_cleaned = campaign_name_dict[campaign].strip()
        start_new = pd.to_datetime(campaign_round_start_date)
        end_new = pd.to_datetime(campaign_round_end_date)

        mask = (expected_structure_df["produit"] == campaign_cleaned) & (
            expected_structure_df["year"] == year
        )
        check_df = expected_structure_df[mask]
        check_df = check_df[["period", "round"]].drop_duplicates()
        overlap_exists = False
        for round in check_df["round"].unique():
            round_period_start = check_df[check_df["round"] == round]["period"].min()
            round_period_end = check_df[check_df["round"] == round]["period"].max()
            if (start_new <= round_period_end) and (end_new >= round_period_start):
                if overwrite_existing_round:
                    current_run.log_info(
                        f"Chevauchement détecté avec le {round} d'une campagne de '{campaign}' existante ({round_period_start.date()} - {round_period_end.date()}). Les configurations existantes seront effacées."
                    )
                    overlap_exists = True
                else:
                    raise ValueError(
                        f"Conflit détecté : La période de la nouvelle campagne chevauche celle du {round} d'une campagne de '{campaign}' déjà existante ({round_period_start.date()} - {round_period_end.date()}). Veuillez soit ajuster les dates de la nouvelle campagne pour éviter ce chevauchement, soit activer l'option pour écraser les configurations existantes."
                    )

        # Checking that the campaign exists in the target data for the selected year
        target_data_check_1 = target_df[target_df["produit"] == campaign_cleaned]
        if target_data_check_1.empty:
            choices = list(target_df["produit"].unique())
            raise ValueError(
                f"Campagne '{campaign_cleaned}' non trouvée. Options possibles : {choices}"
            )

        # Checking that the year exists for the selected campaign in the target data
        target_data_check_2 = target_data_check_1[target_data_check_1["year"] == year]
        if target_data_check_2.empty:
            years = list(target_data_check_1["year"].unique())
            raise ValueError(
                f"Année '{year}' non disponible pour '{campaign_cleaned}'. Années en base : {years}"
            )

        # Checking that the regions selected in campaign_scale are present in the target data for the selected
        # campaign and year
        if "Nationale" in campaign_scale:
            missing_regions = [
                r
                for r in required_regions
                if r not in target_data_check_2["LVL_2_NAME"].unique()
            ]
            if missing_regions:
                current_run.log_error(
                    f"Le choix 'Nationale' nécessite que les cibles soient définies pour toutes les régions. Régions manquantes : {missing_regions}"
                )
                raise
        else:
            missing_regions = [
                r
                for r in campaign_scale
                if r not in target_data_check_2["LVL_2_NAME"].unique()
            ]
            if missing_regions:
                current_run.log_error(
                    f"Les régions sélectionnées ne sont pas toutes présentes dans les données de cibles pour '{campaign_cleaned}' en {year}. Régions manquantes : {missing_regions}"
                )
                raise
        return overlap_exists

    except Exception as e:
        current_run.log_error(f"Erreur de validation de cohérence : {str(e)}")
        raise


def create_configuration_df(
    expected_structure_df: pd.DataFrame,
    campaign: str,
    year: int,
    campaign_round_start_date: str,
    campaign_round_end_date: str,
    overlap_exists: bool,
):
    """
    Creates a dataframe containing the configuration for a new vaccination campaign
    based on the parameters provided by the user.

    Args:
        expected_structure_df (pd.DataFrame): the dataframe containing the expected structure of the data for each campaign
        campaign (str): The campaign for which the configuration is being created.
        year (int): The year of the campaign.
        campaign_round_start_date (str): The start date of the campaign round in the format YYYY-MM-DD.
        campaign_round_end_date (str): The end date of the campaign round in the format YYYY-MM-DD.
        overlap_exists (bool): True if there is an overlap with existing campaign rounds, False otherwise.

    Returns:
        config_df (pd.DataFrame): A dataframe containing the configuration for the new campaign, with one row per
                                  combination of parameters and organizational unit.
    """
    current_run.log_info("Création du dataframe de configuration de la campagne...")

    # identify the round of the campaign, based on prior campaign data in the same year for the same product
    # if the period overlaps with an existing round, the existing round will be overwritten and the new campaign
    # will take the round number of the overwritten round;
    prior_campaigns = expected_structure_df[
        (expected_structure_df["produit"] == campaign_name_dict[campaign])
        & (expected_structure_df["year"] == year)
    ]
    if prior_campaigns.empty:
        current_run.log_info(
            f"Aucune campagne antérieure trouvée pour {campaign_name_dict[campaign]} en {year}. Le round de la campagne sera défini à 1."
        )
        campaign_round = "round 1"
    else:
        prior_campaigns = prior_campaigns[
            ["produit", "year", "round"]
        ].drop_duplicates()
        prior_campaigns["round_num"] = (
            prior_campaigns["round"].str.extract("round (\d+)").astype(int)
        )
        max_round_num = prior_campaigns["round_num"].max()
        if overlap_exists:
            campaign_round = f"round {max_round_num}"
            current_run.log_info(
                f"Campagne antérieure trouvée pour {campaign_name_dict[campaign]} en {year} avec périodes de chevauchement. Le round de la campagne sera défini à {campaign_round}."
            )
        else:
            campaign_round = f"round {max_round_num + 1}"
            current_run.log_info(
                f"Campagne antérieure trouvée pour {campaign_name_dict[campaign]} en {year} sans périodes de chevauchement. Le round de la campagne sera défini à {campaign_round}."
            )
    # create a dataframe with the campaign configuration (use campaign_config_dict to create the columns for
    # vaccination_status, age_range, site_type, and sex type based on the campaign)
    config_df = pd.DataFrame(
        {
            "produit": [campaign],
            "round": [campaign_round],
            "year": [year],
            "campaign_round_start_date": [campaign_round_start_date],
            "campaign_round_end_date": [campaign_round_end_date],
            "vaccination_status": [
                campaign_config_dict[campaign]["vaccination_status"]
            ],
            "age": [campaign_config_dict[campaign]["age"]],
            "site": [campaign_config_dict[campaign]["site"]],
            "sexe": [campaign_config_dict[campaign]["sexe"]],
        }
    )

    # create "period" and "order_day" columns
    config_df["campaign_round_start_date"] = pd.to_datetime(
        config_df["campaign_round_start_date"], format="%Y-%m-%d"
    )
    config_df["campaign_round_end_date"] = pd.to_datetime(
        config_df["campaign_round_end_date"], format="%Y-%m-%d"
    )
    config_df["period"] = config_df.apply(
        lambda row: pd.date_range(
            row["campaign_round_start_date"], row["campaign_round_end_date"]
        ),
        axis=1,
    )
    config_df = config_df.explode("period").reset_index(drop=True)
    config_df["order_day"] = (
        config_df["period"] - config_df["campaign_round_start_date"]
    ).dt.days + 1

    # expand vaccination_status, age_range, site_type, and strategy_type columns to have one row per combination
    # of choices
    config_df = config_df.explode("vaccination_status").reset_index(drop=True)
    config_df = config_df.explode("age").reset_index(drop=True)
    config_df = config_df.explode("site").reset_index(drop=True)
    config_df = config_df.explode("sexe").reset_index(drop=True)

    # clean product name
    config_df["produit"] = config_df["produit"].replace(campaign_name_dict)

    # keep only relevant columns
    config_df = config_df[
        [
            "produit",
            "round",
            "year",
            "period",
            "order_day",
            "vaccination_status",
            "age",
            "site",
            "sexe",
        ]
    ]

    return config_df


def load_data(name: str) -> pd.DataFrame:
    """
    Import data from a specified file in the outputs directory.
    The file should be in parquet format and the name should be provided without the extension.

    Args:
        name (str): Name of the file to be imported (without extension).
    Returns:
        df (pd.DataFrame): DataFrame containing the imported data.
    """
    current_run.log_info(f"Importation du fichier {name}...")
    try:
        if not os.path.exists(OUTPUTS_PATH):
            os.makedirs(OUTPUTS_PATH)

        file_path = os.path.join(
            OUTPUTS_PATH,
            f"{name}.parquet",
        )
        df = pd.read_parquet(file_path)
        current_run.log_info(f"Fichier importé avec succès: {file_path}")
        return df

    except Exception as e:
        current_run.log_error(f"Erreur lors de l'importation du fichier {name}: {e}")
        raise


def add_org_unit_info(
    config_df: pd.DataFrame, org_unit_df: pd.DataFrame, campaign_scale: list
) -> pd.DataFrame:
    """
    Adds organizational unit information to the campaign configuration dataframe
    by merging it with the IASO org unit tree dataframe. The merge is done in a way
    to create one row per combination of campaign configuration and organizational unit,
    based on the selected campaign scale.

    Args:
        config_df (pd.DataFrame): The dataframe containing the campaign configuration.
        org_unit_df (pd.DataFrame): The dataframe containing the cleaned IASO organizational unit tree data.
        campaign_scale (list): The scale of the campaign (e.g., "Nationale", "Agadez", etc.) which determines
                                how the merge is performed.

    Returns:
        merged_df (pd.DataFrame): The merged dataframe containing campaign configuration and organizational unit information.
    """
    current_run.log_info("Ajout des informations des unités organisationnelles...")

    if "Nationale" in campaign_scale:
        merged_df = config_df.merge(
            org_unit_df[["LVL_3_NAME", "LVL_6_NAME", "org_unit_id"]].drop_duplicates(),
            how="cross",
        )

    else:
        merged_df = config_df.merge(
            org_unit_df[org_unit_df["LVL_2_NAME"].isin(campaign_scale)][
                ["LVL_3_NAME", "LVL_6_NAME", "org_unit_id"]
            ].drop_duplicates(),
            how="cross",
        )
    return merged_df


def save_file(df: pd.DataFrame, file_name: str) -> None:
    """
    Save the cleaned org unit tree data to a parquet file.

    Args:
        df (pd.DataFrame): DataFrame containing the cleaned org unit tree data.
        file_name (str): Name of the file to save the DataFrame as.

    Returns:
        None
    """
    current_run.log_info("Enregistrement des données des unités organisationnelles...")
    try:
        if not os.path.exists(CONFIG_PATH):
            os.makedirs(CONFIG_PATH)
        file_path = os.path.join(
            CONFIG_PATH,
            f"{file_name}.parquet",
        )

        df.to_parquet(
            file_path,
            index=False,
        )
        current_run.log_info(
            f"Données des unités organisationnelles enregistrées avec succès: {file_path}"
        )
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'enregistrement du fichier: {e}")
        raise e


def export_to_dataset(df: pd.DataFrame, df_file_path: str, dataset_name: str) -> None:
    """
    Exports a DataFrame to an OpenHexa dataset in multiple formats (xlsx, parquet, csv).

    Args:
        df (pd.DataFrame): The configuration dataframe to export.
        df_file_path (str): The file path where the dataframe is saved.
        dataset_name (str): The name of the OpenHexa dataset.
    """
    current_run.log_info(
        f"Préparation de l'exportation vers le dataset : {dataset_name}..."
    )

    dataset_slug = dataset_name.lower().strip().replace(" ", "-").replace("_", "-")

    # check if dataset already exists
    try:
        dataset = workspace.get_dataset(dataset_slug)
        current_run.log_info(f"Dataset existant trouvé : {dataset_slug}")
    except Exception:
        current_run.log_info(f"Dataset {dataset_name} non trouvé. Création en cours...")
        dataset = workspace.create_dataset(
            name=dataset_name,
            description="Données de configuration de campagne (multi-formats)",
        )

    # define versioning
    latest_version = dataset.latest_version
    version_number = int(latest_version.name.lstrip("v")) + 1 if latest_version else 1
    new_version_name = f"v{version_number}"

    # create local files
    if not os.path.exists(df_file_path):
        os.makedirs(df_file_path)

    base_path = os.path.join(df_file_path, dataset_name)
    files_to_upload = {
        "parquet": f"{base_path}.parquet",
        "xlsx": f"{base_path}.xlsx",
        "csv": f"{base_path}.csv",
    }

    df.to_parquet(files_to_upload["parquet"], index=False)
    df.to_excel(files_to_upload["xlsx"], index=False)
    df.to_csv(files_to_upload["csv"], index=False)

    # upload to Dataset in OH
    version = dataset.create_version(new_version_name)

    for format_type, file_path in files_to_upload.items():
        version.add_file(file_path, os.path.basename(file_path))
        current_run.log_info(
            f"Fichier {format_type} ajouté à la version {new_version_name}"
        )

    current_run.log_info(
        f"Exportation terminée avec succès pour {dataset_name} ({new_version_name})"
    )


if __name__ == "__main__":
    configure_new_campaign()
