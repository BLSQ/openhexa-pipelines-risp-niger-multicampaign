"""
Vaccination target-data + expected-data-structure EXTRACTION pipeline (Niger, multi-campagne).

The manual entry point of the target-data flow: processes ONE uploaded spreadsheet and saves
this run's own target rows + expected-structure rows as per-run files, under "historical targets
processed" and "expected data structure processed". It does not compile
combined_target_data.parquet / expected_data_structure.parquet itself - process_target_data does
that automatically, as the first step of orchestrate_pipelines_flow's chain, from every per-run
file produced here.

Driven by these parameters:

    input_file           : the .xlsx to process (uploaded File).
    campaign_name        : the campaign type - resolves to BOTH the internal IASO
                            campaign_name (choix_campagne, used for period lookup)
                            AND the exact products list (see layouts.CAMPAIGN_CHOICES).
                            products is not a separate parameter.
    year                 : the campaign year.
    rounds                : the round number(s) (multi-select; each -> "round <n>").
    campaign_start_date,
    campaign_end_date    : optional. Only required if this run covers a (year, round,
                            produit) combination not already in
                            expected_structure.HISTORICAL_CAMPAIGNS_CONFIG - see
                            expected_structure.build_campaign_period_df.
    overwrite_existing   : replace already-processed data for the same combination
                            instead of aborting.

The generic engine (target_import) auto-identifies the file's structure, extracts
the base age brackets, builds the requested products, stamps the year and rounds,
then the unchanged org-unit matching stage attaches org_unit_id and region. The
expected-data-structure module (expected_structure.py) then builds the matching
combinatorial "expected" rows (site/status/sex/period) from this SAME run's
matched data - absorbed from create_expected_data_structure_for_historical_campaigns
and configure_new_campaign per the v2 migration plan.

NOTE on the pre-run duplicate/overlap checks below (check_for_existing_slices,
check_for_date_overlap): both read the last COMPILED combined_target_data.parquet /
expected_data_structure.parquet, not the live per-run files. If process_target_data hasn't
recompiled since a previous extraction, a genuinely overlapping combination from that prior
extraction won't be caught by these checks yet (the compiled file they read is stale). This is
an accepted design tradeoff, documented here rather than left implicit.

Per-run output columns (saved into "historical targets processed" / "expected data structure
processed", compiled by process_target_data):
    LVL_3_NAME, cible, age, year, org_unit_id, round, produit, LVL_6_NAME, LVL_2_NAME
    org_unit_id, LVL_3_NAME, LVL_6_NAME, sexe, year, produit, round, age, site,
    vaccination_status, choix_campagne, period, order_day
"""

import os
from datetime import datetime

import pandas as pd
from openhexa.sdk import File, current_run, parameter, pipeline

from config import (
    TARGETS_INPUT_PATH,
    EXPECTED_STRUCTURE_PROCESSED_PATH,
    PROCESSED_TARGETS_PATH,
)
from expected_structure import (
    build_age_round_year_df,
    build_campaign_period_df,
    build_sex_df,
    build_site_df,
    build_status_df,
    combine_expected_structure,
    check_for_date_overlap,
    HISTORICAL_CAMPAIGNS_CONFIG,
)
from geo_match import match_district_to_org_unit_id
from layouts import CAMPAIGN_CHOICES
from run_persistence import (
    check_for_existing_slices,
    find_overlapping_slices,
    remove_slices_from_processed_files,
    run_combinations,
    run_slug,
)
from shared_utils import load_data, save_file
from target_import import TargetImportError, import_target_file
from utils import (
    add_region_names,
    clean_org_unit_id,
    match_csi_to_org_unit_id,
)


@pipeline(
    "extract_target_data",
    name="multi-campagne - 01 Import et traitement d'un fichier de cibles",
)
@parameter(
    "input_file",
    name="Fichier de cibles (.xlsx)",
    help="Fichier Excel de cibles à traiter.",
    type=File,
    required=True,
)
@parameter(
    "campaign_name",
    name="Type de campagne",
    help=("Sélectionnez le type de campagne."),
    type=str,
    choices=[
        "Polio (couplée avec Albendazole et Vitamine A)",
        "Polio (non couplée)",
        "Albendazole et Vitamine A (sans Polio)",
        "Fièvre jaune",
        "Rougeole",
        "Méningite",
        "TCV",
    ],
    required=True,
)
@parameter(
    "year",
    name="Année de la campagne",
    help="Sélectionnez l'année de réalisation de la campagne.",
    type=int,
    choices=[
        2024,
        2025,
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
    required=True,
)
@parameter(
    "rounds",
    name="Numéro(s) de round",
    help="Sélectionnez le(s) round(s) auquel/auxquels la campagne se déroule.",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    multiple=True,
    required=True,
)
@parameter(
    "campaign_start_date",
    name="Date de début de la campagne",
    help=(
        "Veuillez entrer la date de début prévue de la campagne au format AAAA-MM-JJ."
    ),
    type=str,
    required=False,
)
@parameter(
    "campaign_end_date",
    name="Date de fin de la campagne",
    help=("Veuillez entrer la date de fin prévue de la campagne au format AAAA-MM-JJ."),
    type=str,
    required=False,
)
@parameter(
    "overwrite_existing",
    name="Écraser les données existantes en cas de doublon",
    help=(
        "Si des cibles existent déjà pour la même "
        "combinaison produit / année / round, le traitement s'arrête. Activez "
        "cette option pour remplacer ces données par celles du fichier importé."
    ),
    type=bool,
    default=False,
    required=False,
)
def extract_target_data(
    input_file,
    campaign_name,
    year: int,
    rounds,
    campaign_start_date: str = None,
    campaign_end_date: str = None,
    overwrite_existing: bool = False,
):
    """Import, reshape, match and save a single target-data spreadsheet, together
    with the matching expected-data-structure rows for the same campaign/run - as
    per-run files for process_target_data to compile later."""
    campaign_name_internal, products = CAMPAIGN_CHOICES[campaign_name]

    check_year(int(year))
    check_for_existing_slices(
        int(year), list(products), list(rounds), bool(overwrite_existing)
    )
    check_for_date_overlap(
        int(year),
        list(rounds),
        list(products),
        campaign_name_internal,
        campaign_start_date,
        campaign_end_date,
        bool(overwrite_existing),
    )

    iaso_org_unit_tree_df = load_data("iaso_org_unit_tree_raw")
    iaso_org_unit_tree_df_clean = load_data("iaso_org_unit_tree_clean")

    # 1. Auto-detecting structural import -> tidy long frame (age x product x round).
    file_path = resolve_input_file(input_file)
    tidy = import_or_fail(file_path, list(products), int(year), list(rounds))

    # 2-3. Org-unit matching + region names + one-to-many org-unit cleanup (unchanged).
    matched = match_and_clean_org_units(
        tidy, iaso_org_unit_tree_df, iaso_org_unit_tree_df_clean
    )

    # 4. Expected-data-structure for this same run
    expected_slice = build_expected_structure_for_run(
        matched,
        products,
        int(year),
        list(rounds),
        campaign_name_internal,
        campaign_start_date,
        campaign_end_date,
    )

    # 5. Save this run's output in the per-run-file folders
    persist(
        matched,
        expected_slice,
        int(year),
        list(products),
        list(rounds),
        overwrite_existing,
    )


def import_or_fail(file_path, products: list, year: int, rounds: list) -> pd.DataFrame:
    """Run the auto-detecting import engine, turning any failure into an actionable,
    OpenHexa-readable log (one call per line) before re-raising."""
    try:
        return import_target_file(file_path, products, year, rounds)
    except TargetImportError:
        # The engine already logged a detailed, actionable message via log_error.
        current_run.log_error(
            "Traitement interrompu: le fichier de cibles n'a pas pu être interprété. "
            "Consultez le message d'erreur ci-dessus, corrigez le fichier ou les "
            "paramètres, puis relancez le pipeline."
        )
        raise
    except Exception as e:
        current_run.log_error(
            "Erreur inattendue lors de la lecture du fichier de cibles."
        )
        current_run.log_error(f"CAUSE TECHNIQUE: {type(e).__name__}: {e}")
        current_run.log_error(
            "À FAIRE: vérifiez que le fichier est un .xlsx valide et conforme aux "
            "formats habituels (une ligne d'en-tête avec les districts/CSI et des "
            "colonnes de cibles indiquant leur tranche d'âge)."
        )
        current_run.log_error(
            "Si le problème persiste, transmettez ce message et le fichier à "
            "l'équipe technique."
        )
        raise


def match_and_clean_org_units(
    tidy: pd.DataFrame,
    iaso_org_unit_tree_df: pd.DataFrame,
    iaso_org_unit_tree_df_clean: pd.DataFrame,
) -> pd.DataFrame:
    """District files have no LVL_6_NAME column; CSI files do - that's how the engine
    already told us which level this file is at, so no separate flag is needed here."""
    if "LVL_6_NAME" in tidy.columns:
        matched = match_csi_to_org_unit_id(tidy, iaso_org_unit_tree_df_clean)
    else:
        matched = match_district_to_org_unit_id(tidy, iaso_org_unit_tree_df_clean)
        matched = matched.dropna(subset=["org_unit_id"])

    matched = add_region_names(matched, iaso_org_unit_tree_df_clean)
    return clean_org_unit_id(
        matched, iaso_org_unit_tree_df, iaso_org_unit_tree_df_clean
    )


def build_expected_structure_for_run(
    matched: pd.DataFrame,
    products: list,
    year: int,
    rounds: list,
    campaign_name_internal: str,
    campaign_start_date: str,
    campaign_end_date: str,
) -> pd.DataFrame:
    """The product/site/status/sex/age/period combinatorial rows for this run."""
    site_df = build_site_df(products)
    status_df = build_status_df(products)
    sex_df = build_sex_df()
    age_round_year_df = build_age_round_year_df(matched)
    period_df = build_campaign_period_df(
        year,
        rounds,
        products,
        campaign_name_internal,
        campaign_start_date,
        campaign_end_date,
        HISTORICAL_CAMPAIGNS_CONFIG,
    )
    return combine_expected_structure(
        matched, site_df, status_df, sex_df, age_round_year_df, period_df
    )


def persist(
    matched: pd.DataFrame,
    expected_slice: pd.DataFrame,
    year: int,
    products: list,
    rounds: list,
    overwrite_existing: bool,
) -> None:
    """Save this run's per-run files, cleaning up any superseded slice first if
    overwrite mode is on."""
    if overwrite_existing:
        combos = run_combinations(year, products, rounds)
        for folder in (PROCESSED_TARGETS_PATH, EXPECTED_STRUCTURE_PROCESSED_PATH):
            overlaps = find_overlapping_slices(combos, folder)
            if overlaps:
                remove_slices_from_processed_files(overlaps)

    name = run_slug(year, products, rounds)
    save_file(matched, name, folder=PROCESSED_TARGETS_PATH)
    save_file(expected_slice, name, folder=EXPECTED_STRUCTURE_PROCESSED_PATH)
    current_run.log_info(
        f"Fichier de cibles enregistré: {len(matched)} ligne(s) produite(s), "
        f"{len(expected_slice)} ligne(s) de structure attendue produite(s)."
    )
    current_run.log_info(
        "Ces données seront intégrées à combined_target_data / "
        "expected_data_structure lors de la prochaine exécution du pipeline "
        "process_target_data (automatique, premier maillon de "
        "orchestrate_pipelines_flow)."
    )


def resolve_input_file(input_file) -> object:
    """
    Turn the ``File`` parameter into something pandas can read.

    Accepts a path string, an object exposing a path/name attribute, or a
    file-like object. Bare filenames are looked up in the campaign inputs folder.
    """
    candidate = input_file
    if not isinstance(candidate, str):
        for attr in ("path", "fullpath", "name"):
            value = getattr(candidate, attr, None)
            if isinstance(value, str) and value:
                candidate = value
                break

    if isinstance(candidate, str):
        if os.path.exists(candidate):
            return candidate
        alt = os.path.join(TARGETS_INPUT_PATH, os.path.basename(candidate))
        if os.path.exists(alt):
            return alt
        msg = f"Fichier introuvable: {candidate}"
        current_run.log_error(msg)
        raise FileNotFoundError(msg)

    # Otherwise assume a file-like object readable by pandas.
    return input_file


def fail_run(*lines):
    """Log an actionable error line by line, then abort the pipeline run."""
    for line in lines:
        if line:
            current_run.log_error(line)
    raise ValueError(" ".join(str(ln) for ln in lines if ln))


def check_year(year: int) -> None:
    """Warn when the selected campaign year is not the current calendar year."""
    current_year = datetime.now().year
    if year != current_year:
        current_run.log_warning(
            f"ATTENTION: l'année sélectionnée ({year}) est différente de l'année "
            f"en cours ({current_year})."
        )
        current_run.log_warning(
            "Vérifiez que c'est bien l'année de la campagne correspondant au "
            "fichier importé; sinon, corrigez le paramètre 'Année de la campagne'."
        )


if __name__ == "__main__":
    extract_target_data()
