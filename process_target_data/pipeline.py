"""
Vaccination target-data + expected-data-structure pipeline (Niger, multi-campagne).

The pipeline processes ONE uploaded spreadsheet, driven by these parameters:

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
                            config.HISTORICAL_CAMPAIGNS_CONFIG - see expected_structure.
                            build_campaign_period_df.
    overwrite_existing   : replace already-processed data for the same combination
                            instead of aborting.

The generic engine (target_import) auto-identifies the file's structure, extracts
the base age brackets, builds the requested products, stamps the year and rounds,
then the unchanged org-unit matching stage attaches org_unit_id and region. The
expected-data-structure module (expected_structure.py) then builds the matching
combinatorial "expected" rows (site/status/sex/period) from this SAME run's
matched data - absorbed from create_expected_data_structure_for_historical_campaigns
and configure_new_campaign per the v2 migration plan.

Output columns (combined_target_data):
    LVL_3_NAME, cible, age, year, org_unit_id, round, produit, LVL_6_NAME, LVL_2_NAME
Output columns (expected_data_structure):
    org_unit_id, LVL_3_NAME, LVL_6_NAME, sexe, year, produit, round, age, site,
    vaccination_status, choix_campagne, period, order_day
"""

import glob
import os
import re
import unicodedata
from datetime import datetime

import numpy as np
import pandas as pd
from openhexa.sdk import File, current_run, parameter, pipeline

from config import (
    EXPECTED_STRUCTURE_PROCESSED_PATH,
    HISTORICAL_CAMPAIGNS_CONFIG,
    OUTPUTS_PATH,
    PROCESSED_TARGETS_PATH,
    TARGETS_HISTORICAL_PATH,
    TEMP_PATH,
    csi_matching_failed,
)
from expected_structure import (
    build_age_round_year_df,
    build_campaign_period_df,
    build_sex_df,
    build_site_df,
    build_status_df,
    combine_expected_structure,
)
from geo_match import build_district_mapping
from layouts import CAMPAIGN_CHOICES
from shared_utils import export_to_dataset, load_data, save_file
from target_import import TargetImportError, import_target_file
from utils import org_unit_matching


@pipeline(
    "process_target_data",
    # name="multi-campagne - Import et traitement d'un fichier de cibles",
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
    help=(
        "Sélectionnez le type de campagne. Ce choix détermine à la fois la période "
        "de référence (IASO) et les produits attendus dans le fichier."
    ),
    type=str,
    # Literal list (OpenHexa parses choices statically). Keep in sync with
    # layouts.CAMPAIGN_CHOICES.
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
    help="Année de réalisation de la campagne.",
    type=int,
    # Literal list: OpenHexa parses `choices` statically (no function calls).
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
    help="Un ou plusieurs rounds de la campagne.",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    multiple=True,
    required=True,
)
@parameter(
    "campaign_start_date",
    name="Date de début de la campagne",
    help=(
        "Requis uniquement si cette combinaison année/round/produit n'existe pas "
        "déjà dans la configuration historique. Format AAAA-MM-JJ."
    ),
    type=str,
    required=False,
)
@parameter(
    "campaign_end_date",
    name="Date de fin de la campagne",
    help=(
        "Requis uniquement si cette combinaison année/round/produit n'existe pas "
        "déjà dans la configuration historique. Format AAAA-MM-JJ."
    ),
    type=str,
    required=False,
)
@parameter(
    "overwrite_existing",
    name="Écraser les données existantes en cas de doublon",
    help=(
        "Désactivé par défaut. Si des cibles existent déjà pour la même "
        "combinaison produit / année / round, le traitement s'arrête. Activez "
        "cette option pour remplacer ces données par celles du fichier importé."
    ),
    type=bool,
    default=False,
    required=False,
)
def process_target_data(
    input_file,
    campaign_name,
    year: int,
    rounds,
    campaign_start_date: str = None,
    campaign_end_date: str = None,
    overwrite_existing: bool = False,
):
    """Import, reshape, match and export a single target-data spreadsheet, together
    with the matching expected-data-structure rows for the same campaign/run."""
    campaign_name_internal, products = CAMPAIGN_CHOICES[campaign_name]

    check_year(int(year))

    # Fail fast (before any heavy work) if these targets already exist, or if the
    # supplied dates conflict with an already-recorded round - and the overwrite
    # toggle is off.
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

    # 4. Expected-data-structure for this same run, built from `matched` only (not
    #    the full combined dataset - see expected_structure.py's module docstring
    #    for why that's what makes the old Dosso/Tahoua special case unnecessary).
    expected_slice = build_expected_structure_for_run(
        matched,
        products,
        int(year),
        list(rounds),
        campaign_name_internal,
        campaign_start_date,
        campaign_end_date,
    )

    # 5. Save this run's output in the per-run-file folders, then compile both
    #    combined datasets from ALL files in their respective folders. In
    #    overwrite mode the superseded slices are removed only now, i.e. after
    #    the replacement data has been produced successfully.
    persist_and_compile(
        matched, expected_slice, int(year), list(products), list(rounds), overwrite_existing
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
    return clean_org_unit_id(matched, iaso_org_unit_tree_df, iaso_org_unit_tree_df_clean)


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


def persist_and_compile(
    matched: pd.DataFrame,
    expected_slice: pd.DataFrame,
    year: int,
    products: list,
    rounds: list,
    overwrite_existing: bool,
) -> None:
    """Save this run's per-run files, cleaning up any superseded slice first if
    overwrite mode is on, then compile both combined datasets from scratch."""
    if overwrite_existing:
        combos = run_combinations(year, products, rounds)
        for folder in (PROCESSED_TARGETS_PATH, EXPECTED_STRUCTURE_PROCESSED_PATH):
            overlaps = find_overlapping_slices(combos, folder)
            if overlaps:
                remove_slices_from_processed_files(overlaps)

    name = _run_slug(year, products, rounds)
    save_file(matched, name, folder=PROCESSED_TARGETS_PATH)
    save_file(expected_slice, name, folder=EXPECTED_STRUCTURE_PROCESSED_PATH)
    combined_targets = compile_processed_files(
        PROCESSED_TARGETS_PATH, "combined_target_data", "de cibles traité(s)"
    )
    combined_expected = compile_processed_files(
        EXPECTED_STRUCTURE_PROCESSED_PATH,
        "expected_data_structure",
        "de structure attendue traité(s)",
    )
    current_run.log_info(
        f"Cibles combinées: {len(combined_targets)} lignes ({len(matched)} produites "
        f"lors de cette exécution)."
    )
    current_run.log_info(
        f"Structure attendue combinée: {len(combined_expected)} lignes "
        f"({len(expected_slice)} produites lors de cette exécution)."
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
        alt = os.path.join(TARGETS_HISTORICAL_PATH, os.path.basename(candidate))
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


def run_combinations(year: int, products: list, rounds: list) -> set:
    """The (year, produit, round) slices this run would write."""
    return {(int(year), str(p), f"round {int(r)}") for p in products for r in rounds}


def existing_combinations_in_combined() -> set:
    """
    The (year, produit, round) combinations already present in
    combined_target_data - the actual dataset consumers read, and what the
    pre-run duplicate check is evaluated against.

    Missing or unreadable is treated as "nothing exists yet" (e.g. the very
    first run) rather than an error.
    """
    path = os.path.join(OUTPUTS_PATH, "combined_target_data.parquet")
    if not os.path.exists(path):
        return set()
    try:
        existing = pd.read_parquet(path, columns=["year", "produit", "round"])
    except Exception as e:
        current_run.log_warning(
            "Fichier combined_target_data illisible, ignoré lors du contrôle de "
            f"doublons ({e})."
        )
        return set()
    return {
        (int(y), str(p), str(r))
        for y, p, r in existing.drop_duplicates().itertuples(index=False)
    }


def find_overlapping_slices(combos: set, folder: str) -> dict:
    """
    Locate which individual processed files in ``folder`` (either the target or
    the expected-structure per-run-file folder) contain any of ``combos``, so
    their rows can be dropped.

    Used only by the overwrite-mode removal step: combined_target_data is what
    decides WHETHER a combination already exists (see
    existing_combinations_in_combined), but removing it means editing the
    per-run files it's rebuilt from, which requires file-level granularity.
    """
    overlaps = {}
    for path in sorted(glob.glob(os.path.join(folder, "*.parquet"))):
        try:
            existing = pd.read_parquet(path, columns=["year", "produit", "round"])
        except Exception as e:  # unreadable file: warn but don't block the run
            current_run.log_warning(
                f"Fichier traité illisible, ignoré lors du contrôle de doublons: "
                f"{os.path.basename(path)} ({e})."
            )
            continue
        present = {
            (int(y), str(p), str(r))
            for y, p, r in existing.drop_duplicates().itertuples(index=False)
        }
        common = present & combos
        if common:
            overlaps[path] = sorted(common)
    return overlaps


def remove_slices_from_processed_files(overlaps: dict) -> None:
    """Drop the overlapping slices from previously processed files (overwrite mode)."""
    for path, slices in overlaps.items():
        name = os.path.basename(path)
        df = pd.read_parquet(path)
        drop_keys = set(slices)
        keep = ~df.apply(
            lambda r: (int(r["year"]), str(r["produit"]), str(r["round"])) in drop_keys,
            axis=1,
        )
        remaining = df[keep]
        removed = len(df) - len(remaining)
        if remaining.empty:
            os.remove(path)
            current_run.log_warning(
                f"ÉCRASEMENT: le fichier '{name}' ne contenait que des données "
                f"remplacées par cette exécution ({removed} lignes); il a été "
                "supprimé."
            )
        else:
            remaining.to_parquet(path, index=False)
            current_run.log_warning(
                f"ÉCRASEMENT: {removed} ligne(s) remplacée(s) dans le fichier "
                f"'{name}'; {len(remaining)} ligne(s) conservée(s)."
            )


def check_for_existing_slices(
    year: int, products: list, rounds: list, overwrite_existing: bool
) -> None:
    """
    Guard against silently duplicating or overwriting already-processed targets.

    Checked against combined_target_data itself - the dataset consumers actually
    read - rather than the individual per-run files it's rebuilt from. Without
    the overwrite toggle, any overlap aborts the run and the conflicting
    combination(s) are reported. With the toggle on, this only warns; the
    corresponding rows are actually removed later (see find_overlapping_slices /
    remove_slices_from_processed_files), once the new data has been produced
    successfully.
    """
    combos = run_combinations(year, products, rounds)
    conflicts = sorted(combos & existing_combinations_in_combined())
    if not conflicts:
        return

    if overwrite_existing:
        current_run.log_warning(
            f"ÉCRASEMENT ACTIVÉ: {len(conflicts)} combinaison(s) produit / année / "
            "round déjà présente(s) dans combined_target_data seront remplacées "
            "par les données de ce fichier."
        )
        for y, p, r in conflicts:
            current_run.log_warning(f"ÉCRASEMENT: {p} - {y} - {r}.")
        return

    lines = [
        "Des cibles existent déjà pour la ou les combinaisons produit / année / "
        "round sélectionnées.",
        "Traitement interrompu pour éviter de dupliquer ou d'écraser des données "
        "par erreur.",
        "CAUSE: les combinaisons suivantes sont déjà présentes dans "
        "combined_target_data:",
    ]
    for y, p, r in conflicts:
        lines.append(f"  - {p} - {y} - {r}")
    lines += [
        "À FAIRE: deux options possibles.",
        "OPTION 1: si ces données sont déjà correctes, ne relancez pas le "
        "pipeline pour cette combinaison (ou désélectionnez le ou les produits, "
        "années ou rounds concernés).",
        "OPTION 2: si vous souhaitez que le fichier importé remplace les données "
        "existantes, relancez le pipeline en activant l'option "
        "'Écraser les données existantes en cas de doublon'.",
    ]
    fail_run(*lines)


def _needs_new_period(
    year: int,
    rounds: list,
    products: list,
    campaign_name_internal: str,
    campaign_start_date: str,
    campaign_end_date: str,
) -> bool:
    """True only if dates were actually supplied AND at least one (year, round,
    produit) this run covers isn't already in HISTORICAL_CAMPAIGNS_CONFIG - a pure
    historical replay has no new period to check for overlap."""
    if not campaign_start_date or not campaign_end_date:
        return False
    return any(
        (int(year), int(r), campaign_name_internal, p) not in HISTORICAL_CAMPAIGNS_CONFIG
        for r in rounds
        for p in products
    )


def _load_existing_expected_structure():
    """The compiled expected_data_structure, or None if it doesn't exist yet or
    can't be read (both treated as "nothing to check for overlap against")."""
    path = os.path.join(OUTPUTS_PATH, "expected_data_structure.parquet")
    if not os.path.exists(path):
        return None
    try:
        return pd.read_parquet(path, columns=["produit", "year", "round", "period"])
    except Exception as e:
        current_run.log_warning(
            "Fichier expected_data_structure illisible, ignoré lors du contrôle "
            f"de chevauchement de dates ({e})."
        )
        return None


def _find_period_conflicts(
    existing: pd.DataFrame,
    year: int,
    rounds: list,
    products: list,
    campaign_start_date: str,
    campaign_end_date: str,
) -> list:
    """Which existing (produit, round) periods - for the same year, any round OTHER
    than the ones being (re)processed now - overlap the supplied date range."""
    start_new = pd.to_datetime(campaign_start_date, format="%Y-%m-%d")
    end_new = pd.to_datetime(campaign_end_date, format="%Y-%m-%d")
    requested_rounds = {int(r) for r in rounds}
    same_year = existing[existing["year"] == int(year)]

    conflicts = []
    for produit in products:
        subset = same_year[same_year["produit"] == produit]
        for rnd in subset["round"].unique():
            rnd_num = int(re.search(r"\d+", str(rnd)).group())
            if rnd_num in requested_rounds:
                continue  # same round being (re)processed, not a conflict with itself
            period = subset[subset["round"] == rnd]["period"]
            rnd_start, rnd_end = period.min(), period.max()
            if start_new <= rnd_end and end_new >= rnd_start:
                conflicts.append((produit, rnd, rnd_start.date(), rnd_end.date()))
    return conflicts


def check_for_date_overlap(
    year: int,
    rounds: list,
    products: list,
    campaign_name_internal: str,
    campaign_start_date: str,
    campaign_end_date: str,
    overwrite_existing: bool,
) -> None:
    """
    Guard against a NEW campaign round's dates overlapping an already-recorded
    round for the same (produit, year) in expected_data_structure.

    Only meaningful when campaign_start_date/campaign_end_date are actually
    supplied - i.e. this run covers at least one (year, round, produit) not
    already in HISTORICAL_CAMPAIGNS_CONFIG. A pure historical replay has no new
    period to check for overlap, so this is a no-op in that case.
    """
    if not _needs_new_period(
        year, rounds, products, campaign_name_internal, campaign_start_date, campaign_end_date
    ):
        return
    existing = _load_existing_expected_structure()
    if existing is None:
        return

    conflicts = _find_period_conflicts(
        existing, year, rounds, products, campaign_start_date, campaign_end_date
    )
    if not conflicts:
        return

    if overwrite_existing:
        _warn_date_conflicts(conflicts, campaign_start_date, campaign_end_date)
    else:
        _fail_on_date_conflicts(conflicts, campaign_start_date, campaign_end_date)


def _warn_date_conflicts(conflicts: list, campaign_start_date: str, campaign_end_date: str) -> None:
    current_run.log_warning(
        f"CHEVAUCHEMENT DE DATES: {len(conflicts)} round(s) existant(s) ont une "
        "période qui chevauche celle fournie pour cette exécution."
    )
    for produit, rnd, s, e in conflicts:
        current_run.log_warning(
            f"CHEVAUCHEMENT: {produit}, {rnd} ({s} - {e}) chevauche la période "
            f"{campaign_start_date} - {campaign_end_date} fournie."
        )


def _fail_on_date_conflicts(conflicts: list, campaign_start_date: str, campaign_end_date: str) -> None:
    lines = [
        "La période fournie chevauche celle d'un round déjà existant pour le même "
        "produit et la même année.",
        "Traitement interrompu pour éviter un chevauchement de dates involontaire.",
    ]
    for produit, rnd, s, e in conflicts:
        lines.append(
            f"CAUSE: {produit}, {rnd} ({s} - {e}) chevauche la période "
            f"{campaign_start_date} - {campaign_end_date} fournie."
        )
    lines += [
        "À FAIRE: deux options possibles.",
        "OPTION 1: corrigez les dates de la campagne si ce chevauchement est une "
        "erreur.",
        "OPTION 2: si ce chevauchement est intentionnel (remplacement du round "
        "existant), relancez le pipeline en activant l'option 'Écraser les données "
        "existantes en cas de doublon'.",
    ]
    fail_run(*lines)


def _run_slug(year: int, products: list, rounds: list) -> str:
    """Build a filesystem-safe, deterministic name for a run's output file.

    The name encodes the run configuration (year, rounds, products) so that
    re-running the SAME configuration overwrites its own file, while runs with
    different parameters produce distinct files that are all kept.
    """

    def slug(text):
        norm = unicodedata.normalize("NFD", str(text).lower())
        norm = "".join(c for c in norm if unicodedata.category(c) != "Mn")
        return re.sub(r"[^a-z0-9]+", "-", norm).strip("-")

    parts = [str(int(year))]
    parts += [f"r{int(r)}" for r in sorted(int(x) for x in rounds)]
    parts += [slug(p) for p in sorted(products)]
    return "targets_" + "_".join(parts)


def compile_processed_files(folder: str, output_name: str, description: str) -> pd.DataFrame:
    """
    Build `output_name` by concatenating EVERY processed file in `folder` -
    historical and new campaigns alike go through this same pipeline, so both
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


# =========================================================================== #
# Org-unit matching stage (unchanged from the original pipeline).              #
# =========================================================================== #
def match_csi_to_org_unit_id(
    csi_level_target_df: pd.DataFrame, iaso_org_unit_tree_df_clean: pd.DataFrame
) -> pd.DataFrame:
    """Match CSI names to org-unit IDs via fuzzy matching + known corrections."""
    current_run.log_info(
        "Appariement des noms CSI aux identifiants des unités organisationnelles..."
    )
    try:
        target_df_matched, org_unit_tree_check = _fuzzy_match_csi(
            csi_level_target_df, iaso_org_unit_tree_df_clean
        )
        target_df_matched = _apply_manual_csi_corrections(
            target_df_matched, org_unit_tree_check
        )
        _report_unmatched_csi(target_df_matched)

        target_df_matched = target_df_matched.drop(
            columns=[
                "LVL_3_NAME_original",
                "LVL_6_NAME_original",
                "match_score",
                "cleansed_target",
                "cleansed_spatial_match",
            ]
        ).dropna(subset=["org_unit_id"])

        current_run.log_info("Appariement des noms CSI terminé.")
        return target_df_matched
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'appariement des noms CSI: {str(e)}")
        raise


def _fuzzy_match_csi(
    csi_level_target_df: pd.DataFrame, iaso_org_unit_tree_df_clean: pd.DataFrame
) -> tuple:
    """Run the fuzzy matcher and dump its working tables to TEMP_PATH for debugging."""
    iaso_org_unit_tree_for_matching = iaso_org_unit_tree_df_clean[
        ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
    ].drop_duplicates()

    target_df_matched, org_unit_tree_check = org_unit_matching(
        csi_level_target_df, iaso_org_unit_tree_for_matching, threshold=50
    )

    target_df_matched_check = target_df_matched[
        [
            "org_unit_id",
            "LVL_3_NAME_original",
            "LVL_6_NAME_original",
            "LVL_3_NAME",
            "LVL_6_NAME",
            "cleansed_target",
            "cleansed_spatial_match",
            "match_score",
        ]
    ].drop_duplicates()

    if not os.path.exists(TEMP_PATH):
        os.makedirs(TEMP_PATH)
    target_df_matched_check.to_csv(
        os.path.join(TEMP_PATH, "target_df_matched_check.csv"), index=False
    )
    org_unit_tree_check = org_unit_tree_check.drop_duplicates()
    org_unit_tree_check.to_csv(
        os.path.join(TEMP_PATH, "org_unit_tree_check.csv"), index=False
    )
    return target_df_matched, org_unit_tree_check


def _apply_manual_csi_corrections(
    target_df_matched: pd.DataFrame, org_unit_tree_check: pd.DataFrame
) -> pd.DataFrame:
    """Apply config.csi_matching_failed's hand-curated overrides on top of the fuzzy
    match, then fall back LVL_6_NAME to its original (unmatched) spelling wherever no
    org_unit_id was ultimately resolved."""
    cols = ["org_unit_id", "LVL_3_NAME", "LVL_6_NAME"]
    for csi_concat_original, csi_concat_correct in csi_matching_failed.items():
        mask = target_df_matched["cleansed_target"] == csi_concat_original

        if csi_concat_correct is None:
            target_df_matched.loc[mask, cols] = None
            continue

        org_unit_tree_row = org_unit_tree_check.loc[
            org_unit_tree_check["cleansed_spatial"] == csi_concat_correct
        ]
        if org_unit_tree_row.empty:
            # This correction was written because the automated match for
            # csi_concat_original was known to be wrong; its intended target
            # no longer exists in the current IASO tree (renamed/removed
            # since). Fall back to unmatched rather than silently keeping
            # that already-distrusted automated match in place.
            current_run.log_warning(
                f"CORRECTION CSI OBSOLÈTE: la correction manuelle prévue pour "
                f"'{csi_concat_original}' pointe vers '{csi_concat_correct}', "
                "qui n'existe plus dans l'arbre IASO actuel."
            )
            current_run.log_warning(
                f"CORRECTION CSI OBSOLÈTE: '{csi_concat_original}' est donc "
                "traité comme non apparié plutôt que de conserver "
                "l'appariement automatique initial, déjà connu comme "
                "incorrect."
            )
            target_df_matched.loc[mask, cols] = None
            continue

        target_df_matched.loc[mask, cols] = org_unit_tree_row[cols].values[0]

    target_df_matched["LVL_6_NAME"] = np.where(
        target_df_matched["org_unit_id"].isna(),
        target_df_matched["LVL_6_NAME_original"],
        target_df_matched["LVL_6_NAME"],
    )
    return target_df_matched


def _report_unmatched_csi(target_df_matched: pd.DataFrame) -> None:
    """Warn about every CSI that still has no org_unit_id, with the target value
    (cible) that won't appear in the dashboard as a result."""
    unmatched_count = int(target_df_matched["org_unit_id"].isna().sum())
    total_count = len(target_df_matched)
    if unmatched_count == 0:
        return

    unmatched_rows = target_df_matched[target_df_matched["org_unit_id"].isna()]
    total_cible_lost = int(unmatched_rows["cible"].sum())
    per_csi = (
        unmatched_rows.groupby(
            ["LVL_3_NAME_original", "LVL_6_NAME_original"], dropna=False
        )["cible"]
        .sum()
        .reset_index()
    )
    current_run.log_warning(
        f"CSI NON APPARIÉS: {unmatched_count} sur {total_count} entrées "
        "n'ont pu être associées à aucune unité d'organisation de l'arbre "
        f"IASO nettoyé, représentant une cible cumulée de "
        f"{total_cible_lost} qui n'apparaîtra pas dans le tableau de bord."
    )
    # One log call per CSI: OpenHexa collapses newlines within a message.
    for district, csi, cible_sum in per_csi.itertuples(index=False):
        current_run.log_warning(
            f"CSI NON APPARIÉ: '{csi}' (district '{district}') n'est pas "
            "enregistré comme une unité d'organisation valide dans IASO; "
            f"sa cible ({int(cible_sum)}, tous âges/rounds/produits "
            "confondus pour cette entrée) ne sera pas remontée dans le "
            "tableau de bord."
        )
    current_run.log_warning(
        "À FAIRE: corrigez l'orthographe de ces CSI dans le fichier source, "
        "ou faites-les créer dans IASO s'il s'agit de formations sanitaires "
        "réellement manquantes."
    )


def match_district_to_org_unit_id(
    district_level_target_df: pd.DataFrame, iaso_org_unit_tree_df_clean: pd.DataFrame
) -> pd.DataFrame:
    """
    Match district names (LVL_3_NAME) to org-unit IDs.

    District labels are reconciled with the official IASO names by fuzzy matching
    (see geo_match), which handles accents, spelling and administrative-suffix
    variants without a per-name dictionary. Every correction is logged, and names
    that stay below the similarity threshold are reported and excluded rather than
    attached to the wrong district.
    """
    current_run.log_info(
        "Appariement des noms de districts aux identifiants des unités organisationnelles..."
    )
    try:
        iaso_org_unit_tree_for_matching = iaso_org_unit_tree_df_clean[
            ["org_unit_id", "LVL_3_NAME"]
        ].drop_duplicates()
        iaso_names = sorted(
            iaso_org_unit_tree_for_matching["LVL_3_NAME"].dropna().unique()
        )
        raw_names = sorted(district_level_target_df["LVL_3_NAME"].dropna().unique())

        mapping, unmatched, inexact = build_district_mapping(raw_names, iaso_names)
        _report_district_mapping_assumptions(inexact, unmatched)

        target_df_matched = district_level_target_df.copy()
        target_df_matched["LVL_3_NAME"] = (
            target_df_matched["LVL_3_NAME"]
            .map(mapping)
            .fillna(target_df_matched["LVL_3_NAME"])
        )
        target_df_matched = target_df_matched.merge(
            iaso_org_unit_tree_for_matching, on=["LVL_3_NAME"], how="left"
        )
        _report_unmatched_districts(target_df_matched)

        current_run.log_info(
            f"Appariement des districts terminé: {len(mapping)} sur "
            f"{len(raw_names)} noms appariés."
        )
        return target_df_matched
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'appariement des noms de districts: {str(e)}"
        )
        raise


def _report_district_mapping_assumptions(inexact: list, unmatched: list) -> None:
    """Log every inexact-but-accepted match, and every district that stayed below
    the similarity threshold and will be excluded rather than guessed at."""
    for raw, iaso_name, sc in inexact:
        current_run.log_warning(
            f"HYPOTHÈSE: le district '{raw}' du fichier ne correspond pas "
            f"exactement à un nom de l'arbre IASO."
        )
        current_run.log_warning(
            f"HYPOTHÈSE (suite): le traitement continue en l'associant à "
            f"'{iaso_name}' (similarité {sc}/100)."
        )

    for raw, best, sc in unmatched:
        current_run.log_warning(
            f"DISTRICT NON APPARIÉ: '{raw}' ne correspond à aucun district de "
            f"l'arbre des unités d'organisation IASO (meilleure proposition: "
            f"'{best}', similarité {sc}/100, insuffisante)."
        )
        current_run.log_warning(
            f"CONSÉQUENCE: '{raw}' n'est pas reconnu comme une unité "
            "d'organisation valide dans IASO; aucune cible ne sera remontée "
            "pour cette unité dans le tableau de bord."
        )
    if unmatched:
        current_run.log_warning(
            f"À FAIRE: corrigez l'orthographe de ces {len(unmatched)} district(s) "
            "dans le fichier source, ou vérifiez qu'ils existent bien dans IASO."
        )


def _report_unmatched_districts(target_df_matched: pd.DataFrame) -> None:
    unmatched_count = int(target_df_matched["org_unit_id"].isna().sum())
    total_count = len(target_df_matched)
    if unmatched_count == 0:
        return
    unmatched_districts = target_df_matched[
        target_df_matched["org_unit_id"].isna()
    ]["LVL_3_NAME"].unique()
    current_run.log_warning(
        f"{unmatched_count} sur {total_count} entrées n'ont pas pu être "
        "appariées à un org_unit_id et seront supprimées des données cibles."
    )
    current_run.log_warning(
        f"Districts concernés: {', '.join(map(str, unmatched_districts))}."
    )


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


def add_region_names(
    target_df: pd.DataFrame, iaso_org_unit_tree_clean_df: pd.DataFrame
) -> pd.DataFrame:
    """Add LVL_2_NAME (region) by merging on org_unit_id."""
    current_run.log_info("Ajout des noms de région aux données de cibles...")
    try:
        regions_df = iaso_org_unit_tree_clean_df[
            ["org_unit_id", "LVL_2_NAME"]
        ].drop_duplicates()
        target_with_regions_df = target_df.merge(
            regions_df, on="org_unit_id", how="left"
        )
        current_run.log_info("Ajout des noms de région terminé.")
        return target_with_regions_df
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'ajout des noms de région: {e}")
        raise


def clean_org_unit_id(
    target_data_combined: pd.DataFrame,
    iaso_org_unit_tree_raw_df: pd.DataFrame,
    iaso_org_unit_tree_clean_df: pd.DataFrame,
) -> pd.DataFrame:
    """Remap org_unit_id to the final LVL_6_UID-based id (one-to-many cleanup)."""
    current_run.log_info(
        "Récupération des identifiants des unités d'organisation (correspondance un-à-plusieurs)..."
    )
    try:
        uid_to_org_id_df_clean = iaso_org_unit_tree_clean_df[
            ["LVL_6_UID", "org_unit_id"]
        ].drop_duplicates()
        uid_to_org_id_df_raw = iaso_org_unit_tree_raw_df.copy()
        uid_to_org_id_df_raw["LVL_6_UID"] = uid_to_org_id_df_raw.groupby("LVL_6_NAME")[
            "LVL_6_UID"
        ].transform("first")
        uid_to_org_id_df_raw = (
            uid_to_org_id_df_raw[["LVL_6_UID", "org_unit_id"]]
            .drop_duplicates()
            .rename(columns={"org_unit_id": "final_org_unit_id"})
        )
        mapping_df = uid_to_org_id_df_clean.merge(
            uid_to_org_id_df_raw, on="LVL_6_UID", how="inner"
        )[["org_unit_id", "final_org_unit_id"]].drop_duplicates()

        target_data_combined = pd.merge(
            target_data_combined, mapping_df, on="org_unit_id", how="left"
        )
        target_data_combined["org_unit_id"] = target_data_combined[
            "final_org_unit_id"
        ].fillna(target_data_combined["org_unit_id"])
        target_data_combined.drop(columns=["final_org_unit_id"], inplace=True)

        current_run.log_info(
            "Récupération des identifiants des unités d'organisation terminée."
        )
        return target_data_combined
    except Exception as e:
        current_run.log_error(
            f"Erreur lors du processus de récupération des identifiants: {e}"
        )
        raise


if __name__ == "__main__":
    process_target_data()
