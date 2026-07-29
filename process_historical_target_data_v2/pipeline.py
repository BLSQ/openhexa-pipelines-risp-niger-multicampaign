"""
Historical vaccination target-data pipeline (Niger, multi-campagne).

The pipeline processes ONE uploaded spreadsheet, driven by four parameters:

    input_file : the .xlsx to process (uploaded File).
    products   : the product(s) the campaign delivers (multi-select).
    year       : the campaign year.
    rounds     : the round number(s) (multi-select; each -> "round <n>").

The generic engine (target_import) auto-identifies the file's structure, extracts
the base age brackets, builds the requested products, stamps the year and rounds,
then the unchanged org-unit matching stage attaches org_unit_id and region.

Output columns:
    LVL_3_NAME, cible, age, year, org_unit_id, round, produit, LVL_6_NAME, LVL_2_NAME
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
    OUTPUTS_PATH,
    PROCESSED_TARGETS_PATH,
    TARGETS_HISTORICAL_PATH,
    TEMP_PATH,
    csi_matching_failed,
)
from geo_match import build_district_mapping
from shared_utils import export_to_dataset, load_data, save_file
from target_import import TargetImportError, import_target_file
from utils import org_unit_matching


@pipeline(
    "process_historical_target_data_v2",
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
    "products",
    name="Produit(s) de la campagne",
    help="Un ou plusieurs produits délivrés par la campagne.",
    type=str,
    # Literal list (OpenHexa parses choices statically). Keep in sync with
    # layouts.PRODUCT_CHOICES / PRODUCT_DEFS.
    choices=[
        "vaccin polio",
        "rougeole",
        "vitamine A",
        "albendazole",
        "méningite",
        "tcv",
        "fièvre jaune",
    ],
    multiple=True,
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
def process_historical_target_data_v2(
    input_file, products, year: int, rounds, overwrite_existing: bool = False
):
    """Import, reshape, match and export a single target-data spreadsheet."""
    check_year(int(year))

    # Fail fast (before any heavy work) if these targets already exist and the
    # overwrite toggle is off.
    check_for_existing_slices(
        int(year), list(products), list(rounds), bool(overwrite_existing)
    )

    iaso_org_unit_tree_df = load_data("iaso_org_unit_tree_raw")
    iaso_org_unit_tree_df_clean = load_data("iaso_org_unit_tree_clean")

    # 1. Auto-detecting structural import -> tidy long frame (age x product x round).
    file_path = resolve_input_file(input_file)
    try:
        tidy = import_target_file(file_path, list(products), int(year), list(rounds))
    except TargetImportError:
        # The engine already logged a detailed, actionable message via log_error.
        current_run.log_error(
            "Traitement interrompu: le fichier de cibles n'a pas pu être interprété. "
            "Consultez le message d'erreur ci-dessus, corrigez le fichier ou les "
            "paramètres, puis relancez le pipeline."
        )
        raise
    except Exception as e:
        # One log call per line: OpenHexa renders a multi-line message as a single
        # collapsed block, so separate calls are what produce readable output.
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

    # 2. Org-unit matching (unchanged). District files have no LVL_6_NAME column.
    if "LVL_6_NAME" in tidy.columns:
        matched = match_csi_to_org_unit_id(tidy, iaso_org_unit_tree_df_clean)
    else:
        matched = match_district_to_org_unit_id(tidy, iaso_org_unit_tree_df_clean)
        matched = matched.dropna(subset=["org_unit_id"])

    # 3. Region names + one-to-many org-unit cleanup (unchanged).
    matched = add_region_names(matched, iaso_org_unit_tree_df_clean)
    matched = clean_org_unit_id(
        matched, iaso_org_unit_tree_df, iaso_org_unit_tree_df_clean
    )

    # 4. Save this run's output in the "historical targets processed" folder, then
    #    rebuild the combined historical dataset from ALL files in that folder.
    #    In overwrite mode the superseded slices are removed only now, i.e. after
    #    the replacement data has been produced successfully.
    if overwrite_existing:
        overlaps = find_overlapping_slices(
            run_combinations(int(year), list(products), list(rounds))
        )
        if overlaps:
            remove_slices_from_processed_files(overlaps)

    save_processed_output(matched, int(year), list(products), list(rounds))
    combined = rebuild_combined_history()
    current_run.log_info(
        f"Données historiques combinées: {len(combined)} lignes "
        f"({len(matched)} produites lors de cette exécution)."
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


def find_overlapping_slices(combos: set) -> dict:
    """
    Look for already-processed data covering any of ``combos``.

    Returns {processed file path -> sorted list of overlapping (year, produit,
    round) slices}. The 'historical targets processed' folder is the source of
    truth: it is what combined_historical_target_data is rebuilt from.
    """
    overlaps = {}
    for path in sorted(glob.glob(os.path.join(PROCESSED_TARGETS_PATH, "*.parquet"))):
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

    Without the overwrite toggle, any overlap aborts the run and the conflicting
    configuration(s) are reported. With the toggle on, the overlapping slices are
    removed from the previously processed files so this run replaces them.
    """
    combos = run_combinations(year, products, rounds)
    overlaps = find_overlapping_slices(combos)
    if not overlaps:
        return

    conflicts = sorted({s for slices in overlaps.values() for s in slices})
    if overwrite_existing:
        # Only announce here. The actual removal happens once the new data has
        # been produced successfully, so a later failure cannot destroy existing
        # targets without replacing them.
        current_run.log_warning(
            f"ÉCRASEMENT ACTIVÉ: {len(conflicts)} combinaison(s) produit / année / "
            "round déjà présente(s) seront remplacées par les données de ce fichier."
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
        "combined_historical_target_data:",
    ]
    for path, slices in overlaps.items():
        lines.append(f"> Configuration déjà enregistrée: '{os.path.basename(path)}'")
        for y, p, r in slices:
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


def save_processed_output(
    matched: pd.DataFrame, year: int, products: list, rounds: list
) -> str:
    """Persist this run's processed output as its own parquet file in the
    'historical targets processed' folder. Returns the file name (without ext)."""
    if not os.path.exists(PROCESSED_TARGETS_PATH):
        os.makedirs(PROCESSED_TARGETS_PATH)
    name = _run_slug(year, products, rounds)
    file_path = os.path.join(PROCESSED_TARGETS_PATH, f"{name}.parquet")
    try:
        matched.to_parquet(file_path, index=False)
        current_run.log_info(f"Sortie de l'exécution enregistrée: {file_path}")
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'enregistrement de la sortie: {e}")
        raise
    return name


def rebuild_combined_history() -> pd.DataFrame:
    """
    Rebuild "combined_historical_target_data" by concatenating EVERY processed
    output file in the 'historical targets processed' folder.

    Exact duplicate rows (a slice covered by more than one run configuration) are
    collapsed; distinct runs are all preserved.
    """
    files = sorted(glob.glob(os.path.join(PROCESSED_TARGETS_PATH, "*.parquet")))
    if not files:
        raise FileNotFoundError(
            f"Aucun fichier traité trouvé dans {PROCESSED_TARGETS_PATH}."
        )
    current_run.log_info(
        f"Reconstruction du jeu combiné à partir de {len(files)} fichier(s) traité(s)..."
    )
    frames = [pd.read_parquet(f) for f in files]
    combined = (
        pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
    )

    combined_name = "combined_historical_target_data"
    save_file(combined, combined_name)
    export_to_dataset(combined, OUTPUTS_PATH, combined_name)
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

        for csi_concat_original, csi_concat_correct in csi_matching_failed.items():
            if csi_concat_correct is None:
                mask = target_df_matched["cleansed_target"] == csi_concat_original
                target_df_matched.loc[mask, "org_unit_id"] = None
                target_df_matched.loc[mask, "LVL_3_NAME"] = None
                target_df_matched.loc[mask, "LVL_6_NAME"] = None
                continue

            org_unit_tree_row = org_unit_tree_check.loc[
                org_unit_tree_check["cleansed_spatial"] == csi_concat_correct
            ]
            if org_unit_tree_row.empty:
                continue

            lvl_3_name_correct = org_unit_tree_row["LVL_3_NAME"].values[0]
            lvl_6_name_correct = org_unit_tree_row["LVL_6_NAME"].values[0]
            org_unit_id_correct = org_unit_tree_row["org_unit_id"].values[0]
            mask = target_df_matched["cleansed_target"] == csi_concat_original
            target_df_matched.loc[mask, "org_unit_id"] = org_unit_id_correct
            target_df_matched.loc[mask, "LVL_3_NAME"] = lvl_3_name_correct
            target_df_matched.loc[mask, "LVL_6_NAME"] = lvl_6_name_correct

        target_df_matched["LVL_6_NAME"] = np.where(
            target_df_matched["org_unit_id"].isna(),
            target_df_matched["LVL_6_NAME_original"],
            target_df_matched["LVL_6_NAME"],
        )

        unmatched_count = int(target_df_matched["org_unit_id"].isna().sum())
        total_count = len(target_df_matched)
        if unmatched_count > 0:
            unmatched_rows = target_df_matched[target_df_matched["org_unit_id"].isna()]
            unmatched_pairs = (
                unmatched_rows[["LVL_3_NAME_original", "LVL_6_NAME_original"]]
                .drop_duplicates()
                .itertuples(index=False)
            )
            current_run.log_warning(
                f"CSI NON APPARIÉS: {unmatched_count} sur {total_count} entrées "
                "n'ont pu être associées à aucune unité d'organisation de l'arbre "
                "IASO nettoyé."
            )
            # One log call per CSI: OpenHexa collapses newlines within a message.
            for district, csi in unmatched_pairs:
                current_run.log_warning(
                    f"CSI NON APPARIÉ: '{csi}' (district '{district}') n'est pas "
                    "enregistré comme une unité d'organisation valide dans IASO; "
                    "aucune cible ne sera remontée pour cette unité dans le "
                    "tableau de bord."
                )
            current_run.log_warning(
                "À FAIRE: corrigez l'orthographe de ces CSI dans le fichier source, "
                "ou faites-les créer dans IASO s'il s'agit de formations sanitaires "
                "réellement manquantes."
            )

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

        target_df_matched = district_level_target_df.copy()
        target_df_matched["LVL_3_NAME"] = (
            target_df_matched["LVL_3_NAME"]
            .map(mapping)
            .fillna(target_df_matched["LVL_3_NAME"])
        )
        target_df_matched = target_df_matched.merge(
            iaso_org_unit_tree_for_matching, on=["LVL_3_NAME"], how="left"
        )

        unmatched_count = int(target_df_matched["org_unit_id"].isna().sum())
        total_count = len(target_df_matched)
        if unmatched_count > 0:
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
    process_historical_target_data_v2()
