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

import numpy as np
import pandas as pd
from openhexa.sdk import File, current_run, parameter, pipeline

from config import (
    OUTPUTS_PATH,
    PROCESSED_TARGETS_PATH,
    TARGETS_HISTORICAL_PATH,
    TEMP_PATH,
    csi_matching_failed,
    district_name_map,
)
from shared_utils import export_to_dataset, load_data, save_file
from target_import import import_target_file
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
    type=int,
    required=True,
)
@parameter(
    "rounds",
    name="Numéro(s) de round",
    help="Un ou plusieurs numéros de round (ex: 1, 2).",
    type=int,
    multiple=True,
    required=True,
)
def process_historical_target_data_v2(input_file, products, year: int, rounds):
    """Import, reshape, match and export a single target-data spreadsheet."""
    iaso_org_unit_tree_df = load_data("iaso_org_unit_tree_raw")
    iaso_org_unit_tree_df_clean = load_data("iaso_org_unit_tree_clean")

    # 1. Generic structural import -> tidy long frame (age x product x round).
    file_path = resolve_input_file(input_file)
    tidy = import_target_file(file_path, list(products), int(year), list(rounds))

    # 2. Org-unit matching (unchanged). District files have no LVL_6_NAME column.
    if "LVL_6_NAME" in tidy.columns:
        matched = match_csi_to_org_unit_id(tidy, iaso_org_unit_tree_df_clean)
    else:
        tidy["LVL_3_NAME"] = (
            tidy["LVL_3_NAME"].map(district_name_map).fillna(tidy["LVL_3_NAME"])
        )
        matched = match_district_to_org_unit_id(tidy, iaso_org_unit_tree_df_clean)
        matched = matched.dropna(subset=["org_unit_id"])

    # 3. Region names + one-to-many org-unit cleanup (unchanged).
    matched = add_region_names(matched, iaso_org_unit_tree_df_clean)
    matched = clean_org_unit_id(
        matched, iaso_org_unit_tree_df, iaso_org_unit_tree_df_clean
    )

    # 4. Save this run's output in the "historical targets processed" folder, then
    #    rebuild the combined historical dataset from ALL files in that folder.
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

        unmatched_count = target_df_matched["org_unit_id"].isna().sum()
        total_count = len(target_df_matched)
        if unmatched_count > 0:
            unmatched_csis = target_df_matched[target_df_matched["org_unit_id"].isna()][
                "LVL_6_NAME_original"
            ].unique()
            current_run.log_warning(
                f"{unmatched_count} sur {total_count} entrées n'ont pas pu être appariés à un org_unit_id. "
                f"CSIs non appariés : {', '.join(map(str, unmatched_csis))}. "
                "Un appariement manuel est nécessaire pour ces entrées."
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
    """Match district names (LVL_3_NAME) to org-unit IDs via a simple merge."""
    current_run.log_info(
        "Appariement des noms de districts aux identifiants des unités organisationnelles..."
    )
    try:
        iaso_org_unit_tree_for_matching = iaso_org_unit_tree_df_clean[
            ["org_unit_id", "LVL_3_NAME"]
        ].drop_duplicates()

        target_df_matched = district_level_target_df.merge(
            iaso_org_unit_tree_for_matching, on=["LVL_3_NAME"], how="left"
        )

        unmatched_count = target_df_matched["org_unit_id"].isna().sum()
        total_count = len(target_df_matched)
        if unmatched_count > 0:
            unmatched_districts = target_df_matched[
                target_df_matched["org_unit_id"].isna()
            ]["LVL_3_NAME"].unique()
            current_run.log_warning(
                f"{unmatched_count} sur {total_count} entrées n'ont pas pu être appariés à un org_unit_id. "
                f"Districts non appariés : {', '.join(map(str, unmatched_districts))}. "
                "Ces entrées seront supprimées des données cibles."
            )

        current_run.log_info("Appariement des noms de districts terminé.")
        return target_df_matched
    except Exception as e:
        current_run.log_error(
            f"Erreur lors de l'appariement des noms de districts: {str(e)}"
        )
        raise


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
