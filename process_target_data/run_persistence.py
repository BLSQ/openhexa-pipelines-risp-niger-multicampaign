"""
Processed-file bookkeeping: detecting whether a run's (year, produit, round)
combination already exists, locating and removing superseded slices in overwrite
mode, and compiling the per-run files in a folder into one combined dataset.
Moved from pipeline.py - a theme distinct from org-unit matching, expected-data-
structure building, and the spreadsheet-parsing engine.
"""

import glob
import os
import re
import unicodedata

import pandas as pd
from openhexa.sdk import current_run

from config import OUTPUTS_PATH
from shared_utils import export_to_dataset, save_file


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
    # Inlined pipeline.fail_run's exact behavior (not imported, to avoid a
    # circular import back into pipeline.py): log each truthy line, then abort.
    for line in lines:
        if line:
            current_run.log_error(line)
    raise ValueError(" ".join(str(ln) for ln in lines if ln))


def run_slug(year: int, products: list, rounds: list) -> str:
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
