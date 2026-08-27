"""
combined_target_data: compiling every per-run target file extract_target_data has produced into
one combined dataset.

Every per-run file is classified against combined_target_data with two nested checks - see
`file_is_new`:
  1. identity check on (produit, year, round): unmatched -> the whole file is a new campaign
     never configured before, append it.
  2. content check on identity + cible/campaign_start_date/campaign_end_date: unmatched, but
     identity known -> the campaign was modified (corrected target values or dates) - drop
     combined_target_data's rows for that identity and replace them with the file's current
     contents.
  3. otherwise every row the file has is already reflected in combined_target_data - skip it.
On top of that, `compile_combined_target_data` also prunes any (produit, year, round) combo
present in combined_target_data whose per-run file no longer exists in the processed folder at
all - see `_all_current_combos`. This is what lets deleting a per-run file (e.g. a campaign that
was configured by mistake - wrong round number, wrong product) actually remove it from
combined_target_data on the next run, rather than leaving it stranded forever with nothing left
to trigger an overwrite.
combined_target_data stays small (currently ~561K rows), so no category-dtype handling is
needed here - a plain read/concat/dedup is cheap at this scale (contrast expected_structure.py,
which does need it).

file_is_new reads each side in full and selects cols_to_check in pandas afterward, rather than
passing columns= to pd.read_parquet: on this workspace's mounted storage, a column-projected
read (pd.read_parquet(path, columns=[...])) was observed, repeatedly and reproducibly - including
across a delete-and-recreate of the file in question - to return a schema that didn't match the
same path's actual content (verified independently via pq.ParquetFile(path).schema_arrow and a
full, unprojected pd.read_parquet(path), both of which were reliable throughout). Reading in full
costs more I/O per file but was never observed to be wrong.
"""

import glob
import os

import pandas as pd
import pyarrow.parquet as pq
from openhexa.sdk import current_run

from config import OUTPUTS_PATH
from shared_utils import load_data, save_file

IDENTITY_COLS = ["produit", "year", "round"]
TARGET_CONTENT_COLS = IDENTITY_COLS + [
    "cible",
    "campaign_start_date",
    "campaign_end_date",
]
# campaign_start_date/campaign_end_date are part of the content check (not just cible) so that a
# re-run correcting ONLY the campaign dates - target values unchanged - still gets classified as
# an overwrite; expected_data_structure's period depends on these two columns, not on cible.


def file_is_new(file_path: str, combined_output_path: str, cols_to_check: list) -> bool:
    """Return True if a file in the per-run processed folder has a different combo
    of key cols than the combined output, indicating that this file contains new data
    to either append or overwrite in the combined output. False if the file's key-col combo
    is already reflected in the combined output.

    Reads each side in full, then selects cols_to_check - see the module docstring for why
    this doesn't use pd.read_parquet's columns= projection.

    If combined_output_path's own schema is missing one of cols_to_check, it structurally
    cannot already reflect this file's content for that column - e.g. combined_target_data
    written before campaign_start_date/campaign_end_date existed at all. Treating that as
    "new" (rather than raising trying to select a column that isn't there) is what lets the
    first post-migration file needing those columns actually get folded in and add them,
    instead of every run failing on this check forever."""
    combined_schema_cols = set(pq.ParquetFile(combined_output_path).schema_arrow.names)
    if any(c not in combined_schema_cols for c in cols_to_check):
        return True
    combined_output_cols_check = pd.read_parquet(combined_output_path)[
        cols_to_check
    ].drop_duplicates()
    file_cols_check = pd.read_parquet(file_path)[cols_to_check].drop_duplicates()
    if (
        not file_cols_check.merge(
            combined_output_cols_check, how="left", indicator=True
        )
        .query('_merge == "left_only"')
        .empty
    ):
        return True
    return False


def _missing_columns(file_path: str, cols: list) -> list:
    """Which of `cols` this parquet file's own schema doesn't have - a cheap metadata check,
    no row data read."""
    schema_cols = set(pq.ParquetFile(file_path).schema_arrow.names)
    return [c for c in cols if c not in schema_cols]


def classify_files(files: list, output_path: str) -> tuple[list, list]:
    """Buckets every per-run file into (new, overwrite) against output_path, using
    file_is_new's two nested checks. A file in neither list needs no further processing -
    everything it has is already reflected in output_path.

    A file missing one of TARGET_CONTENT_COLS gets its own clear warning rather than the
    generic "unreadable" one below - and is skipped unconditionally, checked before the
    identity check rather than only in its "overwrite" branch: that's not a corrupt file,
    it's one saved by a version of extract_target_data that predates choix_campagne/
    campaign_start_date/campaign_end_date. A file like this can just as easily be a genuinely
    NEW campaign as a modified one - and appending a new-but-columnless file straight into
    combined_target_data would leave those columns NaN for its rows, which
    generate_expected_data_structure's day-by-day period explosion cannot handle (NaN has no
    day count). Silently treating either case as unreadable would also drop a real content
    change (e.g. a date correction) with no actionable signal that anything needs redoing."""
    new_files, overwrite_files = [], []
    for f in files:
        try:
            missing = _missing_columns(f, TARGET_CONTENT_COLS)
            if missing:
                current_run.log_warning(
                    f"'{os.path.basename(f)}' ignoré pour la détection de modification : "
                    f"colonne(s) manquante(s) {missing}. Ce fichier a probablement été "
                    "produit par une version antérieure d'extract_target_data, avant "
                    "l'ajout de choix_campagne/campaign_start_date/campaign_end_date. Si "
                    "cette campagne a été modifiée depuis (dates ou cibles corrigées), "
                    "relancez extract_target_data pour cette campagne (avec l'option "
                    "d'écrasement activée) pour régénérer ce fichier avec les colonnes "
                    "attendues, puis relancez ce pipeline."
                )
                continue
            if file_is_new(f, output_path, IDENTITY_COLS):
                new_files.append(f)
            elif file_is_new(f, output_path, TARGET_CONTENT_COLS):
                overwrite_files.append(f)
            # else: every row this file has is already reflected in output_path - ignore it.
        except Exception as e:
            current_run.log_warning(
                f"Fichier illisible, ignoré : {os.path.basename(f)} ({e})."
            )
    return new_files, overwrite_files


def output_path(output_name: str) -> str:
    return os.path.join(OUTPUTS_PATH, f"{output_name}.parquet")


def _combo_keys(df: pd.DataFrame) -> set:
    """The distinct (produit, year, round) triples a dataframe covers."""
    return {
        (str(p), int(y), str(r))
        for p, y, r in df[IDENTITY_COLS].drop_duplicates().itertuples(index=False)
    }


def _drop_combos(df: pd.DataFrame, combos: set) -> pd.DataFrame:
    """Vectorized removal of every row whose (produit, year, round) is in `combos` - avoids a
    row-wise Python-level .apply() over what can be a many-row dataframe."""
    if not combos or df.empty:
        return df
    keys = df[IDENTITY_COLS].copy()
    keys["produit"] = keys["produit"].astype(str)
    keys["year"] = keys["year"].astype("int64")
    keys["round"] = keys["round"].astype(str)
    existing_index = pd.MultiIndex.from_frame(keys)
    touched_index = pd.MultiIndex.from_frame(
        pd.DataFrame(sorted(combos), columns=IDENTITY_COLS)
    )
    return df[~existing_index.isin(touched_index)]


def _read_full_frames(files: list) -> list:
    """Reads every file in `files` in full - skips and warns on any that can't be read rather
    than failing the whole run."""
    frames = []
    for f in files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception as e:
            current_run.log_warning(
                f"Fichier illisible, ignoré : {os.path.basename(f)} ({e})."
            )
    return frames


def _all_current_combos(files: list) -> set | None:
    """The union of (produit, year, round) combos covered by every per-run file that currently
    exists in the processed folder - including old-format files missing TARGET_CONTENT_COLS,
    which classify_files skips for new/overwrite classification but which are still legitimate
    campaigns, just not yet migrated. Anything in combined_target_data that ISN'T in this set
    has no file backing it anymore - see compile_combined_target_data's pruning step.

    Returns None, instead of a possibly-incomplete set, if any file fails to read - a combo
    missing only because its file happened to fail to read this run must never be mistaken for
    a combo whose file was actually deleted, or pruning would delete data that's still there."""
    combos = set()
    for f in files:
        try:
            combos |= _combo_keys(pd.read_parquet(f))
        except Exception as e:
            current_run.log_warning(
                f"Fichier illisible, ignoré lors de la vérification des combinaisons "
                f"obsolètes : {os.path.basename(f)} ({e}). Suppression des combinaisons "
                "obsolètes désactivée pour cette exécution par précaution."
            )
            return None
    return combos


def compile_combined_target_data(
    folder: str, output_name: str, description: str
) -> tuple[int, bool]:
    """
    Update `output_name` from every processed file in `folder` and return
    (resulting row count, whether anything actually changed) - the second value is what lets
    process_target_data skip rebuilding expected_data_structure on a no-op run.

    Besides folding in new/overwritten files (see classify_files), this also prunes any
    (produit, year, round) combo already in `output_name` whose per-run file has since been
    deleted from `folder` - e.g. a campaign configured by mistake (wrong round, wrong product)
    that was cleaned up at the source. Deleting the file is the whole action needed; this just
    reconciles the compiled output with the processed folder's actual current content on the
    next run.
    """
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"Aucun fichier {description} trouvé dans {folder}.")

    path = output_path(output_name)
    if not os.path.exists(path):
        current_run.log_info(
            f"Compilation initiale de '{output_name}' à partir de {len(files)} fichier(s) "
            f"{description}..."
        )
        frames = _read_full_frames(files)
        combined = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates()
            .reset_index(drop=True)
        )
        save_file(combined, output_name)
        return len(combined), True

    new_files, overwrite_files = classify_files(files, path)
    existing = load_data(output_name)
    current_combos = _all_current_combos(files)
    orphaned_combos = (
        set() if current_combos is None else _combo_keys(existing) - current_combos
    )

    if not new_files and not overwrite_files and not orphaned_combos:
        current_run.log_info(
            f"'{output_name}' déjà à jour : aucun des {len(files)} fichier(s) {description} "
            "ne contient de donnée nouvelle ou modifiée, et aucune combinaison obsolète à "
            "supprimer."
        )
        return len(existing), False

    if orphaned_combos:
        current_run.log_warning(
            f"{len(orphaned_combos)} combinaison(s) produit/année/round présentes dans "
            f"'{output_name}' n'ont plus de fichier correspondant dans {folder} et sont "
            f"supprimées : {sorted(orphaned_combos)}."
        )

    current_run.log_info(
        f"Mise à jour de '{output_name}' : {len(new_files)} fichier(s) {description} "
        f"nouveau(x), {len(overwrite_files)} modifié(s), {len(orphaned_combos)} combinaison(s) "
        f"obsolète(s) supprimée(s), sur {len(files)} fichier(s)."
    )
    new_frames = _read_full_frames(new_files + overwrite_files)
    touched_combos = orphaned_combos.copy()
    for frame in new_frames:
        touched_combos |= _combo_keys(frame)

    kept = _drop_combos(existing, touched_combos)
    del existing
    combined = (
        pd.concat([kept] + new_frames, ignore_index=True)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    save_file(combined, output_name)
    return len(combined), True
