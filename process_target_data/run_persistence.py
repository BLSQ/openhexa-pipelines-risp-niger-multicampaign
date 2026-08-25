"""
Compiling the per-run files extract_target_data has produced into one combined dataset.

Duplicate detection, superseded-slice removal, and the run-slug naming scheme belong to
extract_target_data (see that pipeline's own run_persistence.py) - this pipeline never saves a
per-run file itself, so it has no use for them.

Incremental, not from-scratch: re-reading and re-concatenating every per-run file ever produced
(expected_data_structure alone can run to ~50M rows) got slower and more memory-hungry with
every new campaign, and made this pipeline's one large write more exposed to transient
infrastructure failures (a real "stale file handle" write error hit exactly this kind of large
recompile - see shared_utils.save_file's own retry for the write side of that).

A per-run file is read in full only when it's actually needed:
  - it contains at least one (year, produit, round) combination not yet present in the existing
    compiled output - checked via a cheap, columns-only read of just those three columns on
    both the per-run file and the existing output, never the many other (often wide) columns
    either one carries; or
  - its own mtime is newer than the existing output's last write - catching
    extract_target_data's overwrite mode replacing an already-present combination with fresher
    data for the exact same key, which a key-only comparison alone can't tell apart from
    "already up to date" (the key hasn't changed, only the values behind it have).
Everything else is trusted as-is and never read at all, key columns included. If nothing
qualifies, the read/concat/write is skipped entirely and the existing row count (straight from
parquet metadata, not a data read) is returned.

No separate manifest file to keep in sync: the existing compiled output's own mtime is the only
reference point needed, and extract_target_data's own invariant - overwrite mode always replaces
a combination via a new file, it never removes one without writing its replacement elsewhere -
means there's nothing else that would need remembering across runs.
"""

import glob
import os

import pandas as pd
import pyarrow.parquet as pq
from openhexa.sdk import current_run

from config import OUTPUTS_PATH
from shared_utils import export_to_dataset, save_file

COMBO_COLUMNS = ["year", "produit", "round"]


def _output_path(output_name: str) -> str:
    return os.path.join(OUTPUTS_PATH, f"{output_name}.parquet")


def _existing_row_count(output_name: str) -> int:
    """Row count straight from parquet metadata - never reads or materializes any actual row
    data. Used only for the "nothing to do" fast path."""
    return pq.ParquetFile(_output_path(output_name)).metadata.num_rows


def _read_keys_only(path: str) -> pd.DataFrame:
    """Just the (year, produit, round) columns - cheap even on a many-million-row / many-column
    file, since parquet is columnar and this never touches any other column."""
    return pd.read_parquet(path, columns=COMBO_COLUMNS)


def _combo_keys(df: pd.DataFrame) -> set:
    """The distinct (year, produit, round) triples a dataframe covers - works whether `df` is a
    full read or a keys-only read, since it selects COMBO_COLUMNS itself."""
    return {
        (int(y), str(p), str(r))
        for y, p, r in df[COMBO_COLUMNS].drop_duplicates().itertuples(index=False)
    }


def _drop_combos(df: pd.DataFrame, combos: set) -> pd.DataFrame:
    """Vectorized removal of every row whose (year, produit, round) is in `combos` - avoids a
    row-wise Python-level .apply() over what can be a many-million-row dataframe."""
    if not combos or df.empty:
        return df
    keys = df[COMBO_COLUMNS].copy()
    keys["year"] = keys["year"].astype("int64")
    keys["produit"] = keys["produit"].astype(str)
    keys["round"] = keys["round"].astype(str)
    existing_index = pd.MultiIndex.from_frame(keys)
    touched_index = pd.MultiIndex.from_frame(
        pd.DataFrame(sorted(combos), columns=COMBO_COLUMNS)
    )
    return df[~existing_index.isin(touched_index)]


def _read_full_frames(files: list) -> tuple[list, set]:
    """Reads every file in `files` in full, returning (frames, union of their combo keys) -
    skips and warns on any that can't be read rather than failing the whole run."""
    frames = []
    touched_combos = set()
    for f in files:
        try:
            frame = pd.read_parquet(f)
        except Exception as e:
            current_run.log_warning(f"Fichier illisible, ignoré : {os.path.basename(f)} ({e}).")
            continue
        frames.append(frame)
        touched_combos |= _combo_keys(frame)
    return frames, touched_combos


def _full_compile(files: list, output_name: str) -> int:
    """Read every file in full and (re)write output_name from scratch - used when there's no
    existing compiled output to be incremental against at all."""
    frames, _ = _read_full_frames(files)
    combined = pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
    save_file(combined, output_name)
    export_to_dataset(combined, OUTPUTS_PATH, output_name)
    return len(combined)


def compile_processed_files(folder: str, output_name: str, description: str) -> int:
    """
    Update `output_name` from every processed file in `folder`, incrementally, and return its
    resulting row count (the only thing pipeline.py's own logging needs - the full dataframe is
    never handed back, so nothing here forces holding it in memory a moment longer than the
    write itself requires).

    See the module docstring for exactly which per-run files get a full read and why.
    """
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"Aucun fichier {description} trouvé dans {folder}.")

    output_path = _output_path(output_name)
    if not os.path.exists(output_path):
        current_run.log_info(
            f"Compilation initiale de '{output_name}' à partir de {len(files)} fichier(s) "
            f"{description}..."
        )
        return _full_compile(files, output_name)

    existing_mtime = os.path.getmtime(output_path)
    try:
        existing_keys = _combo_keys(_read_keys_only(output_path))
    except Exception as e:
        current_run.log_warning(
            f"'{output_name}' existant illisible, recompilation complète ({e})."
        )
        return _full_compile(files, output_name)

    candidates = []
    for f in files:
        try:
            file_keys = _combo_keys(_read_keys_only(f))
        except Exception as e:
            current_run.log_warning(
                f"Fichier {description} illisible, ignoré : {os.path.basename(f)} ({e})."
            )
            continue
        # New data (a combination not yet reflected at all), OR this exact file changed since
        # the last compile (extract_target_data's overwrite mode reusing an already-present
        # combination with fresher data - same key, different values, which the key check
        # above alone can't distinguish from "nothing to do").
        if not file_keys <= existing_keys or os.path.getmtime(f) > existing_mtime:
            candidates.append(f)

    if not candidates:
        current_run.log_info(
            f"'{output_name}' déjà à jour : aucun des {len(files)} fichier(s) {description} "
            "ne contient de donnée nouvelle ou modifiée."
        )
        return _existing_row_count(output_name)

    current_run.log_info(
        f"Mise à jour incrémentale de '{output_name}' : {len(candidates)} fichier(s) "
        f"{description} nouveau(x) ou modifié(s) sur {len(files)}."
    )
    existing = pd.read_parquet(output_path)
    new_frames, touched_combos = _read_full_frames(candidates)
    kept = _drop_combos(existing, touched_combos)
    combined = (
        pd.concat([kept] + new_frames, ignore_index=True)
        .drop_duplicates()
        .reset_index(drop=True)
    )

    save_file(combined, output_name)
    export_to_dataset(combined, OUTPUTS_PATH, output_name)
    return len(combined)
