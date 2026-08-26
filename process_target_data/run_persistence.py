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

When a full read of the existing compiled output (or a from-scratch build from every per-run
file) IS unavoidable, category_columns lets the caller name the low-cardinality columns worth
decoding as category dtype - the same technique build_visualisation_tables/data_cleaning.py
already relies on for this exact file (EXPECTED_STRUCTURE_CATEGORY_COLS), duplicated here rather
than imported per this repo's no-cross-pipeline-imports-at-runtime convention. A plain read (or
concat, or drop_duplicates()) of the full, ~50M-row expected_data_structure with no such
optimization materializes every string column as a full one-Python-str-per-row object array -
large enough on its own to exhaust the pod's memory with no Python-level exception ever raised to
explain why. A real production failure, silently killed mid-run with nothing but the last
log_info line to go on, is what prompted _align_categories/_drop_duplicates_low_memory below.
"""

import glob
import os

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from openhexa.sdk import current_run

from config import OUTPUTS_PATH
from shared_utils import export_to_dataset, load_data, save_file

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
    return df[~existing_index.isin(touched_index)].copy()


def _align_categories(frames: list, cols: list) -> None:
    """Give every frame in `frames` the same category dtype (union of the values actually seen
    across ALL of them) for each column in `cols`, in place.

    Ported from build_visualisation_tables/data_cleaning.py's align_categories_for_merge
    (generalized here from two frames to any number - not imported, per this repo's
    no-cross-pipeline-imports convention). pandas only keeps a concatenated/merged column as
    category dtype in the result if every side is categorical with an IDENTICAL category set -
    if any side is plain object dtype, or the sides' categories differ, the result silently
    falls back to full object-dtype strings, reintroducing the exact multi-GB-per-column blowup
    category_columns exists to avoid. Building categories from the union (not just one side's)
    avoids turning a legitimate value into NaN if the frames' vocabularies don't match exactly.
    """
    for col in cols:
        present = [f for f in frames if col in f.columns]
        if not present:
            continue
        categories = pd.unique(np.concatenate([pd.unique(f[col]) for f in present]))
        cat_dtype = pd.CategoricalDtype(categories=categories)
        for f in present:
            f[col] = f[col].astype(cat_dtype)


def _drop_duplicates_low_memory(df: pd.DataFrame) -> pd.DataFrame:
    """Same result as df.drop_duplicates(), for a frame with several category columns at a
    many-million-row scale where plain .drop_duplicates() was measured elsewhere in this
    codebase (build_visualisation_tables/data_cleaning.py's drop_duplicates_low_memory, ported
    here rather than imported) to transiently spike memory to several times the frame's own
    size. Hashing each categorical column's integer codes instead of the column itself avoids
    that - datetime/numeric columns' duplicate check is unchanged."""
    keys = pd.DataFrame(
        {
            col: (
                df[col].cat.codes
                if isinstance(df[col].dtype, pd.CategoricalDtype)
                else df[col].to_numpy()
            )
            for col in df.columns
        },
        index=df.index,
    )
    return df.loc[~keys.duplicated()]


def _concat_and_dedupe(frames: list, category_columns: list) -> pd.DataFrame:
    """The shared tail of both compile paths below: align categories (if any were requested)
    so the concat doesn't silently undo them, concatenate, deduplicate with whichever strategy
    is safe for the result's dtypes, and decategorize before handing back the result.
    """
    if category_columns:
        present_cols = [
            c for c in category_columns if any(c in f.columns for f in frames)
        ]
        _align_categories(frames, present_cols)
        combined = _drop_duplicates_low_memory(pd.concat(frames, ignore_index=True))
        for col in present_cols:
            combined[col] = combined[col].astype(object)
    else:
        combined = pd.concat(frames, ignore_index=True).drop_duplicates()
    return combined.reset_index(drop=True)


def _read_full_frames(files: list, category_columns: list = None) -> tuple[list, set]:
    """Reads every file in `files` in full, returning (frames, union of their combo keys) -
    skips and warns on any that can't be read rather than failing the whole run. Each frame gets
    category_columns decoded as category dtype right after reading (a cheap in-memory downcast -
    these per-run files are individually much smaller than the compiled output, so the read
    itself isn't the risk here; keeping this dtype from the start is what lets the later
    concat/dedup against a similarly-decoded existing frame stay categorical instead of falling
    back to object dtype)."""
    frames = []
    touched_combos = set()
    for f in files:
        try:
            frame = pd.read_parquet(f)
        except Exception as e:
            current_run.log_warning(
                f"Fichier illisible, ignoré : {os.path.basename(f)} ({e})."
            )
            continue
        if category_columns:
            for col in category_columns:
                if col in frame.columns:
                    frame[col] = frame[col].astype("category")
        frames.append(frame)
        touched_combos |= _combo_keys(frame)
    return frames, touched_combos


def _full_compile(files: list, output_name: str, category_columns: list = None) -> int:
    """Read every file in full and (re)write output_name from scratch - used when there's no
    existing compiled output to be incremental against at all."""
    frames, _ = _read_full_frames(files, category_columns)
    combined = _concat_and_dedupe(frames, category_columns)
    save_file(combined, output_name)
    # export_to_dataset(combined, OUTPUTS_PATH, output_name, include_csv=False)
    return len(combined)


def compile_processed_files(
    folder: str, output_name: str, description: str, category_columns: list = None
) -> int:
    """
    Update `output_name` from every processed file in `folder`, incrementally, and return its
    resulting row count (the only thing pipeline.py's own logging needs - the full dataframe is
    never handed back, so nothing here forces holding it in memory a moment longer than the
    write itself requires).

    See the module docstring for exactly which per-run files get a full read, and what
    category_columns is for.
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
        return _full_compile(files, output_name, category_columns)

    existing_mtime = os.path.getmtime(output_path)
    try:
        existing_keys = _combo_keys(_read_keys_only(output_path))
    except Exception as e:
        current_run.log_warning(
            f"'{output_name}' existant illisible, recompilation complète ({e})."
        )
        return _full_compile(files, output_name, category_columns)

    candidates = []
    for f in files:
        try:
            file_keys = _combo_keys(_read_keys_only(f))
        except Exception as e:
            current_run.log_warning(
                f"Fichier {description} illisible, ignoré : {os.path.basename(f)} ({e})."
            )
            continue
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
    existing = load_data(output_name, categories=category_columns)
    new_frames, touched_combos = _read_full_frames(candidates, category_columns)
    kept = _drop_combos(existing, touched_combos)
    combined = _concat_and_dedupe([kept] + new_frames, category_columns)

    save_file(combined, output_name)
    # export_to_dataset(combined, OUTPUTS_PATH, output_name, include_csv=False)
    return len(combined)
