"""
Generic, cross-theme data-cleaning helpers and config used by more than one output
table's build logic - not specific to coverage/stocks/supervision/communications.
"""

import inspect

import numpy as np
import pandas as pd
from openhexa.sdk import current_run

# Month names for add_month_column below.
months_mapping_dict = {
    1: "Janvier",
    2: "Février",
    3: "Mars",
    4: "Avril",
    5: "Mai",
    6: "Juin",
    7: "Juillet",
    8: "Août",
    9: "Septembre",
    10: "Octobre",
    11: "Novembre",
    12: "Décembre",
}

# expected_data_structure can run to ~50M rows - see pipeline._load_expected_structure
# and align_categories_for_merge below for why these two lists matter for memory.
# Lives here (not pipeline.py) because both coverage.py and completeness.py also
# need EXPECTED_STRUCTURE_CATEGORY_COLS for their own merges, and pipeline.py
# already imports FROM those theme files - putting it there would be circular.
EXPECTED_STRUCTURE_COLS = [
    "org_unit_id",
    "year",
    "round",
    "period",
    "age",
    "sexe",
    "produit",
    "vaccination_status",
    "site",
    "choix_campagne",
    "order_day",
]
EXPECTED_STRUCTURE_CATEGORY_COLS = [
    "round",
    "age",
    "sexe",
    "produit",
    "vaccination_status",
    "site",
    "choix_campagne",
]

# campaign name cleaning: not currently referenced by any function in this
# pipeline (grep confirms) - kept for parity with process_iaso_form_data's
# identically-named, actually-used constant, in case it was meant to be wired up
# here too. Flagged, not silently dropped.
campaign_name_cleaning_dict = {
    "men5_tcv": "men5 tcv",
}

# iaso df: same status as campaign_name_cleaning_dict above - not currently
# referenced anywhere in this pipeline.
iaso_df_common_cols = [
    "uuid",
    "form_id",
    "created_at",
    "period",
    "statuschoix_campagne",
    "org_unit_id",
]


def melt_campaign_columns(
    source_df: pd.DataFrame,
    campaign_map: dict,
    id_vars: list,
    tag_col: str,
    var_name: str = "category",
    value_name: str = "value",
    warn_on_missing: bool = False,
) -> pd.DataFrame:
    """
    Melt each campaign's wide IASO columns into one long-format dataframe, tagging
    each melted row with its campaign name.

    Shared by every "one section of wide columns per campaign" table (coverage,
    stocks, supervision, communications) - they differ only in id_vars/campaign_map/
    column names, not in this mechanic, so it was duplicated four times before.

    Args:
        source_df: the wide dataframe to melt from (e.g. combined_iaso_data).
        campaign_map: {campaign_name: [candidate column names]}.
        id_vars: columns to keep as-is (not melted).
        tag_col: name of the column to stamp with the campaign name.
        var_name: name of the melted "which column" column.
        value_name: name of the melted "value" column.
        warn_on_missing: log a warning (and skip) when a campaign has none of its
            columns in source_df, instead of silently skipping it.

    Returns:
        One concatenated long-format dataframe across all campaigns in campaign_map.
    """
    frames = []
    for campaign_name, cols in campaign_map.items():
        valid_cols = list({c for c in cols if c in source_df.columns})
        if not valid_cols:
            if warn_on_missing:
                current_run.log_warning(
                    f"Aucune colonne valide trouvée pour la campagne '{campaign_name}'. "
                    "Cette campagne sera ignorée."
                )
            continue
        melted = pd.melt(
            source_df[id_vars + valid_cols].fillna(0),
            id_vars=id_vars,
            value_vars=valid_cols,
            var_name=var_name,
            value_name=value_name,
        )
        melted[tag_col] = campaign_name
        frames.append(melted)
    return pd.concat(frames, ignore_index=True)


def drop_zero_values(
    df: pd.DataFrame, value_col: str, description: str
) -> pd.DataFrame:
    """
    Drop rows where value_col == 0, warning with the count and proportion dropped.

    Shared by coverage/stocks/supervision/communications, which each had their own
    copy of this - all four had the same bug (fixed here): the warning printed
    len(the boolean mask), i.e. always the FULL row count, not the actual number of
    zero-value rows being dropped (the percentage was already computed correctly,
    only the absolute count in the message text was wrong).

    Args:
        df: dataframe to filter.
        value_col: column to check for zero.
        description: human-readable label for the warning message, e.g.
            "informations du nombre de cas vaccinés".
    """
    is_zero = df[value_col] == 0
    count_zero = int(is_zero.sum())
    if count_zero > 0:
        current_run.log_warning(
            f"{count_zero} entrées ({count_zero / len(df):.2%}) liées aux "
            f"{description} ont été supprimées car aucune valeur n'a été attribuée."
        )
    return df[~is_zero].copy()


def new_cols(
    df: pd.DataFrame, pattern: str, value_col: str, function_list=None
) -> pd.DataFrame:
    """
    Adds new columns to the dataframe based on functions whose names contain a specific pattern.

    Parameters:
        df (pd.DataFrame): The input dataframe to modify.
        pattern (str): A common string in function names to identify relevant functions.
        value_col (str): The name of the column in the dataframe to provide values for the functions.
        function_list (list, optional): A list of functions to use for creating new columns.
                                         If None, functions will be identified based on the pattern.

    Returns:
        df (pd.DataFrame): The modified dataframe with new columns added.
    """
    if not function_list:
        function_list = [
            obj
            for name, obj in globals().items()
            if inspect.isfunction(obj) and pattern in name
        ]

    for fun in function_list:
        new_colname = fun.__name__.rsplit("_", maxsplit=1)[0]
        df.loc[:, new_colname] = df[value_col].map(fun)

    return df


def drop_duplicates_low_memory(df: pd.DataFrame) -> pd.DataFrame:
    """Same result as df.drop_duplicates(), for a frame with several category
    columns at a many-million-row scale where plain .drop_duplicates() was
    measured to transiently spike memory to several times the frame's own size
    (e.g. ~8GB -> ~14GB for expected_data_structure's coverage-merge projection) -
    substantially more than either the input or the (barely smaller) output ever
    need. Root cause not fully pinned down, but working around it is cheap and
    safe: hash on each categorical column's integer codes instead of the column
    itself, since it's the categorical columns specifically that trigger the
    blowup - datetime/numeric columns duplicate check unchanged."""
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


def align_categories_for_merge(
    left: pd.DataFrame, right: pd.DataFrame, cols: list
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Give ``cols`` the same category dtype (same category set, built from the
    union of values seen on both sides) on both ``left`` and ``right``, in place.

    pandas only keeps a merge-key column as category dtype in the result if BOTH
    sides are categorical with an IDENTICAL category set - if either side is plain
    object dtype, or the two sides' categories differ, the merged column silently
    falls back to full object-dtype strings. At expected_data_structure's scale
    (tens of millions of rows), that fallback alone reintroduces multiple GB of
    memory per column - the exact blowup EXPECTED_STRUCTURE_CATEGORY_COLS is meant
    to avoid. Building categories from the union (not just one side's) avoids
    turning a legitimate value into NaN if the two sides' vocabularies don't match
    exactly.

    Deliberately uses pd.unique() on each side separately (a handful of distinct
    values for a genuinely low-cardinality column) rather than concatenating the
    two full-length columns first and calling .unique() once - Index.append()
    between an Index and a many-million-row CategoricalIndex silently upcasts to
    plain object dtype, which would materialize the exact one-Python-str-per-row
    array this whole function exists to avoid, before ever getting to the
    category conversion below.
    """
    for col in cols:
        left_values = pd.unique(left[col])
        right_values = pd.unique(right[col])
        categories = pd.unique(np.concatenate([left_values, right_values]))
        cat_dtype = pd.CategoricalDtype(categories=categories)
        left[col] = left[col].astype(cat_dtype)
        right[col] = right[col].astype(cat_dtype)
    return left, right


def add_month_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'month' column to the dataframe based on the columns 'choix_campagne'/'produit', 'year', 'round' and 'period',
    where it corresponds to the month of the very first period for each unique combination of 'choix_campagne'/'produit',
      'year' and 'round' to allow filtering by month in PBI.

    Args:
        df (pd.DataFrame): DataFrame containing a 'period' column in datetime format.

    Returns:
        df(pd.DataFrame): DataFrame with an added 'month' column representing the month of the 'period'.
    """
    current_run.log_info("Ajout de la colonne 'Mois de la campagne'...")
    try:
        group_cols = (
            ["choix_campagne", "year", "round"]
            if "choix_campagne" in df.columns
            else ["produit", "year", "round"]
        )
        df["month"] = (
            df.groupby(group_cols, observed=True)["period"]
            .transform("min")
            .dt.month.map(months_mapping_dict)
        )

        current_run.log_info("Colonne 'Mois de la campagne' ajoutée avec succès.")
        return df
    except Exception as e:
        msg = f"Erreur lors de l'ajout de la colonne 'Mois de la campagne': {e}"
        current_run.log_error(msg)
        raise
