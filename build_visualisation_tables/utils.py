from openhexa.sdk import current_run
import pandas as pd
from sqlalchemy import inspect
import config


def age_categorizer(string):
    """
    Categorizes age groups based on the input string.

    Parameters:
        string (str): The input string containing age information.

    Returns:
        str: The categorized age group, or "N/A" if no specific age group is covered by the form configuration.
    """
    for key in config.ages_mapping:
        if key in string:
            return config.ages_mapping[key]

    return "N/A"


def site_categorizer(string: str) -> str:
    """
    Categorizes site types based on the input string.

    Parameters:
        string (str): The input string containing site information.

    Returns:
        str: The categorized site type, or "ordinaire" if no specific type is covered by the form configuration.
    """
    for key in config.sites_mapping:
        if key in string:
            return config.sites_mapping[key]

    return "ordinaire"


def produit_categorizer(string: str) -> str:
    """
    Categorizes product types based on the input string.

    Parameters:
        string (str): The input string containing product information.

    Returns:
        str: The categorized product type, or "N/A" if no specific type is covered by the form configuration.
    """
    for key in config.products_mapping:
        if key in string:
            return config.products_mapping[key]

    return "N/A"


def produit_categorizer_stocks(string: str) -> str:
    """
    Categorizes product types based on the input string.

    Parameters:
        string (str): The input string containing product information.

    Returns:
        str: The categorized product type, or "N/A" if no specific product type is covered by the form configuration.
    """
    for key in config.products_mapping_stocks:
        if key in string:
            return config.products_mapping_stocks[key]

    return "N/A"


def vaccination_status_categorizer(string: str) -> str:
    """
    Categorizes vaccination status based on the input string.

    Parameters:
        string (str): The input string containing vaccination information.

    Returns:
        str: The categorized vaccination status, or "zéro dose" if no specific status is covered by the form configuration.
    """
    for key in config.status_mapping:
        if key in string:
            return config.status_mapping[key]

    return "zéro dose"


def product_status_categorizer(string: str) -> str:
    """
    Categorizes product status based on the input string.

    Parameters:
        string (str): The input string containing product status information.

    Returns:
        str: The categorized product status, or "N/A" if no specific status is covered by the form configuration.
    """
    for key in config.stock_status_mapping:
        if key in string:
            return config.stock_status_mapping[key]

    return "N/A"


def supervision_categorizer(string: str) -> str:
    """
    Categorizes supervision types based on the input string.

    Parameters:
        string (str): The input string containing supervision information.

    Returns:
        str: The categorized supervision type, or "N/A" if no specific type is covered by the form configuration.
    """
    for key in config.surveillance_category_mapping:
        if key in string:
            return config.surveillance_category_mapping[key]

    return "N/A"


def communication_categorizer(string: str) -> str:
    """
    Categorizes communication types based on the input string.

    Parameters:
        string (str): The input string containing communication information.

    Returns:
        str: The categorized communication type, or "N/A" if no specific type is covered by the form configuration.
    """
    for key in config.communication_category_mapping:
        if key in string:
            return config.communication_category_mapping[key]

    return "N/A"


def get_communication_category_type(col_name: str, master_groups: dict) -> str:
    """
    Identifies which group (Deployment, Reach, etc.) a column belongs to.

    Parameters:
        col_name (str): The name of the column to categorize.
        master_groups (dict): A dictionary where keys are group names and values are sets of column names.

    Returns:
        str: The name of the group the column belongs to, or "N/A" if it doesn't belong to any group.
    """
    for group_name, list_of_cols in master_groups.items():
        if col_name in list_of_cols:
            return group_name

    return "N/A"


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


def _warn_missing_targets(
    merged: pd.DataFrame, cvrg_subset: pd.DataFrame, level_label: str
) -> None:
    no_target = merged[merged["cible"].isna()]
    if no_target.empty:
        return
    prop = len(no_target) / len(cvrg_subset)
    warn_msg = f"{len(no_target)} entrée(s) ({prop:.2%}) au niveau {level_label} n'ont pas de cible."
    if level_label == "CSI" and "LVL_6_NAME" in no_target.columns:
        affected = no_target["org_unit_id"].unique().tolist()
        warn_msg += f" CSI affectés: {', '.join(map(str, affected))}"
    current_run.log_warning(warn_msg + " Valeur cible: NaN.")


def _compute_cumulative_value(merged: pd.DataFrame, cumsum_keys: list) -> pd.DataFrame:
    """Fill missing daily values with 0, cumulate them per cumsum_keys, and
    standardize the target column as a nullable integer."""
    merged["value"] = merged["value"].fillna(0)
    merged["value_cum"] = merged.groupby(cumsum_keys)["value"].transform("cumsum")
    merged["cible"] = (
        pd.to_numeric(merged["cible"], errors="coerce").round(0).astype("Int64")
    )
    return merged


def _attach_level_names(
    df: pd.DataFrame, name_cols: list, org_unit_tree_df: pd.DataFrame
) -> pd.DataFrame:
    """Attach canonical name columns from the org-unit tree onto `df`, keyed on
    org_unit_id - the reliable, always-present key - rather than trusting
    whatever a later merge happens to carry over. Rows whose org_unit_id doesn't
    resolve in the tree are dropped."""
    df = df.drop(columns=[c for c in name_cols if c in df.columns])
    level_names = org_unit_tree_df[["org_unit_id"] + name_cols].drop_duplicates()
    df = df.merge(level_names, on="org_unit_id", how="left")
    return df.dropna(subset=name_cols)


def _drop_name_cols(df: pd.DataFrame, name_cols: list) -> pd.DataFrame:
    return df.drop(columns=[c for c in name_cols if c in df.columns])


def _attach_csi_names_pre_merge(
    cvrg_subset: pd.DataFrame,
    target_subset: pd.DataFrame,
    org_unit_tree_df: pd.DataFrame,
) -> tuple:
    """CSI join key is org_unit_id (not LVL_3_NAME/LVL_6_NAME), so it's safe - and
    cheaper - to attach the canonical names to the coverage side up front, before
    the merge: every row then has a name whether or not a target matches. Left as
    a post-merge patch-up, a CSI with coverage but no target would get NaN names
    from the merge (target_subset is the only side that carries them), requiring
    a second full-result pass to fill them back in."""
    cvrg_subset = _attach_level_names(
        cvrg_subset, ["LVL_3_NAME", "LVL_6_NAME"], org_unit_tree_df
    )
    target_subset = _drop_name_cols(target_subset, ["LVL_3_NAME", "LVL_6_NAME"])
    return cvrg_subset, target_subset


def _revalidate_district_names_post_merge(
    merged: pd.DataFrame, org_unit_tree_df: pd.DataFrame
) -> pd.DataFrame:
    """Unlike CSI, LVL_3_NAME IS one of the District join keys - re-deriving it
    from the org-unit tree before the merge would risk changing which rows match
    (if the tree's spelling differs from whatever upstream district-anchoring
    already attached to cvrg_subset). So for District, names are (re-)validated
    against the tree AFTER the merge instead."""
    return _attach_level_names(merged, ["LVL_3_NAME"], org_unit_tree_df)


def process_target_level(
    cvrg_subset: pd.DataFrame,
    target_subset: pd.DataFrame,
    join_keys: list,
    final_keys: list,
    cumsum_keys: list,
    level_label: str,
    org_unit_tree_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Helper to merge targets, log warnings, and calculate cumulative values.

    Parameters:
        cvrg_subset (pd.DataFrame): Coverage subset DataFrame.
        target_subset (pd.DataFrame): Target subset DataFrame.
        join_keys (list): List of keys to join on.
        final_keys (list): List of final keys to select after merge.
        cumsum_keys (list): List of keys to calculate cumulative values.
        level_label (str): Label for the level (e.g., "CSI", "District").

    Returns:
        pd.DataFrame: Merged DataFrame with cumulative values.
    """
    try:
        if level_label == "CSI":
            cvrg_subset, target_subset = _attach_csi_names_pre_merge(
                cvrg_subset, target_subset, org_unit_tree_df
            )

        merged = cvrg_subset.merge(target_subset, on=join_keys, how="left")[final_keys]
        _warn_missing_targets(merged, cvrg_subset, level_label)
        merged = _compute_cumulative_value(merged, cumsum_keys)

        if level_label == "District":
            merged = _revalidate_district_names_post_merge(merged, org_unit_tree_df)

        return merged

    except Exception as e:
        current_run.log_error(
            f"Erreur lors du traitement des cibles au niveau {level_label}: {e}"
        )
        raise
