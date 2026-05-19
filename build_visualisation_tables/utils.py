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


def process_target_level(
    cvrg_subset: pd.DataFrame,
    target_subset: pd.DataFrame,
    join_keys: list,
    final_keys: list,
    cumsum_keys: list,
    level_label: str,
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
        # Merge target data with coverage data
        merged = cvrg_subset.merge(target_subset, on=join_keys, how="left")[final_keys]

        # Check for missing targets
        no_target = merged[merged["cible"].isna()]
        if not no_target.empty:
            prop = len(no_target) / len(cvrg_subset)
            warn_msg = f"{len(no_target)} entrée(s) ({prop:.2%}) au niveau {level_label} n'ont pas de cible."

            if level_label == "CSI" and "LVL_6_NAME" in no_target.columns:
                affected = no_target["LVL_6_NAME"].unique().tolist()
                warn_msg += f" CSI affectés: {', '.join(affected)}"

            current_run.log_warning(warn_msg + " Valeur cible: NaN.")

        # Data Cleaning and Cumulative Sum
        merged["value"] = merged["value"].fillna(0)
        merged["value_cum"] = merged.groupby(cumsum_keys)["value"].transform("cumsum")

        # Standardize Target as Nullable Integer
        merged["cible"] = (
            pd.to_numeric(merged["cible"], errors="coerce").round(0).astype("Int64")
        )

        return merged

    except Exception as e:
        current_run.log_error(
            f"Erreur lors du traitement des cibles au niveau {level_label}: {e}"
        )
        raise
