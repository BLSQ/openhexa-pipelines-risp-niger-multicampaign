"""
Normalizing the 'period' column of an IASO submissions dataframe - no IASO-client
dependency, purely date-format handling.
"""

import pandas as pd


def period_form_convert_date(row: pd.Series) -> str:
    """
    Converts various date formats to a standardized 'YYYY-MM-DD' format.

    Parameters:
        row (pd.Series): A pandas Series containing date information.

    Returns:
        str: The converted date in 'YYYY-MM-DD' string format or the original value if conversion fails.
    """
    val_0 = row.iloc[0]

    if len(str(val_0)) == 8 and str(val_0).isdigit():
        return pd.to_datetime(val_0, format="%Y%m%d").strftime("%Y-%m-%d")
    elif len(str(val_0)) >= 8:
        return pd.to_datetime(val_0, format="%Y-%m-%d").strftime("%Y-%m-%d")
    elif len(str(row.iloc[1])) >= 8:
        return pd.to_datetime(row.iloc[1], unit="s").strftime("%Y-%m-%d")
    else:
        return val_0


def period_processing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the 'period' column in the DataFrame to standardize date formats.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing a 'period' column.

    Returns:
        df (pd.DataFrame): The DataFrame with the processed 'period' column.
    """
    df.period = df.period.mask(df.period == "Invalid date", None)
    df.period = df.period.mask(
        df.period.isna(),
        df.created_at.apply(lambda x: pd.to_datetime(x, unit="s").strftime("%Y-%m-%d")),
    )
    df.period = df.period.mask(
        df.period.notna(),
        df[["period", "created_at"]].apply(
            lambda row: period_form_convert_date(row), axis=1
        ),
    )
    return df
