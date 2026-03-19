import pandas as pd
import re
import datetime


def pyramid_selector(df):
    max_id = df["updated_at"].apply(pd.to_datetime).idxmax()
    max_date = df["updated_at"].apply(pd.to_datetime).dt.date.get(max_id)
    if max_date != datetime.date(2023, 7, 14):
        return df.loc[max_id, :]
    else:
        df = df.drop(max_id)
        return pyramid_selector(df)


def parse_age_to_months(age_str):
    """Converts 'X-Y mois/ans' to a (min_months, max_months) tuple."""
    match = re.search(r"(\d+)-(\d+)\s+(mois|ans)", age_str)
    if not match:
        return None

    start, end, unit = match.groups()
    multiplier = 12 if unit == "ans" else 1

    # We treat 'ans' as inclusive of the full year (e.g., 4 ans = up to 59.9 months)
    # Adjusting 'end' for years to cover the full duration:
    start_m = int(start) * multiplier
    end_m = (int(end) + 1) * multiplier if unit == "ans" else int(end)

    return start_m, end_m
