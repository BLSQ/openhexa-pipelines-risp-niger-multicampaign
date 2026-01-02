import pandas as pd
import numpy as np


def dates_sequence_generator(min_date, max_date):
    df = pd.DataFrame(
        pd.date_range(start=min_date, end=max_date, freq="24H", inclusive="both"),
        columns=["period"],
    )
    df["order_day"] = df.period.rank().astype(int)
    return df


def round_assignment(df):
    """
    Assigns rounds to the DataFrame based on the 'period' column,
    using different logic based on the 'choix_campagne' column.
    """

    # Define the boolean mask for the "méningite/tcv" campaign
    is_meningite_tcv = df["produit"].isin(["men5_tcv", "méningite", "tcv"])

    # Define the boolean mask for the 2025 yellow fever campaign
    is_yellow_fever = df["produit"] == "fievre jaune"

    # Define the logic for the méningite/tcv campaign
    meningite_tcv_logic = np.where(
        (df["period"] <= pd.to_datetime("2025-12-02"))
        & (df["period"] >= pd.to_datetime("2025-11-24")),
        "round 1",
        np.where(
            (df["period"] <= pd.to_datetime("2025-12-22"))
            & (df["period"] >= pd.to_datetime("2025-12-15")),
            "round 2",
            "invalid date",
        ),
    )

    # Define the logic for the yellow fever campaign
    yellow_fever_logic = np.where(
        df["period"].dt.year == 2025,
        np.where(
            (pd.to_datetime("2025-10-27") <= df["period"])
            & (df["period"] <= pd.to_datetime("2025-11-04")),
            "round 1",
            np.where(
                (pd.to_datetime("2025-12-10") <= df["period"])
                & (df["period"] <= pd.to_datetime("2025-12-17")),
                "round 2",
                "invalid date",
            ),
        ),
        "invalid date",
    )

    # Define the logic for the "other" campaigns
    other_campaign_logic = np.where(
        df["period"].dt.year < 2025,
        # Logic for 2024 periods (nested)
        np.where(
            (pd.to_datetime("2024-07-10") <= df["period"])
            & (df["period"] <= pd.to_datetime("2024-07-24")),
            "round 1",
            np.where(
                (pd.to_datetime("2024-09-28") <= df["period"])
                & (df["period"] <= pd.to_datetime("2024-10-10")),
                "round 2",
                np.where(
                    (pd.to_datetime("2024-10-25") <= df["period"])
                    & (df["period"] <= pd.to_datetime("2024-11-5")),
                    "round 3",
                    np.where(
                        (pd.to_datetime("2024-11-26") <= df["period"]),
                        "round 4",
                        "invalid date",
                    ),
                ),
            ),
        ),
        # Logic for 2025 and later periods (nested)
        np.where(
            (pd.to_datetime("2025-04-18") <= df["period"])
            & (df["period"] <= pd.to_datetime("2025-05-08")),
            "round 1",
            np.where(
                (pd.to_datetime("2025-06-14") <= df["period"])
                & (df["period"] <= pd.to_datetime("2025-06-21")),
                "round 2",
                np.where(
                    (pd.to_datetime("2025-10-24") <= df["period"])
                    & (df["period"] <= pd.to_datetime("2025-12-08")),
                    "round 3",  # changed round 3 to cover up to 8/12 due to rescheduling of polio campaign from 27/10-31/10 to 5/12-8/12
                    "invalid date",
                ),
            ),
        ),
    )

    df["round"] = np.where(
        is_meningite_tcv,
        meningite_tcv_logic,
        np.where(is_yellow_fever, yellow_fever_logic, other_campaign_logic),
    )

    return df


def year_assignment(df):
    df["year"] = df["period"].dt.year
