"""
expected_data_structure: built whole from combined_target_data every run that changes it.

expected_data_structure has no data of its own - every row is a deterministic function of
combined_target_data plus static config (SEX_TYPE/PRODUCT_STATUS/SITE_TYPE below) and each
campaign's period (persisted onto combined_target_data by extract_target_data's
attach_campaign_metadata, as campaign_start_date/campaign_end_date).
"""

import pandas as pd
from openhexa.sdk import current_run

from shared_utils import load_data, save_file

EXPECTED_STRUCTURE_CATEGORY_COLS = [
    "round",
    "age",
    "sexe",
    "produit",
    "vaccination_status",
    "site",
    "choix_campagne",
    "LVL_2_NAME",
    "LVL_3_NAME",
    "LVL_6_NAME",
]
# Every low-cardinality-ish string column expected_data_structure carries. Decoding these as
# category dtype while building expected_data_structure keeps a many-million-row cross-join from
# ever materializing a full one-Python-str-per-row object array per column - decategorized again
# (a cheap pointer-sharing operation, not a real re-inflation) right before saving, matching the
# plain-string on-disk contract every other pipeline that reads this file expects.

SEX_TYPE = ["TOUS"]
PRODUCT_STATUS = ["zéro dose", "déjà reçu"]
SITE_TYPE = {
    "vaccin polio": {
        "ordinaire",
        "spécial",
        "frontalier",
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "vitamine A": {
        "ordinaire",
        "spécial",
        "frontalier",
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "albendazole": {
        "ordinaire",
        "spécial",
        "frontalier",
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "fièvre jaune": {
        "ordinaire",
        "spécial",
        "frontalier",
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "méningite": {
        "ordinaire",
        "spécial",
        "frontalier",
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "tcv": {
        "ordinaire",
        "spécial",
        "frontalier",
        "transfrontalier : étranger",
        "transfrontalier : Niger",
    },
    "rougeole": {
        "fixe",
        "avancé",
        "mobile",
    },
}


def build_site_df(products: list) -> pd.DataFrame:
    """One row per (produit, site) for the products combined_target_data has, from SITE_TYPE."""
    combos = [(p, site) for p in products for site in sorted(SITE_TYPE.get(p, []))]
    return (
        pd.DataFrame(combos, columns=["produit", "site"])
        .sort_values(["produit", "site"])
        .reset_index(drop=True)
    )


def build_status_df(products: list) -> pd.DataFrame:
    """One row per (produit, vaccination_status) for the products combined_target_data has.

    PRODUCT_STATUS is the same flat list for every product, so this is simpler than a
    per-product lookup, but still built per-product to keep the merge below a plain equi-join
    on "produit".
    """
    combos = [(p, status) for p in products for status in PRODUCT_STATUS]
    return (
        pd.DataFrame(combos, columns=["produit", "vaccination_status"])
        .sort_values(["produit", "vaccination_status"])
        .reset_index(drop=True)
    )


def build_sex_df() -> pd.DataFrame:
    return pd.DataFrame({"sexe": SEX_TYPE})


def _explode_period_bounds(period_bounds_df: pd.DataFrame) -> pd.DataFrame:
    """One row per (produit, year, round, period, order_day) - the day-by-day expansion of each
    combo's [campaign_start_date, campaign_end_date] window. `period_bounds_df` is tiny (one row
    per distinct campaign, not per target row - typically a few dozen), so the small Python loop
    below runs over campaigns, never over target/expected-structure rows; the actual row
    expansion (`index.repeat`) is vectorized."""
    df = period_bounds_df.reset_index(drop=True)
    n_days = (
        df["campaign_end_date"] - df["campaign_start_date"]
    ).dt.days.to_numpy() + 1
    order_day = pd.Series(
        [d for n in n_days for d in range(1, int(n) + 1)], dtype="int64"
    )
    exploded = df.loc[df.index.repeat(n_days)].reset_index(drop=True)
    exploded["order_day"] = order_day
    exploded["period"] = exploded["campaign_start_date"] + pd.to_timedelta(
        exploded["order_day"] - 1, unit="D"
    )
    return exploded[["produit", "year", "round", "period", "order_day"]]


def _raise_if_unmatched(df: pd.DataFrame, step_label: str) -> pd.DataFrame:
    """Checks the merge's `indicator=True` column, then drops it via `del` rather than
    `.drop(columns=...)` - `df` is, by the last merge in the chain, tens of millions of rows;
    `.drop()` always returns a brand-new frame (a full copy of every OTHER column too, just to
    remove this one), while `del df[col]` mutates in place."""
    unmatched = df[df["_merge"] == "left_only"]
    if not unmatched.empty:
        examples = sorted(unmatched["produit"].drop_duplicates().tolist())[:5]
        msg = (
            f"Entrées non appariées lors de la fusion ({step_label}). CAUSE: la "
            "configuration ne couvre pas tous les produits de combined_target_data. "
            f"Exemples de produits en cause: {examples}. À FAIRE: complétez SITE_TYPE / "
            "PRODUCT_STATUS dans expected_structure.py pour ce ou ces produits, puis "
            "relancez le pipeline."
        )
        current_run.log_error(msg)
        raise ValueError(msg)
    del df["_merge"]
    return df


def generate_expected_data_structure(
    target_df: pd.DataFrame, category_columns: list
) -> pd.DataFrame:
    """
    Builds expected_data_structure from combined_target_data + static config: every row is a
    deterministic function of `target_df` (already compiled) plus
    SITE_TYPE/PRODUCT_STATUS/SEX_TYPE and each campaign's persisted period bounds.

    Rows with no campaign_start_date yet - a campaign whose per-run target file predates
    choix_campagne/campaign_start_date/campaign_end_date, and hasn't been re-run through the
    current extract_target_data - are excluded, with a warning naming the (produit, year,
    round) combos left out. Campaigns migrate to the new columns one extract_target_data
    re-run at a time, not all simultaneously, so this keeps expected_data_structure buildable
    for every already-migrated campaign instead of blocking on every campaign being migrated
    at once (the day-by-day period explosion below has no valid day count for a NaN date).

    produit/round (the merge keys below) are left as plain dtype throughout - low-cardinality
    enough to be cheap regardless, and keeping them plain sidesteps having to reconcile two
    independently-built categorical dtypes across frames on a join key. Every OTHER
    low-cardinality column is cast to category on its own small source frame BEFORE any merge,
    not after: none of them are ever a merge key here, so there is no cross-frame alignment
    concern for them - only a many-million-row plain-object-dtype rebuild to avoid.
    """
    unmigrated = target_df[target_df["campaign_start_date"].isna()]
    if not unmigrated.empty:
        combos = sorted(
            unmigrated[["produit", "year", "round"]]
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )
        current_run.log_warning(
            f"{len(combos)} combinaison(s) produit/année/round n'ont pas encore de date de "
            "campagne (fichier source antérieur à l'ajout de choix_campagne/"
            "campaign_start_date/campaign_end_date) et sont exclues de expected_data_structure "
            f"pour cette exécution : {combos}. Relancez extract_target_data pour ces "
            "campagnes (avec l'option d'écrasement activée) pour les inclure."
        )
        target_df = target_df[target_df["campaign_start_date"].notna()]

    org_unit_cols = [
        c
        for c in ["org_unit_id", "LVL_2_NAME", "LVL_3_NAME", "LVL_6_NAME"]
        if c in target_df.columns
    ]
    base_cols = org_unit_cols + ["age", "produit", "year", "round", "choix_campagne"]
    base = target_df[base_cols].drop_duplicates().reset_index(drop=True)

    products = sorted(target_df["produit"].unique())
    site_df = build_site_df(products)
    status_df = build_status_df(products)
    sex_df = build_sex_df()
    period_bounds = target_df[
        ["produit", "year", "round", "campaign_start_date", "campaign_end_date"]
    ].drop_duplicates()
    period_df = _explode_period_bounds(period_bounds)

    for frame in (base, site_df, status_df, sex_df, period_df):
        for col in category_columns:
            if col in frame.columns and col not in ("produit", "round"):
                frame[col] = frame[col].astype("category")

    combined = base.merge(sex_df, how="cross")
    combined = _raise_if_unmatched(
        combined.merge(site_df, on="produit", how="left", indicator=True),
        "produit / site",
    )
    combined = _raise_if_unmatched(
        combined.merge(status_df, on="produit", how="left", indicator=True),
        "produit / statut de vaccination",
    )
    combined = _raise_if_unmatched(
        combined.merge(
            period_df, on=["produit", "year", "round"], how="left", indicator=True
        ),
        "période de campagne",
    )
    return combined.reset_index(drop=True)


def generate_and_save_expected_data_structure() -> int:
    target_df = load_data("combined_target_data")
    combined = generate_expected_data_structure(
        target_df, EXPECTED_STRUCTURE_CATEGORY_COLS
    )
    for col in EXPECTED_STRUCTURE_CATEGORY_COLS:
        if col in combined.columns:
            combined[col] = combined[col].astype(object)
    save_file(combined, "expected_data_structure")
    return len(combined)
