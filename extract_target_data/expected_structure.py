"""
Expected-data-structure generation, absorbed into process_target_data's Configure stage per the
v2 migration plan (docs/ARCHITECTURE.md §4; see the plan file for the full absorption design).

Ported from the retired create_expected_data_structure_for_historical_campaigns.combine_dfs and
configure_new_campaign's date/period logic, with one structural change: everything here operates
on THIS RUN's own org-unit-matched target rows (``matched``) rather than the full,
all-campaigns-combined target dataset. That is what lets the yellow-fever Dosso/Tahoua special
case those old pipelines needed be dropped as dead code: cross-joining against org units from the
whole combined dataset could produce a spurious expected row for a district that only ever had a
DIFFERENT product's target; cross-joining against ``matched`` (this run only) cannot.

Output columns: org_unit_id, LVL_3_NAME, LVL_6_NAME, sexe, year, produit, round, age, site,
vaccination_status, choix_campagne, period, order_day.
"""

import os
import re

import pandas as pd

from config import OUTPUTS_PATH

try:  # pragma: no cover - same logging shim pattern as target_import.py
    from openhexa.sdk import current_run

    def _info(m):
        current_run.log_info(m)

    def _warn(m):
        current_run.log_warning(m)

    def _error(m):
        current_run.log_error(m)
except Exception:  # pragma: no cover
    import logging

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _log = logging.getLogger("expected_structure")
    _info, _warn, _error = _log.info, _log.warning, _log.error


# Expected-data-structure config. Moved here from config.py (was previously
# documented, per CLAUDE.md and ARCHITECTURE.md §4/D2, as living in config.py under
# a "one named constant block" convention) - it lives alongside the functions below
# that are its only consumers, per the "config.py holds paths/connection details
# only" convention (ARCHITECTURE.md §14.2).
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

# Historical campaign dates config
HISTORICAL_CAMPAIGNS_CONFIG = {
    (2024, 1, "polio", "vaccin polio"): {"début": "2024-07-10", "fin": "2024-07-24"},
    (2024, 1, "polio", "vitamine A"): {"début": "2024-07-10", "fin": "2024-07-24"},
    (2024, 1, "polio", "albendazole"): {"début": "2024-07-10", "fin": "2024-07-24"},
    (2024, 2, "polio", "vaccin polio"): {"début": "2024-09-28", "fin": "2024-10-06"},
    (2024, 2, "polio", "vitamine A"): {"début": "2024-09-28", "fin": "2024-10-06"},
    (2024, 2, "polio", "albendazole"): {"début": "2024-09-28", "fin": "2024-10-06"},
    (2024, 3, "polio", "vaccin polio"): {"début": "2024-10-25", "fin": "2024-11-01"},
    (2024, 3, "polio", "vitamine A"): {"début": "2024-10-25", "fin": "2024-11-01"},
    (2024, 3, "polio", "albendazole"): {"début": "2024-10-25", "fin": "2024-11-01"},
    (2024, 4, "polio", "vaccin polio"): {"début": "2024-12-01", "fin": "2024-12-12"},
    (2024, 4, "polio", "vitamine A"): {"début": "2024-12-01", "fin": "2024-12-12"},
    (2024, 4, "polio", "albendazole"): {"début": "2024-12-01", "fin": "2024-12-12"},
    (2025, 1, "polio", "vaccin polio"): {"début": "2025-05-04", "fin": "2025-05-08"},
    (2025, 1, "polio", "vitamine A"): {"début": "2025-05-04", "fin": "2025-05-08"},
    (2025, 1, "polio", "albendazole"): {"début": "2025-05-04", "fin": "2025-05-08"},
    (2025, 1, "rougeole", "rougeole"): {"début": "2025-04-18", "fin": "2025-04-24"},
    (2025, 1, "fièvre jaune", "fièvre jaune"): {
        "début": "2025-10-27",
        "fin": "2025-11-04",
    },
    (2025, 1, "méningite", "méningite"): {"début": "2025-11-24", "fin": "2025-12-02"},
    (2025, 1, "tcv", "tcv"): {"début": "2025-11-24", "fin": "2025-12-02"},
    (2025, 2, "polio", "vaccin polio"): {"début": "2025-06-14", "fin": "2025-06-21"},
    (2025, 2, "polio", "vitamine A"): {"début": "2025-06-14", "fin": "2025-06-21"},
    (2025, 2, "polio", "albendazole"): {"début": "2025-06-14", "fin": "2025-06-21"},
    (2025, 2, "méningite", "méningite"): {"début": "2025-12-15", "fin": "2025-12-22"},
    (2025, 2, "tcv", "tcv"): {"début": "2025-12-15", "fin": "2025-12-22"},
    (2026, 1, "polio", "vaccin polio"): {"début": "2026-01-11", "fin": "2026-01-15"},
    (2026, 1, "fièvre jaune", "fièvre jaune"): {
        "début": "2026-01-20",
        "fin": "2026-01-26",
    },
    (2026, 2, "polio", "vaccin polio"): {"début": "2026-04-24", "fin": "2026-05-01"},
    (2026, 3, "polio", "vaccin polio"): {"début": "2026-07-09", "fin": "2026-07-18"},
    (2026, 1, "jnm", "albendazole"): {"début": "2026-07-02", "fin": "2026-07-09"},
    (2026, 1, "jnm", "vitamine A"): {"début": "2026-07-02", "fin": "2026-07-09"},
}


def fail(*lines):
    """Log an actionable error line by line, then abort - same convention as
    target_import.fail / pipeline.fail_run."""
    clean = [str(ln).strip() for ln in lines if ln and str(ln).strip()]
    for line in clean:
        _error(line)
    raise ValueError(" ".join(clean))


def build_site_df(products: list) -> pd.DataFrame:
    """One row per (produit, site) for this run's products, from SITE_TYPE."""
    combos = [(p, site) for p in products for site in sorted(SITE_TYPE.get(p, []))]
    return (
        pd.DataFrame(combos, columns=["produit", "site"])
        .sort_values(["produit", "site"])
        .reset_index(drop=True)
    )


def build_status_df(products: list) -> pd.DataFrame:
    """One row per (produit, vaccination_status) for this run's products.

    PRODUCT_STATUS is the same flat list for every product (per the locked decision
    that ARCHITECTURE.md §4/D2's config block is authoritative), so this is simpler
    than a per-product lookup, but still built per-product to keep the merge in
    combine_expected_structure a plain equi-join on "produit".
    """
    combos = [(p, status) for p in products for status in PRODUCT_STATUS]
    return (
        pd.DataFrame(combos, columns=["produit", "vaccination_status"])
        .sort_values(["produit", "vaccination_status"])
        .reset_index(drop=True)
    )


def build_sex_df() -> pd.DataFrame:
    return pd.DataFrame({"sexe": SEX_TYPE})


def build_age_round_year_df(matched: pd.DataFrame) -> pd.DataFrame:
    """
    The (year, produit, round, age) combinations this run's matched target data
    actually has - i.e. from layouts.PRODUCT_DEFS via the target-import engine, the
    same single source of truth the target values themselves came from. This is
    what eliminates the age-bracket drift that existed between configure_new_campaign's
    separately-maintained age lists and the target-import engine's own PRODUCT_DEFS.
    """
    return matched[["year", "produit", "round", "age"]].drop_duplicates()


def _make_period_frame(
    campaign_name: str, produit: str, r, year: int, date_range
) -> pd.DataFrame:
    """One period frame: one row per day in date_range, with its 1-based order_day."""
    frame = pd.DataFrame(
        {
            "choix_campagne": campaign_name,
            "produit": produit,
            "round": f"round {int(r)}",
            "year": int(year),
            "period": date_range,
        }
    )
    frame["order_day"] = range(1, len(date_range) + 1)
    return frame


def _resolve_from_historical_lookup(
    year: int, rounds: list, products: list, campaign_name: str, historical_lookup: dict
) -> tuple:
    """Split every (round, produit) this run covers into ready-to-use period frames
    (already dated in historical_lookup) and combos that still need explicit dates."""
    frames = []
    needs_dates = []
    for r in rounds:
        for produit in products:
            key = (int(year), int(r), campaign_name, produit)
            dates = historical_lookup.get(key)
            if dates is None:
                needs_dates.append((r, produit))
                continue
            date_range = pd.date_range(start=dates["début"], end=dates["fin"])
            frames.append(
                _make_period_frame(campaign_name, produit, r, year, date_range)
            )
    return frames, needs_dates


def _missing_combo_hints(
    needs_dates: list, year: int, campaign_name: str, historical_lookup: dict
) -> list:
    """A missing combo can mean "genuinely new" - or it can mean the file's products
    don't all share one campaign period (e.g. the "coupled" polio choice, where
    albendazole/vitamine A have historically sometimes run under campaign_name "jnm"
    on different dates than polio's own "polio" period for the same round). Detect
    the second case and suggest the fix, rather than just asking for dates that
    would then be wrong."""
    hints = []
    for r, produit in needs_dates:
        other_names = sorted(
            {
                k[2]
                for k in historical_lookup
                if k[0] == int(year)
                and k[1] == int(r)
                and k[3] == produit
                and k[2] != campaign_name
            }
        )
        if other_names:
            hints.append(
                f"  HYPOTHÈSE: '{produit}' (round {r}, {year}) existe dans la "
                "configuration historique, mais sous un autre type de campagne "
                f"({', '.join(other_names)}), avec une période probablement "
                "différente de celle des autres produits de cette exécution. "
                "Ce produit devrait peut-être être traité séparément, avec le "
                "type de campagne correspondant, plutôt que dans cette exécution "
                "groupée."
            )
    return hints


def _validate_new_period_request(
    needs_dates: list,
    rounds: list,
    year: int,
    campaign_start_date,
    campaign_end_date,
    hints: list,
) -> tuple:
    """Fail with an actionable message if dates are missing, or if multiple rounds
    are mixed with a genuinely new combination. Returns (start, end) once valid."""
    if not campaign_start_date or not campaign_end_date:
        fail(
            "Dates de campagne manquantes.",
            "CAUSE: les combinaisons suivantes ne sont pas connues dans la "
            "configuration historique et nécessitent les paramètres 'Date de "
            "début de la campagne' / 'Date de fin de la campagne':",
            *(f"  - {p}, round {r}, {year}" for r, p in needs_dates),
            *hints,
            "À FAIRE: si l'une des hypothèses ci-dessus s'applique, relancez le "
            "pipeline avec le type de campagne séparé indiqué. Sinon, "
            "renseignez les deux paramètres de dates, puis relancez le pipeline.",
        )
    if len(set(int(r) for r in rounds)) > 1:
        fail(
            "Impossible de combiner plusieurs rounds avec une nouvelle période.",
            "CAUSE: une seule paire de dates a été fournie, mais plusieurs "
            f"rounds ont été sélectionnés ({sorted(set(int(r) for r in rounds))}) "
            "et au moins une des combinaisons demandées n'existe pas encore dans "
            "la configuration historique.",
            *hints,
            "À FAIRE: si l'une des hypothèses ci-dessus s'applique, relancez le "
            "pipeline avec le type de campagne séparé indiqué. Sinon, relancez "
            "le pipeline avec un seul round à la fois pour toute combinaison "
            "qui n'est pas déjà connue.",
        )
    start = pd.to_datetime(campaign_start_date, format="%Y-%m-%d")
    end = pd.to_datetime(campaign_end_date, format="%Y-%m-%d")
    if end <= start:
        fail(
            "Dates de campagne invalides.",
            f"CAUSE: la date de fin ({campaign_end_date}) doit être postérieure "
            f"à la date de début ({campaign_start_date}).",
            "À FAIRE: corrigez les paramètres de dates, puis relancez le pipeline.",
        )
    return start, end


def build_campaign_period_df(
    year: int,
    rounds: list,
    products: list,
    campaign_name: str,
    campaign_start_date,
    campaign_end_date,
    historical_lookup: dict,
) -> pd.DataFrame:
    """
    Resolve the campaign period (date range + order_day) for every (round, produit)
    this run covers.

    Historical combinations already in ``historical_lookup`` use their known dates -
    a single run can replay several historical rounds this way. Any combination NOT
    found there needs campaign_start_date/campaign_end_date: since one date pair can
    only describe one calendar window, the run must then be restricted to a single
    round (mirrors how configure_new_campaign was always used - one round per run).
    """
    frames, needs_dates = _resolve_from_historical_lookup(
        year, rounds, products, campaign_name, historical_lookup
    )

    if needs_dates:
        hints = _missing_combo_hints(
            needs_dates, year, campaign_name, historical_lookup
        )
        start, end = _validate_new_period_request(
            needs_dates, rounds, year, campaign_start_date, campaign_end_date, hints
        )
        date_range = pd.date_range(start=start, end=end)
        for r, produit in needs_dates:
            frames.append(
                _make_period_frame(campaign_name, produit, r, year, date_range)
            )
    elif campaign_start_date or campaign_end_date:
        _info(
            "Les dates de campagne fournies sont ignorées: toutes les combinaisons "
            "demandées existent déjà dans la configuration historique."
        )

    return pd.concat(frames, ignore_index=True)


def _raise_if_unmatched(df: pd.DataFrame, step_label: str) -> pd.DataFrame:
    unmatched = df[df["_merge"] == "left_only"]
    if not unmatched.empty:
        examples = sorted(unmatched["produit"].drop_duplicates().tolist())[:5]
        fail(
            f"Entrées non appariées lors de la fusion ({step_label}).",
            "CAUSE: la configuration ne couvre pas tous les produits de ce fichier "
            f"de cibles. Exemples de produits en cause: {examples}.",
            "À FAIRE: complétez la configuration (SITE_TYPE / PRODUCT_STATUS / dates "
            "de campagne dans expected_structure.py) pour ce ou ces produits, puis "
            "relancez le pipeline.",
        )
    return df.drop(columns=["_merge"])


def combine_expected_structure(
    matched: pd.DataFrame,
    site_df: pd.DataFrame,
    status_df: pd.DataFrame,
    sex_df: pd.DataFrame,
    age_round_year_df: pd.DataFrame,
    period_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cross-join this run's org units (from ``matched``, not the whole combined
    target dataset) with sex and (year, produit, round, age); merge in site,
    vaccination status, and campaign period. Any unmatched merge is a
    configuration gap and aborts the run rather than silently dropping rows.
    """
    _info("Combinaison des dataframes de la structure attendue...")
    try:
        # District-level runs (match_district_to_org_unit_id) have no LVL_6_NAME at
        # all - unlike v1, which built this from the already-concatenated combined
        # dataset, where pandas' own concat had already filled that column with NaN
        # for district-only rows. Selecting only the columns actually present keeps
        # this working per-run for both levels.
        org_unit_cols = [
            c
            for c in ["org_unit_id", "LVL_2_NAME", "LVL_3_NAME", "LVL_6_NAME"]
            if c in matched.columns
        ]
        org_unit_ids_df = matched[org_unit_cols].drop_duplicates()
        combined = org_unit_ids_df.merge(sex_df, how="cross").merge(
            age_round_year_df, how="cross"
        )

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

        combined = combined.drop_duplicates().reset_index(drop=True)
        _info("Structure attendue combinée avec succès.")
        return combined
    except ValueError:
        raise
    except Exception as e:
        _error(f"Erreur lors de la combinaison de la structure attendue: {e}")
        raise


# =========================================================================== #
# Date-overlap checking (moved from pipeline.py - period-overlap checking is a #
# period/expected-structure concern).                                         #
# =========================================================================== #
def _needs_new_period(
    year: int,
    rounds: list,
    products: list,
    campaign_name_internal: str,
    campaign_start_date: str,
    campaign_end_date: str,
) -> bool:
    """True only if dates were actually supplied AND at least one (year, round,
    produit) this run covers isn't already in HISTORICAL_CAMPAIGNS_CONFIG - a pure
    historical replay has no new period to check for overlap."""
    if not campaign_start_date or not campaign_end_date:
        return False
    return any(
        (int(year), int(r), campaign_name_internal, p) not in HISTORICAL_CAMPAIGNS_CONFIG
        for r in rounds
        for p in products
    )


def _load_existing_expected_structure():
    """The compiled expected_data_structure, or None if it doesn't exist yet or
    can't be read (both treated as "nothing to check for overlap against")."""
    path = os.path.join(OUTPUTS_PATH, "expected_data_structure.parquet")
    if not os.path.exists(path):
        return None
    try:
        return pd.read_parquet(path, columns=["produit", "year", "round", "period"])
    except Exception as e:
        _warn(
            "Fichier expected_data_structure illisible, ignoré lors du contrôle "
            f"de chevauchement de dates ({e})."
        )
        return None


def _find_period_conflicts(
    existing: pd.DataFrame,
    year: int,
    rounds: list,
    products: list,
    campaign_start_date: str,
    campaign_end_date: str,
) -> list:
    """Which existing (produit, round) periods - for the same year, any round OTHER
    than the ones being (re)processed now - overlap the supplied date range."""
    start_new = pd.to_datetime(campaign_start_date, format="%Y-%m-%d")
    end_new = pd.to_datetime(campaign_end_date, format="%Y-%m-%d")
    requested_rounds = {int(r) for r in rounds}
    same_year = existing[existing["year"] == int(year)]

    conflicts = []
    for produit in products:
        subset = same_year[same_year["produit"] == produit]
        for rnd in subset["round"].unique():
            rnd_num = int(re.search(r"\d+", str(rnd)).group())
            if rnd_num in requested_rounds:
                continue  # same round being (re)processed, not a conflict with itself
            period = subset[subset["round"] == rnd]["period"]
            rnd_start, rnd_end = period.min(), period.max()
            if start_new <= rnd_end and end_new >= rnd_start:
                conflicts.append((produit, rnd, rnd_start.date(), rnd_end.date()))
    return conflicts


def check_for_date_overlap(
    year: int,
    rounds: list,
    products: list,
    campaign_name_internal: str,
    campaign_start_date: str,
    campaign_end_date: str,
    overwrite_existing: bool,
) -> None:
    """
    Guard against a NEW campaign round's dates overlapping an already-recorded
    round for the same (produit, year) in expected_data_structure.

    Only meaningful when campaign_start_date/campaign_end_date are actually
    supplied - i.e. this run covers at least one (year, round, produit) not
    already in HISTORICAL_CAMPAIGNS_CONFIG. A pure historical replay has no new
    period to check for overlap, so this is a no-op in that case.
    """
    if not _needs_new_period(
        year, rounds, products, campaign_name_internal, campaign_start_date, campaign_end_date
    ):
        return
    existing = _load_existing_expected_structure()
    if existing is None:
        return

    conflicts = _find_period_conflicts(
        existing, year, rounds, products, campaign_start_date, campaign_end_date
    )
    if not conflicts:
        return

    if overwrite_existing:
        _warn_date_conflicts(conflicts, campaign_start_date, campaign_end_date)
    else:
        _fail_on_date_conflicts(conflicts, campaign_start_date, campaign_end_date)


def _warn_date_conflicts(conflicts: list, campaign_start_date: str, campaign_end_date: str) -> None:
    _warn(
        f"CHEVAUCHEMENT DE DATES: {len(conflicts)} round(s) existant(s) ont une "
        "période qui chevauche celle fournie pour cette exécution."
    )
    for produit, rnd, s, e in conflicts:
        _warn(
            f"CHEVAUCHEMENT: {produit}, {rnd} ({s} - {e}) chevauche la période "
            f"{campaign_start_date} - {campaign_end_date} fournie."
        )


def _fail_on_date_conflicts(conflicts: list, campaign_start_date: str, campaign_end_date: str) -> None:
    lines = [
        "La période fournie chevauche celle d'un round déjà existant pour le même "
        "produit et la même année.",
        "Traitement interrompu pour éviter un chevauchement de dates involontaire.",
    ]
    for produit, rnd, s, e in conflicts:
        lines.append(
            f"CAUSE: {produit}, {rnd} ({s} - {e}) chevauche la période "
            f"{campaign_start_date} - {campaign_end_date} fournie."
        )
    lines += [
        "À FAIRE: deux options possibles.",
        "OPTION 1: corrigez les dates de la campagne si ce chevauchement est une "
        "erreur.",
        "OPTION 2: si ce chevauchement est intentionnel (remplacement du round "
        "existant), relancez le pipeline en activant l'option 'Écraser les données "
        "existantes en cas de doublon'.",
    ]
    # Uses this module's own `fail` (not pipeline.fail_run, which would be a
    # circular import back into pipeline.py) - functionally equivalent for every
    # real call site here (all lines passed are non-blank literal strings), `fail`
    # additionally strips/filters blank lines, which fail_run did not.
    fail(*lines)
