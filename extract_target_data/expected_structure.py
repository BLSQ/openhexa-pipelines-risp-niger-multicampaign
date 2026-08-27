"""
Campaign-period resolution for this run, plus the date-overlap safety check.

expected_data_structure has no data of its own: every row is a deterministic function of
combined_target_data (the target rows) plus static config and each campaign's period, and
process_target_data builds it whole from combined_target_data - this module doesn't build any
expected-structure rows itself. What this module owns is resolving the campaign period for THIS
run - historical lookup first, then the campaign_start_date/campaign_end_date parameters - since
that resolution genuinely needs run-time input process_target_data (no parameters, unattended)
never has. Only the boundary dates are resolved here, not their day-by-day explosion (see
process_target_data's _explode_period_bounds for that); the resolved (produit, round) ->
(start, end) pairs are attached as two columns on this run's own target rows (pipeline.py's
attach_campaign_metadata).

check_for_date_overlap and its helpers read expected_data_structure.parquet's
(produit, year, round, period) columns, exactly as process_target_data builds it.
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


# SEX_TYPE/PRODUCT_STATUS/SITE_TYPE live in process_target_data/pipeline.py, alongside
# build_site_df/build_status_df/build_sex_df and the cross-join that builds expected_data_structure
# from combined_target_data - this module has no use for them.

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


def _resolve_bounds_from_historical_lookup(
    year: int, rounds: list, products: list, campaign_name: str, historical_lookup: dict
) -> tuple:
    """Split every (round, produit) this run covers into ready-to-use (produit, round,
    campaign_start_date, campaign_end_date) rows (already dated in historical_lookup)
    and combos that still need explicit dates. Resolves only the boundary dates - not
    their day-by-day explosion, which process_target_data does once the dates reach
    combined_target_data."""
    rows = []
    needs_dates = []
    for r in rounds:
        for produit in products:
            key = (int(year), int(r), campaign_name, produit)
            dates = historical_lookup.get(key)
            if dates is None:
                needs_dates.append((r, produit))
                continue
            rows.append(
                {
                    "produit": produit,
                    "round": f"round {int(r)}",
                    "campaign_start_date": pd.to_datetime(dates["début"]),
                    "campaign_end_date": pd.to_datetime(dates["fin"]),
                }
            )
    return rows, needs_dates


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


def resolve_campaign_period_bounds(
    year: int,
    rounds: list,
    products: list,
    campaign_name: str,
    campaign_start_date,
    campaign_end_date,
    historical_lookup: dict,
) -> pd.DataFrame:
    """
    Resolve just the campaign period's boundary dates for every (round, produit) this
    run covers - one row per (produit, round) with campaign_start_date/
    campaign_end_date columns, meant to be merged onto this run's own target rows
    (pipeline.py's attach_campaign_metadata) rather than exploded into day rows here.
    process_target_data does that explosion from combined_target_data (see its
    _explode_period_bounds).

    Historical combinations already in ``historical_lookup`` use their known dates -
    a single run can replay several historical rounds this way. Any combination NOT
    found there needs campaign_start_date/campaign_end_date: since one date pair can
    only describe one calendar window, the run must then be restricted to a single
    round (mirrors how configure_new_campaign was always used - one round per run).
    """
    rows, needs_dates = _resolve_bounds_from_historical_lookup(
        year, rounds, products, campaign_name, historical_lookup
    )

    if needs_dates:
        hints = _missing_combo_hints(
            needs_dates, year, campaign_name, historical_lookup
        )
        start, end = _validate_new_period_request(
            needs_dates, rounds, year, campaign_start_date, campaign_end_date, hints
        )
        for r, produit in needs_dates:
            rows.append(
                {
                    "produit": produit,
                    "round": f"round {int(r)}",
                    "campaign_start_date": start,
                    "campaign_end_date": end,
                }
            )
    elif campaign_start_date or campaign_end_date:
        _info(
            "Les dates de campagne fournies sont ignorées: toutes les combinaisons "
            "demandées existent déjà dans la configuration historique."
        )

    return pd.DataFrame(rows)


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
        (int(year), int(r), campaign_name_internal, p)
        not in HISTORICAL_CAMPAIGNS_CONFIG
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
        year,
        rounds,
        products,
        campaign_name_internal,
        campaign_start_date,
        campaign_end_date,
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


def _warn_date_conflicts(
    conflicts: list, campaign_start_date: str, campaign_end_date: str
) -> None:
    _warn(
        f"CHEVAUCHEMENT DE DATES: {len(conflicts)} round(s) existant(s) ont une "
        "période qui chevauche celle fournie pour cette exécution."
    )
    for produit, rnd, s, e in conflicts:
        _warn(
            f"CHEVAUCHEMENT: {produit}, {rnd} ({s} - {e}) chevauche la période "
            f"{campaign_start_date} - {campaign_end_date} fournie."
        )


def _fail_on_date_conflicts(
    conflicts: list, campaign_start_date: str, campaign_end_date: str
) -> None:
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
