"""
Standalone validation for the auto-detecting engine (no OpenHexa needed).

Usage:
    python validate.py <folder_with_xlsx> [reference_parquet]

Runs the engine on each test file using ONLY products/year/rounds (no per-file
configuration) and checks the cible totals. District-level files are also checked
for an exact reproduction of the historical parquet base rows.
"""

import os
import sys
import warnings

warnings.filterwarnings("ignore")
import pandas as pd

from target_import import import_target_file

# (filename, products, year, rounds, rounding, expected {produit: base cible})
CASES = [
    (
        "Population JNV JNM ET DEPRARASITAGE (2).xlsx",
        ["vaccin polio", "vitamine A", "albendazole"],
        2024,
        [1, 2, 3, 4],
        "trunc",
        {"vaccin polio": 7220576, "vitamine A": 6738501, "albendazole": 6130156},
    ),
    (
        "cible_csi_fj_dosso_tahoua (5).xlsx",
        ["fièvre jaune"],
        2025,
        [1],
        "trunc",
        {"fièvre jaune": 8072902},
    ),
    (
        "Cible Men5-TCV CSI (3).xlsx",
        ["méningite", "tcv"],
        2025,
        [1, 2],
        "trunc",
        {"méningite": 15282698, "tcv": 15282698},
    ),
    (
        "cible_jnv_polio_2025 (4).xlsx",
        ["vaccin polio", "vitamine A", "albendazole"],
        2026,
        [1],
        "round",
        {"vaccin polio": 7834424, "vitamine A": 7834424, "albendazole": 7834424},
    ),
    (
        "Cible CSI JNV Avril 2026.xlsx",
        ["vaccin polio", "vitamine A", "albendazole"],
        2026,
        [2],
        "round",
        {"vaccin polio": 8313665, "vitamine A": 8313665, "albendazole": 8313665},
    ),
    (
        "Population Niger_2026.xlsx",
        ["vaccin polio", "albendazole", "vitamine A"],
        2026,
        [3],
        "trunc",
        {"vaccin polio": 8325437, "albendazole": 7130193, "vitamine A": 7699727},
    ),
]

# Known un-automatable outlier (bespoke 6/9-11 notation + component/total mix).
UNSUPPORTED = ["cible_niger_et_refugies_2025 (1).xlsx"]

EXPECTED_COLS = {"LVL_3_NAME", "LVL_6_NAME", "age", "cible", "year", "round", "produit"}


def _load_reference_parquet(path: str):
    if not path or not os.path.exists(path):
        return None
    return (
        pd.read_parquet(path)
        .drop(columns=["org_unit_id", "LVL_2_NAME"], errors="ignore")
        .drop_duplicates()
    )


def _check_cible_totals(
    tidy: pd.DataFrame, products: list, rounds: list, exp: dict
) -> bool:
    """Compare round[0]'s cible total per product against the recorded expectation."""
    all_ok = True
    one = tidy[tidy["round"] == f"round {rounds[0]}"]
    for p in products:
        got = int(one[one["produit"] == p]["cible"].sum())
        ok = got == exp[p]
        all_ok &= ok
        print(
            f"    {p:14s} round {rounds[0]}: {got:>12,}  {'OK' if ok else f'EXPECTED {exp[p]:,}'}"
        )
    return all_ok


def _check_parquet_base(tidy: pd.DataFrame, pq: pd.DataFrame, dmap: dict) -> bool:
    """District-level files have no LVL_6_NAME - check they exactly reproduce the
    historical parquet's base rows for the same (year, produit, round) combos,
    once district names are mapped through the same dmap the pipeline uses."""
    t = tidy.copy()
    t["LVL_3_NAME"] = t["LVL_3_NAME"].map(dmap).fillna(t["LVL_3_NAME"])
    t["LVL_6_NAME"] = None
    keys = ["LVL_3_NAME", "LVL_6_NAME", "age", "year", "round", "produit", "cible"]
    combos = pq[["year", "produit", "round"]].drop_duplicates()
    mine = t.merge(combos, on=["year", "produit", "round"])[keys].drop_duplicates()
    sub = pq.merge(
        t[["year", "produit", "round"]].drop_duplicates(),
        on=["year", "produit", "round"],
    )[keys].drop_duplicates()
    mrg = mine.merge(sub, on=keys, how="outer", indicator=True)
    om = (mrg._merge == "left_only").sum()
    op = (mrg._merge == "right_only").sum()
    ok = om == 0 and op == 0
    print(f"  parquet base check: {'EXACT MATCH' if ok else f'DIFF m={om} p={op}'}")
    return ok


def _check_case(folder: str, case: tuple, pq, dmap: dict) -> bool:
    fn, products, year, rounds, rounding, exp = case
    path = os.path.join(folder, fn)
    print("=" * 92)
    if not os.path.exists(path):
        print(f"{fn}: (not found)")
        return True
    tidy = import_target_file(path, products, year, rounds, rounding=rounding)
    assert set(tidy.columns).issubset(EXPECTED_COLS), tidy.columns
    print(fn)
    ok = _check_cible_totals(tidy, products, rounds, exp)
    if pq is not None and "LVL_6_NAME" not in tidy.columns:
        ok &= _check_parquet_base(tidy, pq, dmap)
    return ok


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    folder = sys.argv[1]
    pq = _load_reference_parquet(sys.argv[2] if len(sys.argv) > 2 else None)
    try:
        from config import district_name_map as dmap
    except Exception:
        dmap = {}

    all_ok = True
    for case in CASES:
        all_ok &= _check_case(folder, case, pq, dmap)

    print("=" * 92)
    print(f"Outlier requiring manual handling (not auto-detectable): {UNSUPPORTED}")
    print("\nALL AUTO-DETECTED CASES PASSED:", all_ok)


if __name__ == "__main__":
    main()
