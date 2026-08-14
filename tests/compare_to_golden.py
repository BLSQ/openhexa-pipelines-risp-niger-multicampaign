"""
Compare a v2-produced visualisation table against its golden (v1) counterpart, restricted to the
D5 reference campaign - vaccin polio / année 2026 / round 3 (docs/ARCHITECTURE.md §8/D5).

Golden outputs live in `docs/Golden outputs/{table_name}.parquet`, captured by hand from the live
v1 DB (Session 2 of the migration plan, docs/INVENTORY.md). As of this writing 7 of the 17
build_visualisation_tables outputs have been captured there:
    ner_vaccination_communications, ner_vaccination_communications_long,
    ner_vaccination_completude, ner_vaccination_couverture,
    ner_vaccination_couverture_csi_district_cibled, ner_vaccination_stock,
    ner_vaccination_supervision
Running this against any other table name will fail with a clear "golden file not found" message,
not a false pass - that's intentional. Still missing: ner_vaccination_cibles_district, the 5 filter
tables, ner_spatial_units, and the 3 undocumented extras.

Tables carry the reference campaign under different columns depending on their grain:
  - produit == "vaccin polio"       (coverage/stock/target-level tables)
  - choix_campagne == "polio"       (IASO-submission-level tables: completeness, supervision,
                                      communications - "polio" is the campaign_name IASO
                                      submissions carry, see process_iaso_form_data/config.py's
                                      campaign_name_mapping_dict; "vaccin polio" the produit maps
                                      to it there)
Tables with neither column (filter/lookup tables, ner_spatial_units) have no reference-campaign
slice to speak of and are compared in full instead.

Usage:
    python tests/compare_to_golden.py <table_name> <path_to_v2_parquet>

Example:
    python tests/compare_to_golden.py ner_vaccination_stock outputs/ner_vaccination_stock.parquet
"""

import os
import sys

import pandas as pd

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "Golden outputs")
YEAR, ROUND, PRODUIT, CAMPAIGN_NAME = 2026, "round 3", "vaccin polio", "polio"

# Columns tried, in order, to establish a stable row identity before comparing values - only the
# ones actually present in a given table are used. Must be exhaustive enough to fully disambiguate
# rows within a table, or ties get compared in whatever order they happen to land in after
# sorting only on the columns that ARE here - which produced spurious mismatches for
# ner_vaccination_communications_long (rows differing only by category/variable) until category
# and variable were added.
KEY_CANDIDATES = [
    "org_unit_id", "age", "produit", "sexe", "vaccination_status", "site", "period",
    "choix_campagne", "category", "variable",
]


def filter_to_reference_campaign(df: pd.DataFrame) -> pd.DataFrame:
    """Restrict to the D5 slice, if the table has campaign-level columns at all."""
    mask = pd.Series(True, index=df.index)
    narrowed = False
    if "year" in df.columns:
        mask &= df["year"].astype("Int64") == YEAR
        narrowed = True
    if "round" in df.columns:
        mask &= df["round"] == ROUND
        narrowed = True
    if "produit" in df.columns:
        mask &= df["produit"] == PRODUIT
        narrowed = True
    elif "choix_campagne" in df.columns:
        mask &= df["choix_campagne"] == CAMPAIGN_NAME
        narrowed = True
    if not narrowed:
        print("  (no year/round/produit/choix_campagne column found - comparing the whole table)")
        return df
    return df[mask].reset_index(drop=True)


def compare(table_name: str, v2_path: str) -> bool:
    golden_path = os.path.join(GOLDEN_DIR, f"{table_name}.parquet")
    if not os.path.exists(golden_path):
        print(f"  [FAIL] no golden file for '{table_name}' at {golden_path}.")
        print("  Not yet captured - see docs/INVENTORY.md's list of the 17 tables, or capture it "
              "from the live v1 DB before comparing this one.")
        return False
    if not os.path.exists(v2_path):
        print(f"  [FAIL] v2 output not found at {v2_path}.")
        return False

    golden = filter_to_reference_campaign(pd.read_parquet(golden_path))
    v2 = filter_to_reference_campaign(pd.read_parquet(v2_path))

    ok = True

    golden_cols, v2_cols = set(golden.columns), set(v2.columns)
    if golden_cols != v2_cols:
        ok = False
        print("  [FAIL] column mismatch.")
        if golden_cols - v2_cols:
            print(f"    missing in v2: {sorted(golden_cols - v2_cols)}")
        if v2_cols - golden_cols:
            print(f"    extra in v2:   {sorted(v2_cols - golden_cols)}")
    else:
        print(f"  [OK] columns match ({len(golden_cols)})")

    if len(golden) != len(v2):
        ok = False
        print(f"  [FAIL] row count: golden={len(golden)} v2={len(v2)}")
    else:
        print(f"  [OK] row count: {len(golden)}")

    # Value comparison uses an actual key-based join, not "sort both sides and compare
    # positionally" - a single extra/missing/reordered key row would otherwise cascade
    # misalignment through every row sorted after it, manufacturing hundreds of false
    # mismatches out of what might be a single real difference.
    common_cols = sorted(golden_cols & v2_cols)
    if common_cols:
        key_cols = [c for c in KEY_CANDIDATES if c in common_cols]
        value_cols = [c for c in common_cols if c not in key_cols]

        if not key_cols:
            print("  [WARN] no key columns identified in KEY_CANDIDATES for this table - "
                  "comparing as unordered multisets (can't attribute mismatches to specific rows)")
            g_sorted = golden[common_cols].sort_values(common_cols).reset_index(drop=True)
            v_sorted = v2[common_cols].sort_values(common_cols).reset_index(drop=True)
            if not g_sorted.equals(v_sorted):
                ok = False
                print("  [FAIL] values differ (as multisets)")
            else:
                print("  [OK] values match (as multisets)")
        else:
            merged = golden[common_cols].merge(
                v2[common_cols], on=key_cols, how="outer", suffixes=("_g", "_v"),
                indicator=True,
            )
            unmatched = merged[merged["_merge"] != "both"]
            if not unmatched.empty:
                ok = False
                n_g = int((merged["_merge"] == "left_only").sum())
                n_v = int((merged["_merge"] == "right_only").sum())
                print(f"  [FAIL] {len(unmatched)} row(s) have no matching key between golden "
                      f"and v2 ({n_g} only in golden, {n_v} only in v2, on key {key_cols})")

            matched = merged[merged["_merge"] == "both"]
            diffs = []
            for col in value_cols:
                gs, vs = matched[f"{col}_g"], matched[f"{col}_v"]
                if pd.api.types.is_numeric_dtype(gs) and pd.api.types.is_numeric_dtype(vs):
                    close = (gs - vs).abs().le(1e-6)
                    mismatch = ~(close | (gs.isna() & vs.isna())).fillna(False)
                else:
                    mismatch = ~((gs.astype(str) == vs.astype(str)) | (gs.isna() & vs.isna()))
                n = int(mismatch.sum())
                if n:
                    diffs.append((col, n))

            if diffs:
                ok = False
                print("  [FAIL] value mismatches by column (on matched keys only):")
                for col, n in diffs:
                    print(f"    {col}: {n} row(s) differ")
            elif unmatched.empty:
                print("  [OK] values match on all common columns")
    else:
        ok = False
        print("  [FAIL] no columns in common - nothing to compare values on")

    return ok


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    result = compare(sys.argv[1], sys.argv[2])
    print("PASS" if result else "FAIL")
    sys.exit(0 if result else 1)
