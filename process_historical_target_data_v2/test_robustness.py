"""
Robustness suite: simulate plausibly-different incoming files and check the engine
still produces the SAME numbers as the untouched original.

Rationale
---------
The engine was developed against 7 real files. New files will differ in small,
predictable ways: an extra logo row, a renamed column, a typo, a moved column,
different accents/casing, blank spacer columns. Each mutation below rewrites a
real workbook, re-runs the engine, and asserts the per-product totals are
unchanged. Mutations that SHOULD raise a clear error are checked too.

Usage:  python test_robustness.py <folder_with_xlsx>
"""

import os
import sys
import warnings

warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd

from target_import import TargetImportError, import_target_file

# Representative file per structure family + its run parameters.
BASES = [
    (
        "Population JNV JNM ET DEPRARASITAGE.xlsx",
        ["vaccin polio", "vitamine A", "albendazole"],
        2024,
        [1],
        "trunc",
    ),
    ("cible_csi_fj_dosso_tahoua.xlsx", ["fièvre jaune"], 2025, [1], "trunc"),
    ("Cible Men5-TCV CSI.xlsx", ["méningite", "tcv"], 2025, [1], "trunc"),
    ("cible_jnv_polio_2025.xlsx", ["vaccin polio"], 2026, [1], "round"),
    ("Cible CSI JNV Avril 2026.xlsx", ["vaccin polio"], 2026, [2], "round"),
    (
        "Population Niger_2026.xlsx",
        ["vaccin polio", "albendazole", "vitamine A"],
        2026,
        [3],
        "trunc",
    ),
    (
        "cible_niger_et_refugies_2025.xlsx",
        ["vaccin polio", "rougeole", "vitamine A", "albendazole"],
        2025,
        [1],
        "trunc",
    ),
]


# --------------------------------------------------------------------------- #
# Mutations: each takes the raw (header=None) frame and returns a new frame.    #
# --------------------------------------------------------------------------- #
def m_prepend_rows(df, n=3):
    """Extra logo/title rows pushed above the header."""
    pad = pd.DataFrame([[np.nan] * df.shape[1]] * n, columns=df.columns)
    pad.iloc[0, 0] = "REPUBLIQUE DU NIGER"
    pad.iloc[1, 0] = "Ministère de la Santé Publique"
    out = pd.concat([pad, df], ignore_index=True)
    out.columns = range(out.shape[1])
    return out


def m_insert_blank_col(df):
    """A blank spacer column inserted before the value block."""
    cols = list(df.columns)
    mid = min(4, len(cols) - 1)
    out = df.copy()
    out.insert(mid, "spacer", np.nan)
    out.columns = range(out.shape[1])
    return out


def m_append_extra_col(df):
    """An unrelated trailing column (comments)."""
    out = df.copy()
    out[df.shape[1]] = "commentaire"
    out.columns = range(out.shape[1])
    return out


def _typo(word):
    """Double a middle letter: 'total' -> 'totall'-ish spelling slip."""
    return word[: len(word) // 2] + word[len(word) // 2] + word[len(word) // 2 :]


def m_typo_headers(df):
    """Introduce spelling slips in geo/total/corrigé header words."""
    targets = (
        "district",
        "districts",
        "region",
        "regions",
        "csi",
        "total",
        "totale",
        "corrigé",
        "corrige",
        "mois",
        "ans",
    )
    out = df.copy()
    for r in range(min(25, out.shape[0])):
        for c in range(out.shape[1]):
            v = out.iat[r, c]
            if isinstance(v, str):
                low = v.lower()
                for t in targets:
                    if t in low and len(t) >= 5:
                        i = low.index(t)
                        out.iat[r, c] = (
                            v[:i] + _typo(v[i : i + len(t)]) + v[i + len(t) :]
                        )
                        break
    return out


def m_upper_no_accents(df):
    """Header casing/accent variation."""
    out = df.copy()
    for r in range(min(25, out.shape[0])):
        for c in range(out.shape[1]):
            v = out.iat[r, c]
            if isinstance(v, str):
                out.iat[r, c] = (
                    v.upper()
                    .replace("É", "E")
                    .replace("È", "E")
                    .replace("Ê", "E")
                    .replace("Ô", "O")
                )
    return out


def m_extra_spaces(df):
    """Stray whitespace/newlines inside header labels."""
    out = df.copy()
    for r in range(min(25, out.shape[0])):
        for c in range(out.shape[1]):
            v = out.iat[r, c]
            if isinstance(v, str):
                out.iat[r, c] = f"  {v.replace(' ', '  ')} \n"
    return out


def m_trailing_junk_rows(df):
    """Footer notes appended after the data."""
    pad = pd.DataFrame([[np.nan] * df.shape[1]] * 3, columns=df.columns)
    pad.iloc[1, 0] = "Source: DGSP/DI - données provisoires"
    pad.iloc[2, 0] = "NB: y compris les réfugiés"
    out = pd.concat([df, pad], ignore_index=True)
    out.columns = range(out.shape[1])
    return out


MUTATIONS = [
    ("extra title rows above header", m_prepend_rows),
    ("blank spacer column inserted", m_insert_blank_col),
    ("extra trailing column", m_append_extra_col),
    ("typos in header keywords", m_typo_headers),
    ("UPPERCASE / accents stripped", m_upper_no_accents),
    ("stray spaces + newlines in headers", m_extra_spaces),
    ("footer note rows appended", m_trailing_junk_rows),
]


def totals(path, products, year, rounds, rounding):
    t = import_target_file(path, products, year, rounds, rounding=rounding)
    one = t[t["round"] == f"round {rounds[0]}"]
    return {p: int(one[one["produit"] == p]["cible"].sum()) for p in products}


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    folder, tmp = sys.argv[1], "/tmp/robustness"
    os.makedirs(tmp, exist_ok=True)

    passed = failed = 0
    failures = []
    for fn, products, year, rounds, rounding in BASES:
        src = os.path.join(folder, fn)
        if not os.path.exists(src):
            print(f"(skip missing {fn})")
            continue
        raw = pd.read_excel(src, header=None)
        base = totals(src, products, year, rounds, rounding)
        print("=" * 88)
        print(f"{fn}\n  baseline: {base}")

        for label, mut in MUTATIONS:
            out = os.path.join(tmp, "mut.xlsx")
            try:
                mutated = mut(raw.copy())
                mutated.to_excel(out, header=False, index=False)
                got = totals(out, products, year, rounds, rounding)
                ok = got == base
            except TargetImportError as e:
                got, ok = f"TargetImportError: {str(e).splitlines()[0]}", False
            except Exception as e:
                got, ok = f"{type(e).__name__}: {e}", False
            print(f"    [{'PASS' if ok else 'FAIL'}] {label}")
            if ok:
                passed += 1
            else:
                failed += 1
                failures.append((fn, label, got, base))

    print("=" * 88)
    print(f"MUTATION RESULTS: {passed} passed, {failed} failed")
    for fn, label, got, base in failures:
        print(f"  FAIL {fn} :: {label}\n       got={got}\n       exp={base}")

    # --- error-path checks: these SHOULD fail with a clear, actionable message ---
    print("\n" + "=" * 88)
    print("ERROR-PATH CHECKS (a clear TargetImportError is the expected outcome)")
    cases = {
        "empty sheet": (pd.DataFrame([[np.nan] * 3] * 4), ["vaccin polio"]),
        "no geo header": (
            pd.DataFrame([["Zone", "0-11 mois"], ["Abala", 5], ["Bilma", 7]]),
            ["vaccin polio"],
        ),
        "no age columns": (
            pd.DataFrame([["Districts", "Cibles"], ["Abala", 5]]),
            ["vaccin polio"],
        ),
        # A product must NOT be produced from only part of its age brackets:
        # here 12-59 is mislabelled 12-60, so polio is incomplete -> must abort.
        "partial age brackets (12-60 instead of 12-59)": (
            pd.DataFrame(
                [
                    ["Districts", "0-11 mois", "12-60 mois"],
                    ["Abala", 100, 900],
                    ["Bilma", 50, 450],
                ]
            ),
            ["vaccin polio"],
        ),
        "unknown product requested": (
            pd.DataFrame(
                [["Districts", "0-11 mois", "12-59 mois"], ["Abala", 100, 900]]
            ),
            ["vaccin inexistant"],
        ),
    }
    for name, (frame, prods) in cases.items():
        out = os.path.join(tmp, "err.xlsx")
        frame.to_excel(out, header=False, index=False)
        try:
            import_target_file(out, prods, 2026, [1])
            print(f"    [FAIL] {name}: no error raised")
        except TargetImportError as e:
            has_fix = "À FAIRE" in str(e)
            print(
                f"    [{'PASS' if has_fix else 'WEAK'}] {name}: {str(e).splitlines()[0]}"
            )
        except Exception as e:
            print(f"    [FAIL] {name}: wrong type {type(e).__name__}: {e}")

    # A complete file with the SAME shape must still succeed (guards against the
    # completeness check being over-zealous).
    out = os.path.join(tmp, "ok.xlsx")
    pd.DataFrame(
        [
            ["Districts", "0-11 mois", "12-59 mois"],
            ["Abala", 100, 900],
            ["Bilma", 50, 450],
        ]
    ).to_excel(out, header=False, index=False)
    try:
        t = import_target_file(out, ["vaccin polio"], 2026, [1])
        ok = int(t["cible"].sum()) == 1500
        print(
            f"    [{'PASS' if ok else 'FAIL'}] complete minimal file still processes "
            f"(total={int(t['cible'].sum())}, expected 1500)"
        )
    except Exception as e:
        print(f"    [FAIL] complete minimal file rejected: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
