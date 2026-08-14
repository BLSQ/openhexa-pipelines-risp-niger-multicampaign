#!/usr/bin/env python3
"""
Sync the single canonical shared/utils.py into every active pipeline's own
shared_utils.py copy.

Why copies exist at all: OpenHEXA's deploy action (blsq/openhexa-push-pipeline-action)
uploads ONE pipeline directory at a time as a self-contained unit - a pipeline can't
import a file that lives outside its own folder at runtime. Every pipeline therefore
needs its own physical copy of the shared I/O helpers. This script is what keeps those
copies in sync: shared/utils.py (repo root) is the only file anyone should hand-edit;
run this (or let CI run it, see each pipeline's push-pipeline.yml) before every deploy.

Usage:
    python scripts/sync_shared_utils.py          # write the copies that are out of date
    python scripts/sync_shared_utils.py --check  # exit 1 if any copy is stale; writes nothing
"""

import argparse
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
CANONICAL = REPO_ROOT / "shared" / "utils.py"

# The v2-architecture pipelines (docs/ARCHITECTURE.md) - the only ones that still
# exist in this repo. The superseded v1 pipelines (generate_targets_templates,
# process_target_data (v1), configure_new_campaign, process_historical_target_data,
# create_expected_data_structure_for_historical_campaigns,
# combine_expected_data_structures) were removed once process_target_data (this
# repo's renamed process_historical_target_data_v2) absorbed everything they did.
ACTIVE_PIPELINES = [
    "extract_org_units",
    "extract_iaso_form_data",
    "process_iaso_form_data",
    "process_target_data",
    "build_visualisation_tables",
    "load_visualisation_tables",
]

HEADER = (
    '"""\n'
    "GENERATED FILE - do not edit directly.\n"
    "Source of truth: shared/utils.py (repo root). Regenerate every copy with:\n"
    "    python scripts/sync_shared_utils.py\n"
    '"""\n\n'
)


def render(canonical_src: str) -> str:
    return HEADER + canonical_src


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="verify only, write nothing"
    )
    args = parser.parse_args()

    rendered = render(CANONICAL.read_text())

    stale = []
    for pipeline in ACTIVE_PIPELINES:
        target = REPO_ROOT / pipeline / "shared_utils.py"
        if not target.exists():
            print(f"WARNING: {target} does not exist, skipping", file=sys.stderr)
            continue
        if target.read_text() == rendered:
            continue
        stale.append(target)
        if not args.check:
            target.write_text(rendered)
            print(f"synced {target.relative_to(REPO_ROOT)}")

    if args.check:
        if stale:
            print("Out of sync with shared/utils.py:")
            for t in stale:
                print(f"  - {t.relative_to(REPO_ROOT)}")
            return 1
        print("All pipeline copies match shared/utils.py.")
        return 0

    if not stale:
        print("All pipeline copies already matched shared/utils.py.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
