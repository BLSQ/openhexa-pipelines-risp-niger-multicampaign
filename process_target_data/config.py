"""
Paths and matching-stage configuration.

Structure detection and campaign semantics are handled by target_import.py /
layouts.py. District name reconciliation is handled by geo_match.py (fuzzy).
This file keeps only the workspace paths and the known CSI fuzzy-match
corrections.
"""

import os
from openhexa.sdk import workspace

# Paths
WORKSPACE_PATH = workspace.files_path
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
PROCESSED_TARGETS_PATH = os.path.join(OUTPUTS_PATH, "historical targets processed")
EXPECTED_STRUCTURE_PROCESSED_PATH = os.path.join(
    OUTPUTS_PATH, "expected data structure processed"
)
TARGETS_HISTORICAL_PATH = os.path.join(
    WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles", "historique"
)
TEMP_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "temp")


# Expected-data-structure config
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
    (2026, 1, "polio", "vitamine A"): {"début": "2026-01-11", "fin": "2026-01-15"},
    (2026, 1, "polio", "albendazole"): {"début": "2026-01-11", "fin": "2026-01-15"},
    (2026, 1, "fièvre jaune", "fièvre jaune"): {
        "début": "2026-01-20",
        "fin": "2026-01-26",
    },
    (2026, 2, "polio", "vaccin polio"): {"début": "2026-04-24", "fin": "2026-05-01"},
    (2026, 2, "polio", "vitamine A"): {"début": "2026-04-24", "fin": "2026-05-01"},
    (2026, 2, "polio", "albendazole"): {"début": "2026-04-24", "fin": "2026-05-01"},
    (2026, 3, "polio", "vaccin polio"): {"début": "2026-07-09", "fin": "2026-07-18"},
    (2026, 3, "jnm", "albendazole"): {"début": "2026-07-02", "fin": "2026-07-09"},
    (2026, 3, "jnm", "vitamine A"): {"début": "2026-07-02", "fin": "2026-07-09"},
}


# CSI fuzzy-match corrections (used by the CSI matcher)
csi_matching_failed = {
    "agadez sabon gari agadez": "agadez sabongari",
    "aguie guidanmalambakabe": "aguie guidan malam bakabe",
    "aguie maiguizaouakagnou": "aguie maiguizaoua kagnou",
    "boboye birni i": "boboye birni ngaoure",
    "boboye birni ii": "boboye birni 2",
    "dosso bella1": "dosso bella i",
    "dosso bellaii": "dosso bella ii",
    "guidan roumdji g roumdji": "guidan roumdji guidan roumdji 1",
    "kollo lakabia": "kollo latakabia sonrai",
    "madaoua galma": "madaoua galma sedentaire",
    "madarounfa harounawa": "madarounfa harounaoua",
    "madarounfa madarounfa": "madarounfa madarounfa 1",
    "magaria baoure": "magaria baoure sarkin gako",
    "maradi sabongari": "maradi sabongari maradi",
    "maradi zariai": "maradi zaria i",
    "maradi zariaii": "maradi zaria ii",
    "maradi zariaiii": "maradi zaria iii",
    "matameye danbarto": "matameye dan barto",
    "matameye matameye 1": "matameye matameye",
    "zinder sabongarizinder": "zinder sabon gari",
    "zinder sabongari zinder": "zinder sabon gari",
}
