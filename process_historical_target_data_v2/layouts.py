"""
Global, file-independent product knowledge used by the auto-detecting engine.

There is deliberately NO per-file layout table here anymore: the engine in
target_import.py discovers each spreadsheet's structure on its own (header row,
geo columns, district/CSI level, and which columns hold which age bracket), and
uses the ``products`` chosen at run time to decide which columns to read.

The only things that genuinely can't be read off a spreadsheet - what age
brackets each product is delivered in, and the words used to name products in
headers - live here as domain constants.
"""

# --------------------------------------------------------------------------- #
# For each product: target age bracket -> ordered source age candidates.       #
# The engine looks for a column matching the first available source age.        #
# --------------------------------------------------------------------------- #
PRODUCT_DEFS = {
    "vaccin polio": {
        "0-11 mois": ["0-11 mois"],
        "12-59 mois": ["12-59 mois"],
    },
    "rougeole": {
        "6-11 mois": ["6-11 mois", "0-11 mois"],
        "12-59 mois": ["12-59 mois"],
    },
    "vitamine A": {
        "6-11 mois": ["6-11 mois", "0-11 mois"],
        "12-24 mois": ["12-24 mois", "12-59 mois"],
    },
    "albendazole": {
        "12-23 mois": ["12-23 mois", "0-11 mois"],
        "24-59 mois": ["24-59 mois", "12-59 mois"],
    },
    "méningite": {
        "1-4 ans": ["1-4 ans"],
        "5-14 ans": ["5-14 ans"],
        "15-19 ans": ["15-19 ans"],
    },
    "tcv": {
        "1-4 ans": ["1-4 ans"],
        "5-14 ans": ["5-14 ans"],
        "15-19 ans": ["15-19 ans"],
    },
    "fièvre jaune": {
        "9-11 mois": ["9-11 mois"],
        "12-59 mois": ["12-59 mois"],
        "5-14 ans": ["5-14 ans"],
        "15-60 ans": ["15-60 ans"],
    },
}

# Products the pipeline offers as a parameter (keep in sync with PRODUCT_DEFS).
PRODUCT_CHOICES = list(PRODUCT_DEFS.keys())

# Header words (normalised, accent-free) that identify a product in a column's
# section header - used only when several products share one sheet with distinct
# columns (e.g. the "POPULATION CIBLE VPO / Vitamine / Albendazole" sheet).
PRODUCT_SYNONYMS = {
    "vpo": "vaccin polio",
    "opv": "vaccin polio",
    "polio": "vaccin polio",
    "vitamine": "vitamine A",
    "vitamin": "vitamine A",
    "albendazole": "albendazole",
    "albendazol": "albendazole",
    "mebendazole": "albendazole",
    "rougeole": "rougeole",
    "measles": "rougeole",
    "meningite": "méningite",
    "men5": "méningite",
    "menacwy": "méningite",
    "tcv": "tcv",
    "typhoide": "tcv",
    "jaune": "fièvre jaune",
}

# When a sheet only provides a single 0-59 months total, split it into the polio
# infant/child brackets with these fixed demographic ratios.
POLIO_INFANT_SPLIT = {"0-11 mois": 0.119140832, "12-59 mois": 0.880859168}
