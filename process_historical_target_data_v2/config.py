"""
Paths and matching-stage configuration.

Campaign *structure* and *semantics* now live in campaign_registry.py; this file
keeps only what the org-unit matching stage needs (paths, the shared district
name canonicalisation used by the exact district merge, and the known CSI
fuzzy-match corrections).
"""

import os

from openhexa.sdk import workspace

# --------------------------------------------------------------------------- #
# Paths                                                                        #
# --------------------------------------------------------------------------- #
WORKSPACE_PATH = workspace.files_path
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
PROCESSED_TARGETS_PATH = os.path.join(OUTPUTS_PATH, "historical targets processed")
TARGETS_HISTORICAL_PATH = os.path.join(
    WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles", "historique"
)
TEMP_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "temp")

# --------------------------------------------------------------------------- #
# District name canonicalisation (shared by ALL district-level campaigns).     #
# The district-level matcher joins on LVL_3_NAME by exact merge, so the raw    #
# spreadsheet district labels are mapped to their official IASO "DS <name>"    #
# form here. This is naming canonicalisation, not a per-file value edit.       #
# --------------------------------------------------------------------------- #
district_name_map = {
    "Abala": "Abala",
    "Abalak": "Abalak",
    "Aderbissanat": "Aderbissinat",
    "Agadez commune": "Agadez",
    "Aguié": "Aguié",
    "Arlit": "Arlit",
    "Ayerou": "Ayorou",
    "Bagaroua": "Bagaroua",
    "Balleyara": "Balleyara",
    "Ballayara": "Balleyara",
    "Banibangou": "Banibangou",
    "Bankilare": "Bankilare",
    "Belbédji": "Belbedji",
    "Bermo": "Bermo",
    "Bilma": "Bilma",
    "Birni N'Konni": "Birni Konni",
    "Boboye": "Boboye",
    "Bosso": "Bosso",
    "Bouza": "Bouza",
    "Dakoro": "Dakoro",
    "Damagaram Takaya": "Damgaram Takaya",
    "Diffa": "Diffa",
    "Dioundou": "Dioundou",
    "Dogondoutchi": "Dogon Doutchi",
    "Dosso": "Dosso",
    "Dungass": "Doungass",
    "Falmey": "Falmey",
    "Filingue": "Fillingue",
    "Gaya": "Gaya",
    "Gazaoua": "Gazaoua",
    "Gotheye": "Gotheye",
    "Goudoumaria": "Goudoumaria",
    "Gouré": "Goure",
    "Guidan Roumdji": "Guidan Roumdji",
    "Iférouane": "Iferouane",
    "Illéla": "Illéla",
    "Ingall": "Ingall",
    "Keita": "Keita",
    "Kollo": "Kollo",
    "Loga": "Loga",
    "Madaoua": "Madaoua",
    "Madarounfa": "Madarounfa",
    "Magaria": "Magaria",
    "Mainé Soroa": "Mainé Soroa",
    "Malbaza": "Malbaza",
    "Maradi Ville": "Maradi Ville",
    "Kantché": "Matamèye",
    "Mayahi": "Mayahi",
    "Mirriah": "Mirriah",
    "N'Guigmi": "N'Guigmi",
    "N'gourti": "N'Gourti",
    "Niamey  I": "Niamey I",
    "Niamey  II": "Niamey II",
    "Niamey  III": "Niamey III",
    "Niamey  IV": "Niamey IV",
    "Niamey  V": "Niamey V",
    "Oullam": "Ouallam",
    "Say": "Say",
    "Tahoua Commune": "Tahoua Commune",
    "Tahoua Département": "Tahoua",
    "Tahoua Ville": "Tahoua Commune",
    "Takeita": "Takeita",
    "Tanout": "Tanout",
    "Tassara": "Tassara",
    "Tchintabaraden": "Tchintabaraden",
    "Tchirozérine ": "Tchirozérine",
    "Tera": "Tera",
    "Tesker": "Tesker",
    "Tessaoua": "Tessaoua",
    "Tibiri (Doutchi)": "Tibiri",
    "Tillabéry": "Tillabery",
    "Tillia": "Tillia",
    "Torodi": "Torodi",
    "Zinder Ville": "Zinder Ville",
}
# Strip keys so the mapping is robust to stray whitespace in the source labels
# (e.g. "Tchirozérine " with a trailing space).
district_name_map = {k.strip(): f"DS {v}" for k, v in district_name_map.items()}

# --------------------------------------------------------------------------- #
# CSI fuzzy-match corrections (used by the CSI matcher).                        #
# --------------------------------------------------------------------------- #
csi_matching_failed = {
    "agadez sabon gari agadez": "agadez sabongari",
    "boboye birni i": "boboye birni ngaoure",
    "boboye birni ii": "boboye birni 2",
    "dosso bella1": "dosso bella i",
    "dosso bellaii": "dosso bella ii",
    "guidan roumdji g roumdji": "guidan roumdji guidan roumdji 1",
    "kollo lakabia": "kollo latakabia sonrai",
    "madarounfa harounawa": "madarounfa harounaoua",
    "madarounfa madarounfa": "madarounfa madarounfa 1",
    "maradi sabongari": "maradi sabongari maradi",
    "maradi zariai": "maradi zaria i",
    "maradi zariaii": "maradi zaria ii",
    "maradi zariaiii": "maradi zaria iii",
    "matameye danbarto": "matameye dan barto",
    "matameye matameye 1": "matameye matameye",
    "zinder sabongarizinder": "zinder sabon gari",
    "zinder sabongari zinder": "zinder sabon gari",
}
