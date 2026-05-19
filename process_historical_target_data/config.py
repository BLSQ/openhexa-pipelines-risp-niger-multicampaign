from openhexa.sdk import workspace
import os

# paths
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(
#     os.getcwd(), "process_historical_target_data", "workspace"
# )  # local
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
TARGETS_HISTORICAL_PATH = os.path.join(
    WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles", "historique"
)
TEMP_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "temp")

# polio 2024
target_polio_2024_cols = [
    "LVL_3_NAME",
    "VPO_0-11 mois",
    "VPO_12-59 mois",
    "VA_6-11 mois",
    "VA_12-59 mois",
    "AL_12-23 mois",
    "AL_24-59 mois",
]

polio_2024_dict_districts_cibles_iaso = {
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
polio_2024_dict_districts_cibles_iaso = {
    key: f"DS {item}" for key, item in polio_2024_dict_districts_cibles_iaso.items()
}

# polio/rougeole 2025
target_polio_rougeole_2025_columns = [
    "LVL_3_NAME",
    "0-11 mois",
    "12-59 mois",
]

age_adjustment_rougeole = {"0-11 mois": "6-11 mois", "12-59 mois": "12-59 mois"}
age_adjustment_albendazole = {"0-11 mois": "12-23 mois", "12-59 mois": "24-59 mois"}
age_adjustment_vitA = {"0-11 mois": "6-11 mois", "12-59 mois": "12-24 mois"}


# yellow fever 2025/2026
target_yellow_fever_2025_2026_columns = [
    "LVL_3_NAME",
    "LVL_6_NAME",
    "9-11 mois_urban",
    "12-59 mois_urban",
    "5-14 ans_urban",
    "15-60 ans_urban",
    "9-11 mois_avancee",
    "12-59 mois_avancee",
    "5-14 ans_avancee",
    "15-60 ans_avancee",
    "9-11 mois_mobile",
    "12-59 mois_mobile",
    "5-14 ans_mobile",
    "15-60 ans_mobile",
]

target_yellow_fever_2025_2026_age_ranges = [
    "9-11 mois",
    "12-59 mois",
    "5-14 ans",
    "15-60 ans",
]

# men5/tcv 2025
target_men5_tcv_2025_columns_dict = {
    "Districts sanitaire": "LVL_3_NAME",
    "CSI": "LVL_6_NAME",
    "1-4ans": "1-4 ans",
    "5-14ans": "5-14 ans",
    "15-19ans": "15-19 ans",
}

# polio 2026 r1
target_polio_2026_r1_columns = [
    "LVL_2_NAME",
    "LVL_3_NAME",
    "LVL_6_NAME",
    "cible",
]

# polio 2026 r2
target_polio_2026_r2_dict = {
    "Districts": "LVL_3_NAME",
    "CSI": "LVL_6_NAME",
    "0-11 Mois (corrigé)": "0-11 mois",
    "12-59 Mois (corrigé)": "12-59 mois",
}

# CSI matching
csi_matching_failed = {
    ## incorrectly matched
    "agadez sabon gari agadez": "agadez sabongari",  # 3766050
    "boboye birni i": "boboye birni ngaoure",  # 3759984
    "boboye birni ii": "boboye birni 2",  # 3759990
    "dosso bella1": "dosso bella i",  # 3759392
    "dosso bellaii": "dosso bella ii",  # 3759394
    "guidan roumdji g roumdji": "guidan roumdji guidan roumdji 1",  # 3764387
    "kollo lakabia": "kollo latakabia sonrai",  # 3760314
    "madarounfa harounawa": "madarounfa harounaoua",  # 3764938
    "madarounfa madarounfa": "madarounfa madarounfa 1",  # 3765113
    "maradi sabongari": "maradi sabongari maradi",  # 3764144
    "maradi zariai": "maradi zaria i",  # 3764129
    "maradi zariaii": "maradi zaria ii",  # 3764165
    "maradi zariaiii": "maradi zaria iii",  # 3764133
    "matameye danbarto": "matameye dan barto",  # 3763041
    "matameye matameye 1": "matameye matameye",  # 3763116
    "zinder sabongarizinder": "zinder sabon gari",  # 3763586
    "zinder sabongari zinder": "zinder sabon gari",  # 3763586
    ## unmatched (as of 20/04/2026)
    # "dakoro kalgo": None,
    # "fillingue sabon gari": None,
    # "gazaoua gangara": None,
    # "guidan roumdji n wala": None,
    # "illela dangada wanke": None,
    # "madaoua bouzout": None,
    # "madaoua mallamawa": None,
    # "madaoua nakoni": None,
    # "madaoua tambeye nomade": None,
    # "madarounfa gabi": None,
    # "maradi ali dan sofo 2": None,
    # "magaria magaria": None,
    # "magaria magaria urbain i": None,
    # "niamey i kossey": None,
    # "niamey i tondikoirey": None,
    # "niamey ii dan zama koira": None,
    # "niamey iv bossey bongou": None,
    # "niamey iv nord est sarykoubou": None,
    # "ouallam diney": None,
    # "tahoua commune sabongari": None,
    # "tahoua commune sabon gari": None,
    # "tahoua sabon gari": None,
    # "tanout zermou": None,
    # "tessaoua toudou": None,
    # "tchintabaraden bagare": None,
    # "tera tessa": None,
    # "tibiri sabon gari": None,
}
