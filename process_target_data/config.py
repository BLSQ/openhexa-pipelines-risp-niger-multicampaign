iaso_playground_connector_slug = {
    "url": "https://iaso-playground.bluesquare.org",
    "username": "fernando_di_demo",
    "password": "13.5	19.5	10.5",
}

iaso_connector_slug = {
    "url": "https://iaso.bluesquare.org",
    "username": "fernando_diniger",
    "password": "hbe8quh1hjm*cyx6AQH",
}

iaso_form_id = 1186

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
    "Tahoua Ville": "Tahoua Commune",  # unclear that's the case
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

age_adjustment_rougeole = {"0-11 mois": "6-11 mois"}

# yellow fever 2025
target_yellow_fever_2025_columns = [
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

target_yellow_fever_2025_age_ranges = [
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

# polio 2025 r3
target_polio_2025_r3_columns = [
    "LVL_2_NAME",
    "LVL_3_NAME",
    "LVL_6_NAME",
    "cible",
]

# CSI matching
csi_matching_failed = {
    "boboye birni ii": "boboye birni 2",  # 3759990
    "boboye birni i": "boboye birni ngaoure",  # 3759984
    "dosso bella 1": "dosso bella i",  # 3759392
    "dosso bella ii": "dosso bella ii",  # 3759394
    "magaria magaria urbain i": "magaria magaria",  # 3764089
    "magaria magaria urbain ii": "magaria magaria ii",  # 3764072
    "niamey ii nord lazaret": "niamey ii cabinet de soin nord lazaret cloturer",  # 3761891
    "niamey iv camp fan": "niamey iv camp bano",  # 3762220
    "tibiri tibiri urbain": "tibiri tibiri doutchi",  # 3759790
    "tchintabaraden bagare": "",
    "dogon doutchi 2361": "",
}
