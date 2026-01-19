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

# paths
outputs_path = "niger_june_24/outputs/"
target_historical_data_path = "niger_june_24/inputs/cibles/historique/"
target_other_data_path = "niger_june_24/inputs/cibles/autres"
temp_path = "niger_june_24/temp/"

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

# CSI matching
csi_matching_failed = {
    "abalak fachi": "abalak fachi tabalack",  # 3758127
    "abalak urbain2": "abalak urbain 2 abalak",  # 3758175
    "agadez sabon gari agadez": "agadez sabongari",  # 3766050
    "aguie guidandaweye": "aguie guidan daweye",  # 3764316
    "boboye birni i": "boboye birni ngaoure",  # 3759984
    "boboye birni ii": "boboye birni 2",  # 3759990
    "boboye oummize koira": "boboye oumou ize koira",  # 3759951
    "dakoro dandadi": "dakoro dan dadi",  # 3764637
    "dogon doutchi 2361": None,
    "dogon doutchi bawada guida": "dogon doutchi bawada",  # 3759582
    "dosso bella1": "dosso bella i",  # 3759392
    "dosso bellaii": "dosso bella ii",  # 3759394
    "dosso garbey gorou": "dosso garbey gorou bessa",  # 3759281
    "dosso manguekoira": "dosso mangue koira",  # 3759249
    "dosso waliokoira": "dosso walio koira",  # 3759425
    "fillingue tassi s k": "fillingue tassi sofa koira",  # 3760897
    "gazaoua gangara": None,
    "guidan roumdji eloum": "guidan roumdji eloum makeraoua",  # 3764575
    "guidan roumdji g roumdji": "guidan roumdji guidan roumdji 1",  # 3764387
    "guidan roumdji g sori": "guidan roumdji guidan sori",  # 3764527
    "guidan roumdji n wala": None,
    "guidan roumdji saesaboua": "guidan roumdji sae saboua",  # 3764507
    "illela dandaji": "illela dan dadji",  # 3758485
    "illela zourare": None,
    "illela illela": "dioundou illela",  # 3759060
    "ingall t tagait": "ingall tigdan tagait",  # 3765680
    "kollo koneberi": "kollo kone beri",  # 3760377
    "kollo koutoukalekoirazeno": "kollo koutoukale koirazeno",  # 3760403
    "kollo lakabia": "kollo latakabia sonrai",  # 3760314
    "loga loga": "loga loga i",  # 3759137
    "loga malankoara": "loga malam koira",  # 3759194
    "madaoua mallamawa": None,
    "madarounfa gabi": None,
    "magaria baoure": "mirriah baoure issoufou",  # 3763301
    "magaria magaria urbain i": "magaria magaria",  # 3764089
    "magaria magaria urbain ii": "magaria magaria ii",  # 3764072
    "maradi sabongari": None,
    "maradi zariai": "maradi zaria i",  # 3764129
    "maradi zariaii": "maradi zaria ii",  # 3764165
    "maradi zariaiii": "maradi zaria iii",  # 3764133
    "niamey ii nord lazaret": "niamey ii cabinet de soin nord lazaret cloturer",  # 3761891
    "niamey iv bossey bongou": None,
    "niamey iv camp fan": "niamey iv camp bano",  # 3762220
    "ouallam diney": "boboye diney zongou",  # 3760066
    "ouallam tondi kwindi": "ouallam tondikwindi",  # 3761213
    "ouallam tondi koirey": "ouallam tondikoirey",  # 3761195
    "say ganki": "say ganki bassarou",  # 3761109
    "say t foulbe": "say tchalla foulbe",  # 3761140
    "tahoua commune sabongari": "tahoua commune cabinet medical sabon gari",  # 3758832
    "tchintabaraden bagare": None,
    "tchintabaraden darha": "tchintabaraden zigat darha",  # 3758016
    "tera bangare": "tera bangare e 2024",  # 3761341
    "tera bouppou": "tera bouppo 2023",  # 3761328
    "tera tessa": None,
    "tibiri sabon gari": None,
    "tibiri tibiri urbain": "tibiri tibiri doutchi",  # 3759790
    "zinder sabongarizinder": "zinder sabongari",  # 3764862
}

list_of_valid_campaigns = [
    "vaccin polio",
    "rougeole",
    "fièvre jaune",
    "méningite",
    "tcv",
    "albendazole",
    "vitamine A",
]
