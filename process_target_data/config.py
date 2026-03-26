from openhexa.sdk import workspace
import os

# paths
WORKSPACE_PATH = workspace.files_path
# WORKSPACE_PATH = os.path.join(os.getcwd(), "process_target_data", "workspace")  # local
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")
TARGET_OTHER_DATA_PATH = os.path.join(
    WORKSPACE_PATH, PROJECT_FOLDER, "inputs", "cibles", "autres"
)
TEMP_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "temp")

# configs
campaign_rename_dict = {
    "Polio": "vaccin polio",
    "Rougeole": "rougeole",
    "Fièvre jaune": "fièvre jaune",
    "Méningite": "méningite",
    "TCV": "tcv",
    "Albendazole": "albendazole",
    "Vitamine A": "vitamine A",
}

templates_required_cols_district = [
    "Pays",
    "Région",
    "District Sanitaire",
]

templates_required_cols_csi = [
    *templates_required_cols_district,
    "Commune",
    "CSI",
]


site_strategy_types_dict = {
    "Ordinaire": "ordinaire",
    "Spécial": "spécial",
    "Frontalier": "frontalier",
    "Transfrontalier : étranger": "transfrontalier : étranger",
    "Transfrontalier : Niger": "transfrontalier : Niger",
    "Fixe": "fixe",
    "Avancée": "avancé",
    "Mobile": "mobile",
}

cols_for_melting = ["Région", "District Sanitaire", "year", "produit"]

csi_district_rename_dict = {
    "Région": "LVL_2_NAME",
    "District Sanitaire": "LVL_3_NAME",
    "CSI": "LVL_6_NAME",
}

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
