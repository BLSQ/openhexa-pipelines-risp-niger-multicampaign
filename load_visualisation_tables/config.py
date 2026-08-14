import os

from openhexa.sdk import workspace

# paths
WORKSPACE_PATH = workspace.files_path
PROJECT_FOLDER = "multi-campagne"
OUTPUTS_PATH = os.path.join(WORKSPACE_PATH, PROJECT_FOLDER, "outputs")

# table names
VISUALISATION_TABLE_NAMES = [
    "ner_vaccination_couverture",
    "ner_vaccination_couverture_csi_district_cibled",
    "ner_vaccination_completude",
    "ner_vaccination_stock",
    "ner_vaccination_supervision",
    "ner_vaccination_communications_long",
    "ner_vaccination_communications",
    "ner_vaccination_cibles_district",
    "ner_vaccination_campaign_filter_table",
    "ner_vaccination_month_filter_table",
    "ner_vaccination_round_filter_table",
    "ner_vaccination_year_filter_table",
    "ner_vaccination_products_filter_table",
    "ner_vaccination_combination_filter_table",
    "ner_spatial_units",
    "ner_spatial_units_non_dynamic",
    "ner_vaccination_campaign_round_summary",
]
