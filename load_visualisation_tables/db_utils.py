"""
DB-loading/writing helpers for load_visualisation_tables: reading a saved table back
with a lighter memory footprint, and pushing it to the OpenHEXA database.
"""

import sqlalchemy as sa
from openhexa.sdk import current_run, workspace

from shared_utils import load_data

# The 17 tables build_visualisation_tables produces, in the order they get loaded and
# pushed. Keep in sync by hand with build_visualisation_tables/pipeline.py's own
# outputs_dict keys if a table is ever added, renamed or removed (see CLAUDE.md).
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

# Chunk size used when writing a dataframe to the DB via to_sql.
DB_WRITE_CHUNKSIZE = 10_000


def load_data_light(table_name: str):
    """
    load data, then downcast every low-cardinality text column to category.
    """
    df = load_data(table_name)
    for col in df.select_dtypes(include="object").columns:
        if df[col].nunique() < len(df) // 2:
            df[col] = df[col].astype("category")
    return df


def write_to_db(df, table_name: str) -> None:
    """
    Write the dataframe to a DB table with a given name. If the table already
    exists, it will be replaced.

    Args:
        df (pd.DataFrame): The dataframe to write.
        table_name (str): The name of the table to write to.

    Returns:
        None
    """
    current_run.log_info(f"Écriture des données dans la table DB {table_name}...")
    try:
        engine = sa.create_engine(workspace.database_url)
        connection = engine.connect()
        df.to_sql(
            name=table_name,
            con=connection,
            if_exists="replace",
            index=False,
            chunksize=DB_WRITE_CHUNKSIZE,
        )
        current_run.log_info(f"Données écrites dans la table DB {table_name}")
        connection.close()
    except Exception as e:
        msg = (
            f"Erreur lors de l'écriture des données dans la table DB {table_name}: {e}"
        )
        current_run.log_error(msg)
        raise
