"""
Load stage of the v2 architecture (docs/ARCHITECTURE.md §2/§3): pushes the tables
build_visualisation_tables (Transform) produces to the OpenHEXA database.

Split out of build_visualisation_tables, which used to do table generation AND the
DB push in one pipeline. Transform now only produces and saves/exports the tables
(same parquet + dataset artifact convention every other pipeline in this repo uses
to hand off to the next stage); this pipeline reads those artifacts back and is the
only place `write_to_db` runs.
"""

import sqlalchemy as sa
from openhexa.sdk import current_run, pipeline, workspace

from config import VISUALISATION_TABLE_NAMES
from shared_utils import load_data


@pipeline(
    "load_visualisation_tables",
    name="multi-campagne - Envoi des tables de visualisation vers la base de données",
)
def load_visualisation_tables():
    """
    Loads each visualisation table build_visualisation_tables saved to the
    workspace and pushes it to the OpenHEXA database, replacing any existing
    table of the same name.
    """
    for table_name in VISUALISATION_TABLE_NAMES:
        df = load_data(table_name)
        write_to_db(df, table_name)


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
        )
        current_run.log_info(f"Données écrites dans la table DB {table_name}")
        connection.close()
    except Exception as e:
        msg = (
            f"Erreur lors de l'écriture des données dans la table DB {table_name}: {e}"
        )
        current_run.log_error(msg)
        raise


if __name__ == "__main__":
    load_visualisation_tables()
