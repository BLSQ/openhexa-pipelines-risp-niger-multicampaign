"""
Load stage of the v2 architecture (docs/ARCHITECTURE.md §2/§3): pushes the tables
build_visualisation_tables (Transform) produces to the OpenHEXA database.

Split out of build_visualisation_tables, which used to do table generation AND the
DB push in one pipeline. Transform now only produces and saves/exports the tables
(same parquet + dataset artifact convention every other pipeline in this repo uses
to hand off to the next stage); this pipeline reads those artifacts back and is the
only place `write_to_db` runs.
"""

import gc

from openhexa.sdk import pipeline

from db_utils import VISUALISATION_TABLE_NAMES, load_data_light, write_to_db


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
        df = load_data_light(table_name)
        write_to_db(df, table_name)
        del df
        gc.collect()


if __name__ == "__main__":
    load_visualisation_tables()
