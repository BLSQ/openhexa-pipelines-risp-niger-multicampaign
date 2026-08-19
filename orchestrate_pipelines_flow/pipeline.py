from openhexa.sdk import pipeline

from config import PIPELINE_ACTIONS
from openhexa_client import get_hexa_connection
from pipeline_runner import launch_action


@pipeline(
    "orchestrate_pipelines_flow",
    name="multi-campagne - 02 - Orchestrate ETL pipelines",
)
def orchestrate_pipelines_flow():
    """
    This pipeline orchestrates the execution of the Multi-campaign pipelines in sequence
    """
    hexa = get_hexa_connection()
    for name, action in PIPELINE_ACTIONS.items():
        launch_action(hexa, action, name, action["params"])


if __name__ == "__main__":
    orchestrate_pipelines_flow()
