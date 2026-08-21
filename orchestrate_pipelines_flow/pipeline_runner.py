"""
Generic "run a remote OpenHEXA pipeline action and poll it to completion" logic -
independent of which specific pipelines are being run (see config.PIPELINE_ACTIONS
for that) and of the low-level API client itself (see openhexa_client.py).
"""

import time
from ast import literal_eval

import papermill as pm
import requests
from openhexa.sdk import current_run

from openhexa_client import OpenHEXAClient


def launch_action(hexa: OpenHEXAClient, action: dict, name: str, params: dict) -> None:
    """
    Launches an action based on its type.

    Parameters:
        hexa (OpenHEXAClient): The OpenHEXA client object.
        action (dict): The action to be launched.
        name (str): The name of the action.
        params (dict): The parameters for the action.

    Returns:
        None
    """
    current_run.log_info(f"Lancement de {action['type']} pour {name}")
    if action["type"] == "pipeline":
        execute_pipeline(hexa, action, params)
    elif action["type"] == "papermill":
        pm.execute_notebook(
            f"{action['url']}.ipynb", f"{action['url']}-output.ipynb", parameters=params
        )
        current_run.log_info("Papermill exécuté")


def execute_pipeline(hexa: OpenHEXAClient, action: dict, params: dict) -> None:
    """
    Execute a pipeline run and monitor its status. If the pipeline run does not end with
    status "success" (including when it never reached a terminal status after 3 launch
    attempts) - this makes the orchestrator's own run fail too, instead of silently
    continuing to the next pipeline in the chain.

    Args:
        hexa (OpenHEXAClient): An instance of the OpenHEXAClient class.
        action (dict): The action to be executed in the pipeline.
        params (dict): The parameters for the pipeline run.

    Returns:
        None

    Raises:
        RuntimeError:

    """
    attempt = 1
    r = run_pipeline(action, params)
    run_status = "Échec du lancement du pipeline"
    nb_messages_alread_shown = 0
    while attempt <= 3 and run_status not in ["success", "failed", "stopped"]:
        try:
            r.raise_for_status()
            res_run = get_pipeline_run_data(hexa, r)
            messages = res_run.get("messages", [])
            run_status = res_run["status"]
            if len(messages) > 0:
                nb_messages_alread_shown = display_new_messages(
                    nb_messages_alread_shown, messages
                )
        except Exception as e:
            attempt += 1
            current_run.log_info(f"Tentative {attempt} échouée: {e}")
            r = run_pipeline(action, params)
        time.sleep(10)
    current_run.log_info(f"Statut d'exécution du pipeline: {run_status}")
    if run_status != "success":
        raise RuntimeError(
            f"Le pipeline pour {action.get('url', '?')} s'est terminé avec le statut "
            f"'{run_status}' au lieu de 'success' - arrêt de l'orchestration."
        )


def get_pipeline_run_data(hexa: OpenHEXAClient, r: requests.models.Response) -> dict:
    """
    Retrieves the pipeline run data for a given run ID.

    Parameters:
    - hexa (OpenHEXAClient): An instance of the OpenHEXAClient class.
    - r (requests.models.Response): The response of the pipeline.

    Returns:
    - dict: The pipeline run data.

    """
    run_id = literal_eval(r.content.decode("utf-8"))["run_id"]
    res_run = hexa.pipelinerun(run_id)["pipelineRun"]
    return res_run


def display_new_messages(nb_messages_alread_shown: int, messages: list[str]) -> int:
    """
    Display new messages from a given index.

    Args:
        nb_messages_alread_shown (int): The number of messages already shown.
        messages (list[str]): A list of messages.

    Returns:
        int: The updated number of messages already shown.
    """
    for m in messages[nb_messages_alread_shown:]:
        current_run.log_info(f"----> {m['message']}")
        nb_messages_alread_shown += 1
    return nb_messages_alread_shown


def run_pipeline(action: dict, params: dict) -> requests.models.Response:
    """
    Run the pipeline with the given action and parameters.

    Args:
        action (dict): The action to be performed.
        params (dict): The parameters for the action.

    Returns:
        requests.models.Response: The response from the pipeline.

    """
    r = requests.post(
        action["url"], json=params, headers={"content-type": "application/json"}
    )
    return r
