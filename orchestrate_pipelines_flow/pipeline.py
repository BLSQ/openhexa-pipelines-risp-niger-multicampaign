"""Template for newly generated pipelines."""

from openhexa.sdk import current_run, pipeline, workspace
import requests
import papermill as pm
from ast import literal_eval
import time


@pipeline(
    "orchestrate_pipelines_flow",
    name="multi-campagne - 02 - Pipeline de sauvegarde des données multi-campagnes dans la DB",
)
def orchestrate_pipelines_flow():
    """
    This pipeline orchestrates the execution of the Multi-campaign pipelines in sequence
    """
    hexa = get_hexa_connection()
    actions = define_actions()
    for action in actions:
        launch_action(hexa, actions[action], action, actions[action]["params"])


def get_hexa_connection():
    connection = workspace.custom_connection("risp-ner-campagnes-connection")
    RISP_NER_CAMPAIGN_TOKEN = connection.token
    hexa = OpenHEXAClient("https://app.openhexa.org")
    hexa.authenticate(with_token=RISP_NER_CAMPAIGN_TOKEN)
    current_run.log_info("Connecté à OpenHEXA")
    return hexa


def define_actions():
    return {
        "multi-campagne-import-et-traitement-des-donnees-de-cibles": {
            "type": "pipeline",
            "url": "https://api.openhexa.org/pipelines/MjI4NzA4ODQtZjQzMy00OGZmLTkyOTUtOWVmZWZjZDY2MWZlOjF3MTNXOTpRMnFoS2d3ck1QVjRQRERrZ3pOdUtYYktUSlU5RkhwN2VFOUhUWmwzUzcw/run",
            "params": {},
        },
        "multi-campagne-etablissement-de-la-structure-des-donnees-attendues": {
            "type": "pipeline",
            "url": "https://api.openhexa.org/pipelines/ZjIzYzgyMzctODk2Ni00OWQ2LWFlYmQtZmQxNWJiNjQ1OTM1OjF3MTNXdDpYZFlncVI5cUVRMTRUNHJMNmJJaWNuR2ZadkU0eUFobXUtMG9NUU1Cd0Rn/run",
            "params": {},
        },
        "multi-campagne-extraction-et-traitement-des-donnees-du-formulaire-iaso": {
            "type": "pipeline",
            "url": "https://api.openhexa.org/pipelines/NDgzMzcyMDMtNjFlNS00NzYxLTk2YjQtMWIxZWU0MDc3NWJkOjF3MTNYSzpMSHRHWERsMFhtTnFnTVRmTjBQWnlzQzVWVU5TRE1ibU9CTjNkZVhNVDlj/run",
            "params": {},
        },
        "multi-campagne-construction-des-tableaux-pour-la-visualisation": {
            "type": "pipeline",
            "url": "https://api.openhexa.org/pipelines/NGE0NGY1MTctMTQxNS00MjA2LWExYTItN2VjYTJmY2M4ZDRmOjF3MTNYZzpDd0ppVDJFWmVvZVJXOGNjalhtLUFZRGVMdURpOTN3OUwwRWlLTmczT3JV/run",
            "params": {},
        },
    }


class OpenHEXAClient:
    def __init__(self, base_url):
        self.url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {"Content-Type": "application/json", "User-Agent": "OpenHEXA Python Client"}
        )

    def authenticate(
        self,
        with_credentials: tuple[str, str] | None = None,
        with_token: str | None = None,
    ):
        """
        with_credentials: tuple of email and password
        with_token: JWT token
        """
        if with_credentials:
            resp = self._graphql_request(
                """
                mutation Login($input: LoginInput!) {
                    login(input: $input) {
                        success
                    }
                }
            """,
                {
                    "input": {
                        "email": with_credentials[0],
                        "password": with_credentials[1],
                    }
                },
            )
            resp.raise_for_status()
            print(resp.json())
            data = resp.json()["data"]
            if data["login"]["success"]:
                self.session.headers["Cookie"] = resp.headers["Set-Cookie"]
            else:
                raise Exception("Login failed")
        elif with_token:
            self.session.headers.update({"Authorization": f"Bearer {with_token}"})

    def pipelinerun(self, runid):
        res = self.query(
            f"""
            query {{
             		pipelineRun (id: "{runid}" )
              		{{run_id
                     executionDate
                     status
                     messages {{ 
      		            message
      		            timestamp}}
                        }}
                }}"""
        )

        return res

    def _graphql_request(self, operation, variables=None):
        return self.session.post(
            f"{self.url}/graphql", json={"query": operation, "variables": variables}
        )

    def query(self, operation, variables=None):
        resp = self._graphql_request(operation, variables)
        if resp.status_code == 400:
            raise Exception(resp.json()["errors"][0]["message"])
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errors"):
            raise Exception(payload["errors"])
        return payload["data"]


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
    Execute a pipeline run and monitor its status.

    Args:
        hexa (OpenHEXAClient): An instance of the OpenHEXAClient class.
        action (dict): The action to be executed in the pipeline.
        params (dict): The parameters for the pipeline run.

    Returns:
        None

    Raises:
        Exception: If the pipeline run fails to launch.

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


if __name__ == "__main__":
    orchestrate_pipelines_flow()
