"""
Generic OpenHEXA API client: authenticate against the OpenHEXA server and run
GraphQL queries/mutations against it. No orchestrate_pipelines_flow-specific logic
lives here - see pipeline_runner.py for the "run a pipeline and poll it" logic that
uses this client.
"""

import requests
from openhexa.sdk import current_run, workspace

OPENHEXA_BASE_URL = "https://app.openhexa.org"


def get_hexa_connection() -> "OpenHEXAClient":
    """
    Establish a connection to the OpenHEXA platform using a custom workspace connection.

    Args:
        None

    Returns:
        OpenHEXAClient: An authenticated OpenHEXA client instance.
    """
    connection = workspace.custom_connection("risp-ner-campagnes-connection")
    RISP_NER_CAMPAIGN_TOKEN = connection.token
    hexa = OpenHEXAClient(OPENHEXA_BASE_URL)
    hexa.authenticate(with_token=RISP_NER_CAMPAIGN_TOKEN)
    current_run.log_info("Connecté à OpenHEXA")
    return hexa


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
    ) -> None:
        """
        Authenticate the client using either credentials (email and password) or a token.

        Args:
            with_credentials (tuple[str, str], optional): A tuple containing the email and
                                                          password for authentication.
                                                          Defaults to None.
            with_token (str, optional): A token for authentication. Defaults to None.

        Returns:
            None
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

    def pipelinerun(self, runid) -> dict:
        """
        Retrieve the pipeline run data for a given run ID.

        Args:
            runid (str): The ID of the pipeline run to retrieve.

        Returns:
            dict: The pipeline run data.
        """
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

    def _graphql_request(
        self, operation: str, variables: dict | None = None
    ) -> requests.Response:
        """
        Sends a GraphQL request to the OpenHEXA API.

        Args:
            operation (str): The GraphQL query or mutation to be executed.
            variables (dict, optional): A dictionary of variables to be included
            in the GraphQL request. Defaults to None.

        Returns:
            requests.Response: The response object from the GraphQL request.
        """
        return self.session.post(
            f"{self.url}/graphql", json={"query": operation, "variables": variables}
        )

    def query(self, operation: str, variables: dict | None = None) -> dict:
        """
        Sends a GraphQL query to the OpenHEXA API.

        Args:
            operation (str): The GraphQL query to be executed.
            variables (dict, optional): A dictionary of variables to be included
            in the GraphQL request. Defaults to None.

        Returns:
            dict: The data returned from the GraphQL query.
        """
        resp = self._graphql_request(operation, variables)
        if resp.status_code == 400:
            raise Exception(resp.json()["errors"][0]["message"])
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errors"):
            raise Exception(payload["errors"])
        return payload["data"]
