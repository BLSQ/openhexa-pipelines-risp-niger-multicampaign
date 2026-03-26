import numpy as np
import pandas as pd
import requests, json

import re
import random
import io

import datetime

from typing import Tuple, Dict, Any, List

from fuzzywuzzy import fuzz, process
import unicodedata


def request_explanatory_decorator(function):
    """
    A decorator to wrap requests functions and provide detailed error messages
    for HTTP errors and JSON decoding issues.

    Parameters:
        function (callable): The requests function to be wrapped.

    Returns:
        callable: The wrapped function with enhanced error handling.

    """

    def wrapper_request(
        url: str, headers: dict, process_message: str
    ) -> requests.Response | None:
        """
        Wrapper function to handle API requests and provide detailed error messages.

        Parameters:
            url (str): The URL to send the request to.
            headers (dict): The headers to include in the request.
            process_message (str): A message describing the process for error context.

        Returns:
            requests.Response | None: The response object from the GET request, or None if an error occurred.
        """
        try:
            r = function(url, headers)
            r.raise_for_status()
            if not r.text:
                print("ERROR: Empty response body")
            elif not r.text.strip():
                print("ERROR: Response contains only whitespace")
            else:
                r.json()
                return r
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            print(f"Response body: {r.text}")
        except requests.exceptions.JSONDecodeError as e:
            print(f"JSON Decode Error: {e}")
            print(f"Response content: {r.text}")

            return r
        except requests.exceptions.RequestException as e:
            print(
                f"It seem that some error occured during: {process_message}",
                r.status_code,
                e,
            )

    return wrapper_request


@request_explanatory_decorator
def request_with_explanation(url: str, headers: dict):
    """
    Sends a GET request to the specified URL with the provided headers.

    Parameters:
        url (str): The URL to send the request to.
        headers (dict): The headers to include in the request.

    Returns:
        requests.Response: The response object from the GET request.
    """
    return requests.get(url, headers=headers)


class Conector_from_Dict:
    """
    Class to create a connector from a dictionary containing connection parameters.
    """

    def __init__(self, iaso_connector_slug: Dict[str, Any]):
        """
        Initializes the connector with the provided connection parameters.

        Parameters:
            iaso_connector_slug (Dict[str, Any]): A dictionary containing 'username', 'password', and 'url' keys.
        """
        self.username = iaso_connector_slug["username"]
        self.password = iaso_connector_slug["password"]
        self.url = iaso_connector_slug["url"]


class IASOConnectionHandler:
    """
    Class to handle connection to IASO using provided connector details.

    Parameters:
        iaso_connector_slug (Dict[str, Any]): A dictionary containing 'username', 'password', and 'url' keys.

    """

    def __init__(self, iaso_connector_slug: Dict[str, Any]):
        """
        Initializes the IASO connection handler with the provided connector details.

        Parameters:
            iaso_connector_slug (Dict[str, Any]): A dictionary containing 'username', 'password', and 'url' keys.
        """
        self.iaso_connector = Conector_from_Dict(iaso_connector_slug)
        self.headers = self.connection()
        self.session = requests.Session()
        self.instance_info_cols = [
            "uuid",
            "form_id",
            "form_name",
            "org_unit_id",
            "org_unit_updated_at",
            "created_at",
            "period",
            "status",
        ]

    def connection(self) -> Dict[str, Any]:
        """
        Establishes a connection to the IASO API and retrieves authentication headers.

        Parameters:
            None

        Returns:
            headers (Dict[str, Any]): A dictionary containing the authentication headers.
        """
        creds = {
            "username": self.iaso_connector.username,
            "password": self.iaso_connector.password,
        }
        r = requests.post(f"{self.iaso_connector.url}/api/token/", json=creds)
        token = r.json().get("access")
        headers = {"Authorization": "Bearer %s" % token}
        return headers

    def _get_form_metadata(self, form_id: int) -> Dict[str, Any]:
        """
        Retrieves metadata for a specific form from the IASO API.

        Parameters:
            form_id (int): The ID of the form to retrieve metadata for.

        Returns:
            form_metadata_dict (Dict[str, Any]): A dictionary containing the form metadata.
        """
        fields_scope_list = [
            "id",
            "name",
            "form_id",
            "org_unit_type_ids",
            "period_type",
            "single_per_period",
            "latest_form_version",
        ]
        url = f"{self.iaso_connector.url}/api/forms/{form_id}/?fields={','.join(fields_scope_list)}"
        r = requests.get(url, headers=self.headers)
        form_metadata_dict = json.loads(r.content)
        return form_metadata_dict

    def _get_raw_ou_tree_frame_from_orgtype_id(
        self, org_unit_type_id: int, validation_status: str = "all"
    ) -> pd.DataFrame:
        """
        Retrieves the raw organizational unit tree frame for a specific organization unit type ID.

        Parameters:
            org_unit_type_id (int): The ID of the organization unit type.
            validation_status (str, optional): The validation status filter. Defaults to "all".

        Returns:
            org_df (pd.DataFrame): A dataframe containing the organizational unit information.
        """
        url = (
            f"{self.iaso_connector.url}/api/orgunits/"
            + "?order=id&page=1&searches=[{%22validation_status%22:%22"
            + f"{validation_status}"
            + "%22,%22orgUnitTypeId%22:%22"
            + f"{org_unit_type_id}"
            + "%22}]&xlsx=true"
        )
        r = request_with_explanation(
            url, self.headers, "file recover from IASO Instance for OrgType Info"
        )
        org_df = pd.read_excel(io.BytesIO(r.content), engine="openpyxl")
        return org_df

    def _generate_ou_treecolnames_dict_from_orgtype_id(
        self, org_unit_type_id: int
    ) -> List[List | Dict[str, str]]:
        """
        Generates a list of organizational unit tree column names and a corresponding dictionary
        mapping from organization unit type ID.

        Parameters:
            org_unit_type_id (int): The ID of the organization unit type.

        Returns:
            extendend_cols_dict (List[List, Dict]): A list containing the column names list and a dictionary mapping of column names.
        """
        url_depth = f"{self.iaso_connector.url}/api/v2/orgunittypes/{org_unit_type_id}/?fields=depth"
        org_depth = request_with_explanation(
            url_depth, self.headers, "OrgType depth Request"
        ).json()["depth"]

        base_cols = [
            "ID",
            "Nom",
            "Référence externe",
            "Source",
            "Validé",
            "Date de modification",
        ]
        extendend_cols = [f"parent {i}" for i in range(1, org_depth)] + [
            f"Ref Ext parent {i}" for i in range(1, org_depth)
        ]
        extendend_cols.extend(base_cols)

        base_dict = {
            "Date de modification": "updated_date",
            "ID": "org_unit_id",
            "Nom": f"LVL_{org_depth}_NAME",
            "Référence externe": f"LVL_{org_depth}_UID",
        }
        extendend_dict = {
            f"parent {i}": f"LVL_{org_depth - i}_NAME" for i in range(1, org_depth)
        }
        extendend_dict.update(
            {
                f"Ref Ext parent {i}": f"LVL_{org_depth - i}_UID"
                for i in range(1, org_depth)
            }
        )
        extendend_dict.update(base_dict)

        extendend_cols_dict = [extendend_cols, extendend_dict]

        return extendend_cols_dict

    def _get_ou_tree_frame_from_orgtype_id(
        self, org_unit_type_id: int, validation_status: str = "all"
    ) -> pd.DataFrame:
        """
        Retrieves the organizational unit tree frame for a specific organization unit type ID,
        with columns renamed according to the organization unit type.

        Parameters:
            org_unit_type_id (int): The ID of the organization unit type.
            validation_status (str, optional): The validation status filter. Defaults to "all".

        Returns:
            org_df (pd.DataFrame): A dataframe containing the organizational unit information with renamed columns.
        """
        org_df = self._get_raw_ou_tree_frame_from_orgtype_id(
            org_unit_type_id, validation_status
        )
        extendend_cols, extendend_dict = (
            self._generate_ou_treecolnames_dict_from_orgtype_id(org_unit_type_id)
        )
        present_cols = [col for col in org_df.columns if col in extendend_cols]
        org_df = org_df[present_cols].rename(columns=extendend_dict)
        return org_df

    def get_ou_tree_dataframe_from_the_form(
        self, form_id: int, validation_status: str = "all"
    ) -> pd.DataFrame:
        """
        Retrieves the organizational unit tree dataframe for a specific form ID.

        Parameters:
            form_id (int): The ID of the form to retrieve organizational unit data for.
            validation_status (str, optional): The validation status filter. Defaults to "all".

        Returns:
            org_df (pd.DataFrame): A dataframe containing the organizational unit information for the form.
        """
        org_df_total = [
            self._get_ou_tree_frame_from_orgtype_id(orgtype_id, validation_status)
            for orgtype_id in self._get_form_metadata(form_id)["org_unit_type_ids"]
        ]
        org_df = pd.concat(org_df_total, ignore_index=True)
        return org_df


def pyramid_selector(df: pd.DataFrame) -> pd.Series:
    """
    Selects the most recent row, excluding entries from 2023-07-14.

    Parameters:
        df (pd.DataFrame): The input dataframe containing an 'updated_date' column.

    Returns:
        pd.Series: The row with the most recent updated_date, excluding the forbidden date.
    """
    dates = pd.to_datetime(df["updated_date"])
    mask = dates.dt.date != datetime.date(
        2023, 7, 14
    )  # Filter out the "forbidden" date (2023-07-14)
    valid_df_dates = dates[mask]

    if valid_df_dates.empty:
        return pd.Series(dtype="object")

    max_idx = valid_df_dates.idxmax()
    most_recent_row = df.loc[max_idx]
    return most_recent_row
