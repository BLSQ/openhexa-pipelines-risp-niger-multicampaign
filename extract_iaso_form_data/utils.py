import io
import time
from openhexa.sdk import current_run
import pandas as pd
import requests
import json
from typing import Tuple, Dict, Any, List


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


def period_form_convert_date(row: pd.Series) -> str:
    """
    Converts various date formats to a standardized 'YYYY-MM-DD' format.

    Parameters:
        row (pd.Series): A pandas Series containing date information.

    Returns:
        str: The converted date in 'YYYY-MM-DD' string format or the original value if conversion fails.
    """
    val_0 = row.iloc[0]

    if len(str(val_0)) == 8 and str(val_0).isdigit():
        return pd.to_datetime(val_0, format="%Y%m%d").strftime("%Y-%m-%d")
    elif len(str(val_0)) >= 8:
        return pd.to_datetime(val_0, format="%Y-%m-%d").strftime("%Y-%m-%d")
    elif len(str(row.iloc[1])) >= 8:
        return pd.to_datetime(row.iloc[1], unit="s").strftime("%Y-%m-%d")
    else:
        return val_0


def period_processing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the 'period' column in the DataFrame to standardize date formats.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing a 'period' column.

    Returns:
        df (pd.DataFrame): The DataFrame with the processed 'period' column.
    """
    df.period = df.period.mask(df.period == "Invalid date", None)
    df.period = df.period.mask(
        df.period.isna(),
        df.created_at.apply(lambda x: pd.to_datetime(x, unit="s").strftime("%Y-%m-%d")),
    )
    df.period = df.period.mask(
        df.period.notna(),
        df[["period", "created_at"]].apply(
            lambda row: period_form_convert_date(row), axis=1
        ),
    )
    return df


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

    def _get_form_dataframe_tuple_from_url(
        self, url: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Retrieves the form data structure from the provided URL.

        Parameters:
            url (str): The URL to retrieve the form data from.

        Returns:
            form_df_raw_tuple (Tuple[pd.DataFrame, pd.DataFrame]): A tuple containing the survey and choices DataFrames.
        """
        try:
            r = requests.get(url)
            assert r.status_code == 200
            content = io.BytesIO(r.content)
            form_survey = pd.read_excel(content, sheet_name="survey")
            form_choices = pd.read_excel(content, sheet_name="choices")
        except AssertionError:
            print(
                "It seem that some error occured during file recover from IASO Instance"
            )

        form_df_raw_tuple = (form_survey, form_choices)
        return form_df_raw_tuple

    def _get_data_structure_from_form_tuple(
        self, form_df_raw_tuple: Tuple[pd.DataFrame, pd.DataFrame]
    ) -> None:
        """
        Processes the raw survey and choice forms DataFrames to define and store the
        internal data structure.

        This method extracts field names and types from the survey sheet and maps
        them to their corresponding option lists from the choices sheet. It stores
        the final schema in 'self.form_data_structure_df' for use in data
        validation and formatting.

        Parameters:
            form_df_raw_tuple (Tuple[pd.DataFrame, pd.DataFrame]):
                A tuple containing the (survey, choices) DataFrames from the XLSForm.

        Returns:
            None: Updates the instance attributes in-place.
        """
        survey_tab_df = form_df_raw_tuple[0]
        needed_cols = ["type", "name", "relevant", "required", "constraint"]

        for col in [_ for _ in needed_cols if _ not in survey_tab_df.columns]:
            survey_tab_df[col] = None

        survey_tab_df = survey_tab_df.dropna(subset="name")[needed_cols]
        survey_tab_df["choice_name"] = survey_tab_df.type.str.split(" ", n=1).str[-1]
        survey_tab_df["type"] = survey_tab_df.type.str.split(" ", n=1).str[0]
        survey_tab_df = survey_tab_df[
            survey_tab_df.type.isin(
                ["integer", "select_one", "select_multiple", "text"]
            )
        ]

        required_mapping = {"yes": 1, "no": 0}
        constraint_mapping = {".>=0": 1, "other": 0}
        survey_tab_df["required_bool"] = (
            survey_tab_df["required"].map(required_mapping).fillna(0).astype(int)
        )
        survey_tab_df["positive_bool"] = (
            survey_tab_df["constraint"].map(constraint_mapping).fillna(0).astype(int)
        )

        survey_tab_df = survey_tab_df[
            [
                "name",
                "required_bool",
                "positive_bool",
                "relevant",
                "type",
                "choice_name",
            ]
        ].drop_duplicates()

        if form_df_raw_tuple[1].empty:
            survey_tab_df["choices"] = None
        else:
            choices_tab = form_df_raw_tuple[1]
            list_label_name = [
                col for col in choices_tab.columns if col in ["list_name", "list name"]
            ][0]
            choices_tab = choices_tab.dropna(subset=list_label_name)
            if choices_tab.shape[0] > 0:
                choices_label_dict = (
                    choices_tab.groupby(list_label_name)["name"].apply(list).to_dict()
                )
                survey_tab_df["choices"] = survey_tab_df["choice_name"].map(
                    choices_label_dict
                )

        self.form_data_structure_df = survey_tab_df
        self.form_content_form_structure_base_columns_list = survey_tab_df.name.unique()

    def get_data_structure_from_the_form(self, form_id: int) -> None:
        """
        Retrieves the data structure from the form specified by form_id.

        Parameters:
            form_id (int): The ID of the form to retrieve data from.

        Returns:
            None: Updates the instance attributes in-place with the form data structure.
        """
        excel_url = self._get_form_metadata(form_id)["latest_form_version"]["xls_file"]
        self._get_data_structure_from_form_tuple(
            self._get_form_dataframe_tuple_from_url(excel_url)
        )

    def get_cols_from_the_form(self, type_filter: str | None = None) -> zip:
        """
        Retrieves columns from the form data structure, optionally filtered by type.

        Parameters:
            type_filter (str, optional): The type of columns to filter by (e.g., 'integer').
                                        Defaults to None.

        Returns:
            col_info_zip(zip): A generator yielding tuples of (name, required_bool, type, choices).
        """
        df = self.form_data_structure_df

        if type_filter:
            df = df[df.type == type_filter]

        col_info_zip = zip(
            df.name.tolist(),
            df.required_bool.tolist(),
            df.type.tolist(),
            df.choices.tolist(),
        )

        return col_info_zip

    def _json_iaso_instance_info_extractor(
        self, instance_json: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Extracts instance information from the provided JSON data.

        Parameters:
            instance_json (Dict[str, Any]): The JSON data containing instance information.

        Returns:
            instance_full_df (pd.DataFrame): A dataframe containing the extracted instance information.
        """
        self.instance_info_cols = [
            "uuid",
            "form_id",
            "org_unit_id",
            "org_unit_updated_at",
            "created_at",
            "period",
            "status",
        ]
        instance_normalized_df = pd.json_normalize(instance_json, sep="_")
        full_cols_sub = [
            _
            for _ in instance_normalized_df.columns
            if _ in self.instance_info_cols
            or _.replace("file_content_", "")
            in self.form_content_form_structure_base_columns_list
        ]
        instance_full_df = instance_normalized_df[full_cols_sub]

        return instance_full_df

    def _json_iaso_crawler(self, json_query_answer: Dict[str, Any]) -> pd.DataFrame:
        """
        Crawls through the JSON query answer and extracts instance information.

        Parameters:
            json_query_answer (Dict[str, Any]): The JSON data containing instances information.

        Returns:
            instance_full_df (pd.DataFrame): A dataframe containing the extracted instance information.
        """
        instance_full_df = [
            self._json_iaso_instance_info_extractor(instance_json)
            for instance_json in json_query_answer["instances"]
        ]

        try:
            instance_full_df = pd.concat(instance_full_df, ignore_index=True)
            instance_full_df = instance_full_df.drop_duplicates(subset="uuid")
            instance_full_df.columns = [
                col.replace("file_content_", "") for col in instance_full_df.columns
            ]

        except Exception as e:
            current_run.log_error(f"Erreur lors de la concaténation des données : {e}")
            raise

        return instance_full_df

    def _json_request_extract(
        self,
        form_id: int,
        limit_batch: int = 50,
        dateFrom: str = None,
        dateTo: str = None,
    ) -> pd.DataFrame:
        """
        Extracts JSON data from IASO API for a specific form ID with pagination support.

        Parameters:
            form_id (int): The ID of the form to extract data for.
            limit_batch (int, optional): The number of records to fetch per batch. Defaults to 50.
            dateFrom (str, optional): The start date for filtering records. Defaults to None.
            dateTo (str, optional): The end date for filtering records. Defaults to None.

        Returns:
            form_full_df (pd.DataFrame): A dataframe containing the extracted JSON data.
        """
        instances_endpoint = f"{self.iaso_connector.url}/api/instances/"
        form_endpoint = instances_endpoint + f"?form_ids={form_id}"
        base_full_endpoint = form_endpoint + f"&limit={limit_batch}"

        if dateFrom:
            base_full_endpoint = base_full_endpoint + f"&dateFrom={dateFrom}"
        if dateTo:
            base_full_endpoint = base_full_endpoint + f"&dateTo={dateTo}"

        r = request_with_explanation(
            base_full_endpoint,
            self.headers,
            f"Form {form_id} Submission Request Page 1",
        )
        json_extract = r.json()
        total_pages = json_extract["pages"]
        print(total_pages)
        instance_full_df = self._json_iaso_crawler(json_extract)
        form_full_df = [instance_full_df]

        for page_id in range(2, total_pages + 1):
            time.sleep(0.5)
            r = request_with_explanation(
                base_full_endpoint + f"&page={page_id}",
                self.headers,
                f"Form {form_id} Submission Request Page {page_id}",
            )
            form_full_df.append(self._json_iaso_crawler(r.json()))

        form_full_df = pd.concat(form_full_df, ignore_index=True)
        form_full_df = form_full_df.drop_duplicates(subset="uuid")
        return form_full_df

    def _submmission_df_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formats the submission dataframe by processing the 'period' column and converting integer columns.

        Parameters:
            df (pd.DataFrame): The input submission dataframe.

        Returns:
            df (pd.DataFrame): The formatted submission dataframe.
        """
        df = period_processing(df)
        for col_info_zip in self.get_cols_from_the_form("integer"):
            col_name = col_info_zip[0]

            if col_name in df.columns:
                try:
                    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
                    df[col_name] = df[col_name].astype(float)
                except Exception:
                    pass

        return df

    def extract_submissions_info(
        self, form_id: int, dateFrom: str = None, dateTo: str = None
    ) -> pd.DataFrame:
        """
        Extracts and formats submission information for a specific form ID.

        Parameters:
            form_id (int): The ID of the form to extract submissions for.
            dateFrom (str, optional): The start date for filtering records. Defaults to None.
            dateTo (str, optional): The end date for filtering records. Defaults to None.

        Returns:
            instance_full_df (pd.DataFrame): The formatted submission dataframe.
        """
        self.get_data_structure_from_the_form(form_id)
        instance_full_df = self._json_request_extract(form_id, 50, dateFrom, dateTo)
        if instance_full_df.empty:
            return instance_full_df
        else:
            instance_full_df = self._submmission_df_formatting(instance_full_df)
            return instance_full_df
