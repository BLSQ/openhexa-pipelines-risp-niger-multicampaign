import io
import numpy as np
import pandas as pd
import requests
import random
import json
import datetime
from typing import Tuple, Dict, Any, List


def round_assignment(df):
    """
    Assigns rounds to the DataFrame based on the 'period' column,
    using different logic based on the 'choix_campagne' column.
    """

    # Define the boolean masks for the campaign diseases
    is_meningite_tcv = df["choix_campagne"].isin(["men5_tcv", "méningite", "tcv"])
    is_yellow_fever = df["choix_campagne"].isin(
        ["Fievre_Jaune", "fievre jaune", "fièvre jaune"]
    )
    is_polio = df["choix_campagne"].isin(["POLIOMYELITE", "polio"])
    is_rougeole = df["choix_campagne"].isin(["rougeole"])

    # Define the logic for the méningite/tcv campaign
    meningite_tcv_logic = np.where(
        (df["period"] <= pd.to_datetime("2025-12-02"))
        & (df["period"] >= pd.to_datetime("2025-11-24")),
        "round 1",
        np.where(
            (pd.to_datetime("2025-12-15") <= df["period"])
            & (df["period"] <= pd.to_datetime("2025-12-22")),
            "round 2",
            "date invalide",
        ),
    )

    # Define the logic for the yellow fever campaign
    yellow_fever_logic = np.where(
        (pd.to_datetime("2025-10-27") <= df["period"])
        & (df["period"] <= pd.to_datetime("2025-11-04")),
        "round 1",
        np.where(
            (pd.to_datetime("2026-01-20") <= df["period"])
            & (df["period"] <= pd.to_datetime("2026-01-26")),
            "round 1",
            "date invalide",
        ),
    )

    # Define the logic for the polio campaigns
    polio_campaign_logic = np.where(
        (pd.to_datetime("2024-07-10") <= df["period"])
        & (df["period"] <= pd.to_datetime("2024-07-24")),
        "round 1",
        np.where(
            (pd.to_datetime("2024-09-28") <= df["period"])
            & (df["period"] <= pd.to_datetime("2024-10-06")),
            "round 2",
            np.where(
                (pd.to_datetime("2024-10-25") <= df["period"])
                & (df["period"] <= pd.to_datetime("2024-11-01")),
                "round 3",
                np.where(
                    (pd.to_datetime("2024-12-01") <= df["period"])
                    & (df["period"] <= pd.to_datetime("2024-12-12")),
                    "round 4",
                    np.where(
                        (pd.to_datetime("2025-05-04") <= df["period"])
                        & (df["period"] <= pd.to_datetime("2025-05-08")),
                        "round 1",
                        np.where(
                            (pd.to_datetime("2025-06-14") <= df["period"])
                            & (df["period"] <= pd.to_datetime("2025-06-21")),
                            "round 2",
                            np.where(
                                (pd.to_datetime("2026-01-11") <= df["period"])
                                & (df["period"] <= pd.to_datetime("2026-01-15")),
                                "round 1",
                                "date invalide",
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )

    # Define the logic for the rougeole campaigns
    rougeole_campaign_logic = np.where(
        (pd.to_datetime("2025-04-18") <= df["period"])
        & (df["period"] <= pd.to_datetime("2025-04-24")),
        "round 1",
        "date invalide",
    )

    df["round"] = np.where(
        is_meningite_tcv,
        meningite_tcv_logic,
        np.where(
            is_yellow_fever,
            yellow_fever_logic,
            np.where(
                is_polio,
                polio_campaign_logic,
                np.where(is_rougeole, rougeole_campaign_logic, "campagne inconnue"),
            ),
        ),
    )

    return df


def year_assignment(df):
    """
    Assigns the year to the DataFrame based on the 'period' column.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing a 'period' column.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'year' column.
    """
    df["year"] = df["period"].dt.year


def request_explanatory_decorator(function):
    """
    A decorator to wrap requests functions and provide detailed error messages
    for HTTP errors and JSON decoding issues.

    Parameters:
        function (callable): The requests function to be wrapped.

    Returns:
        callable: The wrapped function with enhanced error handling.

    """

    def wrapper_request(url, headers, process_message):
        """
        Wrapper function to handle requests and provide detailed error messages.

        Parameters:
            url (str): The URL to send the request to.
            headers (dict): The headers to include in the request.
            process_message (str): A message describing the process for error context.

        Returns:
            Response object if successful, None otherwise.
        """
        try:
            r = function(url, headers)
            # Check if response is successful
            r.raise_for_status()

            # Check if content exists
            if not r.text:
                print("ERROR: Empty response body")
            elif not r.text.strip():
                print("ERROR: Response contains only whitespace")
            else:
                # Try to parse JSON
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
def request_with_explanation(url, headers):
    """
    Sends a GET request to the specified URL with the provided headers.

    Parameters:
        url (str): The URL to send the request to.
        headers (dict): The headers to include in the request.

    Returns:
        Response object from the GET request.
    """
    return requests.get(url, headers=headers)


def period_form_convert_date(row):
    """
    Converts various date formats to a standardized 'YYYY-MM-DD' format.

    Parameters:
        row (pd.Series): A pandas Series containing date information.

    Returns:
        str: The converted date in 'YYYY-MM-DD' format or the original value if conversion fails.
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
        pd.DataFrame: The DataFrame with the processed 'period' column.
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


def empty_generator(values: np.ndarray, empty_quality: float) -> np.ndarray:
    """
    Generates NaN values based on a quality threshold.

    Parameters:
        values (np.ndarray): The input values to process.
        empty_quality (float): The threshold for generating empty values (between 0 and 1)

    Returns:
        np.ndarray: An array with some values replaced by NaN based on the empty_quality threshold
    """
    return np.where(
        np.random.default_rng().uniform(size=len(values)) > empty_quality,
        values,
        np.nan,
    )


def empty_generator_object(values: np.ndarray, empty_quality: float) -> np.ndarray:
    """
    Generates None values based on a quality threshold.

    Parameters:
        values (np.ndarray): The input values to process.
        empty_quality (float): The threshold for generating empty values (between 0 and 1
    """
    return np.where(
        np.random.default_rng().uniform(size=len(values)) > empty_quality, values, None
    )


def outlier_generator(values: np.ndarray) -> np.ndarray:
    """
    Generates outlier values.

    Parameters:
        values (np.ndarray): The input values to process.

    Returns:
        np.ndarray: An array with some values replaced by outliers.
    """
    outlier_thr = 0.005
    return np.where(
        np.random.default_rng().uniform(size=len(values)) < outlier_thr,
        values,
        np.multiply(np.random.default_rng().uniform(size=len(values)) * 100, values),
    )


def numbers_per_param(
    # param: str,
    # constraint_positive: bool,
    param_required: bool,
    size_picked: float,
    stability_picked: float,
    empty_quality: float,
    periods: list,
    integer_value: bool = True,
):
    """
    Generates a list of numbers based on specified parameters.

    Parameters:
        param (str): The parameter name.
        constraint_positive (bool): If True, ensures generated numbers are positive.
        param_required (bool): If True, ensures that a number is always generated.
        size_picked (float): The mean value for the normal distribution.
        stability_picked (float): The standard deviation factor for the normal distribution.
        empty_quality (float): The quality threshold for generating empty values (between 0 and 1).
        periods (list): The list of periods for which numbers are to be generated.
        integer_value (bool, optional): If True, rounds the generated numbers to integers. Defaults to True.

    Returns:
        list: A list of generated numbers for each period.
    """

    raw_values = np.random.default_rng().normal(
        loc=size_picked, scale=size_picked * stability_picked, size=len(periods)
    )
    # choices = ['normal', 'poisson', 'exponential']

    # generator_choice = random.choices(choices, k=1)[0]
    # if generator_choice == 'normal':
    #     raw_values = np.random.default_rng().normal(loc=size_picked,
    #                                         scale=size_picked*stability_picked,
    #                                         size=len(periods))
    # elif generator_choice == 'poisson':
    #     raw_values = np.random.default_rng().poisson(lam=size_picked, size=len(periods))

    # elif generator_choice == 'exponential':
    #     raw_values = np.random.default_rng().exponential(scale=size_picked*stability_picked, size=len(periods))
    values = outlier_generator(raw_values)
    if integer_value:
        values = values.round(0)
    if not param_required:
        values = empty_generator(values, empty_quality)

    return list(values)


def choice_from_list_for_periods(
    choices_list: list,
    periods: list,
    n_choices: int = False,
    param_required: bool = False,
    empty_quality: int = 0.70,
) -> list:
    """
    Generates a list of choices for given periods from a list of choices.

    Parameters:
        choices_list (list): The list of possible choices.
        periods (list): The list of periods for which choices are to be generated.
        n_choices (int, optional): The number of choices to select for each period. Defaults to False.
        param_required (bool, optional): If True, ensures that a choice is always made. Defaults to False.
        empty_quality (int, optional): The quality threshold for generating empty values. Defaults to 0.70.

    Returns:
        list: A list of generated choices for each period.
    """
    generator_choice_list = list()

    if n_choices == 1:
        while len(generator_choice_list) < len(periods):
            generator_choice_list.append(random.sample(choices_list, k=n_choices)[0])
    elif n_choices:
        while len(generator_choice_list) < len(periods):
            generator_choice_list.append(
                " ".join(map(str, random.sample(choices_list, k=n_choices)))
            )
    else:
        while len(generator_choice_list) < len(periods):
            lenght = random.choice(range(1, len(choices_list) + 1))
            generator_choice_list.append(
                " ".join(map(str, random.sample(choices_list, k=lenght)))
            )

    if not param_required:
        generator_choice_list = list(
            empty_generator_object(generator_choice_list, empty_quality)
        )

    return generator_choice_list


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

    def __init__(self, iaso_connector_slug):
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

        # Parameter for records generations
        self.sizelist = [10, 50, 100]
        self.stable_condition = [0.05, 0.10, 0.20]
        self.quality_reportig = [0.025, 0.05, 0.1]

    def connection(self) -> Dict[str, Any]:
        """
        Establishes a connection to the IASO API and retrieves authentication headers.

        Parameters:
            None

        Returns:
            Dict[str, Any]: A dictionary containing the authentication headers.
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
            Dict[str, Any]: A dictionary containing the form metadata.
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

    def _get_form_dataframe_tuple_from_url(self, url: str) -> Tuple[Any]:
        """
        Retrieves the form data structure from the provided URL.

        Parameters:
            url (str): The URL to retrieve the form data from.

        Returns:
            Tuple[Any]: A tuple containing the survey and choices DataFrames.
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
        self, form_df_raw_tuple: Tuple[Any]
    ) -> pd.DataFrame:
        """
        Extracts the data structure from the form dataframes (survey and choices).

        Parameters:
            form_df_raw_tuple (Tuple[pd.DataFrame, pd.DataFrame]):
                A tuple containing the survey and choices dataframes.

        Returns:
            pd.DataFrame: A dataframe representing the form data structure.
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
            None
        """
        excel_url = self._get_form_metadata(form_id)["latest_form_version"]["xls_file"]
        self._get_data_structure_from_form_tuple(
            self._get_form_dataframe_tuple_from_url(excel_url)
        )

    def get_cols_from_the_form(self, type: str | None = None):
        """
        Retrieves columns from the form data structure, optionally filtered by type.

        Parameters:
            type (str, optional): The type of columns to filter by. Defaults to None.

        Returns:
            generator: A generator yielding tuples of (name, required_bool, type, choices).
        """
        if type:
            filtered_param_df = self.form_data_structure_df[
                self.form_data_structure_df.type == type
            ]
            sub_cols = lambda: zip(
                filtered_param_df.name.tolist(),
                filtered_param_df.required_bool.tolist(),
                filtered_param_df.type.tolist(),
                filtered_param_df.choices.tolist(),
            )
        else:
            sub_cols = lambda: zip(
                self.form_data_structure_df.name.tolist(),
                self.form_data_structure_df.required_bool.tolist(),
                self.form_data_structure_df.type.tolist(),
                self.form_data_structure_df.choices.tolist(),
            )
        return sub_cols

    def _get_raw_ou_tree_frame_from_orgtype_id(
        self, org_unit_type_id: int, validation_status: str = "all"
    ) -> pd.DataFrame:
        """
        Retrieves the raw organizational unit tree frame for a specific organization unit type ID.

        Parameters:
            org_unit_type_id (int): The ID of the organization unit type.
            validation_status (str, optional): The validation status filter. Defaults to "all".

        Returns:
            pd.DataFrame: A dataframe containing the organizational unit information.
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
        org_df = pd.read_excel(r.content, engine="openpyxl")
        return org_df

    def _generate_ou_treecolnames_dict_from_orgtype_id(
        self, org_unit_type_id: int
    ) -> List:
        """
        Generates a list of organizational unit tree column names and a corresponding dictionary
        mapping from organization unit type ID.

        Parameters:
            org_unit_type_id (int): The ID of the organization unit type.

        Returns:
            List: A list containing the column names and a dictionary mapping.
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
        return [extendend_cols, extendend_dict]

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
            pd.DataFrame: A dataframe containing the organizational unit information with renamed columns.
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
            pd.DataFrame: A dataframe containing the organizational unit information for the form.
        """
        org_df_total = [
            self._get_ou_tree_frame_from_orgtype_id(orgtype_id, validation_status)
            for orgtype_id in self._get_form_metadata(form_id)["org_unit_type_ids"]
        ]
        return pd.concat(org_df_total, ignore_index=True)

    def _json_iaso_instance_info_extractor(
        self, instance_json: Dict[str, Any]
    ) -> List[pd.DataFrame]:
        """
        Extracts instance information from the provided JSON data.

        Parameters:
            instance_json (Dict[str, Any]): The JSON data containing instance information.

        Returns:
            List[pd.DataFrame]: A list of dataframes containing the extracted instance information.
        """
        # ['uuid', 'form_id','org_unit_id','org_unit_created_at','org_unit_updated_at','form_name', 'created_at', 'updated_at',
        #'period','status', 'latitude', 'longitude', 'altitude', 'form_name']

        self.instance_info_cols = [
            "uuid",
            "form_id",
            "org_unit_id",
            "org_unit_updated_at",
            "created_at",
            "period",
            "status",
        ]
        ins_uuid = instance_json["uuid"]
        ins_form_id = instance_json["form_id"]
        instance_normalized_df = pd.json_normalize(instance_json, sep="_")
        instance_info_cols_sub = [
            _ for _ in instance_normalized_df.columns if _ in self.instance_info_cols
        ]
        full_cols_sub = [
            _
            for _ in instance_normalized_df.columns
            if _ in self.instance_info_cols
            or _.replace("file_content_", "")
            in self.form_content_form_structure_base_columns_list
        ]
        instance_full_df = instance_normalized_df[full_cols_sub]

        # instance_submission_info_df = instance_normalized_df[instance_info_cols_sub]
        # instance_file_content_df = pd.json_normalize(instance_json['file_content'])
        # instance_file_content_df = instance_file_content_df.assign(uuid=ins_uuid)
        # instance_full_df=instance_file_content_df.merge(instance_submission_info_df)

        return instance_full_df

    def _json_iaso_crawler(
        self, json_query_answer: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Crawls through the JSON query answer and extracts instance information.

        Parameters:
            json_query_answer (Dict[str, Any]): The JSON data containing instances information.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing dataframes with
            the extracted instance information.
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

        except:
            instance_full_df = pd.DataFrame()

        return instance_full_df

    def _json_request_extract(
        self,
        form_id: int,
        limit_batch: int = 50,
        dateFrom: str = None,
        dateTo: str = None,
    ) -> Dict[str, Any]:
        """
        Extracts JSON data from IASO API for a specific form ID with pagination support.

        Parameters:
            form_id (int): The ID of the form to extract data for.
            limit_batch (int, optional): The number of records to fetch per batch. Defaults to 50.
            dateFrom (str, optional): The start date for filtering records. Defaults to None.
            dateTo (str, optional): The end date for filtering records. Defaults to None.

        Returns:
            Dict[str, Any]: A dictionary containing the extracted JSON data.
        """
        instances_endpoint = f"{self.iaso_connector.url}/api/instances/"

        # # Time Filtering of the query
        # dateFrom = pd.to_datetime(dateFrom, format='%Y-%m-%d').date()
        # dateTo = pd.to_datetime(dateTo, format='%Y-%m-%d').date()
        # modificationDateFrom = pd.to_datetime(modificationDateFrom, format='%Y-%m-%d').date()
        # modificationDateTo = pd.to_datetime(modificationDateTo, format='%Y-%m-%d').date()

        # format_date_from = f"&dateFrom={self.dateFrom}" if not pd.isna(self.dateFrom) else ""
        # format_date_to = f"&dateTo={self.dateTo}" if not pd.isna(self.dateTo) else ""
        # format_modif_date_from = f"&modificationDateFrom={self.modificationDateFrom}" if not pd.isna(self.modificationDateFrom) else ""
        # format_modif_date_to = f"&modificationDateTo={self.modificationDateTo}" if not pd.isna(self.modificationDateTo) else ""

        # format_period = "".join([format_date_from, format_date_to, format_modif_date_from, format_modif_date_to])

        form_endpoint = instances_endpoint + f"?form_ids={form_id}"  # + format_period
        base_full_endpoint = form_endpoint + f"&limit={limit_batch}"  # + format_period

        if dateFrom:
            base_full_endpoint = (
                base_full_endpoint + f"&dateFrom={dateFrom}"
            )  # + format_period
        if dateTo:
            base_full_endpoint = (
                base_full_endpoint + f"&dateTo={dateTo}"
            )  # + format_period

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
            r = request_with_explanation(
                base_full_endpoint + f"&page={page_id}",
                self.headers,
                f"Form {form_id} Submission Request Page {page_id}",
            )
            form_full_df.append(self._json_iaso_crawler(r.json()))

        form_full_df = pd.concat(form_full_df, ignore_index=True)
        form_full_df = form_full_df.drop_duplicates(subset="uuid")
        return form_full_df
        # return json_extract

    def _submmission_df_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Formats the submission dataframe by processing the 'period' column and converting integer columns.

        Parameters:
            df (pd.DataFrame): The input submission dataframe.

        Returns:
            pd.DataFrame: The formatted submission dataframe.
        """
        df = period_processing(df)
        for col_info_zip in self.get_cols_from_the_form("integer")():
            try:
                df[col_info_zip[0]] = pd.to_numeric(
                    df[col_info_zip[0]], errors="coerce"
                )
                df[col_info_zip[0]] = df[col_info_zip[0]].astype(float)
            except:
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
            pd.DataFrame: The formatted submission dataframe.
        """
        self.get_data_structure_from_the_form(form_id)
        instance_full_df = self._json_request_extract(form_id, 200, dateFrom, dateTo)
        # json_extract=self._json_request_extract(form_id)
        # instance_full_df = self._json_iaso_crawler(json_extract)
        if instance_full_df.empty:
            return instance_full_df
        else:
            instance_full_df = self._submmission_df_formatting(instance_full_df)
            # full_df.rename(columns= {'org_unit_id' : 'ID', 'period':'PERIOD'}, inplace = True)
            return instance_full_df

    def all_param_data_generation_per_orgunit(
        self,
        ogunit_id: str,
        ind: int,
        sizes_picked: List[float],
        stabilitys_picked: List[float],
        base_qualitys_picked: List[float],
        periods: List[str],
    ) -> pd.DataFrame:
        """
        Generates parameter data for a specific organizational unit and period.

        Parameters:
            ogunit_id (str): The ID of the organizational unit.
            ind (int): The index for selecting size, stability, and quality parameters.
            sizes_picked (List[float]): A list of size parameters.
            stabilitys_picked (List[float]): A list of stability parameters.
            base_qualitys_picked (List[float]): A list of quality parameters.
            periods (List[str]): A list of periods for which data is to be generated.

        Returns:
            pd.DataFrame: A dataframe containing the generated parameter data.
        """
        param_df = []

        for param_name, param_required, type, choices in self.get_cols_from_the_form(
            "integer"
        )():
            raw_values = numbers_per_param(
                param_name,
                True,
                param_required,
                sizes_picked[ind],
                stabilitys_picked[ind],
                base_qualitys_picked[ind],
                periods,
            )
            param_df.append(
                pd.DataFrame({"period": periods, "VALUE": raw_values}).assign(
                    PARAMETER=param_name
                )
            )

        for param_name, param_required, type, choices in self.get_cols_from_the_form(
            "select_one"
        )():
            raw_values = choice_from_list_for_periods(
                choices, periods, 1, param_required, base_qualitys_picked[ind]
            )
            param_df.append(
                pd.DataFrame({"period": periods, "VALUE": raw_values}).assign(
                    PARAMETER=param_name
                )
            )

        for param_name, param_required, type, choices in self.get_cols_from_the_form(
            "select_multiple"
        )():
            raw_values = choice_from_list_for_periods(
                choices, periods, False, param_required, base_qualitys_picked[ind]
            )
            param_df.append(
                pd.DataFrame({"period": periods, "VALUE": raw_values}).assign(
                    PARAMETER=param_name
                )
            )

        param_df = pd.concat(param_df)
        param_df = param_df.assign(org_unit_id=ogunit_id)
        return param_df

    def generate_fake_full_data(self, form_id: int, periods: List[str]) -> pd.DataFrame:
        """
        Generates fake full data for a specific form ID and periods.

        Parameters:
            form_id (int): The ID of the form to generate data for.
            periods (List[str]): A list of periods for which data is to be generated.

        Returns:
            pd.DataFrame: A dataframe containing the generated fake data.
        """
        self.get_data_structure_from_the_form(form_id)
        df_org_unit = self.get_ou_tree_dataframe_from_the_form(form_id)
        base_org_tree_id_list = df_org_unit.org_unit_id.unique()

        sizes_picked = random.choices(self.sizelist, k=len(base_org_tree_id_list))
        stabilitys_picked = random.choices(
            self.stable_condition, k=len(base_org_tree_id_list)
        )
        base_qualitys_picked = random.choices(
            self.quality_reportig, k=len(base_org_tree_id_list)
        )

        full_df = [
            self.all_param_data_generation_per_orgunit(
                ogunit_id,
                ind,
                sizes_picked,
                stabilitys_picked,
                base_qualitys_picked,
                periods,
            )
            for ind, ogunit_id in enumerate(base_org_tree_id_list)
        ]
        full_df_pivot = pd.concat(full_df)
        full_df_pivot = pd.pivot(
            data=full_df_pivot,
            index=["period", "org_unit_id"],
            columns="PARAMETER",
            values="VALUE",
        ).reset_index()
        full_df_pivot["period"] = pd.to_datetime(
            full_df_pivot.period, format="%Y-%m-%d"
        )
        return full_df_pivot.round(0)
