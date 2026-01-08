import numpy as np
import pandas as pd
import requests, json

import re
import random
import sqlalchemy as sa

import datetime

from openhexa.sdk import workspace

from typing import Tuple, Dict, Any, List

from fuzzywuzzy import fuzz, process
import unicodedata


def request_explanatory_decorator(function):
    def wrapper_request(url, headers, process_message):
        try:
            r = function(url, headers)
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
    return requests.get(url, headers=headers)


def period_form_convert_date(row):
    """
    Convert period field
    """
    if len(str(row[0])) == 8 and str(row[0]).isdigit():
        return pd.to_datetime(row[0], format="%Y%m%d").strftime("%Y-%m-%d")
    elif len(str(row[0])) >= 8:
        return pd.to_datetime(row[0], format="%Y-%m-%d").strftime("%Y-%m-%d")
    elif len(str(row[1])) >= 8:
        return pd.to_datetime(row[1], unit="s").strftime("%Y-%m-%d")
    else:
        return row[0]


def period_processing(df: pd.DataFrame) -> pd.DataFrame:
    # Traitement de la colonne period au cas par cas
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


# -------------


def empty_generator(values, empty_quality):
    """Generates empty values based on a quality threshold."""
    return np.where(
        np.random.default_rng().uniform(size=len(values)) > empty_quality,
        values,
        np.nan,
    )


def empty_generator_object(values, empty_quality):
    """Generates empty values based on a quality threshold."""
    return np.where(
        np.random.default_rng().uniform(size=len(values)) > empty_quality, values, None
    )


def outlier_generator(values):
    """Generates outlier values."""
    outlier_thr = 0.005
    return np.where(
        np.random.default_rng().uniform(size=len(values)) < outlier_thr,
        values,
        np.multiply(np.random.default_rng().uniform(size=len(values)) * 100, values),
    )


def choice_from_list_for_periods(
    choices_list: list,
    periods: list,
    n_choices: int = False,
    param_required: bool = False,
    empty_quality: int = 0.70,
) -> list:
    # so far we assume without replacement and that's why we use random.sample
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


def numbers_per_param(
    param,
    constraint_positive,
    param_required,
    size_picked,
    stability_picked,
    empty_quality,
    periods,
    integer_value=True,
):
    """Generates numbers per parameter."""

    raw_values = np.random.default_rng().normal(
        loc=size_picked, scale=size_picked * stability_picked, size=len(periods)
    )
    """
    choices = ['normal', 'poisson', 'exponential']
    
    generator_choice = random.choices(choices, k=1)[0]
    if generator_choice == 'normal':
        raw_values = np.random.default_rng().normal(loc=size_picked,
                                            scale=size_picked*stability_picked,
                                            size=len(periods))
    elif generator_choice == 'poisson':
        raw_values = np.random.default_rng().poisson(lam=size_picked, size=len(periods))
    
    elif generator_choice == 'exponential':
        raw_values = np.random.default_rng().exponential(scale=size_picked*stability_picked, size=len(periods))
    """

    values = outlier_generator(raw_values)
    if integer_value:
        values = values.round(0)
    if not param_required:
        values = empty_generator(values, empty_quality)

    return list(values)


class Conector_from_Dict:
    def __init__(self, iaso_connector_slug: Dict[str, Any]):
        self.username = iaso_connector_slug["username"]
        self.password = iaso_connector_slug["password"]
        self.url = iaso_connector_slug["url"]


class IASOConnectionHandler:
    def __init__(self, iaso_connector_slug):
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

    def _get_credentials_on_slug_name(self, slug_name: str):
        # TODO
        pass

    def connection(self) -> Dict[str, Any]:
        creds = {
            "username": self.iaso_connector.username,
            "password": self.iaso_connector.password,
        }
        r = requests.post(f"{self.iaso_connector.url}/api/token/", json=creds)
        token = r.json().get("access")
        headers = {"Authorization": "Bearer %s" % token}
        return headers

    def _get_form_metadata(self, form_id: int) -> Dict[str, Any]:
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
        try:
            r = requests.get(url)
            assert r.status_code == 200
            content = r.content
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
        """ """
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
        excel_url = self._get_form_metadata(form_id)["latest_form_version"]["xls_file"]
        self._get_data_structure_from_form_tuple(
            self._get_form_dataframe_tuple_from_url(excel_url)
        )

    def get_cols_from_the_form(self, type=None):
        """
        The idea is to retrieve all the columns as an iterable zip
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
        org_df_total = [
            self._get_ou_tree_frame_from_orgtype_id(orgtype_id, validation_status)
            for orgtype_id in self._get_form_metadata(form_id)["org_unit_type_ids"]
        ]
        return pd.concat(org_df_total, ignore_index=True)

    # ---------------------------------

    def _json_iaso_instance_info_extractor(
        self, instance_json: Dict[str, Any]
    ) -> List[pd.DataFrame]:
        """
        Extracts JSON instance information.
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
        Browse IASO JSON responses.
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

    def _json_request_extract(self, form_id: int) -> Dict[str, Any]:
        """
        Extracts submission information from IASO.
        """
        instances_endpoint = f"{self.iaso_connector.url}/api/instances/"
        """
        #Time Filtering of the query
        
        dateFrom = pd.to_datetime(dateFrom, format='%Y-%m-%d').date()
        dateTo = pd.to_datetime(dateTo, format='%Y-%m-%d').date()
        modificationDateFrom = pd.to_datetime(modificationDateFrom, format='%Y-%m-%d').date()
        modificationDateTo = pd.to_datetime(modificationDateTo, format='%Y-%m-%d').date()
        
        format_date_from = f"&dateFrom={self.dateFrom}" if not pd.isna(self.dateFrom) else ""
        format_date_to = f"&dateTo={self.dateTo}" if not pd.isna(self.dateTo) else ""
        format_modif_date_from = f"&modificationDateFrom={self.modificationDateFrom}" if not pd.isna(self.modificationDateFrom) else ""
        format_modif_date_to = f"&modificationDateTo={self.modificationDateTo}" if not pd.isna(self.modificationDateTo) else ""
        
        format_period = "".join([format_date_from, format_date_to, format_modif_date_from, format_modif_date_to])
        """
        form_endpoint = instances_endpoint + f"?form_ids={form_id}"  # + format_period

        r = request_with_explanation(
            form_endpoint, self.headers, "Form Submission Request"
        )
        json_extract = r.json()
        return json_extract

    def _submmission_df_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def extract_submissions_info(self, form_id: int) -> pd.DataFrame:
        """
        Extracts submission information from IASO.
        """
        self.get_data_structure_from_the_form(form_id)
        json_extract = self._json_request_extract(form_id)
        instance_full_df = self._json_iaso_crawler(json_extract)
        if instance_full_df.empty:
            return instance_full_df
        else:
            instance_full_df = self._submmission_df_formatting(instance_full_df)
            # full_df.rename(columns= {'org_unit_id' : 'ID', 'period':'PERIOD'}, inplace = True)
            return instance_full_df

    # ---------------

    def all_param_data_generation_per_orgunit(
        self,
        ogunit_id: str,
        ind: int,
        sizes_picked: List[float],
        stabilitys_picked: List[float],
        base_qualitys_picked: List[float],
        periods: List[str],
    ) -> pd.DataFrame:
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
        Generates full data.
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


def age_categorizer(string):
    ages = {
        "0_11_mois": "0-11 mois",
        "12_59_mois": "12-59 mois",
        "12-59 mois": "12-59 mois",
        "6_11_mois": "6-11 mois",
        "6-11 mois": "6-11 mois",
        "9-11 mois": "9-11 mois",
        "9_11_mois": "9-11 mois",
        "12_24_mois": "12-24 mois",
        "12_23": "12-23 mois",
        "24_59": "24-59 mois",
        "6/9-11": "0-11 mois",
        "12-59 mois": "12-59 mois",
        "5_14_ans": "5-14 ans",
        "15_60_ans": "15-60 ans",
    }

    for key in ages:
        if key in string:
            return ages[key]

    return "TOUS"


def pop_segment_categorizer(string):
    ages = {
        "Refugiers": "Refugiers",
        "Autochtone": "Autochtone",
    }

    for key in ages:
        if key in string:
            return ages[key]

    return "Total"


def site_categorizer(string):
    sites = {
        "site_ordinaire": "ordinaire",
        "site_speciaux_nomad": "nomade",
        "site_speciaux_gares": "gare",
        "site_speciaux_marche": "marche",
        "site_speciaux_point_eau": "point_eau",
        "site_speciaux_postefron": "frontalier",
        "site_trans_front_cote_front": "transfrontalier_etranger",
        "site_trans_front_cote_niger": "transfrontarlier_Niger",
        "site_speciaux_deplace_int": "deplaces_internes",
        "site_speciaux_refugie": "refugies",
        "site_speciaux": "special",
        "site_speciaux_autre": "autre",
    }

    for key in sites:
        if key in string:
            return sites[key]

    return "TOUS"


def produit_categorizer(string):
    products = {
        "vitamine_a": "vitamine A",
        "vit_a": "vitamine A",
        "vpo": "vaccin polio",
        "polio": "flacons polio",
        "albendazole": "albendazole",
        "depara": "albendazole",
        "fievre_jaune": "fièvre jaune",
    }

    for key in products:
        if key in string:
            return products[key]

    return "TOUS"


def vaccination_status_categorizer(string):
    status = {"zero_dose": "zero dose", "deja_recu": "deja recu"}

    for key in status:
        if key in string:
            return status[key]

    return "zero dose"


def new_cols(df, pattern, value_col, function_list=None):
    """use functions and function names to create new columns"""
    """takes in a dataframe, a common string in function names and the name of the variable to provide values"""
    """adds new columns using the function names and modifies the dataframe in place"""
    if not function_list:
        function_list = [
            f
            for f in globals().values()
            if type(f) == type(lambda *args: None) and pattern in f.__name__
        ]

    for fun in function_list:
        new_colname = fun.__name__.rsplit("_", maxsplit=1)[0]
        df.loc[:, new_colname] = df.loc[:, value_col].map(fun)
    return df


def round_assignment(df):
    """
    Assigns rounds to the DataFrame based on the 'period' column,
    using different logic based on the 'choix_campagne' column.
    """

    # Define the boolean mask for the "méningite/tcv" campaign
    is_meningite_tcv = df["produit"].isin(["men5_tcv", "méningite", "tcv"])

    # Define the boolean mask for the 2025 yellow fever campaign
    is_yellow_fever = df["produit"] == "fievre jaune"

    # Define the logic for the méningite/tcv campaign
    meningite_tcv_logic = np.where(
        (df["period"] <= pd.to_datetime("2025-12-02"))
        & (df["period"] >= pd.to_datetime("2025-11-24")),
        "round 1",
        np.where(
            (df["period"] <= pd.to_datetime("2025-12-22"))
            & (df["period"] >= pd.to_datetime("2025-12-15")),
            "round 2",
            "invalid date",
        ),
    )

    # Define the logic for the yellow fever campaign
    yellow_fever_logic = np.where(
        df["period"].dt.year == 2025,
        np.where(
            (pd.to_datetime("2025-10-27") <= df["period"])
            & (df["period"] <= pd.to_datetime("2025-11-04")),
            "round 1",
            np.where(
                (pd.to_datetime("2025-12-10") <= df["period"])
                & (df["period"] <= pd.to_datetime("2025-12-17")),
                "round 2",
                "invalid date",
            ),
        ),
        "invalid date",
    )

    # Define the logic for the "other" campaigns
    other_campaign_logic = np.where(
        df["period"].dt.year < 2025,
        # Logic for 2024 periods (nested)
        np.where(
            (pd.to_datetime("2024-07-10") <= df["period"])
            & (df["period"] <= pd.to_datetime("2024-07-24")),
            "round 1",
            np.where(
                (pd.to_datetime("2024-09-28") <= df["period"])
                & (df["period"] <= pd.to_datetime("2024-10-10")),
                "round 2",
                np.where(
                    (pd.to_datetime("2024-10-25") <= df["period"])
                    & (df["period"] <= pd.to_datetime("2024-11-5")),
                    "round 3",
                    np.where(
                        (pd.to_datetime("2024-11-26") <= df["period"]),
                        "round 4",
                        "invalid date",
                    ),
                ),
            ),
        ),
        # Logic for 2025 and later periods (nested)
        np.where(
            (pd.to_datetime("2025-04-18") <= df["period"])
            & (df["period"] <= pd.to_datetime("2025-05-08")),
            "round 1",
            np.where(
                (pd.to_datetime("2025-06-14") <= df["period"])
                & (df["period"] <= pd.to_datetime("2025-06-21")),
                "round 2",
                np.where(
                    (pd.to_datetime("2025-10-24") <= df["period"])
                    & (df["period"] <= pd.to_datetime("2025-12-08")),
                    "round 3",  # changed round 3 to cover up to 8/12 due to rescheduling of polio campaign from 27/10-31/10 to 5/12-8/12
                    "invalid date",
                ),
            ),
        ),
    )

    df["round"] = np.where(
        is_meningite_tcv,
        meningite_tcv_logic,
        np.where(is_yellow_fever, yellow_fever_logic, other_campaign_logic),
    )

    return df


def year_assignment(df):
    df["year"] = df["period"].dt.year


def dates_sequence_generator(min_date, max_date):
    df = pd.DataFrame(
        pd.date_range(start=min_date, end=max_date, freq="24H", inclusive="both"),
        columns=["period"],
    )
    df["order_day"] = df.period.rank().astype(int)
    return df


def pyramid_selector(df):
    max_id = df["updated_date"].apply(pd.to_datetime).idxmax()
    max_date = df["updated_date"].apply(pd.to_datetime).dt.date.get(max_id)
    if max_date != datetime.date(2023, 7, 14):
        return df.loc[max_id, :]
    else:
        df = df.drop(max_id)
        return pyramid_selector(df)


def strip_accents(s):
    if not isinstance(s, str):
        return s
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def normalize_string(text):
    """
    Normalizes a string:
    - Lowercase & Accent removal
    - Removes suffixes even if glued to text (e.g., 'CSITagadofet' -> 'tagadofet')
    - Removes special characters
    - Collapses internal spaces
    """
    if not isinstance(text, str):
        return ""

    noisy_words = (
        r"\b(csi|cs|ds|chr|hd|creni|crenam|cloturee|cloture|departement|region|ville)"
    )

    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join([c for c in text if unicodedata.category(c) != "Mn"])
    text = re.sub(noisy_words, "", text, flags=re.IGNORECASE)
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    return " ".join(text.split()).strip()


def org_unit_matching(
    target_df: pd.DataFrame, spatial_unit_df: pd.DataFrame, threshold: int = 90
) -> pd.DataFrame:
    """
    Matches organization unit levels (LVL 3 (ds), LVL 6 (csi)) between two DataFrames using fuzzy string matching.

    Args:
        target_df (pd.DataFrame): DataFrame containing the target data for each combination of LVL 2, LVL 3, and LVL 6 names.
        spatial_unit_df (pd.DataFrame): DataFrame containing the org unit IDs for each combination of LVL 2, LVL 3, LVL 4, LVL 5 and LVL 6 names.

    Returns:
        pd.DataFrame: target_df with matched organization units and their corresponding org unit IDs.
    """
    # 1. Preparation
    rename_map = {
        "LVL_3_NAME": "LVL_3_NAME_original",
        "LVL_6_NAME": "LVL_6_NAME_original",
    }

    target_df = target_df.rename(columns=rename_map)
    target = target_df[["LVL_3_NAME_original", "LVL_6_NAME_original"]].copy()
    target = target.drop_duplicates()
    spatial = spatial_unit_df.copy()

    # 2. Create Cleansed Concatenations
    target["cleansed_target"] = target.apply(
        lambda r: f"{normalize_string(str(r['LVL_3_NAME_original']))} {normalize_string(str(r['LVL_6_NAME_original']))}",
        axis=1,
    )
    spatial["cleansed_spatial"] = spatial.apply(
        lambda r: f"{normalize_string(str(r['LVL_3_NAME']))} {normalize_string(str(r['LVL_6_NAME']))}",
        axis=1,
    )

    # 3. Collect ALL Potential Match Candidates
    all_potential_candidates = []
    spatial_list = spatial["cleansed_spatial"].tolist()
    spatial_indices = spatial.index.tolist()

    for idx_t, query in target["cleansed_target"].items():
        if query and query.strip():
            matches = process.extract(
                query,
                spatial_list,
                scorer=lambda s1, s2: max(
                    fuzz.ratio(s1, s2), fuzz.partial_ratio(s1, s2)
                ),
                limit=5,
            )

            for match in matches:
                matched_str = match[0]
                score = match[1]

                if score >= threshold:
                    try:
                        list_idx = match[2]
                    except IndexError:
                        list_idx = spatial_list.index(matched_str)

                    idx_s = spatial_indices[list_idx]
                    all_potential_candidates.append(
                        {"target_idx": idx_t, "spatial_idx": idx_s, "score": score}
                    )

    # 4. Global Greedy Matching Logic
    all_potential_candidates.sort(key=lambda x: x["score"], reverse=True)

    assigned_target_indices = set()
    assigned_spatial_indices = set()
    final_assignment = {}

    for match in all_potential_candidates:
        t_idx = match["target_idx"]
        s_idx = match["spatial_idx"]

        if (
            t_idx not in assigned_target_indices
            and s_idx not in assigned_spatial_indices
        ):
            final_assignment[t_idx] = (s_idx, match["score"])
            assigned_target_indices.add(t_idx)
            assigned_spatial_indices.add(s_idx)

    # 5. Finalize Results
    target["match_index"] = target.index.map(
        lambda x: final_assignment[x][0] if x in final_assignment else None
    )
    target["match_score"] = target.index.map(
        lambda x: final_assignment[x][1] if x in final_assignment else 0
    )

    cols_to_pull = [
        "org_unit_id",
        "LVL_3_NAME",
        "LVL_6_NAME",
        "cleansed_spatial",
    ]

    final_df = target.merge(
        spatial[cols_to_pull],
        left_on="match_index",
        right_index=True,
        how="left",
        suffixes=("", "_matched"),
    )

    final_df = final_df.rename(columns={"cleansed_spatial": "cleansed_spatial_match"})
    final_df = target_df.merge(
        final_df, on=["LVL_3_NAME_original", "LVL_6_NAME_original"], how="left"
    )

    # Make sure that all entries merged back to target_df
    count_initial = target_df.shape[0]
    count_final = final_df.shape[0]
    if count_initial != count_final:
        raise ValueError(
            f"Row count mismatch after merging back to target_df: {count_initial} vs {count_final}"
        )

    return final_df.drop(columns=["match_index"]), spatial
