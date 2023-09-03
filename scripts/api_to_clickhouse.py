import logging

import clickhouse_driver
import pandas as pd
import requests
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pandas import DataFrame
from requests import Response

from scripts.etl_api import ETLApi


class ApiToClickhouseOperator(ETLApi, BaseOperator):
    @apply_defaults
    def __init__(
        self,
        api_url: str,
        db_url: str,
        target_table_name: str,
        ddl: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.api_url: str = api_url
        self.db_url = db_url
        self.target_table_name = target_table_name
        self.ddl = ddl

    def execute(self, context):
        self.build()

    def extract(self) -> Response:
        logging.info(f"Get request from url: {self.api_url}")
        response = requests.get(self.api_url)
        logging.info(f"Response status code: {response.status_code}")
        if not response.ok:
            raise Exception("Response error, status code: ", response.status_code)
        return response

    def transform(self, response: Response) -> DataFrame:
        return pd.json_normalize(response.json())

    def load(self, df: DataFrame) -> int:
        client = clickhouse_driver.Client.from_url(self.db_url)
        client.execute(self.ddl)
        logging.info(f"Start write to {self.target_table_name}.")
        num_rows = client.insert_dataframe(
            f"INSERT INTO {self.target_table_name} VALUES",
            df,
            settings={"use_numpy": True},
        )
        logging.info(f"Finish write to {self.target_table_name}.")
        return num_rows
