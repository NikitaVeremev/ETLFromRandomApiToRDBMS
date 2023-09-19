import logging

import pandas as pd
from flatten_json import flatten
import requests
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pandas import DataFrame
from requests import Response
from sqlalchemy import create_engine

from scripts.etl_api import ETLApi


class NHLApiToPostgresOperator(ETLApi, BaseOperator):
    @apply_defaults
    def __init__(
        self, api_url: str, db_url: str, target_table_name: str, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.api_url: str = api_url
        self.db_url = db_url
        self.target_table_name = target_table_name

    def execute(self, context):
        self.build()

    def extract(self) -> Response:
        logging.info(f"Get request from url: {self.api_url}")
        response = requests.get(self.api_url)
        logging.info(f"Response status code: {response.status_code}")
        response.raise_for_status()
        return response

    def transform(self, response: Response) -> DataFrame:
        res = response.json()
        res["stats"] = [flatten(d) for d in res["stats"]]
        df = pd.DataFrame([res])
        df = df.explode("stats")
        df2 = pd.json_normalize(df["stats"], meta_prefix="stats_")
        df = df.drop("stats", axis=1)
        return df.join(df2)

    def load(self, df: DataFrame) -> int:
        engine = create_engine(self.db_url)
        logging.info(f"Start write to {self.target_table_name}.")
        num_rows = df.to_sql(self.target_table_name, engine, if_exists="append")
        logging.info(f"Finish write to {self.target_table_name}.")
        engine.dispose()
        return num_rows
