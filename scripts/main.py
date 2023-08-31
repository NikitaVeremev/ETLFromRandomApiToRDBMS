import requests
import pandas as pd
import logging
from pandas import DataFrame
from requests import Response
from sqlalchemy import create_engine

from scripts.etl_api import ETLApi


class ETLApiImpl(ETLApi):
    def __init__(self, api_url: str, db_url: str, target_table_name: str):
        self.api_url: str = api_url
        self.db_url = db_url
        self.target_table_name = target_table_name

    def extract(self) -> Response:
        logging.info(f"Get request from url: {self.api_url}")
        response = requests.get(self.api_url)
        logging.info(f"Response status code: {response.status_code}")
        if not response.ok:
            raise Exception("Response error, status code: ", response.status_code)
        return response

    def transform(self, response: Response) -> DataFrame:
        return pd.json_normalize(response.json())

    def load(self, df: DataFrame):
        engine = create_engine(self.db_url)
        logging.info(f"Start write to {self.target_table_name}.")
        df.to_sql(self.target_table_name, engine, if_exists='append')
        logging.info(f"Finish write to {self.target_table_name}.")
