import pandas as pd
from airflow.models import BaseOperator
from flatten_json import flatten
from pandas import DataFrame
from requests import Response

from scripts.api_to_postgres import ApiToPostgresOperator


class NHLApiToPostgresOperator(ApiToPostgresOperator, BaseOperator):

    def transform(self, response: Response) -> DataFrame:
        res = response.json()
        res["stats"] = [flatten(d) for d in res["stats"]]
        df = pd.DataFrame([res])
        df = df.explode("stats")
        df2 = pd.json_normalize(df["stats"])
        df = df.drop("stats", axis=1)
        return df.join(df2)
