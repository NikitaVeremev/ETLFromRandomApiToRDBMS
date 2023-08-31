import logging

from pandas import DataFrame
from requests import Response


class ETLApi:
    def extract(self) -> Response:
        pass

    def transform(self, response: Response) -> DataFrame:
        pass

    def load(self, df: DataFrame) -> None:
        pass

    def build(self) -> None:
        logging.info(f"Start Extract stage.")
        response = self.extract()
        logging.info(f"Start Transform stage.")
        df = self.transform(response)
        logging.info(f"Start Load stage.")
        self.load(df)
        logging.info(f"Finish Load stage.")
