import logging
from abc import abstractmethod, ABC

from pandas import DataFrame
from requests import Response


class ETLApi(ABC):

    @abstractmethod
    def extract(self) -> Response:
        pass

    @abstractmethod
    def transform(self, response: Response) -> DataFrame:
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> int:
        pass

    def build(self) -> None:
        logging.info("Start Extract stage.")
        response = self.extract()
        logging.info("Start Transform stage.")
        df = self.transform(response)
        logging.info("Start Load stage.")
        num_rows = self.load(df)
        logging.info(f"Finish Load stage. Number of rows affected: {num_rows}")
