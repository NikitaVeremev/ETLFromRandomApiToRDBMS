import logging
from abc import abstractmethod, ABC

from pandas import DataFrame
from requests import Response


class ETLApi(ABC):
    """
    Абстрактный класс для реализации ETL процесса из Api источника.
    """

    @abstractmethod
    def extract(self) -> Response:
        """
        Здесь необходимо реализовать шаг Extract.

        Извлечение данных в сыром виде из Api источника.
        :return: Response с источника.
        """

    @abstractmethod
    def transform(self, response: Response) -> DataFrame:
        """
        Здесь необходимо реализовать шаг Transform.

        Трансформация данных из Api источника.
        :param response: Ответ с источника.
        :return: Изменённый DataFrame.
        """

    @abstractmethod
    def load(self, df: DataFrame) -> int:
        """
        Здесь необходимо реализовать шаг Load.

        Загрузка DataFrame в приёмник.
        :param df: Изменённый DataFrame.
        :return: Число изменённых строк на приёмнике.
        """

    def build(self) -> int:
        """
        Реализация ETL логики.
        :return: Число изменённых строк на приёмнике.
        """
        logging.info("Start Extract stage.")
        response = self.extract()
        logging.info("Start Transform stage.")
        df = self.transform(response)
        logging.info("Start Load stage.")
        num_rows = self.load(df)
        logging.info(f"Finish Load stage. Number of rows affected: {num_rows}")
        return num_rows
