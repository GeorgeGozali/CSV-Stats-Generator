import glob
import os
from datetime import datetime

import pandas as pd


class MetadataParser:
    """
    This Class scans directory and writes csv files
    and write details to GCloud

    Attributes
        path - full path of the directory containing csv files

    Methods
        __init__(path=os.environ.get("path"))
            constructor of the class

        bytes_to_mb(num: int) -> float
            transforms bytes to mb

        list_csvs() -> list[dict]
            returns list containing dicts with file details

        write_to_json(_list: list[dict]) -> None
            writes list to json file
    """

    csvs: list[dict] = []

    def __init__(self, path):
        self.path = path
        assert os.path.exists(path), f"path '{path}' does not exists"

    @staticmethod
    def bytes_to_mb(num: int) -> float:
        return float("{0:.2f}".format(float(num) / (1024 * 1024)))

    def csv_file_size(self, item: str, _dict: dict) -> None:
        byte_size = os.path.getsize(item)
        _dict["csv_file_size_in_mb"] = self.bytes_to_mb(byte_size)

    @staticmethod
    def df_of_csv_rows_n(_dict: dict, df) -> None:
        _dict["df_of_csv_rows_n"] = df.shape[0]

    @staticmethod
    def df_of_csv_columns_n(_dict: dict, df) -> None:
        _dict["df_of_csv_columns_n"] = df.shape[1]

    @staticmethod
    def df_one_column_size_in_mb(_dict, df_size, df) -> None:
        value = float(f"{df_size / df.shape[1]:.2f}")
        _dict["df_one_column_size_in_mb"] = value

    @staticmethod
    def df_size_in_mb(_dict, df_size) -> None:
        value = float("{0:.2f}".format(df_size))
        _dict["df_size_in_mb"] = value

    @staticmethod
    def set_name(_dict, filename) -> None:
        _dict["name"] = filename.replace(".csv", "_csv")

    def list_csvs(self) -> list[dict]:
        """This method read csv files and write details into the list

        Returns:
            list[dict]: list containing dictionaries
            with each csv files details
        """
        dict_list: list[dict] = []

        list_dir = glob.glob(f"{self.path}/*.csv")
        for idx, item in enumerate(list_dir, 1):
            filename = item.split("/")[-1]

            df = pd.read_csv(item)
            df_size = df.memory_usage(deep=True).values.sum() / (1024 * 1024)

            item_dict: dict = {}

            self.set_name(item_dict, filename)
            self.csv_file_size(item, item_dict)
            self.df_of_csv_rows_n(item_dict, df)
            self.df_of_csv_columns_n(item_dict, df)
            self.df_one_column_size_in_mb(item_dict, df_size, df)
            self.df_size_in_mb(item_dict, df_size)
            item_dict["created"] = datetime.now().isoformat("T", "seconds")
            item_dict["updated"] = datetime.now().isoformat("T", "seconds")
            dict_list.append(item_dict)

        return dict_list

    @staticmethod
    def write_to_json(_list: list[dict], path: str) -> None:
        # write data to json file with timestamp
        date = datetime.now().isoformat("T", "seconds")

        df = pd.DataFrame(_list)
        df.to_json(
            f"{path}/data/summary_{date}.json",
            orient='records',
            lines=True
            )
