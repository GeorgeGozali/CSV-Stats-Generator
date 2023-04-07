from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import os
from pathlib import Path


class WriteDb:
    """
    This Class works with csv files

    Methods:
        __init__: initializer
        get_client: returns bigcuery.Client
        table_exists: checks if table exists, returns bool
        create_table: creates table in bigquery dataset
        check_filename: checks if data exists in table
        write_to_db: writes json to bigquery table
        update: updates data in bigquery table
    """
    # TODO: here init should get table_id and check if project and dataset is correctly imported
    def __init__(self, gcloud_key: Path):
        """Class initializer

        Args:
            gcloud_key (path: Path): gets json file path, checks if file exists
        """
        self.gcloud_key = gcloud_key

        assert os.path.isfile(self.gcloud_key), f"file '{self.gcloud_key}' does not exists"

    def get_client(self) -> bigquery.Client:
        """This method authenticates google service account

        Returns:
            bigquery.Client: bigquery.Client
        """
        credentials = service_account.Credentials.from_service_account_file(self.gcloud_key)

        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
            )
        return client

    def table_exists(
            self,
            table_id: str,
            client: bigquery.Client
    ) -> bool:
        """
        This method checks if table exsits into bigQuery dataset

        Args:
            table_id (str): table_id is full bigquery path: 'project_name.dataset.table_id'
            client (bigquery.Client): bigquery.Client

        Returns:
            bool: True or False
        """

        try:
            client.get_table(table_id)
            return True
        except NotFound:
            return False

    def create_table(
            self,
            table_id: str,
            client: bigquery.Client
            ) -> None:
        """
        This method creates table with given table_id

        Args:
            table_id (str): full bigquery path like: 'project_name.dataset.table_id'
            client (bigquery.Client): bigquery.Client
        """
        schema = [
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("csv_file_size_in_mb", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("df_of_csv_rows_n", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("df_of_csv_columns_n", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("df_one_column_size_in_mb", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("df_size_in_mb", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("created", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated", "TIMESTAMP", mode="REQUIRED"),

        ]

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id)
        )

    def check_filename(
        self,
        table_id: str,
        name: str,
        client: bigquery.Client
    ) -> bool:
        """
        This method checks if csv_file is already writed at table

        Args:
            table_id (str): full bigquery path like: 'project_name.dataset.table_id'
            name (str): csv file filename
            client (bigquery.Client): bigquery.Client_

        Returns:
            bool: True or False
        """
        QUERY = (
            f'SELECT name FROM `{table_id}` '
            f'WHERE name = "{name}" '
            'LIMIT 1'
        )

        query_job = client.query(QUERY)
        rows = list(query_job.result())

        if rows:
            return True
        else:
            return False

    def write_to_db(
            self,
            _dict: list[dict[str, str]],
            table_id: str,
            client: bigquery.Client
            ) -> None:
        """
        This method writes list[dict] data into table

        Args:
            _dict (list[dict[str, str]]): list[dict] data to write
            table_id (str): full bigquery path like: 'project_name.dataset.table_id'
            client (bigquery.Client): bigquery.Client
        """
        # TODO: i need to find out other method
        # after using this method it needs to pass 1 hour half to can update
        errors = client.insert_rows_json(table_id, _dict)
        if errors == []:
            print("Data has been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def update(self, _dict: dict[str, str], table_id: str, client: bigquery.Client):
        """
        This method updates each row, if csv file is already at table

        Args:
            _dict (dict[str, str]): dict[str, str] data
            table_id (str): full bigquery path like: 'project_name.dataset.table_id'_
            client (bigquery.Client): bigquery.Client
        """
        name = _dict.get("name")
        csv_file_size_in_mb = _dict.get("csv_file_size_in_mb")
        df_of_csv_rows_n = _dict.get("df_of_csv_rows_n")
        df_of_csv_columns_n = _dict.get("df_of_csv_columns_n")
        df_one_column_size_in_mb = _dict.get("df_one_column_size_in_mb")
        df_size_in_mb = _dict.get("df_size_in_mb")
        updated = datetime.now().isoformat("T", "seconds")
        update_query = (
            f'UPDATE `{table_id}`\n'
            f'''SET
                csv_file_size_in_mb = {csv_file_size_in_mb},
                df_of_csv_rows_n = {df_of_csv_rows_n},
                df_of_csv_columns_n = {df_of_csv_columns_n},
                df_one_column_size_in_mb = {df_one_column_size_in_mb},
                df_size_in_mb = {df_size_in_mb},
                updated = '{updated}'
            '''
            f"WHERE name = '{name}'"
        )

        query_job = client.query(update_query)
        query_job.result()
        print(f"{query_job.num_dml_affected_rows} rows updated.")
