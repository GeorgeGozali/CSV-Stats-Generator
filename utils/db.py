from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import os


class WriteDb:
    # TODO: here init should get table_id and check if project and dataset is correctly imported
    def __init__(self, gcloud_key):
        self.gcloud_key = gcloud_key

        assert os.path.isfile(self.gcloud_key), f"file '{self.gcloud_key}' does not exists"

    def get_client(self) -> bigquery.Client:
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

        try:
            client.get_table(table_id)  # Make an API request.
            return True
        except NotFound:
            return False

    def create_table(
            self,
            table_id: str,
            client: bigquery.Client
            ) -> None:
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
        # TODO: i need to find out other method
        # after using this method it needs to pass 1 hour half to can update
        errors = client.insert_rows_json(table_id, _dict)
        if errors == []:
            print("Data has been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def update(self, _dict: dict[str, str], table_id: str, client: bigquery.Client):
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
