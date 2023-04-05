import psycopg2
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import os


class WriteDb:
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

    def create_table(
            self,
            project_id: str,
            dataset: str,
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
        table_id = f"{project_id}.{dataset}.{table_id}"

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id)
        )


    # TODO: i need to change this to check filename into bigquery
    def check_filename(self, name: str) -> bool:
        name = name.replace(".csv", "_csv")
        search_query = f"""
            SELECT name FROM csv_data
            WHERE name='{name}';
        """
        self.cursor.execute(search_query)
        result = self.cursor.fetchone()
        if result:
            return True
        return False

    # TODO: change to write data into bigquery
    def write_to_db(self, _dict: dict[str, dict]) -> None:
        insert_query = """
            INSERT INTO csv_data (
                name,
                csv_file_size_in_mb,
                df_of_csv_rows_n,
                df_of_csv_columns_n,
                df_one_column_size_in_mb,
                df_size_in_mb,
                created,
                updated
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        name = list(_dict)[0]

        inner_dict: dict[str, str] = _dict.get(name)
        csv_file_size_in_mb = inner_dict.get("csv_file_size_in_mb")
        df_of_csv_rows_n = inner_dict.get("df_of_csv_rows_n")
        df_of_csv_columns_n = inner_dict.get("df_of_csv_columns_n")
        df_one_column_size_in_mb = inner_dict.get("df_one_column_size_in_mb")
        df_size_in_mb = inner_dict.get("df_size_in_mb")
        created = inner_dict.get("created")
        updated = inner_dict.get("created")

        self.cursor.execute(insert_query, (
            name,
            csv_file_size_in_mb,
            df_of_csv_rows_n,
            df_of_csv_columns_n,
            df_one_column_size_in_mb,
            df_size_in_mb,
            created,
            updated
        ))
        self.conn.commit()

    # TODO: update data into bigquery
    def update(self, _dict: dict[str, dict]):
        name = list(_dict)[0]
        inner_dict: dict[str, str] = _dict.get(name)
        csv_file_size_in_mb = inner_dict.get("csv_file_size_in_mb")
        df_of_csv_rows_n = inner_dict.get("df_of_csv_rows_n")
        df_of_csv_columns_n = inner_dict.get("df_of_csv_columns_n")
        df_one_column_size_in_mb = inner_dict.get("df_one_column_size_in_mb")
        df_size_in_mb = inner_dict.get("df_size_in_mb")
        updated = datetime.now().isoformat("T", "seconds")
        update_query = f"""
            UPDATE csv_data
            SET
                csv_file_size_in_mb = {csv_file_size_in_mb},
                df_of_csv_rows_n = {df_of_csv_rows_n},
                df_of_csv_columns_n = {df_of_csv_columns_n},
                df_one_column_size_in_mb = {df_one_column_size_in_mb},
                df_size_in_mb = {df_size_in_mb},
                updated = '{updated}'
            WHERE name = '{name}';
        """
        self.cursor.execute(update_query)
        self.conn.commit()
