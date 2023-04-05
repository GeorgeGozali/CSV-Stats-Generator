import psycopg2
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import os


class WriteDb:
    def __init__(self,
                 db_name,
                 db_user,
                 db_pass,
                 db_host,
                 db_port,
                 gcloud_key
                 ):
        print("Starting connection...")
        self.conn = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port,
        )
        print("Connection has started!")
        self.cursor = self.conn.cursor()

        assert os.path.isfile(gcloud_key), f"file '{gcloud_key}' does not exists"

    def create_table(self):
        create_query = """
        CREATE TABLE IF NOT EXISTS csv_data (
            id BIGSERIAL PRIMARY KEY,
            name TEXT UNIQUE,
            csv_file_size_in_mb FLOAT,
            df_of_csv_rows_n INTEGER,
            df_of_csv_columns_n INTEGER,
            df_one_column_size_in_mb FLOAT,
            df_size_in_mb FLOAT,
            created TIMESTAMP,
            updated TIMESTAMP
        );
        """
        print("Creating table...")
        self.cursor.execute(create_query)
        self.conn.commit()
        print("Table has created!")

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
