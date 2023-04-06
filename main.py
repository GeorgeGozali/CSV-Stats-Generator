import os

from dotenv import load_dotenv

from utils.csvs import MetadataParser
from utils.db import WriteDb

if __name__ == "__main__":
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path)

    DIR_PATH = os.environ.get("DIR_PATH")
    GCLOUD_KEY = os.environ.get("GCLOUD_KEY")
    PROJECT_ID = os.environ.get("PROJECT_ID")
    DATASET = os.environ.get("DATASET")
    TABLE_ID = os.environ.get("TABLE_ID")

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_ID}"

    csv_obj: MetadataParser = MetadataParser(path=DIR_PATH)

    data: list[dict[str, str]] = csv_obj.list_csvs()
    csv_obj.write_to_json(data)

    db_obj: WriteDb = WriteDb(gcloud_key=GCLOUD_KEY)

    client = db_obj.get_client()

    # check if table exists, if not, create one
    if not db_obj.table_exists(table_id, client):
        db_obj.create_table(table_id, client)
    for dict_item in data:
        name = dict_item.get("name")
        # for key, value in dict_item.items():
        if db_obj.check_filename(table_id, name, client):
            db_obj.update(dict_item, table_id, client)
        else:
            # TODO: here will go create mthod
            db_obj.write_to_db([dict_item], table_id, client)
