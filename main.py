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

    csv_obj = MetadataParser(path=DIR_PATH)
    data = csv_obj.list_csvs()
    csv_obj.write_to_json(data)

    db_obj = WriteDb(gcloud_key=GCLOUD_KEY)

    client = db_obj.get_client()
    db_obj.create_table(PROJECT_ID, DATASET, TABLE_ID, client)
    for dict_item in data:
        for key, values in dict_item.items():
            if db_obj.check_filename(key):
                db_obj.update(dict_item)
            else:
                db_obj.write_to_db(dict_item)
