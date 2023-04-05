import os

from dotenv import load_dotenv

from utils.csvs import MetadataParser
from utils.db import WriteDb

if __name__ == "__main__":
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path)

    DIR_PATH = os.environ.get("DIR_PATH")
    DB_NAME = os.environ.get("DB_NAME")
    DB_USER = os.environ.get("DB_USER")
    DB_PASS = os.environ.get("DB_PASS")
    DB_HOST = os.environ.get("DB_HOST")
    DB_PORT = os.environ.get("DB_PORT")
    GCLOUD_KEY = os.environ.get("GCLOUD_KEY")

    csv_obj = MetadataParser(path=DIR_PATH)
    data = csv_obj.list_csvs()
    csv_obj.write_to_json(data)

    db_obj = WriteDb(
        db_name=DB_NAME,
        db_user=DB_USER,
        db_pass=DB_PASS,
        db_host=DB_HOST,
        db_port=DB_PORT,
        gcloud_key=GCLOUD_KEY
    )
    db_obj.create_table()
    for dict_item in data:
        for key, values in dict_item.items():
            if db_obj.check_filename(key):
                db_obj.update(dict_item)
            else:
                db_obj.write_to_db(dict_item)
