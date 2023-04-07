from utils import envs

from utils.csvs import MetadataParser
from utils.db import WriteDb

if __name__ == "__main__":

    table_id = f"{envs.PROJECT_ID}.{envs.DATASET}.{envs.TABLE_ID}"

    csv_obj: MetadataParser = MetadataParser(path=envs.CSVS_DIR)

    data: list[dict[str, str]] = csv_obj.list_csvs()
    csv_obj.write_to_json(data, envs.PROJECT_DIR)

    db_obj: WriteDb = WriteDb(gcloud_key=envs.GCLOUD_KEY)

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
