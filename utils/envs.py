import os

from dotenv import load_dotenv

current_dir = os.path.abspath(os.path.dirname(__file__))

PROJECT_DIR = os.path.abspath(os.path.join(current_dir, ".."))

dotenv_path = os.path.join(PROJECT_DIR, ".env")
load_dotenv(dotenv_path)


GCLOUD_KEY = os.environ.get("GCLOUD_KEY")
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET = os.environ.get("DATASET")
TABLE_ID = os.environ.get("TABLE_ID")
CSVS_DIR = os.environ.get("CSVS_DIR")
