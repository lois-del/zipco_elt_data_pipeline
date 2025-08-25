import os
import logging
import sys
import traceback
from dotenv import load_dotenv
from utils import (get_data, clean_headers)
from sqlalchemy import create_engine, text

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # Always load .env from the script folder so that relative paths work even when called by a scheduler

load_dotenv(os.path.join(BASE_DIR, ".env")) #load secrets and config from .env

# File logger for scheduler runs
LOG_PATH = os.path.join(BASE_DIR, "etl.log") #set log file path that is stable regardless of where the script runs from
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s" #write information into the .logs
)

url = os.getenv("API_URL")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

    
df = get_data(url)

engine = create_engine(os.getenv("DB_URL"))
df.to_sql('raw_properties', engine, if_exists='replace', index=False)

def main() -> int:
    logging.info("ETL start")
    try:
        df = get_data(url)
        df.to_sql("raw_properties", engine, if_exists="replace", index=False)

        df_properties_clean = clean_headers(df)
        df_properties_clean.to_sql("transformed_listing", engine, if_exists="replace", index=False)

        logging.info("Rows: raw=%s clean=%s", len(df), len(df_properties_clean))
        logging.info("ETL success")
        return 0
    except Exception:
        logging.error("ETL failed\n%s", traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())