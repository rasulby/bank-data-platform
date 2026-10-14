import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    return psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "localhost"),
        port=os.getenv("SOURCE_DB_HOST_PORT", "5434"),
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
        dbname=os.environ["SOURCE_DB_NAME"],
    )