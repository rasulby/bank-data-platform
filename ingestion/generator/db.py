import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    # No defaults on purpose: a missing variable should fail loudly
    # instead of silently connecting to the wrong place.
    return psycopg2.connect(
        host=os.environ["SOURCE_DB_HOST"],
        port=os.environ["SOURCE_DB_HOST_PORT"],
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
        dbname=os.environ["SOURCE_DB_NAME"],
    )
