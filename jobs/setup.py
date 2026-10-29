"""Create the bucket and register the real PostgreSQL CDC connector."""

import time

import requests

from jobs.config import env, identifier, object_store
from jobs.source import prepare


def setup():
    prepare()
    s3 = object_store()
    bucket = env("LAKE_BUCKET")
    if bucket not in [item["Name"] for item in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=bucket)
    schema = identifier("BANK_SCHEMA")
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": env("SOURCE_DB_HOST"),
        "database.port": env("SOURCE_DB_PORT"),
        "database.user": env("SOURCE_DB_USER"),
        "database.password": env("SOURCE_DB_PASSWORD"),
        "database.dbname": env("SOURCE_DB_NAME"),
        "topic.prefix": env("CDC_TOPIC_PREFIX"),
        "table.include.list": ",".join(
            f"{schema}.{name}"
            for name in ["customers", "cards", "transactions", "pipeline_barriers"]
        ),
        "slot.name": env("CDC_SLOT"),
        "publication.name": env("CDC_PUBLICATION"),
        "publication.autocreate.mode": "filtered",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "false",
        "time.precision.mode": "connect",
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
    base = f"{env('CONNECT_URL')}/connectors/{env('CDC_CONNECTOR')}"
    response = requests.put(base + "/config", json=config, timeout=30)
    response.raise_for_status()
    for _ in range(60):
        try:
            response = requests.get(base + "/status", timeout=10)
        except requests.ConnectionError:
            time.sleep(2)
            continue
        # PUT is acknowledged before Connect's status topic is updated.
        # A new connector can therefore briefly return 404 during startup.
        if response.status_code in {404, 409, 500, 502, 503, 504}:
            time.sleep(2)
            continue
        response.raise_for_status()
        state = response.json()
        if (
            state["connector"]["state"] == "RUNNING"
            and state.get("tasks")
            and all(task["state"] == "RUNNING" for task in state["tasks"])
        ):
            print(f"Bucket={bucket}; CDC connector is running", flush=True)
            return
        if any(task["state"] == "FAILED" for task in state.get("tasks", [])):
            raise RuntimeError(f"CDC connector failed: {state}")
        time.sleep(2)
    raise TimeoutError("CDC connector did not become ready")


if __name__ == "__main__":
    setup()
