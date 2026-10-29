"""Publish a date partition atomically to ClickHouse after reading Iceberg."""

import argparse
from datetime import date

from jobs.config import identifier, warehouse
from jobs.observability import LINEAGE
from jobs.spark_common import session, table

COLUMNS = [
    "txn_id",
    "card_id",
    "customer_id",
    "card_type",
    "merchant",
    "amount",
    "currency",
    "status",
    "txn_time",
    "txn_date",
    *LINEAGE,
]
DDL = """txn_id Int64, card_id Int64, customer_id Int64, card_type String,
merchant String, amount Decimal(18,2), currency String, status String,
txn_time DateTime64(3,'UTC'), txn_date Date"""


CH_METADATA = {
    name: (
        "Nullable(Int32)"
        if kind == "INT"
        else "Nullable(Int64)"
        if kind == "BIGINT"
        else "Nullable(String)"
        if kind == "STRING"
        else "Nullable(DateTime64(3, 'UTC'))"
    )
    for name, kind in LINEAGE.items()
}
CH_METADATA["loaded_at"] = "Nullable(DateTime64(3, 'UTC'))"


def ensure_tables(client, db):
    client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
    for name in ["raw_transactions", "incoming_transactions"]:
        full = f"{db}.{name}"
        client.command(
            f"CREATE TABLE IF NOT EXISTS {full} ({DDL}) ENGINE=MergeTree PARTITION BY txn_date ORDER BY txn_id"
        )
        description = client.query(f"DESCRIBE TABLE {full}").result_rows
        existing = {row[0] for row in description}
        txn_type = next(row[1] for row in description if row[0] == "txn_time")
        if txn_type.replace(" ", "") != "DateTime64(3,'UTC')":
            client.command(
                f"ALTER TABLE {full} MODIFY COLUMN txn_time DateTime64(3,'UTC') SETTINGS mutations_sync=2"
            )
        for column, kind in CH_METADATA.items():
            if column not in existing:
                client.command(f"ALTER TABLE {full} ADD COLUMN {column} {kind} DEFAULT NULL")
        if "loaded_at" not in existing:
            # Freeze old rows as NULL before changing the default: their historical
            # load time was not recorded and must not be invented during migration.
            client.command(
                f"ALTER TABLE {full} MATERIALIZE COLUMN loaded_at SETTINGS mutations_sync=2"
            )
        client.command(
            f"ALTER TABLE {full} MODIFY COLUMN loaded_at Nullable(DateTime64(3, 'UTC')) DEFAULT now64(3, 'UTC')"
        )


def load(spark, day, allow_empty=False):
    day = date.fromisoformat(day)
    db = identifier("CLICKHOUSE_DB")
    client = warehouse()
    try:
        ensure_tables(client, db)
        rows = (
            spark.table(table("transactions"))
            .where(f"txn_date = DATE '{day}'")
            .select(*COLUMNS)
            .collect()
        )
        if not rows and not allow_empty:
            raise ValueError("Refusing to replace a partition with no rows")
        client.command(f"ALTER TABLE {db}.incoming_transactions DROP PARTITION '{day}'")
        if rows:
            client.insert(
                f"{db}.incoming_transactions", [tuple(row) for row in rows], column_names=COLUMNS
            )
            client.command(
                f"ALTER TABLE {db}.raw_transactions REPLACE PARTITION '{day}' FROM {db}.incoming_transactions"
            )
        else:
            client.command(f"ALTER TABLE {db}.raw_transactions DROP PARTITION '{day}'")
        for entity, key, ddl in [
            ("customers", "customer_id", "customer_id Int64, full_name String, city String"),
            (
                "cards",
                "card_id",
                "card_id Int64, customer_id Int64, card_type String, issued_date String, annual_fee String",
            ),
        ]:
            client.command(
                f"CREATE TABLE IF NOT EXISTS {db}.{entity} ({ddl}) ENGINE=MergeTree ORDER BY {key}"
            )
            client.command(f"CREATE TABLE IF NOT EXISTS {db}.incoming_{entity} AS {db}.{entity}")
            data = spark.table(table(entity)).collect()
            client.command(f"TRUNCATE TABLE {db}.incoming_{entity}")
            if data:
                client.insert(
                    f"{db}.incoming_{entity}",
                    [tuple(row) for row in data],
                    column_names=data[0].__fields__,
                )
            client.command(f"EXCHANGE TABLES {db}.{entity} AND {db}.incoming_{entity}")
        print(f"load day={day} rows={len(rows)} output={db}.raw_transactions", flush=True)
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    spark = session("bank-warehouse-load")
    try:
        load(spark, args.date)
    finally:
        spark.stop()
