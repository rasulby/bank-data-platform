"""Build current state from CDC, then join transactions with card/customer dimensions."""

from datetime import date, datetime, timezone
import argparse

from jobs.config import env, identifier
from jobs.observability import OBSERVATIONS, LINEAGE
from jobs.spark_common import create_tables, session, table

SCHEMAS = {
    "customers": "customer_id LONG, full_name STRING, city STRING",
    "cards": "card_id LONG, customer_id LONG, card_type STRING, issued_date INT, annual_fee STRING",
    "transactions": "txn_id LONG, card_id LONG, merchant STRING, amount STRING, currency STRING, status STRING, txn_time STRING",
}
KEYS = {"customers": "customer_id", "cards": "card_id", "transactions": "txn_id"}


def current(spark, entity):
    from pyspark.sql import Window, functions as F

    topic = f"{env('CDC_TOPIC_PREFIX')}.{identifier('BANK_SCHEMA')}.{entity}"
    events = spark.table(table("raw_events")).where(F.col("topic") == topic)
    rows = events.select(
        F.get_json_object("payload", "$.op").alias("op"),
        F.get_json_object("payload", "$.source.lsn").cast("long").alias("lsn"),
        "event_offset",
        "topic",
        "event_partition",
        *OBSERVATIONS,
        F.from_json(
            F.coalesce(
                F.get_json_object("payload", "$.after"), F.get_json_object("payload", "$.before")
            ),
            SCHEMAS[entity],
            {"mode": "FAILFAST"},
        ).alias("row"),
    ).select("op", "lsn", "event_offset", "topic", "event_partition", *OBSERVATIONS, "row.*")
    window = Window.partitionBy(KEYS[entity]).orderBy(
        F.col("lsn").desc_nulls_last(), F.col("event_offset").desc()
    )
    latest = rows.withColumn("rank", F.row_number().over(window)).where(
        (F.col("rank") == 1) & (F.col("op") != "d")
    )
    if entity == "transactions":
        return (
            latest.withColumnRenamed("op", "cdc_operation")
            .withColumnRenamed("lsn", "source_lsn")
            .withColumnRenamed("topic", "source_topic")
            .withColumnRenamed("event_partition", "source_partition")
            .withColumnRenamed("event_offset", "source_offset")
            .drop("rank")
        )
    return latest.drop(
        "rank", "op", "lsn", "event_offset", "topic", "event_partition", *OBSERVATIONS
    )


def transform(spark, day, allow_empty=False):
    from pyspark.sql import functions as F

    day = date.fromisoformat(day).isoformat()
    txns = current(spark, "transactions").withColumn("txn_time", F.to_timestamp("txn_time"))
    txns = txns.withColumn("txn_date", F.to_date("txn_time")).where(F.col("txn_date") == day)
    cards = current(spark, "cards").withColumn(
        "issued_date",
        F.date_add(F.lit("1970-01-01").cast("date"), F.col("issued_date")).cast("string"),
    )
    customers = current(spark, "customers")
    result = (
        txns.join(cards.select("card_id", "customer_id", "card_type"), "card_id")
        .join(customers.select("customer_id"), "customer_id")
        .select(
            "txn_id",
            "card_id",
            "customer_id",
            "card_type",
            F.trim("merchant").alias("merchant"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            "currency",
            "status",
            "txn_time",
            "txn_date",
            *[name for name in LINEAGE if name != "processed_at"],
        )
    )
    result.persist()
    try:
        count = result.count()
        if not count and not allow_empty:
            raise ValueError("No transactions found; ingestion or CDC may have failed")
        output = result.withColumn("processed_at", F.lit(datetime.now(timezone.utc)))
        output.writeTo(table("transactions")).overwrite(
            F.col("txn_date") == F.lit(day).cast("date")
        )
        spark.catalog.refreshTable(table("transactions"))
        # Small reference tables are replaced as a full snapshot.
        for name, frame in [("customers", customers), ("cards", cards)]:
            frame.writeTo(table(name)).using("iceberg").createOrReplace()
            spark.catalog.refreshTable(table(name))
        print(
            f"transform day={day} rows_written={count} output={table('transactions')}", flush=True
        )
    finally:
        result.unpersist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    spark = session("bank-lake-transform")
    try:
        create_tables(spark)
        transform(spark, args.date)
    finally:
        spark.stop()
