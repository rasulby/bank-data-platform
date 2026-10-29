"""Compare real source, lake, warehouse, dbt and exported report results."""

import argparse
import csv
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from jobs.config import env, identifier, object_store, postgres, warehouse
from jobs.spark_common import session, table


def verify(spark, day):
    from pyspark.sql import functions as F

    day = date.fromisoformat(day).isoformat()
    schema = identifier("BANK_SCHEMA")
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(
            f"""SELECT count(*), coalesce(sum(amount),0), coalesce(sum(txn_id),0) FROM {schema}.transactions
            WHERE txn_time >= %s::date AND txn_time < %s::date+interval '1 day' """,
            (day, day),
        )
        source = cur.fetchone()
        cur.execute(
            f"SELECT amount FROM {schema}.transactions WHERE txn_id=%s",
            (date.fromisoformat(day).toordinal() * 1000000,),
        )
        amount_row = cur.fetchone()
        source_amount = amount_row[0] if amount_row else None
    lake = spark.table(table("transactions")).where(F.col("txn_date") == day)
    lake_stats = tuple(
        lake.agg(
            F.count("*"),
            F.coalesce(F.sum("amount"), F.lit(0)),
            F.coalesce(F.sum("txn_id"), F.lit(0)),
        ).first()
    )
    raw = spark.table(table("raw_events"))
    event_count = raw.count()
    unique_events = raw.select("topic", "event_partition", "event_offset").distinct().count()
    assert event_count == unique_events and event_count > 0, "Duplicate or missing raw events"
    db = identifier("CLICKHOUSE_DB")
    result = {
        "date": day,
        "source": source[0],
        "raw_events_all_dates": event_count,
        "iceberg": lake_stats[0],
    }
    client = warehouse()
    try:
        for name in ["raw_transactions", "stg_transactions", "fct_transactions"]:
            stats = client.query(f"""SELECT count(), sum(amount), sum(txn_id) FROM {db}.{name}
                WHERE txn_date=toDate('{day}')""").first_row
            assert tuple(stats) == tuple(source), f"{name}: {stats} != source {source}"
            result[name] = stats[0]
        serving = client.query(f"""SELECT sum(transaction_count),sum(total_amount)
            FROM {db}.daily_summary WHERE txn_date=toDate('{day}')""").first_row
        assert tuple(serving) == tuple(source[:2]), "Serving totals do not match source"
        result["serving_transactions"] = serving[0]
        current_rows = client.query(
            f"SELECT amount FROM {db}.fct_transactions WHERE txn_id={date.fromisoformat(day).toordinal() * 1000000}"
        ).result_rows
        current_amount = current_rows[0][0] if current_rows else None
        assert current_amount == source_amount, "CDC update was not applied"
        result["first_transaction_amount"] = str(current_amount)
        timestamped, invalid_order = client.query(f"""SELECT
            countIf(ingested_at IS NOT NULL AND processed_at IS NOT NULL AND loaded_at IS NOT NULL
                    AND kafka_timestamp IS NOT NULL AND cdc_emitted_at IS NOT NULL),
            countIf(processed_at < ingested_at OR loaded_at < processed_at)
            FROM {db}.fct_transactions WHERE txn_date=toDate('{day}')""").first_row
        assert timestamped == source[0], (
            "Timing columns missing; run the date again after migration"
        )
        assert invalid_order == 0, "Processing clock/order problem"
        result["timestamped_transactions"] = timestamped
        result["negative_processing_latencies"] = invalid_order

        for name in ["dim_customers", "dim_cards"]:
            count = client.query(f"SELECT count() FROM {db}.{name}").first_row[0]
            assert count > 0, f"{name} is empty"
            result[name] = count
    finally:
        client.close()
    assert tuple(lake_stats) == tuple(source), f"Iceberg: {lake_stats} != source {source}"
    with (Path(env("REPORT_DIR")) / f"daily_{day}.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert sum(int(row["transaction_count"]) for row in rows) == source[0], "Report count mismatch"
    assert sum(Decimal(row["total_amount"]) for row in rows) == source[1], "Report sum mismatch"
    # Check that real Parquet and Iceberg metadata artifacts exist in object storage.
    keys = []
    for page in object_store().get_paginator("list_objects_v2").paginate(Bucket=env("LAKE_BUCKET")):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    assert any(key.endswith(".parquet") for key in keys), "No Parquet objects"
    assert any(key.endswith(".metadata.json") for key in keys), "No Iceberg metadata"
    result["report_transactions"] = source[0]
    output = Path(env("REPORT_DIR")) / f"verify_{day}.json"
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    spark = session("bank-integration-verify")
    try:
        verify(spark, args.date)
    finally:
        spark.stop()
