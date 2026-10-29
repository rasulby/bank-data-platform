"""Read real Debezium events with Structured Streaming into an Iceberg raw table."""

import time
import uuid
from datetime import datetime, timezone

from jobs.config import env, identifier, postgres
from jobs.spark_common import create_tables, session, table


def kafka_stream(spark):
    from pyspark.sql import functions as F

    schema = identifier("BANK_SCHEMA")
    topics = ",".join(
        f"{env('CDC_TOPIC_PREFIX')}.{schema}.{name}"
        for name in ["customers", "cards", "transactions", "pipeline_barriers"]
    )
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS"))
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
        .select(
            "topic",
            F.col("partition").alias("event_partition"),
            F.col("offset").alias("event_offset"),
            F.col("value").cast("string").alias("payload"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("timestampType").alias("kafka_timestamp_type"),
        )
    )
    for name, path in [("source_changed_at", "$.source.ts_ms"), ("cdc_emitted_at", "$.ts_ms")]:
        milliseconds = F.get_json_object("payload", path).cast("long")
        stream = stream.withColumn(name, F.when(milliseconds > 0, F.timestamp_millis(milliseconds)))
    return stream.withColumn("ingested_at", F.current_timestamp()).select(
        "topic",
        "event_partition",
        "event_offset",
        "payload",
        "source_changed_at",
        "cdc_emitted_at",
        "kafka_timestamp",
        "kafka_timestamp_type",
        "ingested_at",
    )


def write_raw(batch, batch_id):
    from pyspark.sql import functions as F

    from jobs.observability import OBSERVATIONS

    batch.persist()
    try:
        count = batch.count()
        if count:
            # Record receipt after the Kafka batch has actually been read into
            # cache, not at trigger start (which can precede the newest record).
            observed = batch.withColumn("ingested_at", F.lit(datetime.now(timezone.utc)))
            observed.createOrReplaceTempView("incoming_events")
            updates = ", ".join(
                f"target.{name}=coalesce(target.{name},source.{name})" for name in OBSERVATIONS
            )
            batch.sparkSession.sql(f"""MERGE INTO {table("raw_events")} target USING incoming_events source
                ON target.topic=source.topic AND target.event_partition=source.event_partition
                AND target.event_offset=source.event_offset
                WHEN MATCHED AND target.ingested_at IS NULL THEN UPDATE SET {updates}
                WHEN NOT MATCHED THEN INSERT *""")
            batch.sparkSession.catalog.refreshTable(table("raw_events"))
        print(
            f"raw microbatch={batch_id} rows_read={count} output={table('raw_events')}", flush=True
        )
        return count
    finally:
        batch.unpersist()


def ingest(spark):
    from pyspark.sql import functions as F

    marker = str(uuid.uuid4())
    schema = identifier("BANK_SCHEMA")
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(f"INSERT INTO {schema}.pipeline_barriers (barrier_id) VALUES (%s)", (marker,))
    stream = kafka_stream(spark)

    def write_batch(batch, batch_id):
        write_raw(batch, batch_id)

    def drain():
        query = (
            stream.writeStream.foreachBatch(write_batch)
            .option("checkpointLocation", env("CHECKPOINT_DIR") + "/bank-cdc-v2")
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination(180)
        if query.isActive:
            query.stop()
            raise TimeoutError("Streaming drain exceeded 180 seconds")

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        drain()
        spark.catalog.refreshTable(table("raw_events"))
        found = (
            spark.table(table("raw_events"))
            .where(F.get_json_object("payload", "$.after.barrier_id") == marker)
            .limit(1)
            .count()
        )
        if found:
            # Capture end offsets again after observing the source barrier.
            drain()
            print(f"CDC barrier received: {marker}", flush=True)
            return
        time.sleep(3)
    raise TimeoutError("Source barrier not received; check Debezium status and Kafka topics")


if __name__ == "__main__":
    spark = session("bank-cdc-ingest")
    try:
        create_tables(spark)
        ingest(spark)
    finally:
        spark.stop()
