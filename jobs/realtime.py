"""Long-lived Spark microbatches: raw -> lake -> warehouse -> dbt -> report.

The worker owns the single-writer lock for this entire process. The same Spark
session is reused; no additional Spark JVM or Airflow processing is started.
"""

import json
import os
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

from jobs.config import env
from jobs.control import publish, quality
from jobs.ingest import kafka_stream, write_raw
from jobs.load import load
from jobs.spark_common import create_tables, session, table
from jobs.transform import transform

STATE = Path("/state/realtime.json")
STOP = Path("/state/realtime.stop")


def update_state(**values):
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    state.update(values, state_updated_at=datetime.now(timezone.utc).isoformat())
    temporary = STATE.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2))
    temporary.replace(STATE)


def affected_dates(batch, spark):
    from pyspark.sql import functions as F

    tx = batch.where(F.get_json_object("payload", "$.source.table") == "transactions")
    days = set()
    for side in ["before", "after"]:
        rows = (
            tx.select(
                F.to_date(F.to_timestamp(F.get_json_object("payload", f"$.{side}.txn_time"))).alias(
                    "day"
                )
            )
            .distinct()
            .collect()
        )
        days.update(str(row.day) for row in rows if row.day is not None)
    dimension_change = (
        batch.where(F.get_json_object("payload", "$.source.table").isin("cards", "customers"))
        .limit(1)
        .count()
    )
    if dimension_change:
        # Reference changes can affect any historical enriched partition, including
        # a transaction whose reference row arrived one microbatch later.
        history = spark.table(table("raw_events"))
        for side in ["before", "after"]:
            rows = (
                history.where(F.get_json_object("payload", "$.source.table") == "transactions")
                .select(
                    F.to_date(
                        F.to_timestamp(F.get_json_object("payload", f"$.{side}.txn_time"))
                    ).alias("day")
                )
                .distinct()
                .collect()
            )
            days.update(str(row.day) for row in rows if row.day is not None)
    return sorted(days)


def run():
    interval = int(env("REALTIME_INTERVAL_SECONDS"))
    if not 5 <= interval <= 300:
        raise ValueError("REALTIME_INTERVAL_SECONDS must be 5..300")
    spark = session("bank-realtime-cdc")
    processing = threading.Event()
    query = None
    try:
        create_tables(spark)
        # Reuse existing business tables from the initial successful batch run.
        if not spark.catalog.tableExists(table("cards")):
            raise ValueError("Run one successful make run before starting realtime")

        def microbatch(batch, batch_id):
            processing.set()
            try:
                update_state(phase="processing", batch_id=batch_id)
                rows = write_raw(batch, batch_id)
                days = affected_dates(batch, batch.sparkSession) if rows else []
                for day in days:
                    transform(batch.sparkSession, day, allow_empty=True)
                    load(batch.sparkSession, day, allow_empty=True)
                if days:
                    for action in ["run", "test"]:
                        subprocess.run(
                            [
                                "dbt",
                                action,
                                "--project-dir",
                                "/app/transformation",
                                "--profiles-dir",
                                "/app/transformation",
                            ],
                            check=True,
                            timeout=180,
                        )
                    for day in days:
                        quality(day, compare_source=False, allow_empty=True)
                        publish(day, allow_empty=True)
                update_state(
                    phase="waiting",
                    last_completed_at=datetime.now(timezone.utc).isoformat(),
                    batch_id=batch_id,
                    input_rows=rows,
                    dates=days,
                    published=bool(days),
                )
                print(
                    f"realtime batch={batch_id} input_rows={rows} dates={days} published={bool(days)}",
                    flush=True,
                )
            finally:
                processing.clear()

        update_state(
            status="running",
            phase="starting",
            started_at=datetime.now(timezone.utc).isoformat(),
            checkpoint=env("CHECKPOINT_DIR") + "/bank-realtime-v2",
            interval_seconds=interval,
        )
        query = (
            kafka_stream(spark)
            .writeStream.foreachBatch(microbatch)
            .option("checkpointLocation", env("CHECKPOINT_DIR") + "/bank-realtime-v2")
            .trigger(processingTime=f"{interval} seconds")
            .start()
        )
        while query.isActive:
            if query.awaitTermination(1):
                break
            if STOP.exists() and not processing.is_set() and not query.status["isTriggerActive"]:
                query.stop()
                break
        if query.exception():
            raise RuntimeError(str(query.exception()))
        update_state(
            status="stopped", phase="stopped", stopped_at=datetime.now(timezone.utc).isoformat()
        )
    except Exception as error:
        update_state(status="failed", phase="failed", error=str(error))
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    os.environ.setdefault("TZ", "UTC")
    run()
