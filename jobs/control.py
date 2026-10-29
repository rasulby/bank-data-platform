"""Lightweight non-Spark steps, executed by the external job service."""

import argparse
import csv
import json
from datetime import date
from pathlib import Path

from jobs.config import env, identifier, postgres, warehouse


def quality(day, fail=False, compare_source=True, allow_empty=False):
    day = date.fromisoformat(day).isoformat()
    db = identifier("CLICKHOUSE_DB")
    client = warehouse()
    try:
        count, unique, invalid = client.query(f"""SELECT count(), uniqExact(txn_id),
            countIf(amount <= 0) FROM {db}.fct_transactions WHERE txn_date=toDate('{day}')""").first_row
        target_stats = client.query(
            f"SELECT count(), sum(amount), sum(txn_id) FROM {db}.fct_transactions WHERE txn_date=toDate('{day}')"
        ).first_row
        if compare_source:
            with postgres() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT count(*), coalesce(sum(amount),0), coalesce(sum(txn_id),0) FROM {identifier('BANK_SCHEMA')}.transactions WHERE txn_time >= %s::date AND txn_time < %s::date + interval '1 day'",
                    (day, day),
                )
                expected = cur.fetchone()
        else:
            # The operational source can change during streaming. Compare against
            # the materialized microbatch, not a later moving source snapshot.
            expected = client.query(
                f"SELECT count(), sum(amount), sum(txn_id) FROM {db}.raw_transactions WHERE txn_date=toDate('{day}')"
            ).first_row
        missing_times = client.query(
            f"SELECT countIf(ingested_at IS NULL OR processed_at IS NULL OR loaded_at IS NULL OR kafka_timestamp IS NULL) FROM {db}.fct_transactions WHERE txn_date=toDate('{day}')"
        ).first_row[0]
        checks = {
            "non_empty_or_deleted_partition": count > 0 or allow_empty,
            "unique_key": count == unique,
            "positive_amount": invalid == 0,
            "deliberate_failure": not fail,
            "snapshot_reconciliation": tuple(expected) == tuple(target_stats),
            "required_processing_times": missing_times == 0,
        }
        print(json.dumps(checks), flush=True)
        if not all(checks.values()):
            raise ValueError("Quality checks failed")
    finally:
        client.close()


def publish(day, allow_empty=False):
    day = date.fromisoformat(day).isoformat()
    client = warehouse()
    try:
        result = client.query(
            f"SELECT * FROM {identifier('CLICKHOUSE_DB')}.daily_summary WHERE txn_date=toDate('{day}') ORDER BY currency"
        )
        if not result.result_rows and not allow_empty:
            raise ValueError("Daily summary is empty")
        target = Path(env("REPORT_DIR")) / f"daily_{day}.csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(".tmp")
        with temp.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(result.column_names)
            writer.writerows(result.result_rows)
        temp.replace(target)
        print(f"publish rows={len(result.result_rows)} output={target}", flush=True)
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["quality", "publish"])
    parser.add_argument("--date", required=True)
    parser.add_argument("--fail", action="store_true")
    args = parser.parse_args()
    if args.stage == "quality":
        quality(args.date, args.fail)
    else:
        publish(args.date)
