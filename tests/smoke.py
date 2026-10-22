"""Run the DAG against real services and check every layer."""

import csv
import json
import subprocess
import sys
from datetime import date, timedelta

from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session
from psycopg2 import sql

from pipeline.tasks import connect, raw_path, report_path, table


def run(day, conf=None, expected="success"):
    result = subprocess.run(
        ["airflow", "dags", "test", "bank_batch", day, "--conf", json.dumps(conf or {})],
        check=False,
    )
    with create_session() as session:
        record = (
            session.query(DagRun)
            .filter(DagRun.dag_id == "bank_batch")
            .order_by(DagRun.execution_date.desc())
            .all()
        )
        matches = [item for item in record if item.execution_date.date().isoformat() == day]
        if not matches or matches[0].state != expected:
            raise AssertionError(f"{day}: expected DAG status {expected}")
    if expected == "success" and result.returncode:
        raise RuntimeError("airflow dags test failed")


def counts(day):
    with raw_path(day).open(newline="") as handle:
        report = {"csv": len(list(csv.DictReader(handle)))}
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT count(*) FROM {} WHERE txn_date=%s").format(
                table("source", "batch_transactions")
            ),
            (day,),
        )
        report["source"] = cur.fetchone()[0]
        for layer in ["raw", "staging", "curated"]:
            cur.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE txn_date=%s").format(table(layer)), (day,)
            )
            report[layer] = cur.fetchone()[0]
        cur.execute(
            sql.SQL("SELECT sum(transaction_count) FROM {} WHERE txn_date=%s").format(
                table("serving", "daily_summary")
            ),
            (day,),
        )
        report["serving_transactions"] = int(cur.fetchone()[0] or 0)
    with report_path(day).open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    report["report_transactions"] = sum(int(row["transaction_count"]) for row in rows)
    if min(report.values()) < 1000 or len(set(report.values())) != 1:
        raise AssertionError(report)
    print(day, json.dumps(report), flush=True)
    return report


if __name__ == "__main__":
    day = sys.argv[1] if len(sys.argv) > 1 else "2026-01-15"
    run(day)
    first = counts(day)
    run(day)
    if counts(day) != first:
        raise AssertionError("Repeat changed row counts")
    second = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    run(second)
    counts(second)
    counts(day)
    print("SMOKE PASSED: success, repeat, two dates, all layers")
