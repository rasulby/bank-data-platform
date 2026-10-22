"""Batch pipeline tasks for one logical date."""

import csv
import logging
import os
from datetime import date
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

LOG = logging.getLogger(__name__)
COLUMNS = ["txn_id", "txn_date", "merchant", "amount", "currency", "status"]


def connect():
    return psycopg2.connect(
        host=os.environ["SOURCE_DB_HOST"],
        port=os.environ["SOURCE_DB_HOST_PORT"],
        user=os.environ["SOURCE_DB_USER"],
        password=os.environ["SOURCE_DB_PASSWORD"],
        dbname=os.environ["SOURCE_DB_NAME"],
    )


def table(layer, name="transactions"):
    return sql.Identifier(os.environ[f"{layer.upper()}_SCHEMA"], name)


def raw_path(ds):
    return Path(os.environ["RAW_DIR"]) / f"ds={date.fromisoformat(ds)}" / "transactions.csv"


def report_path(ds):
    return Path(os.environ["REPORT_DIR"]) / f"daily_{date.fromisoformat(ds)}.csv"


def log(stage, event, source, target, rows=0, written=None):
    LOG.info(
        "%s %s input=%s output=%s rows_read=%s rows_written=%s",
        stage,
        event,
        source,
        target,
        rows,
        rows if written is None else written,
    )


def prepare(ds):
    """Create sample source rows, skipping existing transaction IDs."""
    day = date.fromisoformat(ds)
    log("prepare", "start", "synthetic generator", "source PostgreSQL")
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(os.environ["SOURCE_SCHEMA"])
            )
        )
        # Keep batch data separate from the older generator tables.
        cur.execute(
            sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
            txn_id BIGINT PRIMARY KEY, txn_date DATE NOT NULL,
            merchant TEXT NOT NULL, amount NUMERIC(18,2) NOT NULL,
            currency TEXT NOT NULL, status TEXT NOT NULL)""").format(
                table("source", "batch_transactions")
            )
        )
        n = int(os.environ["BATCH_ROWS"])
        if n < 1000:
            raise ValueError("BATCH_ROWS must be at least 1000")
        rows = [
            (
                day.toordinal() * 1000000 + i,
                day,
                ["Market", "Pharmacy", "Restaurant"][i % 3],
                (i % 200 + 1),
                "AZN",
                "DECLINED" if i % 10 == 0 else "SUCCESS",
            )
            for i in range(n)
        ]
        inserted = execute_values(
            cur,
            sql.SQL("INSERT INTO {} VALUES %s ON CONFLICT (txn_id) DO NOTHING RETURNING txn_id")
            .format(table("source", "batch_transactions"))
            .as_string(conn),
            rows,
            fetch=True,
        )
        for layer in ["source", "raw", "staging", "curated", "serving"]:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(os.environ[f"{layer.upper()}_SCHEMA"])
                )
            )
        for layer in ["raw", "staging", "curated"]:
            cur.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
                txn_id BIGINT, txn_date DATE, merchant TEXT,
                amount NUMERIC(18,2), currency TEXT, status TEXT)""").format(table(layer))
            )
    log("prepare", "end", "synthetic generator", "source PostgreSQL", n, len(inserted))


def ingest(ds, fail=False):
    target = raw_path(ds)
    log("ingest", "start", table("source", "batch_transactions"), target)
    if fail:
        raise RuntimeError("Deliberate ingestion failure")
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT * FROM {} WHERE txn_date=%s ORDER BY txn_id").format(
                table("source", "batch_transactions")
            ),
            (ds,),
        )
        rows = cur.fetchall()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    with temporary.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        writer.writerows(rows)
    temporary.replace(target)
    log("ingest", "end", table("source", "batch_transactions"), target, len(rows))


def load_raw(ds):
    source = raw_path(ds)
    log("load_raw", "start", source, table("raw"))
    with source.open(newline="") as handle:
        rows = list(csv.reader(handle))[1:]
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("DELETE FROM {} WHERE txn_date=%s").format(table("raw")), (ds,))
        if rows:
            execute_values(
                cur, sql.SQL("INSERT INTO {} VALUES %s").format(table("raw")).as_string(conn), rows
            )
    log("load_raw", "end", source, table("raw"), len(rows))


def transform(ds):
    log("transform", "start", table("raw"), table("curated"))
    with connect() as conn, conn.cursor() as cur:
        for source, target in [("raw", "staging"), ("staging", "curated")]:
            cur.execute(sql.SQL("DELETE FROM {} WHERE txn_date=%s").format(table(target)), (ds,))
            cur.execute(
                sql.SQL("""INSERT INTO {} SELECT DISTINCT txn_id, txn_date,
                trim(merchant), amount, currency, status FROM {} WHERE txn_date=%s""").format(
                    table(target), table(source)
                ),
                (ds,),
            )
            count = cur.rowcount
            log("transform", "end", table(source), table(target), count)


def quality(ds, fail=False):
    log("quality", "start", table("curated"), "quality checks")
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("""SELECT count(*), count(txn_id), count(DISTINCT txn_id),
            count(*) FILTER (WHERE amount <= 0 OR amount IS NULL)
            FROM {} WHERE txn_date=%s""").format(table("curated")),
            (ds,),
        )
        count, keys, unique, invalid = cur.fetchone()
        checks = {
            "non_empty": count > 0,
            "not_null_key": keys == count,
            "unique_key": unique == count,
            "positive_amount": invalid == 0,
            "deliberate_failure": not fail,
        }
        for name, passed in checks.items():
            LOG.info("quality %s=%s", name, passed)
        if not all(checks.values()):
            raise ValueError(f"Data quality failed: {checks}")
    log("quality", "end", table("curated"), "quality checks", count, 0)


def publish(ds):
    target = report_path(ds)
    log("publish", "start", table("curated"), target)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("""CREATE OR REPLACE VIEW {} AS SELECT txn_date,
            currency, count(*) AS transaction_count, sum(amount) AS total_amount,
            count(*) FILTER (WHERE status='DECLINED') AS declined_count
            FROM {} GROUP BY txn_date, currency""").format(
                table("serving", "daily_summary"), table("curated")
            )
        )
        cur.execute(
            sql.SQL("SELECT * FROM {} WHERE txn_date=%s ORDER BY currency").format(
                table("serving", "daily_summary")
            ),
            (ds,),
        )
        rows = cur.fetchall()
        headers = [column.name for column in cur.description]
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    with temporary.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)
    temporary.replace(target)
    log("publish", "end", table("curated"), target, sum(row[2] for row in rows), len(rows))
