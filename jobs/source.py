"""Small synthetic source: customers, issued cards and card transactions."""

import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from psycopg2 import sql
from psycopg2.extras import execute_values

from jobs.config import env, identifier, postgres


def prepare():
    schema = identifier("BANK_SCHEMA")
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.customers (
            customer_id BIGINT PRIMARY KEY, full_name TEXT NOT NULL, city TEXT NOT NULL)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.cards (
            card_id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES {schema}.customers,
            card_type TEXT NOT NULL, issued_date DATE NOT NULL, annual_fee NUMERIC(12,2) NOT NULL)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.transactions (
            txn_id BIGINT PRIMARY KEY, card_id BIGINT NOT NULL REFERENCES {schema}.cards,
            merchant TEXT NOT NULL, amount NUMERIC(18,2) NOT NULL CHECK(amount>0),
            currency TEXT NOT NULL, status TEXT NOT NULL, txn_time TIMESTAMPTZ NOT NULL)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.pipeline_barriers (
            barrier_id TEXT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
        # Existing rows retain NULL audit times; defaults apply to new writes only.
        for column in ["created_at", "updated_at"]:
            cur.execute(
                f"ALTER TABLE {schema}.transactions ADD COLUMN IF NOT EXISTS {column} TIMESTAMPTZ"
            )
            cur.execute(
                f"ALTER TABLE {schema}.transactions ALTER COLUMN {column} SET DEFAULT clock_timestamp()"
            )
        cur.execute(f"""CREATE OR REPLACE FUNCTION {schema}.touch_transaction() RETURNS trigger AS $$
            BEGIN NEW.updated_at=clock_timestamp(); RETURN NEW; END;
            $$ LANGUAGE plpgsql""")
        cur.execute(f"DROP TRIGGER IF EXISTS bank_transaction_updated ON {schema}.transactions")
        cur.execute(f"""CREATE TRIGGER bank_transaction_updated BEFORE UPDATE ON {schema}.transactions
            FOR EACH ROW EXECUTE FUNCTION {schema}.touch_transaction()""")
        for table in ["customers", "cards", "transactions", "pipeline_barriers"]:
            cur.execute(f"ALTER TABLE {schema}.{table} REPLICA IDENTITY FULL")
        cur.execute(f"CREATE INDEX IF NOT EXISTS bank_txn_time ON {schema}.transactions(txn_time)")
    print(f"Source schema ready: {schema}", flush=True)


def seed(day):
    day = date.fromisoformat(day)
    schema = identifier("BANK_SCHEMA")
    count = int(env("BATCH_ROWS"))
    if not 1000 <= count <= 100000:
        raise ValueError("BATCH_ROWS must be between 1000 and 100000")
    customers = [
        (i, f"Customer {i:03d}", ["Baku", "Ganja", "Sumgait"][i % 3]) for i in range(1, 101)
    ]
    cards = [
        (
            i,
            (i - 1) % 100 + 1,
            "PREMIUM" if i % 5 == 0 else "STANDARD",
            date(2026, 1, 1),
            Decimal("50.00") if i % 5 == 0 else Decimal("10.00"),
        )
        for i in range(1, 201)
    ]
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    rows = [
        (
            day.toordinal() * 1000000 + i,
            i % 200 + 1,
            ["Market", "Pharmacy", "Restaurant"][i % 3],
            Decimal(i % 200 + 1),
            "AZN",
            "DECLINED" if i % 10 == 0 else "SUCCESS",
            start + timedelta(seconds=i % 86400),
        )
        for i in range(count)
    ]
    with postgres() as conn, conn.cursor() as cur:
        for table, values in [("customers", customers), ("cards", cards), ("transactions", rows)]:
            columns = (
                "(txn_id,card_id,merchant,amount,currency,status,txn_time)"
                if table == "transactions"
                else ""
            )
            execute_values(
                cur,
                f"INSERT INTO {schema}.{table} {columns} VALUES %s ON CONFLICT DO NOTHING",
                values,
            )
        cur.execute(
            f"SELECT count(*) FROM {schema}.transactions WHERE txn_time >= %s AND txn_time < %s",
            (start, start + timedelta(days=1)),
        )
        print(f"source day={day} rows={cur.fetchone()[0]}", flush=True)


def change(day):
    """Repeatable CDC demonstration: update one transaction and insert another."""
    day = date.fromisoformat(day)
    schema = identifier("BANK_SCHEMA")
    first = day.toordinal() * 1000000
    extra = first + 900000
    with postgres() as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE {schema}.transactions SET amount=999.00 WHERE txn_id=%s", (first,))
        if not cur.rowcount:
            raise ValueError("Run the pipeline for this date before the CDC demo")
        cur.execute(
            f"""INSERT INTO {schema}.transactions (txn_id,card_id,merchant,amount,currency,status,txn_time) VALUES
            (%s,1,'CDC demo',25.00,'AZN','SUCCESS',%s) ON CONFLICT DO NOTHING""",
            (extra, datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)),
        )
    print(f"CDC update txn_id={first} amount=999; insert txn_id={extra}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "seed", "change"])
    parser.add_argument("--date", default="2026-01-15")
    args = parser.parse_args()
    if args.action == "prepare":
        prepare()
    elif args.action == "seed":
        seed(args.date)
    else:
        change(args.date)
