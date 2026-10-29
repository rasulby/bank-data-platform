"""Generate bounded live source traffic; Debezium, not this script, publishes to Kafka."""

import argparse
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from jobs.config import identifier, postgres


def generate(day, duration, rate):
    day = date.fromisoformat(day)
    if not 1 <= duration <= 600 or not 1 <= rate <= 10:
        raise ValueError("Use duration 1..600 seconds and rate 1..10 cycles/second")
    schema = identifier("BANK_SCHEMA")
    base = day.toordinal() * 1000000
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    conn = postgres()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {schema}.transactions WHERE txn_id=%s", (base,))
            if not cur.fetchone():
                raise ValueError("Run the pipeline for this date before starting live traffic")
            # Reserved IDs do not overlap generated rows or the fixed CDC demo row.
            cur.execute(
                f"""SELECT coalesce(max(txn_id), %s) FROM {schema}.transactions
                WHERE txn_id >= %s AND txn_id < %s""",
                (base + 499999, base + 500000, base + 800000),
            )
            next_id = cur.fetchone()[0] + 1
        deadline = time.monotonic() + duration
        tick = 0
        last_inserted = None
        print(f"Live source started: date={day} duration={duration}s rate={rate}/s", flush=True)
        while time.monotonic() < deadline:
            event_id = next_id + tick
            if event_id >= base + 800000:
                raise ValueError("Live ID range exhausted; use another date")
            amount = Decimal(tick % 100 + 1)
            now = datetime.now(timezone.utc)
            # Today's live records use the actual event clock. An explicit older
            # date remains a documented historical/backfill demonstration.
            event_time = now if day == now.date() else start + timedelta(seconds=tick % 86400)
            with conn, conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {schema}.transactions (txn_id,card_id,merchant,amount,currency,status,txn_time) VALUES
                    (%s,1,'Live demo',%s,'AZN','SUCCESS',%s)""",
                    (event_id, amount, event_time),
                )
                cur.execute(
                    f"""UPDATE {schema}.transactions SET amount=amount+1
                    WHERE txn_id=%s RETURNING amount""",
                    (base,),
                )
                updated = cur.fetchone()[0]
                deleted = None
                if tick % 3 == 2 and last_inserted is not None:
                    cur.execute(
                        f"DELETE FROM {schema}.transactions WHERE txn_id=%s AND merchant='Live demo'",
                        (last_inserted,),
                    )
                    deleted = last_inserted
            print(
                f"INSERT txn_id={event_id} amount={amount}; UPDATE txn_id={base} amount={updated}"
                + (f"; DELETE txn_id={deleted}" if deleted else ""),
                flush=True,
            )
            last_inserted = event_id
            tick += 1
            time.sleep(1 / rate)
        print(
            f"Live source finished: {tick} cycles for {day}. With realtime active, wait for catch-up, stop it, then verify; otherwise run the batch pipeline.",
            flush=True,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--rate", type=int, default=1)
    args = parser.parse_args()
    try:
        generate(args.date, args.duration, args.rate)
    except KeyboardInterrupt:
        print("Live source stopped. Committed changes remain available to CDC.", flush=True)
