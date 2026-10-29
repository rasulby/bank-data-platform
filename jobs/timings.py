"""Read row-level provenance without starting another Spark session."""

import argparse
from datetime import date

from jobs.config import identifier, warehouse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    day = date.fromisoformat(args.date).isoformat()
    client = warehouse()
    try:
        result = client.query(f"""SELECT txn_id, amount, txn_time,
            source_changed_at, cdc_emitted_at, kafka_timestamp, kafka_timestamp_type,
            ingested_at, processed_at, loaded_at,
            dateDiff('millisecond',source_changed_at,loaded_at) AS source_to_current_load_ms,
            source_topic, source_partition, source_offset, source_lsn, cdc_operation
            FROM {identifier("CLICKHOUSE_DB")}.fct_transactions
            WHERE txn_date=toDate('{day}') ORDER BY source_changed_at DESC,txn_id LIMIT 10""")
        print("\t".join(result.column_names))
        for row in result.result_rows:
            print("\t".join(str(value) for value in row))
    finally:
        client.close()
