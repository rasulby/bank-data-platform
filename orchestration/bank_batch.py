from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline import tasks


def ingest_day(ds, dag_run, **kwargs):
    tasks.ingest(ds, fail=dag_run.conf.get("fail_ingest", False))


def check_day(ds, dag_run, **kwargs):
    tasks.quality(ds, fail=dag_run.conf.get("fail_quality", False))


with DAG(
    "bank_batch",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    description="PostgreSQL -> CSV -> raw -> staging -> curated -> report",
) as dag:
    prepare = PythonOperator(task_id="prepare_source", python_callable=tasks.prepare)
    ingest = PythonOperator(task_id="ingest", python_callable=ingest_day)
    load = PythonOperator(task_id="load_raw", python_callable=tasks.load_raw)
    transform = PythonOperator(task_id="transform", python_callable=tasks.transform)
    quality = PythonOperator(task_id="quality", python_callable=check_day)
    publish = PythonOperator(task_id="publish", python_callable=tasks.publish)
    prepare >> ingest >> load >> transform >> quality >> publish
