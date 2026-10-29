"""Orchestration only. All data processing runs in the external Spark worker."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "bank_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
    description="PostgreSQL CDC -> Kafka -> Spark/Iceberg -> ClickHouse/dbt -> report",
    tags=["bank", "cdc", "spark"],
) as dag:
    seed = BashOperator(
        task_id="seed_source",
        bash_command='python /opt/airflow/scripts/job_client.py seed --date "$RUN_DATE"',
        env={"RUN_DATE": "{{ ds }}"},
        append_env=True,
    )
    ingest = BashOperator(
        task_id="ingest_cdc",
        bash_command='python /opt/airflow/scripts/job_client.py ingest --date "$RUN_DATE" --fail "$FAIL"',
        env={
            "RUN_DATE": "{{ ds }}",
            "FAIL": '{{ "true" if dag_run.conf.get("fail_ingest", false) else "false" }}',
        },
        append_env=True,
    )
    transform = BashOperator(
        task_id="transform_lake",
        bash_command='python /opt/airflow/scripts/job_client.py transform --date "$RUN_DATE"',
        env={"RUN_DATE": "{{ ds }}"},
        append_env=True,
    )
    load = BashOperator(
        task_id="load_warehouse",
        bash_command='python /opt/airflow/scripts/job_client.py load --date "$RUN_DATE"',
        env={"RUN_DATE": "{{ ds }}"},
        append_env=True,
    )
    dbt = BashOperator(
        task_id="dbt_models",
        bash_command='python /opt/airflow/scripts/job_client.py dbt --date "$RUN_DATE"',
        env={"RUN_DATE": "{{ ds }}"},
        append_env=True,
    )
    quality = BashOperator(
        task_id="quality_checks",
        bash_command='python /opt/airflow/scripts/job_client.py quality --date "$RUN_DATE" --fail "$FAIL"',
        env={
            "RUN_DATE": "{{ ds }}",
            "FAIL": '{{ "true" if dag_run.conf.get("fail_quality", false) else "false" }}',
        },
        append_env=True,
    )
    publish = BashOperator(
        task_id="publish_report",
        bash_command='python /opt/airflow/scripts/job_client.py publish --date "$RUN_DATE"',
        env={"RUN_DATE": "{{ ds }}"},
        append_env=True,
    )
    seed >> ingest >> transform >> load >> dbt >> quality >> publish
