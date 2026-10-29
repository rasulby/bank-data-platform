"""Run a real DAG then check metadata status. Invoked inside Airflow."""

import json
import subprocess
import sys
from datetime import date

from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session

if __name__ == "__main__":
    day = date.fromisoformat(sys.argv[1]).isoformat()
    conf = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    expected = "failed" if any(conf.values()) else "success"
    subprocess.run(
        ["airflow", "dags", "test", "bank_pipeline", day, "--conf", json.dumps(conf)], check=False
    )
    with create_session() as session:
        run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == "bank_pipeline", DagRun.run_id == f"manual__{day}T00:00:00+00:00"
            )
            .one()
        )
        assert run.state == expected, f"Expected {expected}, got {run.state}"
        states = {task.task_id: task.state for task in run.get_task_instances(session=session)}
        if expected == "failed":
            assert states["publish_report"] == "upstream_failed", states
            # A writer conflict or setup error must not masquerade as the deliberate
            # ingestion/quality failure the presentation is meant to demonstrate.
            if conf.get("fail_ingest"):
                assert states["seed_source"] == "success" and states["ingest_cdc"] == "failed", (
                    states
                )
            elif conf.get("fail_quality"):
                assert states["dbt_models"] == "success" and states["quality_checks"] == "failed", (
                    states
                )
        print(json.dumps({"date": day, "state": run.state, "tasks": states}), flush=True)
