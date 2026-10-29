FROM apache/airflow:2.10.5-python3.11
RUN mkdir -p /opt/airflow/state /opt/airflow/logs
COPY --chown=airflow:root orchestration /opt/airflow/dags
COPY --chown=airflow:root scripts/job_client.py /opt/airflow/scripts/job_client.py
COPY --chown=airflow:root tests/airflow_smoke.py /opt/airflow/tests/airflow_smoke.py
COPY --chown=airflow:root infra/airflow/start.sh /opt/airflow/scripts/start-airflow.sh
COPY --chown=airflow:root scripts/realtime_client.py /opt/airflow/scripts/realtime_client.py
