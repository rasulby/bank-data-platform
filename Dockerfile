FROM apache/airflow:2.10.5-python3.11
RUN mkdir -p /opt/airflow/state /opt/airflow/data /opt/airflow/logs
COPY requirements/pipeline.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY --chown=airflow:root pipeline /opt/airflow/pipeline
COPY --chown=airflow:root orchestration /opt/airflow/dags
ENV PYTHONPATH=/opt/airflow
COPY --chown=airflow:root tests /opt/airflow/tests
