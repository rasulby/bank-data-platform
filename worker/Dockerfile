FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl tini && rm -rf /var/lib/apt/lists/*
COPY requirements/worker.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# Resolve JAVA_HOME on both ARM and x86 at runtime.
WORKDIR /app
COPY scripts/download_jars.py /tmp/download_jars.py
RUN python /tmp/download_jars.py
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*
COPY jobs /app/jobs
COPY worker /app/worker
COPY transformation /app/transformation
COPY tests /app/tests
ENV PYTHONPATH=/app SPARK_LOCAL_IP=127.0.0.1
RUN mkdir -p /state/jobs /state/checkpoints /state/reports && useradd -u 10001 -m runner && chown -R runner:runner /state /app
USER runner
ENTRYPOINT ["/usr/bin/tini", "--", "/app/worker/entrypoint.sh"]
CMD ["python", "-m", "worker.server"]
