# Dagster (webserver + daemon) image. Also carries dbt-core/dbt-spark and the
# Spark Connect client so it can orchestrate the whole medallion pipeline.
FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends git build-essential libpq-dev \
 && rm -rf /var/lib/apt/lists/*

ENV PIP_NO_CACHE_DIR=1 PYTHONUNBUFFERED=1
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENV DAGSTER_HOME=/opt/dagster/app
WORKDIR /opt/dagster/app

# Code and dbt project are bind-mounted at runtime (see docker-compose.yml).
ENTRYPOINT ["/opt/entrypoint.sh"]
