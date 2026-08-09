# JupyterHub: multi-user, SSO'd interactive dev on the human/BI plane.
#
# Connect-first: this image ships a pure-Python Spark Connect client
# (pyspark[connect]) -- no JVM/Spark runtime. All compute is delegated to the
# shared `spark` Connect server (the exact same server Dagster and dbt use), and
# governance/auth flows through Unity Catalog PER USER (see uc_notebook.py +
# jupyterhub_config.py). The pyspark[connect] version MUST equal the Spark
# server version or the gRPC handshake fails (the classic Spark Connect footgun).
#
# Based on the official JupyterHub image, which already bundles the
# configurable-http-proxy (Node) so we don't have to.
FROM quay.io/jupyterhub/jupyterhub:5

ARG PYSPARK_VERSION=4.1.0

ENV PIP_NO_CACHE_DIR=1 PYTHONUNBUFFERED=1 PYTHONWARNINGS=ignore

# git: dev essential in the hosted env -- editing + committing dbt models and
# `dbt deps` (package installs). (dbt debug also checks for it.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# JupyterLab + single-user server, Keycloak OAuthenticator, the Spark Connect
# client, and data libs. Let pandas/pyarrow float to whatever pyspark[connect]
# requires (avoids the resolver conflict seen when pinning them).
RUN python3 -m pip install --no-cache-dir \
      jupyterlab \
      notebook \
      oauthenticator \
      "pyspark[connect]==${PYSPARK_VERSION}" \
      pandas pyarrow matplotlib \
      grpcio grpcio-status \
      "dlt==1.29.1" \
      "dbt-core==1.10.22" "dbt-spark==1.10.3"
# ^ dlt + dbt match the Dagster image so notebooks behave identically to
#   production. dbt-spark uses method:session against SPARK_REMOTE (the shared
#   Spark Connect server); pyspark[connect] above satisfies the session backend.
#   Devs run `uc-dbt build` (see /usr/local/bin/uc-dbt) so dbt runs as THEM.

# Per-user UC identity helper importable from any notebook (on PYTHONPATH).
RUN mkdir -p /opt/nbtools /home/dev/notebooks
ENV PYTHONPATH=/opt/nbtools

WORKDIR /srv/jupyterhub
EXPOSE 8000

# jupyterhub_config.py + uc_notebook.py are mounted from the host (see
# docker-compose.yml) so they can be iterated without a rebuild.
CMD ["jupyterhub", "-f", "/srv/jupyterhub/jupyterhub_config.py"]
