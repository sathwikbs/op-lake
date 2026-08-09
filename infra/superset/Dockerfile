# Superset image for the HUMAN plane (per-user Unity Catalog RBAC over Spark):
#   - pyspark[connect]   -> pure-Python Spark Connect client (SAME version as the
#                           Spark server or the gRPC handshake fails)
#   - spark-connect-uc   -> our SQLAlchemy dialect + Superset engine-spec that
#                           inject a per-user UC token (installed for entry points)
#   - Authlib            -> Keycloak OAuth SSO + per-user token dance
#   - psycopg2-binary    -> Postgres metadata database driver
ARG SUPERSET_VERSION=4.1.1
FROM apache/superset:${SUPERSET_VERSION}

# MUST match the Spark server (SPARK_VERSION); mismatched Connect clients fail.
ARG PYSPARK_VERSION=4.1.0

USER root

# Install the per-user Spark Connect connector (registers entry points for the
# SQLAlchemy dialect + Superset engine-spec). Source is also bind-mounted over
# /app/pythonpath for live edits; entry-point metadata comes from this install.
WORKDIR /opt/spark_connect_uc
COPY pyproject.toml ./
COPY spark_connect_uc ./spark_connect_uc
# pyspark[connect] 4.1 requires pandas>=2.2 / pyarrow>=15, above Superset 4.1.1's
# declared caps (pandas<2.1, pyarrow<15). We let pandas/pyarrow float UP to what
# pyspark needs and verify Superset still runs (its cap is conservative, not hard).
# cryptography is the exception: we pin <43 because cryptography 50 breaks
# pyopenssl/TLS in the image (a real runtime failure, not just a version warning).
RUN pip install --no-cache-dir \
      "cryptography>=42.0.4,<43" \
      "pyspark[connect]==${PYSPARK_VERSION}" \
      grpcio grpcio-status \
      Authlib \
      psycopg2-binary \
      .

WORKDIR /app
USER superset
