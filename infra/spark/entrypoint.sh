#!/usr/bin/env bash
#
# Renders spark-defaults.conf from the environment and launches the Spark
# Connect server in the foreground.
#
# ZERO AMBIENT IDENTITY (fail-closed): the compute edge deliberately holds NO
# Unity Catalog token by default. We do NOT inject the UC admin token here.
# Every session MUST present its own UC token in session conf
# (spark.sql.catalog.<catalog>.token):
#   * automation plane -> the team service-account token (Dagster/dbt),
#   * human/BI plane    -> the logged-in user's token (Superset/Jupyter).
# A session that presents no token authenticates as nobody and Unity Catalog
# denies it. This removes the old fail-OPEN behaviour where any unauthenticated
# session inherited full admin access.
set -euo pipefail

TEMPLATE="${SPARK_CONF_DIR}/spark-defaults.conf.template"
RENDERED="${SPARK_CONF_DIR}/spark-defaults.conf"

# Empty default token == no ambient identity. Kept as an env var so the template
# renders a well-formed (empty-valued) conf line.
UC_TOKEN=""
export UC_TOKEN
echo "[spark] Zero ambient identity: no default UC token. Sessions must present their own."

# Render config (envsubst replaces ${VAR} references).
envsubst < "${TEMPLATE}" > "${RENDERED}"
echo "[spark] Effective spark-defaults.conf (token redacted):"
grep -v -i token "${RENDERED}" || true

# Assemble the connector packages. Artifact names are Spark-version specific.
PKGS="io.delta:delta-spark_${SPARK_MAJOR_MINOR}_${SCALA_VERSION}:${DELTA_VERSION}"
PKGS="${PKGS},io.unitycatalog:unitycatalog-spark_${SPARK_MAJOR_MINOR}_${SCALA_VERSION}:${UC_SPARK_VERSION}"
PKGS="${PKGS},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}"
echo "[spark] Packages: ${PKGS}"

# SPARK_NO_DAEMONIZE keeps the server process in the foreground (PID 1-ish),
# so the container stays up and Docker can health-check the port.
# Spark 4.x bundles the Spark Connect server jar in the distribution.
export SPARK_NO_DAEMONIZE=true
exec "${SPARK_HOME}/sbin/start-connect-server.sh" \
  --packages "${PKGS}" \
  --conf "spark.jars.ivy=/opt/spark/.ivy2"
