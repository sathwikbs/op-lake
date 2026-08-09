# Spark compute node exposing a Spark Connect server (:15002),
# pre-wired with Delta Lake + the Unity Catalog connector + S3A (MinIO).
ARG SPARK_VERSION=4.1.0
FROM apache/spark:${SPARK_VERSION}

# Re-declare build args after FROM so they are visible in this stage.
ARG SPARK_MAJOR_MINOR=4.1
ARG SCALA_VERSION=2.13
ARG DELTA_VERSION=4.3.0
ARG UC_SPARK_VERSION=0.5.0
ARG HADOOP_AWS_VERSION=3.4.2

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends gettext-base curl \
 && rm -rf /var/lib/apt/lists/*

# Use a dedicated, writable conf dir so we can render spark-defaults at runtime.
ENV SPARK_HOME=/opt/spark
ENV SPARK_CONF_DIR=/opt/spark-conf
ENV HOME=/opt/spark
# Bake connector versions in for the entrypoint to assemble the --packages list.
ENV SPARK_MAJOR_MINOR=${SPARK_MAJOR_MINOR} \
    SCALA_VERSION=${SCALA_VERSION} \
    DELTA_VERSION=${DELTA_VERSION} \
    UC_SPARK_VERSION=${UC_SPARK_VERSION} \
    HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION}

COPY conf/spark-defaults.conf.template ${SPARK_CONF_DIR}/spark-defaults.conf.template
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh \
 && mkdir -p ${SPARK_CONF_DIR} /opt/spark/.ivy2 \
 && chown -R 185:185 ${SPARK_CONF_DIR} /opt/spark/.ivy2

# Pre-resolve the connector packages (Delta + Unity Catalog + hadoop-aws) at
# BUILD time so every transitive jar is baked into the image's ivy cache. At
# runtime the entrypoint's `--packages` resolves entirely OFFLINE from this
# cache -- no Maven download. This makes iteration fast (a `docker compose
# down -v` no longer triggers a multi-minute re-download: the named ivy_cache
# volume initialises from this baked dir) and is airgap-friendly for pentest.
RUN PKGS="io.delta:delta-spark_${SPARK_MAJOR_MINOR}_${SCALA_VERSION}:${DELTA_VERSION},io.unitycatalog:unitycatalog-spark_${SPARK_MAJOR_MINOR}_${SCALA_VERSION}:${UC_SPARK_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}" \
 && echo ':quit' | "${SPARK_HOME}/bin/spark-shell" \
      --packages "${PKGS}" \
      --conf spark.jars.ivy=/opt/spark/.ivy2 \
      --conf spark.ui.enabled=false \
 && chown -R 185:185 /opt/spark/.ivy2

USER 185
EXPOSE 15002 4040
ENTRYPOINT ["/opt/entrypoint.sh"]
