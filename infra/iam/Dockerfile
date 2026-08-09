# IAM reconciler -- governance-plane sidecar.
#
# Lives WITH Unity Catalog (same image, so it has bin/uc + the admin token), not
# in the orchestration plane. It reconciles Keycloak group membership into
# per-principal Unity Catalog grants (group emulation for UC OSS) on a schedule
# and/or on demand. Isolating the admin credential to this hardened, single-
# purpose container keeps least-privilege: Dagster never holds UC admin.
#
# Built FROM the UC image (Alpine) purely to inherit bin/uc + the Java runtime;
# we add python3 to run the reconciler scripts.
ARG UC_VERSION=v0.5.0
FROM unitycatalog/unitycatalog:${UC_VERSION}

USER root
# python3 to run the reconciler; py3-yaml so it can read personas.yaml.
RUN apk add --no-cache python3 py3-yaml

# bin/uc invokes `java` directly. The base image only puts Java on PATH via a
# login shell's profile, but the reconciler shells out with a non-login `sh -c`,
# so make java resolvable unconditionally (JAVA_HOME is set by the base image).
ENV JAVA_HOME=/usr/lib/jvm/default-jvm
ENV PATH="/usr/lib/jvm/default-jvm/bin:${PATH}"

# Scripts (sync_group_grants.py, provision_service_account.py, personas.yaml,
# state/) are mounted at /iam from the host so they can be iterated + so the
# reconciler's state file persists and is inspectable. See docker-compose.yml.
ENV UC_RECONCILER_INPLACE=1 \
    UC_HOME=/home/unitycatalog \
    PYTHONUNBUFFERED=1

WORKDIR /home/unitycatalog
ENTRYPOINT ["/bin/sh", "/iam/reconciler-entrypoint.sh"]
