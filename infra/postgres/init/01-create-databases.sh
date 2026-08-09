#!/bin/bash
# Creates the logical databases used by the platform:
#   - Unity Catalog metadata  (UC_DB)
#   - Dagster run/event/schedule storage (DAGSTER_DB)
#   - Superset metadata (SUPERSET_DB)
#   - Keycloak identity store (KEYCLOAK_DB) -- durable, survives container recreate
# Runs automatically on first Postgres startup.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    SELECT 'CREATE DATABASE ${UC_DB}'
      WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${UC_DB}')\gexec
    SELECT 'CREATE DATABASE ${DAGSTER_DB}'
      WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DAGSTER_DB}')\gexec
    SELECT 'CREATE DATABASE ${SUPERSET_DB}'
      WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${SUPERSET_DB}')\gexec
    SELECT 'CREATE DATABASE ${KEYCLOAK_DB}'
      WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${KEYCLOAK_DB}')\gexec
EOSQL

echo "Databases ${UC_DB}, ${DAGSTER_DB}, ${SUPERSET_DB} and ${KEYCLOAK_DB} are ready."
