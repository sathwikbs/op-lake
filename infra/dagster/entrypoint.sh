#!/usr/bin/env bash
# Generates the dbt manifest (needed by dagster-dbt to build assets), then
# hands off to the requested Dagster command (webserver or daemon).
set -euo pipefail

echo "[dagster] Generating dbt manifest via 'dbt parse'..."
dbt parse \
  --project-dir "${DBT_PROJECT_DIR}" \
  --profiles-dir "${DBT_PROFILES_DIR}" \
  || echo "[dagster] WARNING: dbt parse failed; dbt assets may not load."

exec "$@"
