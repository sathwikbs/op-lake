#!/usr/bin/env bash
# Initializes Superset (metadata migrations, admin user, roles) and registers
# the Spark Connect connection (per-user OAuth -> Unity Catalog), then serves.
set -e

echo "[superset] Running metadata DB migrations..."
superset db upgrade

echo "[superset] Creating local admin user (break-glass; humans log in via Keycloak SSO)..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN}" \
  --firstname Admin --lastname User \
  --email admin@platform.local \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true

echo "[superset] Initializing roles/permissions..."
superset init

echo "[superset] Registering Spark Connect database connection (per-user OAuth -> UC)..."
python /app/docker/register_spark.py || echo "[superset] WARNING: Spark registration failed."

echo "[superset] Starting web server on :8088"
exec gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 4 \
  --worker-class gthread \
  --threads 4 \
  --timeout 120 \
  "superset.app:create_app()"
