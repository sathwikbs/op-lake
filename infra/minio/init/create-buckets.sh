#!/bin/sh
# Creates the lakehouse bucket and uploads the sample raw CSVs.
# Idempotent: safe to re-run.
set -eu

echo "Waiting for MinIO to accept connections..."
for _ in $(seq 1 60); do
  if mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; then
    echo "MinIO is up."
    break
  fi
  sleep 2
done
# Fail loudly if we still cannot reach MinIO.
mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

echo "Creating bucket: ${LAKEHOUSE_BUCKET}"
mc mb --ignore-existing "local/${LAKEHOUSE_BUCKET}"

# ---------------------------------------------------------------------------
# Permanent, bucket-scoped storage credential for Unity Catalog.
#
# UC loads its storage credential ONCE at boot and vends it as-is (no
# hot-reload). Handing it a NON-EXPIRING, bucket-scoped MinIO service account
# means UC never needs a restart or rotation to keep storage creds valid --
# while still confining that credential to the lakehouse bucket. UC's own RBAC
# still gates who may obtain it via the credential-vending API. (On real cloud
# this is replaced by per-request STS via UC's instance role.)
# ---------------------------------------------------------------------------
if [ -n "${UC_STORAGE_ACCESS_KEY:-}" ] && [ -n "${UC_STORAGE_SECRET_KEY:-}" ]; then
  echo "Ensuring UC storage service account (bucket-scoped, no-expiry)..."
  cat > /tmp/uc-storage-policy.json <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::${LAKEHOUSE_BUCKET}","arn:aws:s3:::${LAKEHOUSE_BUCKET}/*"]}]}
JSON
  if mc admin user svcacct info local "${UC_STORAGE_ACCESS_KEY}" >/dev/null 2>&1; then
    echo "  svcacct ${UC_STORAGE_ACCESS_KEY} already exists; re-applying scoped policy."
    mc admin user svcacct edit --policy /tmp/uc-storage-policy.json local "${UC_STORAGE_ACCESS_KEY}" || true
  else
    mc admin user svcacct add \
      --access-key "${UC_STORAGE_ACCESS_KEY}" \
      --secret-key "${UC_STORAGE_SECRET_KEY}" \
      --policy /tmp/uc-storage-policy.json \
      local "${MINIO_ROOT_USER}"
  fi
fi

# ---------------------------------------------------------------------------
# MinIO is ADMIN-ONLY (Keycloak-governed console access).
#
# Design decision: teams/users do NOT get direct MinIO access. All team data
# access flows through Unity Catalog (Superset / Jupyter / dbt), which vends
# short-lived, path-scoped credentials to the engine -- so there is no parallel
# Keycloak->MinIO STS channel to govern. The only MinIO policy we create is
# `platform-admin` (full data-plane on the bucket), attached to a platform
# admin's console session via the Keycloak `minio_policy` claim. The old
# per-team STS policies + the dead minio_sts.py helper were removed: MinIO is a
# break-glass admin tool, not a team data path. (Network segmentation also keeps
# MinIO unreachable from the app tier; only the data tier + the admin console
# route touch it.)
# ---------------------------------------------------------------------------
echo "Creating admin-only MinIO policy (Keycloak minio_policy claim -> policy)..."

# platform-admin: full DATA-plane on the bucket (no MinIO admin actions).
cat > /tmp/pol-platform-admin.json <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],
  "Resource":["arn:aws:s3:::${LAKEHOUSE_BUCKET}","arn:aws:s3:::${LAKEHOUSE_BUCKET}/*"]}]}
JSON
mc admin policy create local platform-admin /tmp/pol-platform-admin.json 2>/dev/null || \
  mc admin policy create local platform-admin /tmp/pol-platform-admin.json || true

# Remove any LEGACY per-team MinIO policies from earlier builds (MinIO is now
# admin-only; team data access flows through Unity Catalog, not direct MinIO).
mc admin policy rm local team-analytics 2>/dev/null || true

# NOTE: no Spark "staging" service account is created anymore. Spark reaches
# object storage ONLY through UC credential vending (credScopedFs) -- there is no
# static object-store key in the compute plane. Ingestion writes UC-managed
# tables via Spark, so no direct-MinIO key is needed.

# Layout inside the bucket:
#   raw/     -> landed source files (ingested into bronze by Dagster)
#   bronze/  -> bronze Delta tables
#   silver/  -> silver Delta tables (dbt)
#   gold/    -> gold Delta tables (dbt)
echo "Uploading sample raw data..."
mc cp /data/orders.csv    "local/${LAKEHOUSE_BUCKET}/raw/orders.csv"
mc cp /data/customers.csv "local/${LAKEHOUSE_BUCKET}/raw/customers.csv"

echo "MinIO storage initialized."
mc ls --recursive "local/${LAKEHOUSE_BUCKET}"
