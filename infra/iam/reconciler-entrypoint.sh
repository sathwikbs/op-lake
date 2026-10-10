#!/bin/sh
# Governance-plane IAM reconciler loop.
#
# Periodically reconciles Keycloak group membership -> Unity Catalog per-principal
# grants (group emulation for UC OSS). Runs co-located with UC so it can call
# bin/uc directly with the admin token (UC_RECONCILER_INPLACE=1). For EVENT-based
# reconciliation (e.g. a Keycloak admin-event webhook or an operator action),
# trigger an immediate pass out-of-band:
#     docker exec dataplatform-iam-reconciler-1 python3 /iam/sync_group_grants.py reconcile
set -eu

cd "${UC_HOME:-/home/unitycatalog}"
INTERVAL="${RECONCILE_INTERVAL_SECONDS:-60}"
TOKEN_FILE="${UC_TOKEN_FILE:-etc/conf/token.txt}"

echo "[iam-reconciler] starting; interval=${INTERVAL}s, KC_BASE=${KC_BASE:-?}"

# Wait for UC's admin token.txt (shared uc_conf volume) before the first pass.
i=0
while [ ! -s "${TOKEN_FILE}" ] && [ "$i" -lt 60 ]; do
  echo "[iam-reconciler] waiting for ${TOKEN_FILE}..."
  sleep 2
  i=$((i + 1))
done

# One-time provisioning, all idempotent. Each step needs Keycloak and UC, which
# may still be starting when this container comes up, so the steps are retried
# every interval until all of them succeed (a single failed attempt used to
# leave the default automation SA un-downgraded until the next container start).
provision() {
  rc=0
  # Keycloak groups + realm-role attachments AND team launch roles/groups.
  python3 /iam/sync_group_grants.py ensure-groups || {
    echo "[iam-reconciler] ensure-groups failed"; rc=1; }
  python3 /iam/sync_group_grants.py ensure-teams || {
    echo "[iam-reconciler] ensure-teams failed"; rc=1; }
  # Ensure the declared namespace (catalogs + schemas from personas.yaml) exists
  # so USE-CATALOG/USE-SCHEMA grants always have a target.
  python3 /iam/sync_group_grants.py bootstrap || {
    echo "[iam-reconciler] bootstrap failed"; rc=1; }
  # Provision declared service accounts (default automation SA + per-team SAs):
  # Keycloak client + Vault secret + UC principal + persona grants. This also
  # downgrades the default automation SA to least-privilege. Re-run
  # `ensure-sas` after editing personas.yaml.
  python3 /iam/sync_group_grants.py ensure-sas || {
    echo "[iam-reconciler] ensure-sas failed"; rc=1; }
  return "$rc"
}

PROVISIONED=0
while true; do
  if [ "$PROVISIONED" = 0 ]; then
    if provision; then
      PROVISIONED=1
      echo "[iam-reconciler] provisioning complete"
    else
      echo "[iam-reconciler] provisioning incomplete; retrying in ${INTERVAL}s (grants still reconcile per-human)"
    fi
  fi
  echo "[iam-reconciler] $(date -u '+%Y-%m-%dT%H:%M:%SZ') reconcile"
  python3 /iam/sync_group_grants.py reconcile || \
    echo "[iam-reconciler] reconcile failed (will retry next interval)"
  sleep "${INTERVAL}"
done
