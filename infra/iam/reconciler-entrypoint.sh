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

# Ensure Keycloak groups + realm-role attachments AND team launch roles/groups
# exist once, then loop on the grant reconcile (idempotent, state-file driven).
python3 /iam/sync_group_grants.py ensure-groups || \
  echo "[iam-reconciler] ensure-groups failed (will still reconcile)"
python3 /iam/sync_group_grants.py ensure-teams || \
  echo "[iam-reconciler] ensure-teams failed (will still reconcile)"
# Ensure the declared namespace (catalogs + schemas from personas.yaml) exists
# so USE-CATALOG/USE-SCHEMA grants always have a target. Idempotent.
python3 /iam/sync_group_grants.py bootstrap || \
  echo "[iam-reconciler] bootstrap failed (will still reconcile)"
# Provision declared service accounts (default automation SA + per-team SAs):
# Keycloak client + Vault secret + UC principal + persona grants. This also
# downgrades the default automation SA to least-privilege. Runs once (SA config
# changes are rare); re-run `ensure-sas` after editing personas.yaml.
python3 /iam/sync_group_grants.py ensure-sas || \
  echo "[iam-reconciler] ensure-sas failed (grants still reconcile per-human)"

while true; do
  echo "[iam-reconciler] $(date -u '+%Y-%m-%dT%H:%M:%SZ') reconcile"
  python3 /iam/sync_group_grants.py reconcile || \
    echo "[iam-reconciler] reconcile failed (will retry next interval)"
  sleep "${INTERVAL}"
done
