#!/usr/bin/env bash
# ============================================================
# Phase-gate smoke harness (HOST orchestrator).
#
# Re-run this after EVERY hardening phase. It:
#   1. copies + runs infra/test/smoke.py inside the Dagster container
#      (automation + governance: dlt->bronze, dbt silver/gold, RBAC, no bypass);
#   2. checks the browser UIs are reachable from the host;
#   3. asserts NO static object-store key leaked into the compute plane.
#
# Reachability URLs are overridable via env so the harness survives the Phase-3
# gateway cutover (set SUPERSET_URL=https://superset.platform.local, etc.).
#
# Usage:   ./infra/test/smoke.sh
# Exit code is non-zero if any check fails.
# ============================================================
set -uo pipefail

PROJECT="${COMPOSE_PROJECT_NAME:-dataplatform}"
DAGSTER_CTR="${DAGSTER_CTR:-${PROJECT}-dagster-webserver-1}"

# Compute-plane containers that MUST NOT carry a static object-store secret.
COMPUTE_CTRS=(
  "${PROJECT}-spark-1"
  "${PROJECT}-superset-1"
  "${PROJECT}-jupyterhub-1"
  "${PROJECT}-dagster-webserver-1"
  "${PROJECT}-dagster-daemon-1"
)

# Browser UIs (host side). Phase 3+: everything is behind the HTTPS gateway on
# *.localtest.me (which resolves to 127.0.0.1). `curl -k` tolerates the local CA.
LANDING_URL="${LANDING_URL:-https://home.localtest.me}"
SUPERSET_URL="${SUPERSET_URL:-https://superset.localtest.me/health}"
JUPYTER_URL="${JUPYTER_URL:-https://jupyter.localtest.me/hub/login}"
DAGSTER_URL="${DAGSTER_URL:-https://dagster.localtest.me}"
KEYCLOAK_URL="${KEYCLOAK_URL:-https://keycloak.localtest.me/realms/datalake/.well-known/openid-configuration}"

PASS=0
FAIL=0
pass() { echo "  [PASS] $1"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL + 1)); }

echo "############################################################"
echo "# SMOKE HARNESS  (project=${PROJECT})"
echo "############################################################"

# ---- 1. in-container automation + governance ----
echo "== running in-container checks (${DAGSTER_CTR}) =="
if docker cp "$(dirname "$0")/smoke.py" "${DAGSTER_CTR}:/tmp/smoke.py" >/dev/null 2>&1 &&
   docker exec "${DAGSTER_CTR}" python /tmp/smoke.py; then
  pass "in-container automation+governance"
else
  fail "in-container automation+governance"
fi

# ---- 2. browser UI reachability ----
echo "== browser UI reachability =="
reach() { # name url  (any HTTP response = up; connect failure = down)
  local name="$1" url="$2" code
  code="$(curl -sk -o /dev/null -w '%{http_code}' --max-time 10 "$url" 2>/dev/null)"
  if [ "$code" != "000" ] && [ -n "$code" ]; then pass "$name reachable (HTTP $code)"; else fail "$name unreachable"; fi
}
reach "landing"   "$LANDING_URL"
reach "superset"  "$SUPERSET_URL"
reach "jupyterhub" "$JUPYTER_URL"
reach "dagster (oauth2-proxy)" "$DAGSTER_URL"
reach "keycloak"  "$KEYCLOAK_URL"

# ---- 3. no static object-store key in the compute plane ----
echo "== no static key in compute plane =="
for ctr in "${COMPUTE_CTRS[@]}"; do
  if ! docker ps --format '{{.Names}}' | grep -q "^${ctr}$"; then
    echo "  [skip] ${ctr} not running"; continue
  fi
  leak="$(docker exec "$ctr" env 2>/dev/null \
    | grep -iE 'minioadmin|(AWS_SECRET|S3_SECRET|S3_ACCESS)[A-Z_]*=.+|MINIO_ROOT_PASSWORD=.+' || true)"
  if [ -z "$leak" ]; then pass "${ctr} has no static object-store key"; else fail "${ctr} leaks: ${leak}"; fi
done

echo "############################################################"
echo "# SMOKE SUMMARY: ${PASS} passed, ${FAIL} failed"
echo "############################################################"
[ "$FAIL" -eq 0 ]
