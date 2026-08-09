#!/bin/sh
#
# Admin-plane UC credential refresher (scheduled restart, self-minting).
#
# WHY THIS EXISTS
#   Unity Catalog v0.5.0 loads its object-store credential ONCE at boot and vends
#   that same MinIO STS session for the life of the process -- it does NOT
#   hot-reload server.properties, exposes no working runtime reload endpoint, and
#   refuses a permanent (session-token-less) key for vending. The only supported
#   way to install a fresh credential is to (re)start UC: our custom UC image runs
#   uc-entrypoint.sh on EVERY start and mints a brand-new bucket-scoped STS session
#   before exec'ing the server (mint-on-start).
#
#   Therefore a UC restart == a fresh credential. This sidecar simply triggers that
#   restart on a schedule COMFORTABLY INSIDE the STS TTL, so the vended session is
#   always young and can never expire under a running workload (the failure we hit
#   at the 7-day boundary). It is a fail-safe refresher, not a rotation daemon.
#
# ADMIN-ONLY / BLAST RADIUS
#   To restart a sibling container this holds the Docker socket, which is
#   root-equivalent on the host. Keep it a single-purpose, admin-plane container
#   (like iam-reconciler): nothing else runs here, no ports are exposed, and end
#   users never reach it. For a tighter prod posture put a docker-socket-proxy in
#   front and allow only POST /containers/*/restart (see README).
#
# BEHAVIOUR
#   Loop: sleep INTERVAL -> find UC by compose labels -> `docker restart` -> log.
#   The first restart happens one INTERVAL after boot (UC just minted a fresh
#   session at startup, so there is nothing to refresh yet).
#
set -eu

INTERVAL="${UC_REFRESH_INTERVAL_SECONDS:-518400}"   # default 6 days (< 7-day TTL)
TTL="${STS_DURATION_SECONDS:-604800}"               # informational: MinIO STS max
PROJECT="${COMPOSE_PROJECT:-dataplatform}"
SERVICE="${UC_SERVICE:-unitycatalog}"

log() { echo "[uc-cred-refresher] $(date -u +%FT%TZ) $*"; }

log "starting: interval=${INTERVAL}s ttl=${TTL}s project=${PROJECT} service=${SERVICE}"

# Guard-rail: refreshing must beat expiry. If mis-set, clamp to a safe margin.
if [ "${INTERVAL}" -ge "${TTL}" ]; then
  SAFE=$(( TTL - 86400 ))                            # one full day of margin
  [ "${SAFE}" -lt 3600 ] && SAFE=3600
  log "WARN: interval (${INTERVAL}s) >= STS TTL (${TTL}s); clamping to ${SAFE}s."
  INTERVAL="${SAFE}"
fi

find_uc() {
  docker ps -q \
    --filter "label=com.docker.compose.project=${PROJECT}" \
    --filter "label=com.docker.compose.service=${SERVICE}" | head -n1
}

# Verify we can actually talk to the daemon early (fail fast if socket missing).
if ! docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
  log "ERROR: cannot reach Docker daemon (is the socket mounted?). Exiting."
  exit 1
fi

while true; do
  sleep "${INTERVAL}"
  CID="$(find_uc || true)"
  if [ -z "${CID}" ]; then
    log "WARN: UC container not found by labels; will retry next cycle."
    continue
  fi
  log "restarting UC (${CID}) -> mint-on-start re-mints a fresh STS session."
  if docker restart "${CID}" >/dev/null 2>&1; then
    log "restart OK; UC came back with a fresh, full-TTL storage session."
  else
    log "ERROR: docker restart failed for ${CID}; will retry next cycle."
  fi
done
