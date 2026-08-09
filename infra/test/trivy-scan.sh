#!/usr/bin/env bash
# ============================================================
# Opt-in image CVE scan (Phase 7 hardening).
#
# Scans every image the compose stack uses with Trivy, run from the official
# aquasec/trivy container so nothing needs installing on the host. This is NOT
# part of the smoke gate (network + time heavy); run it before a release or a
# pentest to triage HIGH/CRITICAL CVEs.
#
#   ./infra/test/trivy-scan.sh                 # HIGH,CRITICAL (default)
#   TRIVY_SEVERITY=CRITICAL ./infra/test/trivy-scan.sh
# ============================================================
set -uo pipefail

SEV="${TRIVY_SEVERITY:-HIGH,CRITICAL}"
CACHE="${PWD}/.trivy-cache"
mkdir -p "$CACHE"

images="$(docker compose config --images 2>/dev/null | sort -u)"
[ -z "$images" ] && { echo "no images found (run from repo root)"; exit 1; }

rc=0
for img in $images; do
  echo "############################################################"
  echo "# TRIVY: $img  (severity=$SEV)"
  echo "############################################################"
  docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$CACHE:/root/.cache" \
    aquasec/trivy:latest image \
      --scanners vuln --severity "$SEV" --no-progress --exit-code 0 "$img" || rc=1
done
exit $rc
