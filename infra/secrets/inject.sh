#!/bin/sh
# ============================================================
# Vault-render CONSUMER shim (Phase 2 hardening).
#
# Prepended to a service's entrypoint. Sources the per-service secret env file
# that `vault-init` rendered into the shared `secrets_render` volume, then execs
# the real entrypoint. This is the "Vault Agent renders; app consumes env"
# pattern -- the service itself needs no Vault client, only its rendered file.
#
#   entrypoint: ["/bin/sh", "/secrets/inject.sh", <real-entrypoint>, <args...>]
#   environment: { SECRETS_FILE: <service>, NEEDS_SPARK_REMOTE: "1"? }
# ============================================================
set -u

SECRETS_DIR="${SECRETS_DIR:-/run/secrets-rendered}"
SVC="${SECRETS_FILE:?SECRETS_FILE must name this service's rendered env file}"
FILE="${SECRETS_DIR}/${SVC}.env"

# vault-init is a depends_on completed_successfully, but tolerate a brief race.
i=0
while [ ! -f "$FILE" ] && [ "$i" -lt 60 ]; do sleep 1; i=$((i + 1)); done

if [ -f "$FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$FILE"
  set +a
  echo "[inject] loaded rendered secrets for '${SVC}'"
else
  echo "[inject] WARNING: ${FILE} missing; starting without rendered secrets" >&2
fi

# Composite: Spark Connect remote embeds the (secret) pre-shared token. Build it
# from the token so the token never has to be baked into a compose string.
if [ "${NEEDS_SPARK_REMOTE:-0}" = "1" ] && [ -n "${SPARK_CONNECT_TOKEN:-}" ]; then
  export SPARK_REMOTE="sc://spark-connect:15003/;use_ssl=true;token=${SPARK_CONNECT_TOKEN}"
fi

# Phase 3: install an extra trusted CA (Caddy's local root) into a service's
# trust dir before it starts, so it trusts the https://keycloak.localtest.me
# issuer. Used by MinIO (its OIDC client fetches JWKS over TLS from the gateway).
# EXTRA_CA_SRC is populated by the caddy_data mount; wait briefly as Caddy mints
# its CA at first start.
if [ -n "${EXTRA_CA_SRC:-}" ] && [ -n "${EXTRA_CA_DEST:-}" ]; then
  j=0
  while [ ! -f "$EXTRA_CA_SRC" ] && [ "$j" -lt 60 ]; do sleep 1; j=$((j + 1)); done
  if [ -f "$EXTRA_CA_SRC" ]; then
    mkdir -p "$(dirname "$EXTRA_CA_DEST")"
    cp "$EXTRA_CA_SRC" "$EXTRA_CA_DEST"
    echo "[inject] installed extra CA -> ${EXTRA_CA_DEST}"
  else
    echo "[inject] WARNING: EXTRA_CA_SRC ${EXTRA_CA_SRC} not found; TLS to gateway may fail" >&2
  fi
fi

exec "$@"
