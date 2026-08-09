#!/bin/sh
# ============================================================
# vault-init: the ONLY Vault-aware bootstrap component (Phase 2 hardening).
#
#   1. init + unseal Vault (unseal key/root token persisted to /vault/keys for
#      local dev; on cloud this is replaced by KMS auto-unseal).
#   2. enable KV v2 at secret/ and GENERATE strong, unique secrets on first run
#      (idempotent: existing values in Vault are never regenerated -- Vault is
#      the source of truth).
#   3. RENDER per-service secret env files into the shared `secrets_render`
#      volume (consumed by each service's inject.sh shim), an oauth2-proxy
#      config file, and the Keycloak realm (client secrets substituted from
#      Vault) into the `kc_import` volume.
#   4. mint a scoped token for Dagster's runtime VaultSecretProvider.
#
# No real secret ever lives in a committed file: this script MINTS them and
# stores them only in Vault + the runtime-only render volume.
# ============================================================
set -u
export VAULT_ADDR="${VAULT_ADDR:-http://vault:8200}"

KEYS=/vault/keys/init.txt
RENDER=/run/secrets-rendered
KCT=/kc-template/realm-export.json
KCOUT=/kc-import/realm-export.json

mkdir -p "$RENDER" /vault/keys "$(dirname "$KCOUT")"
log() { echo "[vault-init] $*"; }

# ---- 1. wait for Vault, then init + unseal ----
i=0
while :; do
  vault status >/dev/null 2>&1 && break
  [ "$?" = "2" ] && break   # 2 = up but sealed
  i=$((i + 1)); [ "$i" -gt 60 ] && { log "vault unreachable"; exit 1; }
  sleep 1
done

if ! vault status 2>/dev/null | grep -q 'Initialized *true'; then
  log "initializing vault (1 key share, threshold 1 -- local dev)"
  vault operator init -key-shares=1 -key-threshold=1 >"$KEYS"
fi
UNSEAL=$(grep 'Unseal Key 1:' "$KEYS" | awk '{print $NF}')
ROOT=$(grep 'Initial Root Token:' "$KEYS" | awk '{print $NF}')

if vault status 2>/dev/null | grep -q 'Sealed *true'; then
  log "unsealing"
  vault operator unseal "$UNSEAL" >/dev/null
fi
export VAULT_TOKEN="$ROOT"

vault secrets enable -path=secret kv-v2 >/dev/null 2>&1 || true

# ---- 2. generate secrets (idempotent) ----
rand()    { head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | cut -c1-32; }
randb64() { head -c 32 /dev/urandom | base64 | tr -d '\n'; }
randhex() { od -An -tx1 -N 32 /dev/urandom | tr -dc 'a-f0-9'; }
# EXACTLY 32 alnum chars (= 32 bytes) -- oauth2-proxy's AES cookie key must be
# 16/24/32 bytes, so over-generate then trim to a guaranteed 32.
rand32()  { head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | cut -c1-32; }

ensure() { # ensure <path> <generator-fn>
  p="secret/$1"
  if ! vault kv get -field=value "$p" >/dev/null 2>&1; then
    v=$("$2")
    vault kv put "$p" value="$v" >/dev/null
    log "generated $1"
  fi
}
get() { vault kv get -field=value "secret/$1"; }

ensure platform/core/postgres            rand
ensure platform/core/keycloak_admin      rand
ensure platform/core/minio_root          rand
ensure platform/core/superset_secret_key randb64
ensure platform/core/superset_admin      rand
ensure platform/core/spark_connect_token rand
ensure platform/core/oauth2_cookie       rand32
ensure platform/core/jupyter_crypt       randhex
for c in unitycatalog superset jupyterhub dagster minio platform-cli; do
  ensure "platform/oidc/$c" rand
done
# Default automation SA (seeded so it exists at first boot). Team SAs
# (sa-team-<team>[-<role>]) are AUTO-PROVISIONED by the IAM reconciler from
# personas.yaml -- their secrets are minted into Vault at reconcile time.
ensure platform/sa/sa-platform-automation rand

# ---- 4. Dagster runtime read policy + token ----
cat >/tmp/dagster.hcl <<'EOF'
path "secret/data/platform/sa/*"           { capabilities = ["read"] }
path "secret/data/platform/oidc/superset"  { capabilities = ["read"] }
EOF
vault policy write dagster-read /tmp/dagster.hcl >/dev/null
DAGSTER_TOKEN=$(vault token create -policy=dagster-read -period=768h -field=token)

# ---- 4b. IAM reconciler policy + token (auto-provision team service accounts) ----
# The reconciler mints new SA secrets when a team declares a service_account in
# personas.yaml, so it needs create/read/update on the SA secret path (scoped to
# secret/platform/sa/* only -- it cannot read core/oidc secrets).
cat >/tmp/reconciler.hcl <<'EOF'
path "secret/data/platform/sa/*"     { capabilities = ["create", "read", "update", "delete"] }
path "secret/metadata/platform/sa/*" { capabilities = ["read", "list", "delete"] }
EOF
vault policy write reconciler-sa /tmp/reconciler.hcl >/dev/null
RECON_TOKEN=$(vault token create -policy=reconciler-sa -period=768h -field=token)

# ---- 3. render per-service secret env files ----
PG=$(get platform/core/postgres)
KCADM=$(get platform/core/keycloak_admin)
MINIO=$(get platform/core/minio_root)
SUPSK=$(get platform/core/superset_secret_key)
SUPADM=$(get platform/core/superset_admin)
SPARKTOK=$(get platform/core/spark_connect_token)
COOKIE=$(get platform/core/oauth2_cookie)
JCRYPT=$(get platform/core/jupyter_crypt)
UCS=$(get platform/oidc/unitycatalog)
SUPS=$(get platform/oidc/superset)
JUPS=$(get platform/oidc/jupyterhub)
DAGS=$(get platform/oidc/dagster)
MINS=$(get platform/oidc/minio)
SAAUTO=$(get platform/sa/sa-platform-automation)

umask 022

cat >"$RENDER/postgres.env" <<EOF
POSTGRES_PASSWORD=$PG
EOF

cat >"$RENDER/minio.env" <<EOF
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=$MINIO
MINIO_IDENTITY_OPENID_CLIENT_SECRET=$MINS
EOF

cat >"$RENDER/minio-init.env" <<EOF
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=$MINIO
EOF

cat >"$RENDER/keycloak.env" <<EOF
KC_BOOTSTRAP_ADMIN_PASSWORD=$KCADM
KC_DB_PASSWORD=$PG
EOF

cat >"$RENDER/unitycatalog.env" <<EOF
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=$MINIO
UC_OIDC_CLIENT_SECRET=$UCS
EOF

cat >"$RENDER/spark.env" <<EOF
SPARK_CONNECT_TOKEN=$SPARKTOK
EOF

# Dagster: DB + spark token via env; SA secrets come from Vault at RUNTIME via
# VaultSecretProvider (VAULT_ADDR/VAULT_TOKEN). SUPERSET_OIDC_CLIENT_SECRET is
# rendered too so the smoke harness can run the analyst password-grant probe.
cat >"$RENDER/dagster.env" <<EOF
DAGSTER_PG_PASSWORD=$PG
SPARK_CONNECT_TOKEN=$SPARKTOK
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=$DAGSTER_TOKEN
SUPERSET_OIDC_CLIENT_SECRET=$SUPS
EOF

cat >"$RENDER/superset.env" <<EOF
SUPERSET_SECRET_KEY=$SUPSK
SUPERSET_ADMIN_PASSWORD=$SUPADM
POSTGRES_PASSWORD=$PG
SUPERSET_OIDC_CLIENT_SECRET=$SUPS
SPARK_CONNECT_TOKEN=$SPARKTOK
EOF

cat >"$RENDER/jupyterhub.env" <<EOF
OIDC_CLIENT_SECRET=$JUPS
JUPYTERHUB_CRYPT_KEY=$JCRYPT
SPARK_CONNECT_TOKEN=$SPARKTOK
EOF

cat >"$RENDER/reconciler.env" <<EOF
KEYCLOAK_ADMIN_PASSWORD=$KCADM
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=$RECON_TOKEN
VAULT_KV_MOUNT=secret
VAULT_SA_PREFIX=platform/sa
EOF

# oauth2-proxy is distroless (no shell for a shim): render a config file it reads
# via --config, so its client + cookie secrets never sit on the command line.
cat >"$RENDER/oauth2-proxy.cfg" <<EOF
client_secret = "$DAGS"
cookie_secret = "$COOKIE"
EOF

# ---- 3b. render the Keycloak realm with Vault-sourced client secrets ----
if [ -f "$KCT" ]; then
  sed \
    -e "s|__RENDER_UC_OIDC_CLIENT_SECRET__|$UCS|g" \
    -e "s|__RENDER_SUPERSET_OIDC_CLIENT_SECRET__|$SUPS|g" \
    -e "s|__RENDER_JUPYTERHUB_OIDC_CLIENT_SECRET__|$JUPS|g" \
    -e "s|__RENDER_DAGSTER_OIDC_CLIENT_SECRET__|$DAGS|g" \
    -e "s|__RENDER_MINIO_OIDC_CLIENT_SECRET__|$MINS|g" \
    -e "s|__RENDER_PLATFORM_CLI_SECRET__|$(get platform/oidc/platform-cli)|g" \
    -e "s|__RENDER_SA_PLATFORM_AUTOMATION_SECRET__|$SAAUTO|g" \
    "$KCT" >"$KCOUT"
  log "rendered Keycloak realm -> $KCOUT"
fi

chmod -R a+rX "$RENDER" 2>/dev/null || true
log "done: secrets in Vault, per-service env + realm rendered."
