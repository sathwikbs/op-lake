#!/usr/bin/env bash
#
# Mint-on-start wrapper for Unity Catalog.
#
# Runs as root as the FIRST step of every UC (re)start:
#   1. (Re)seed server.properties + hibernate.properties from the mounted
#      template (the template is the source of truth for all NON-secret config:
#      issuers, audiences, endpoints, managed-table settings).
#   2. Mint a FRESH, bucket-scoped MinIO STS AssumeRole session and inject it
#      into s3.*.0. Because UC reads storage creds only at boot (no hot-reload),
#      minting here guarantees UC never starts with an EXPIRED session -- even
#      after a crash-triggered restart. On real cloud UC self-vends per-request
#      via its instance role; this is the local equivalent.
#   3. Fix perms so the non-root `unitycatalog` user can (re)create its
#      token.txt + signing keys in the shared conf volume.
#   4. Drop privileges and exec ./bin/start-uc-server as `unitycatalog`,
#      scrubbing the MinIO parent + AWS creds from the server's environment so
#      they live only in this wrapper, never in the long-running UC process.
#
set -euo pipefail

CONF_DIR="${UC_CONF_DIR:-/home/unitycatalog/etc/conf}"
TEMPLATE="${UC_TEMPLATE:-/template/server.properties}"
CONF="${CONF_DIR}/server.properties"

S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
BUCKET="${LAKEHOUSE_BUCKET:-lakehouse}"
REGION="${S3_REGION:-us-east-1}"
DURATION="${STS_DURATION_SECONDS:-604800}"

mkdir -p "${CONF_DIR}"

# The template is the source of truth for ALL non-secret settings. Re-seed every
# start so config edits take effect on restart; UC's own token.txt / signing
# keys in the same dir are left untouched (preserved across restarts).
echo "[uc-start] Seeding ${CONF} from template."
cp "${TEMPLATE}" "${CONF}"

# Vault-rendered secrets (Phase 2): source the MinIO parent credential (for STS
# mint-on-start) and the Keycloak OIDC client secret. These come from the shared
# secrets_render volume written by vault-init -- no secret is baked in the image.
RENDERED_ENV="${RENDERED_ENV:-/run/secrets-rendered/unitycatalog.env}"
if [ -f "${RENDERED_ENV}" ]; then
  echo "[uc-start] Sourcing Vault-rendered secrets from ${RENDERED_ENV}."
  # shellcheck disable=SC1090
  . "${RENDERED_ENV}"
fi
if [ -n "${UC_OIDC_CLIENT_SECRET:-}" ]; then
  sed -i "s|^server.client-secret=.*|server.client-secret=${UC_OIDC_CLIENT_SECRET}|" "${CONF}"
  echo "[uc-start] Injected OIDC client secret from Vault."
fi

# Phase 3: trust the Caddy gateway's LOCAL CA so UC can fetch JWKS over TLS from
# the single deterministic Keycloak issuer (https://keycloak.localtest.me). The
# root is written by Caddy into the shared caddy_data volume at startup.
CADDY_CA="${CADDY_CA:-/caddy-data/caddy/pki/authorities/local/root.crt}"
CACERTS="${JAVA_HOME:-/usr/lib/jvm/default-jvm}/lib/security/cacerts"
[ -f "${CACERTS}" ] || CACERTS="$(find / -name cacerts -path '*/security/*' 2>/dev/null | head -1)"
if [ -n "${CACERTS}" ]; then
  for _ in $(seq 1 30); do [ -f "${CADDY_CA}" ] && break; sleep 2; done
  if [ -f "${CADDY_CA}" ]; then
    keytool -importcert -noprompt -alias caddy-local-ca -file "${CADDY_CA}" \
      -keystore "${CACERTS}" -storepass changeit >/dev/null 2>&1 \
      && echo "[uc-start] Imported Caddy local CA into ${CACERTS}." \
      || echo "[uc-start] Caddy CA already trusted (or import skipped)."
  else
    echo "[uc-start] WARNING: Caddy CA not found at ${CADDY_CA}; JWKS over TLS may fail." >&2
  fi
fi
TEMPLATE_DIR="$(dirname "${TEMPLATE}")"
if [ -f "${TEMPLATE_DIR}/hibernate.properties" ]; then
  echo "[uc-start] Seeding hibernate.properties (Postgres metadata store)."
  cp "${TEMPLATE_DIR}/hibernate.properties" "${CONF_DIR}/hibernate.properties"
fi

# ---------------------------------------------------------------------------
# Storage credential the server will load at boot and VEND as-is (UC has no
# hot-reload, so whatever we install here is what it serves until next start).
# Two modes:
#
#   STATIC (default when UC_STORAGE_ACCESS_KEY is set): install a PERMANENT,
#     bucket-scoped MinIO service-account key with an EMPTY session token. It
#     never expires, so UC never needs a restart or rotation to keep creds
#     valid -- the failure-free posture. (On cloud, replace with real per-request
#     STS vended via UC's instance role.)
#
#   STS (fallback when UC_STORAGE_ACCESS_KEY is unset): mint a fresh, short-lived
#     bucket-scoped STS session at every start (mint-on-start). Self-heals across
#     restarts but the session expires after STS_DURATION_SECONDS, so a very-long
#     -lived process must eventually restart to re-mint.
# ---------------------------------------------------------------------------
if [ -n "${UC_STORAGE_ACCESS_KEY:-}" ] && [ -n "${UC_STORAGE_SECRET_KEY:-}" ]; then
  echo "[uc-start] STATIC storage mode: permanent bucket-scoped key (no STS, no expiry)."
  sed -i "s|^s3.accessKey.0=.*|s3.accessKey.0=${UC_STORAGE_ACCESS_KEY}|" "${CONF}"
  sed -i "s|^s3.secretKey.0=.*|s3.secretKey.0=${UC_STORAGE_SECRET_KEY}|" "${CONF}"
  # No session token -> a non-expiring credential. UC vends accessKey/secretKey
  # as-is; Spark's SimpleAWSCredentialsProvider uses them directly.
  sed -i "s|^s3.sessionToken.0=.*|s3.sessionToken.0=|"                   "${CONF}"

  echo "[uc-start] Validating permanent key can list s3://${BUCKET}..."
  AWS_ACCESS_KEY_ID="${UC_STORAGE_ACCESS_KEY}" \
  AWS_SECRET_ACCESS_KEY="${UC_STORAGE_SECRET_KEY}" \
  AWS_DEFAULT_REGION="${REGION}" \
    aws --endpoint-url "${S3_ENDPOINT}" s3 ls "s3://${BUCKET}" >/dev/null
  echo "[uc-start] Permanent key ${UC_STORAGE_ACCESS_KEY:0:4}... installed (no-expiry)."
else
  export AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER:?MINIO_ROOT_USER or UC_STORAGE_ACCESS_KEY required}"
  export AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD required}"
  export AWS_DEFAULT_REGION="${REGION}"

  echo "[uc-start] Waiting for MinIO at ${S3_ENDPOINT}..."
  for _ in $(seq 1 60); do
    if aws --endpoint-url "${S3_ENDPOINT}" s3 ls >/dev/null 2>&1; then break; fi
    sleep 2
  done

  # Inline session policy => temp creds can only touch the lakehouse bucket
  # (intersection with the parent user's policy). This is the storage-level RBAC.
  POLICY=$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::${BUCKET}","arn:aws:s3:::${BUCKET}/*"]}]}
JSON
)

  echo "[uc-start] STS mint-on-start: minting fresh bucket-scoped session (TTL=${DURATION}s)..."
  read -r AK SK ST < <(
    aws --endpoint-url "${S3_ENDPOINT}" sts assume-role \
      --role-arn "arn:aws:iam::minio:user/${MINIO_ROOT_USER}" \
      --role-session-name "uc-start-$(date +%s)" \
      --duration-seconds "${DURATION}" \
      --policy "${POLICY}" \
      --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
      --output text
  )

  if [ -z "${AK}" ] || [ -z "${SK}" ] || [ -z "${ST}" ]; then
    echo "[uc-start] ERROR: STS AssumeRole returned incomplete credentials." >&2
    exit 1
  fi

  echo "[uc-start] Injecting vended STS credentials into ${CONF}."
  # Use '|' delimiters; session tokens contain '/' and '+'.
  sed -i "s|^s3.accessKey.0=.*|s3.accessKey.0=${AK}|"       "${CONF}"
  sed -i "s|^s3.secretKey.0=.*|s3.secretKey.0=${SK}|"       "${CONF}"
  sed -i "s|^s3.sessionToken.0=.*|s3.sessionToken.0=${ST}|" "${CONF}"

  echo "[uc-start] Validating temp creds can list s3://${BUCKET}..."
  AWS_ACCESS_KEY_ID="${AK}" AWS_SECRET_ACCESS_KEY="${SK}" AWS_SESSION_TOKEN="${ST}" \
    aws --endpoint-url "${S3_ENDPOINT}" s3 ls "s3://${BUCKET}" >/dev/null
  echo "[uc-start] Fresh session ${AK:0:4}... valid ${DURATION}s."
fi

# UC runs as the non-root `unitycatalog` user and must be able to CREATE its
# signing keys (public_key.der) and admin token.txt in this shared conf dir.
chmod -R a+rwX "${CONF_DIR}" 2>/dev/null || true

echo "[uc-start] Starting UC server."
# Drop to the non-root server user AND scrub the parent MinIO / AWS creds so the
# long-running UC process never inherits them (they belong only to this wrapper).
exec env -u MINIO_ROOT_USER -u MINIO_ROOT_PASSWORD \
         -u UC_STORAGE_ACCESS_KEY -u UC_STORAGE_SECRET_KEY \
         -u UC_OIDC_CLIENT_SECRET \
         -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_DEFAULT_REGION \
     su-exec unitycatalog:unitycatalog ./bin/start-uc-server
