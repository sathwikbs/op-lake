#!/bin/sh
#
# Generate a self-signed TLS cert for the Spark Connect gRPC proxy (idempotent).
#
# WHY: OSS Spark Connect has no native TLS; it only offers a pre-shared bearer
# token (spark.connect.authenticate.token). But PySpark's ChannelBuilder refuses
# to send a token to a NON-localhost host over plaintext -- it forces SSL. So we
# terminate TLS at this nginx proxy and forward plaintext to spark:15002 on the
# trusted internal network. Clients trust this cert via GRPC_DEFAULT_SSL_ROOTS_FILE_PATH.
#
# The cert lives on a shared named volume so both the proxy (server side) and the
# client containers (as the trusted root) read the exact same material.
#
set -eu

CERT_DIR="${CERT_DIR:-/certs}"
if [ -s "${CERT_DIR}/server.crt" ] && [ -s "${CERT_DIR}/server.key" ]; then
  echo "[connect-tls] cert already present, reusing."
  exit 0
fi

echo "[connect-tls] installing openssl..."
apk add --no-cache openssl >/dev/null

echo "[connect-tls] generating self-signed cert (CN=spark-connect)..."
# SANs cover the in-cluster service name AND localhost/127.0.0.1 so the same cert
# works for host-based access if you choose to expose the proxy on the host.
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "${CERT_DIR}/server.key" \
  -out    "${CERT_DIR}/server.crt" \
  -days 3650 \
  -subj "/CN=spark-connect" \
  -addext "subjectAltName=DNS:spark-connect,DNS:localhost,IP:127.0.0.1"

# World-readable so non-root client processes can load it as their trust root.
chmod -R a+rX "${CERT_DIR}"
echo "[connect-tls] done."
