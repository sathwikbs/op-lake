#!/usr/bin/env python3
"""Identity-aware access probe: see EXACTLY what a given principal can do under
Unity Catalog RBAC, over the real Spark Connect edge.

It mints a token for the identity, exchanges it at Unity Catalog (RFC 8693) for a
per-user/SA UC token, opens an ISOLATED Spark Connect session bound to that token
(`spark.sql.catalog.<catalog>.token`), and probes each medallion schema. This is
the validation harness used at every phase gate of the platform build.

Two identity kinds:
  user  <username> <password>      # human, Keycloak direct-grant (local dev)
  sa    <client_id> <client_secret># service account, client-credentials (M2M)

Runs inside any container that has pyspark[connect] + SPARK_REMOTE (superset,
jupyter, dagster). Example:
  docker cp infra/iam/whoami.py dataplatform-superset-1:/tmp/whoami.py
  docker exec dataplatform-superset-1 python /tmp/whoami.py user analyst analyst

Env overrides: SPARK_REMOTE, KC_TOKEN_URL, UC_EXCHANGE_URL, UC_CATALOG,
OIDC_CLIENT_ID, OIDC_CLIENT_SECRET (client used for the user password grant).
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

CATALOG = os.environ.get("UC_CATALOG", "analytics")
KC_TOKEN_URL = os.environ.get(
    "KC_TOKEN_URL",
    "http://keycloak:8080/realms/datalake/protocol/openid-connect/token",
)
UC_EXCHANGE_URL = os.environ.get(
    "UC_EXCHANGE_URL", "http://unitycatalog:8080/api/1.0/unity-control/auth/tokens"
)
# Confidential client used to carry the user's password grant (has the audience
# + email mappers UC needs). The seeded 'superset' client works for local dev.
OIDC_CLIENT_ID = os.environ.get("OIDC_CLIENT_ID", "superset")
OIDC_CLIENT_SECRET = os.environ.get("OIDC_CLIENT_SECRET", "superset-secret")

# Table per schema used for the read probe (any granted table proves access).
PROBE_TABLES = {"bronze": "orders", "silver": "stg_orders", "gold": "customer_order_summary"}


def _post(url: str, form: dict) -> dict:
    data = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _kc_user_token(username: str, password: str) -> str:
    return _post(KC_TOKEN_URL, {
        "grant_type": "password", "client_id": OIDC_CLIENT_ID,
        "client_secret": OIDC_CLIENT_SECRET, "username": username,
        "password": password, "scope": "openid",
    })["access_token"]


def _kc_sa_token(client_id: str, client_secret: str) -> str:
    return _post(KC_TOKEN_URL, {
        "grant_type": "client_credentials", "client_id": client_id,
        "client_secret": client_secret, "scope": "openid",
    })["access_token"]


def _exchange_for_uc(kc_token: str) -> str:
    return _post(UC_EXCHANGE_URL, {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token": kc_token,
    })["access_token"]


def _probe(spark, sql: str) -> str:
    try:
        rows = spark.sql(sql).collect()
        return f"OK ({len(rows)} rows)"
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if "403" in msg or "PERMISSION_DENIED" in msg or "denied" in msg.lower():
            return "DENIED (403)"
        return f"ERR: {msg.splitlines()[0][:100]}"


def main() -> int:
    if len(sys.argv) < 4 or sys.argv[1] not in ("user", "sa"):
        print(__doc__)
        return 2
    kind, ident, secret = sys.argv[1], sys.argv[2], sys.argv[3]

    try:
        kc = _kc_user_token(ident, secret) if kind == "user" else _kc_sa_token(ident, secret)
        uc = _exchange_for_uc(kc)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:160]
        print(f"[{ident}] NO UC IDENTITY: auth/exchange failed HTTP {e.code}: {body}")
        return 0

    from pyspark.sql import SparkSession

    remote = os.environ["SPARK_REMOTE"]
    spark = SparkSession.builder.remote(remote).create()
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.token", uc)
    try:
        spark.catalog.setCurrentCatalog(CATALOG)
    except Exception:  # noqa: BLE001
        pass

    schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
    print(f"[{ident}] identity={kind}  visible schemas: {schemas}")
    for layer, tbl in PROBE_TABLES.items():
        print(f"[{ident}] read {layer:6s}: {_probe(spark, f'SELECT count(*) FROM {CATALOG}.{layer}.{tbl}')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
