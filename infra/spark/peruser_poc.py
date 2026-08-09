#!/usr/bin/env python3
"""LINCHPIN PoC: does a Spark Connect CLIENT session, given a per-user UC token,
get that user's RBAC enforced by Unity Catalog?

Runs inside a container that has pyspark[connect] + SPARK_REMOTE (e.g. dagster).
Flow: mint analyst Keycloak token -> exchange at UC for a per-user UC token ->
register a SECOND catalog handle ('asuser') bound to that token in the client
session -> query gold. If UC enforces per-user RBAC, an ungranted analyst is
DENIED on 'asuser' while the default 'analytics' (admin token) still works.
"""
import json, os, sys, urllib.parse, urllib.request
from pyspark.sql import SparkSession

KC = "http://keycloak:8080/realms/datalake/protocol/openid-connect/token"
UC_EXCHANGE = "http://unitycatalog:8080/api/1.0/unity-control/auth/tokens"
UC_URI = "http://unitycatalog:8080"


def _post(url, form):
    data = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def analyst_uc_token():
    kc = _post(KC, {"grant_type": "password", "client_id": "superset",
                    "client_secret": "superset-secret", "username": "analyst",
                    "password": "analyst", "scope": "openid"})["access_token"]
    return _post(UC_EXCHANGE, {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token": kc,
    })["access_token"]


def try_sql(spark, label, sql):
    print(f"\n=== {label}: {sql}")
    try:
        rows = spark.sql(sql).collect()
        print(f"  OK ({len(rows)} rows): {[r.asDict() for r in rows][:5]}")
        return True
    except Exception as e:
        print(f"  DENIED/ERROR: {str(e)[:900]}")
        return False


def main():
    role = sys.argv[1] if len(sys.argv) > 1 else "analyst"
    spark = SparkSession.builder.getOrCreate()

    if role == "analyst":
        uc = analyst_uc_token()
        print(f"[analyst] minted UC token (len={len(uc)}); overriding catalog token")
        spark.conf.set("spark.sql.catalog.analytics.token", uc)
    else:
        print("[admin] using server default catalog token")

    try_sql(spark, f"{role} list schemas", "SHOW SCHEMAS IN analytics")
    try_sql(spark, f"{role} read gold",
            "SELECT count(*) AS n FROM analytics.gold.customer_order_summary")


if __name__ == "__main__":
    main()
