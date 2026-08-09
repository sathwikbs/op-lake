"""Per-user Unity Catalog identity for hosted notebooks.

Import this in any notebook cell:

    import uc_notebook
    spark = uc_notebook.uc_session()      # a Spark session that is *you*
    uc_notebook.whoami()                   # what can you see?

How it works (identical trust path to Superset, but for JupyterLab):
JupyterHub authenticates you against Keycloak and the spawner injects YOUR
Keycloak tokens into this notebook's environment (KC_ACCESS_TOKEN /
KC_REFRESH_TOKEN). This helper refreshes that token if needed, exchanges it for
a Unity Catalog token (RFC 8693), and binds it as the per-session catalog token
on a fresh Spark Connect session. Unity Catalog then enforces RBAC as YOU --
there is no ambient admin identity on the compute edge.
"""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

KC_TOKEN_URL = os.environ.get(
    "KC_INTERNAL_TOKEN_URL",
    "http://keycloak:8080/realms/datalake/protocol/openid-connect/token",
)
UC_EXCHANGE_URL = os.environ.get(
    "UC_EXCHANGE_URL", "http://unitycatalog:8080/api/1.0/unity-control/auth/tokens"
)
CATALOG = os.environ.get("UC_CATALOG", "analytics")
CLIENT_ID = os.environ.get("OIDC_CLIENT_ID", "jupyterhub")
CLIENT_SECRET = os.environ.get("OIDC_CLIENT_SECRET", "jupyterhub-secret")


def _post_form(url: str, form: dict) -> dict:
    data = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _kc_access_token() -> str:
    """Return a fresh Keycloak access token for the logged-in user.

    Access tokens are short-lived; if we were given a refresh token, use it so
    long notebook sessions keep working (the confidential client secret is
    required for the refresh grant)."""
    refresh = os.environ.get("KC_REFRESH_TOKEN")
    if refresh:
        try:
            form = {
                "grant_type": "refresh_token",
                "client_id": CLIENT_ID,
                "refresh_token": refresh,
            }
            if CLIENT_SECRET:  # confidential client (hosted hub); public CLI omits it
                form["client_secret"] = CLIENT_SECRET
            r = _post_form(KC_TOKEN_URL, form)
            os.environ["KC_ACCESS_TOKEN"] = r["access_token"]
            if r.get("refresh_token"):
                os.environ["KC_REFRESH_TOKEN"] = r["refresh_token"]
            return r["access_token"]
        except Exception:  # noqa: BLE001 - fall back to the injected token
            pass
    tok = os.environ.get("KC_ACCESS_TOKEN")
    if not tok:
        raise RuntimeError(
            "No Keycloak token in this notebook's environment. Are you running "
            "under JupyterHub SSO? (KC_ACCESS_TOKEN unset)"
        )
    return tok


def uc_token() -> str:
    """Exchange the user's Keycloak token for a Unity Catalog token."""
    kc = _kc_access_token()
    r = _post_form(UC_EXCHANGE_URL, {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token": kc,
    })
    tok = r.get("access_token")
    if not tok:
        raise RuntimeError(f"UC token exchange returned no access_token: {r}")
    return tok


def uc_session(app_name: str | None = None):
    """Return an isolated Spark Connect session bound to YOUR Unity Catalog token."""
    from pyspark.sql import SparkSession

    remote = os.environ["SPARK_REMOTE"]
    spark = SparkSession.builder.remote(remote).create()
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.token", uc_token())
    try:
        spark.catalog.setCurrentCatalog(CATALOG)
    except Exception:  # noqa: BLE001
        pass
    return spark


def whoami(spark=None):
    """Print the schemas you can see and a read probe per medallion layer."""
    spark = spark or uc_session()
    user = os.environ.get("KC_USERNAME", "?")
    schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
    print(f"[{user}] visible schemas in {CATALOG}: {schemas}")
    # Probe a real read per layer -- SHOW TABLES isn't gated the same way as data
    # access, so we actually read one table to reflect true SELECT permission.
    probe = {"bronze": "orders", "silver": "stg_orders", "gold": "customer_order_summary"}
    for layer, tbl in probe.items():
        try:
            spark.sql(f"SELECT count(*) FROM {CATALOG}.{layer}.{tbl}").collect()
            status = "readable"
        except Exception as e:  # noqa: BLE001
            status = "DENIED" if ("403" in str(e) or "PERMISSION" in str(e)) else "n/a"
        print(f"[{user}] read {layer:6s}: {status}")
    return spark
