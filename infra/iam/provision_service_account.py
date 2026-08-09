#!/usr/bin/env python3
"""Hierarchical IAM: provision a UC-governed service account (delegated admin).

Enforces the two-plane governance model:

  1. ENTITLEMENT (identity plane, Keycloak): the *requester* must hold the
     'sa-creator' realm role. Unentitled requesters are rejected before any
     change is made.
  2. IDENTITY CREATION (Keycloak Admin API): a confidential client with
     client-credentials (the passwordless service account) is created, with
     audience + email mappers so UC can validate and map the principal.
  3. AUTHORIZATION (data plane, Unity Catalog): the service account is
     registered as a UC principal and granted exactly the persona's template
     (see personas.yaml). UC remains the single authority for data access.
  4. AUDIT: every request (allow or deny) is appended to audit.log.

Run on the host (talks to Keycloak via the gateway https://keycloak.localtest.me
and applies UC grants via the
unitycatalog container). In cloud this same flow targets the managed IdP +
UC SDK; the broker uses a scoped 'manage-clients' service account instead of
the local Keycloak admin.

Usage:
  provision_service_account.py --requester-user engineer --requester-password engineer \
      --name orders-ingest --persona ingestion-bot
"""
import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
# IAM policy is authored in YAML (readable, inline comments). YAML is a superset
# of JSON so the in-memory structure is identical to the old personas.yaml.
import yaml  # noqa: E402  (py3-yaml is installed in the IAM image)
import iam_namespace  # noqa: E402  (generic catalog/schema topology engine)
PERSONAS = yaml.safe_load((HERE / "personas.yaml").read_text())
NS = iam_namespace.Namespace(PERSONAS)
AUDIT_LOG = HERE / "audit.log"

KC_BASE = os.environ.get("KC_BASE", "https://keycloak.localtest.me")
REALM = os.environ.get("KC_REALM", "datalake")
UC_CONTAINER = os.environ.get("UC_CONTAINER", "dataplatform-unitycatalog-1")
# Confidential client used for the requester's password-grant login.
LOGIN_CLIENT = os.environ.get("UC_OIDC_CLIENT_ID", "unitycatalog")
LOGIN_SECRET = os.environ.get("UC_OIDC_CLIENT_SECRET", "unitycatalog-secret")
KC_ADMIN = os.environ.get("KEYCLOAK_ADMIN", "admin")
KC_ADMIN_PW = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin")

# Internal issuer engines use at runtime (so UC can validate + fetch JWKS).
INTERNAL_TOKEN_URL = "http://keycloak:8080/realms/%s/protocol/openid-connect/token" % REALM


def audit(**kw):
    kw["ts"] = datetime.now(timezone.utc).isoformat()
    with AUDIT_LOG.open("a") as fh:
        fh.write(json.dumps(kw) + "\n")


def _post_form(url, form):
    data = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _decode_roles(jwt):
    import base64
    payload = jwt.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    claims = json.loads(base64.urlsafe_b64decode(payload))
    return set(claims.get("realm_access", {}).get("roles", []))


def _kc_admin_token():
    return _post_form(f"{KC_BASE}/realms/master/protocol/openid-connect/token", {
        "grant_type": "password", "client_id": "admin-cli",
        "username": KC_ADMIN, "password": KC_ADMIN_PW,
    })["access_token"]


def _kc_admin_call(method, path, token, body=None):
    url = f"{KC_BASE}/admin/realms/{REALM}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, (r.read().decode() or "")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def _uc(*args):
    """Run bin/uc with the UC admin token.

    Two execution modes so the same code serves host tooling AND the
    governance-plane reconciler container:
      * UC_RECONCILER_INPLACE=1 -> call bin/uc LOCALLY (we ARE co-located with UC,
        e.g. the iam-reconciler container that has bin/uc + the admin token.txt).
      * otherwise -> `docker exec` into the UC container (host usage, e.g. a dev
        running provision_service_account.py from their laptop).
    """
    quoted = " ".join('"%s"' % a for a in args)
    if os.environ.get("UC_RECONCILER_INPLACE") == "1":
        uc_home = os.environ.get("UC_HOME", "/home/unitycatalog")
        token_file = os.environ.get("UC_TOKEN_FILE", "etc/conf/token.txt")
        # We are NOT localhost to the UC server here (unlike a docker-exec into
        # the UC container), so point bin/uc at UC explicitly.
        server = os.environ.get("UC_URI", "http://unitycatalog:8080")
        inner = (f'bin/uc --server "{server}" '
                 f'--auth_token "$(cat {token_file})" {quoted}')
        return subprocess.run(["sh", "-c", inner], cwd=uc_home,
                              capture_output=True, text=True)
    inner = 'bin/uc --auth_token "$(cat etc/conf/token.txt)" ' + quoted
    cmd = ["docker", "exec", "-w", "/home/unitycatalog", UC_CONTAINER, "sh", "-c", inner]
    return subprocess.run(cmd, capture_output=True, text=True)


def provision(requester_user, requester_password, name, persona_name, catalog=None,
              secret=None):
    catalog = catalog or NS.primary_catalog
    persona = PERSONAS["personas"].get(persona_name)
    sa_client = f"sa-{name}"
    sa_email = f"{sa_client}@platform.local"

    # --- 1. Entitlement check (identity plane) ---
    try:
        tok = _post_form(f"{KC_BASE}/realms/{REALM}/protocol/openid-connect/token", {
            "grant_type": "password", "client_id": LOGIN_CLIENT, "client_secret": LOGIN_SECRET,
            "username": requester_user, "password": requester_password, "scope": "openid",
        })["access_token"]
    except urllib.error.HTTPError as e:
        print(f"DENY: requester login failed: {e.code} {e.read().decode()}")
        audit(action="provision", result="deny", reason="login_failed",
              requester=requester_user, sa=sa_client, persona=persona_name)
        return 2

    roles = _decode_roles(tok)
    if "sa-creator" not in roles:
        print(f"DENY: '{requester_user}' lacks the 'sa-creator' entitlement (roles={sorted(roles)}).")
        audit(action="provision", result="deny", reason="not_entitled",
              requester=requester_user, roles=sorted(roles), sa=sa_client, persona=persona_name)
        return 2
    if persona is None:
        print(f"DENY: unknown persona '{persona_name}'. Known: {list(PERSONAS['personas'])}")
        audit(action="provision", result="deny", reason="unknown_persona",
              requester=requester_user, sa=sa_client, persona=persona_name)
        return 2

    print(f"ALLOW: '{requester_user}' is entitled (sa-creator). Provisioning '{sa_client}' as '{persona_name}'.")

    # --- 2. Create the Keycloak service account (passwordless client-credentials) ---
    admin_tok = _kc_admin_token()
    # Deterministic secret (--secret) lets local dev pin the SA credential in
    # .env for reproducibility; otherwise generate a strong random one. In cloud
    # this is always random and stored in a secret manager / Vault.
    sa_secret = secret or secrets.token_urlsafe(24)
    rep = {
        "clientId": sa_client, "enabled": True, "protocol": "openid-connect",
        "publicClient": False, "serviceAccountsEnabled": True,
        "standardFlowEnabled": False, "directAccessGrantsEnabled": False,
        "secret": sa_secret,
        "attributes": {"persona": persona_name, "created_by": requester_user},
        "protocolMappers": [
            {"name": "aud-unitycatalog", "protocol": "openid-connect",
             "protocolMapper": "oidc-audience-mapper",
             "config": {"included.client.audience": "unitycatalog",
                        "id.token.claim": "false", "access.token.claim": "true"}},
            {"name": "email-claim", "protocol": "openid-connect",
             "protocolMapper": "oidc-hardcoded-claim-mapper",
             "config": {"claim.name": "email", "claim.value": sa_email,
                        "jsonType.label": "String", "id.token.claim": "false",
                        "access.token.claim": "true", "userinfo.token.claim": "false"}},
        ],
    }
    status, resp = _kc_admin_call("POST", "/clients", admin_tok, rep)
    if status == 201:
        print(f"  [keycloak] created service-account client '{sa_client}'")
    elif status == 409:
        print(f"  [keycloak] client '{sa_client}' already exists (reusing)")
    else:
        print(f"  [keycloak] FAILED to create client: {status} {resp}")
        audit(action="provision", result="error", reason="kc_create_failed",
              requester=requester_user, sa=sa_client, persona=persona_name, detail=resp)
        return 1

    # --- 3. Register the UC principal + 4. apply persona grants (data plane) ---
    _uc("user", "create", "--name", name, "--email", sa_email)  # idempotent-ish
    # Expand the persona's grant templates over the namespace model. If a
    # specific --catalog was requested, restrict to grants on that catalog.
    expanded = NS.expand(persona["grants"], scope=persona.get("scope"))
    if catalog and catalog != NS.primary_catalog:
        expanded = [g for g in expanded
                    if g["name"] == catalog or g["name"].split(".", 1)[0] == catalog]
    applied = []
    for g in expanded:
        r = _uc("permission", "create", "--securable_type", g["securable"],
                "--name", g["name"], "--privilege", g["privilege"], "--principal", sa_email)
        ok = r.returncode == 0 and "Exception" not in (r.stdout + r.stderr)
        applied.append({"securable": g["securable"], "name": g["name"],
                        "privilege": g["privilege"], "ok": ok})
    granted = [f"{a['privilege']} on {a['name']}" for a in applied if a["ok"]]
    print(f"  [uc] principal {sa_email} granted: {granted}")

    audit(action="provision", result="allow", requester=requester_user, sa=sa_client,
          email=sa_email, persona=persona_name, catalog=catalog, grants=applied)

    print("\nService account ready (passwordless, client-credentials):")
    print(f"  client_id     = {sa_client}")
    print(f"  client_secret = {sa_secret}")
    print(f"  uc_principal  = {sa_email}")
    print(f"  token_url     = {INTERNAL_TOKEN_URL}")
    return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--requester-user", required=True)
    p.add_argument("--requester-password", required=True)
    p.add_argument("--name", required=True, help="logical SA name (client becomes sa-<name>)")
    p.add_argument("--persona", required=True, choices=list(PERSONAS["personas"]))
    p.add_argument("--catalog", default=None)
    p.add_argument("--secret", default=None,
                   help="pin the SA client secret (local-dev reproducibility); random if omitted")
    a = p.parse_args()
    sys.exit(provision(a.requester_user, a.requester_password, a.name, a.persona,
                       a.catalog, a.secret))


if __name__ == "__main__":
    main()
