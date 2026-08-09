#!/usr/bin/env python3
"""IAM foundation check: Keycloak client-credentials -> UC token-exchange.

Proves the "one token" M2M path a service account (e.g. Dagster) will use:
  1. client-credentials grant at Keycloak  -> external access token (JWT)
  2. inspect iss / aud / email claims       -> must satisfy UC validation
  3. RFC 8693 token-exchange at UC          -> UC-issued access token
  4. call a UC API with the UC token         -> authorized per RBAC

Run INSIDE the compose network (so the token's issuer == keycloak:8080, which
is what UC validates and can fetch JWKS from).
"""
import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

KC = os.environ.get("KC_TOKEN_URL",
                    "http://keycloak:8080/realms/datalake/protocol/openid-connect/token")
UC = os.environ.get("UC_URI", "http://unitycatalog:8080")
CLIENT_ID = os.environ.get("SA_CLIENT_ID", "platform-cli")
CLIENT_SECRET = os.environ.get("SA_CLIENT_SECRET", "platform-cli-secret")


def _post(url, form):
    data = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _decode(jwt):
    payload = jwt.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def main():
    print(f"[1] client-credentials grant at Keycloak as '{CLIENT_ID}'")
    tok = _post(KC, {"grant_type": "client_credentials",
                     "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET})["access_token"]
    c = _decode(tok)
    print(f"    iss   = {c.get('iss')}")
    print(f"    aud   = {c.get('aud')}")
    print(f"    email = {c.get('email')}   azp = {c.get('azp')}")

    print("[2] RFC 8693 token-exchange at UC")
    try:
        ex = _post(f"{UC}/api/1.0/unity-control/auth/tokens", {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token": tok,
        })
    except urllib.error.HTTPError as e:
        print(f"    EXCHANGE FAILED: {e.code} {e.read().decode()}")
        sys.exit(1)
    uc_tok = ex["access_token"]
    print(f"    EXCHANGE OK -> UC token ({len(uc_tok)} chars)")

    print("[3] call UC API with the UC token (authorized per RBAC)")
    req = urllib.request.Request(f"{UC}/api/2.1/unity-catalog/catalogs",
                                 headers={"Authorization": f"Bearer {uc_tok}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        cats = json.load(r)
    names = [x.get("name") for x in cats.get("catalogs", [])]
    print(f"    catalogs visible to service account: {names}")
    print("IAM FOUNDATION: PASS")


if __name__ == "__main__":
    main()
