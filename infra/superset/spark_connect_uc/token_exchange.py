"""Exchange a Keycloak (OIDC) access token for a Unity Catalog access token.

UC's Iceberg/native REST endpoints only accept UC-issued tokens, minted via an
RFC 8693 token exchange at ``/api/1.0/unity-control/auth/tokens``. UC validates
the subject token against its ``allowed-issuers``/``audiences`` before minting,
so the resulting token carries the *user's* identity and UC enforces their RBAC.

The exchange runs on the internal network (``UC_URI`` = http://unitycatalog:8080).
"""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"
_ACCESS = "urn:ietf:params:oauth:token-type:access_token"


def uc_exchange_path(uc_uri: str) -> str:
    return uc_uri.rstrip("/") + "/api/1.0/unity-control/auth/tokens"


def exchange_kc_for_uc(kc_access_token: str, uc_uri: str | None = None, timeout: int = 30) -> str:
    """Return a UC access token minted for the identity in ``kc_access_token``.

    Raises on failure so callers can decide to trigger an OAuth2 re-auth or
    surface a permission error -- we never silently fall back to a shared token.
    """
    uc_uri = uc_uri or os.environ.get("UC_URI", "http://unitycatalog:8080")
    data = urllib.parse.urlencode({
        "grant_type": _GRANT,
        "requested_token_type": _ACCESS,
        "subject_token_type": _ACCESS,
        "subject_token": kc_access_token,
    }).encode()
    req = urllib.request.Request(
        uc_exchange_path(uc_uri),
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.load(resp)
    except urllib.error.HTTPError as err:  # surface WHY UC rejected the token
        body = err.read().decode("utf-8", "replace")[:500]
        claims = {}
        try:
            import base64

            p = kc_access_token.split(".")[1]
            p += "=" * (-len(p) % 4)
            c = json.loads(base64.urlsafe_b64decode(p))
            claims = {"iss": c.get("iss"), "aud": c.get("aud"), "exp": c.get("exp")}
        except Exception:  # noqa: BLE001
            pass
        raise RuntimeError(
            f"UC exchange HTTP {err.code}: {body} | subject-token {claims}"
        ) from err
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"UC token exchange returned no access_token: {payload}")
    return token
