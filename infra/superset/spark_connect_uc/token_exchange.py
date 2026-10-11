"""Exchange a Keycloak (OIDC) access token for a Unity Catalog access token.

UC's Iceberg/native REST endpoints only accept UC-issued tokens, minted via an
RFC 8693 token exchange at ``/api/1.0/unity-control/auth/tokens``. UC validates
the subject token against its ``allowed-issuers``/``audiences`` before minting,
so the resulting token carries the *user's* identity and UC enforces their RBAC.

The exchange runs on the internal network (``UC_URI`` = http://unitycatalog:8080).
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
import time
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
        c = _jwt_claims(kc_access_token)
        claims = {"iss": c.get("iss"), "aud": c.get("aud"), "exp": c.get("exp")}
        raise RuntimeError(
            f"UC exchange HTTP {err.code}: {body} | subject-token {claims}"
        ) from err
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"UC token exchange returned no access_token: {payload}")
    return token


def _jwt_claims(token: str) -> dict:
    """Decode a JWT payload WITHOUT verifying it (Keycloak/UC already did); {} if not a JWT."""
    try:
        p = token.split(".")[1]
        p += "=" * (-len(p) % 4)
        return json.loads(base64.urlsafe_b64decode(p))
    except Exception:  # noqa: BLE001
        return {}


# One UC token per Keycloak access token. Every exchange mints a NEW UC token (fresh
# `jti`), so without this each query got a different token and per-user Spark
# sessions could never be reused. A cached UC token is handed out only while the
# Keycloak token it was minted from is still valid (minus a margin): identity
# freshness stays bounded by Keycloak, not by the UC token (which carries no `exp`).
_CACHE_MARGIN_SECONDS = 60
_CACHE_FALLBACK_TTL_SECONDS = 300      # Keycloak token without `exp` (not expected)
_CACHE_MAX_ENTRIES = 1024
_cache_lock = threading.Lock()
_uc_tokens: dict[str, tuple[str, float]] = {}   # sha256(kc token) -> (uc token, valid until)


def exchange_kc_for_uc_cached(kc_access_token: str, uc_uri: str | None = None) -> str:
    """Like ``exchange_kc_for_uc`` but reuses the UC token for the same Keycloak token."""
    key = hashlib.sha256(kc_access_token.encode()).hexdigest()
    now = time.time()
    with _cache_lock:
        hit = _uc_tokens.get(key)
        if hit and hit[1] > now:
            return hit[0]
    uc_token = exchange_kc_for_uc(kc_access_token, uc_uri)
    exp = _jwt_claims(kc_access_token).get("exp")
    valid_until = (exp - _CACHE_MARGIN_SECONDS) if exp else (now + _CACHE_FALLBACK_TTL_SECONDS)
    with _cache_lock:
        for k in [k for k, (_, until) in _uc_tokens.items() if until <= now]:
            del _uc_tokens[k]
        while len(_uc_tokens) >= _CACHE_MAX_ENTRIES:
            del _uc_tokens[min(_uc_tokens, key=lambda k: _uc_tokens[k][1])]
        if valid_until > now:
            _uc_tokens[key] = (uc_token, valid_until)
    return uc_token
