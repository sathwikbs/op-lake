"""Superset configuration -- HUMAN-plane BI with Keycloak SSO.

Identity model (no service account behind dashboards):
  1. Users log into Superset via Keycloak SSO (AUTH_OAUTH).
  2. For the Spark Connect database (see register_spark.py + the
     spark_connect_uc engine-spec), Superset obtains each user's Keycloak token
     (SIP-85 OAuth2 + impersonate_user) and the engine-spec exchanges it for a
     Unity Catalog token set as the per-session Spark catalog token -- so UC
     enforces RBAC for THAT user. No shared service account behind dashboards.

Keycloak via the TLS gateway: the browser hits Keycloak at the public URL
(https://keycloak.localtest.me) for the interactive `authorize` step, while
Superset's backend does the code->token exchange over the internal network
(keycloak:8080). With KC_HOSTNAME set, Keycloak stamps a SINGLE deterministic
issuer (https://keycloak.localtest.me/realms/datalake). Unity Catalog trusts that
one issuer (server.allowed-issuers) and fetches its JWKS from it over TLS via the
gateway (the keycloak.localtest.me docker alias), so per-user token exchange
validates. No split-horizon / no JWKS bridge.
"""
import os

# Register our Spark Connect + Unity Catalog SQLAlchemy dialect (also declared as
# an entry point in the installed package; this makes it robust to either path).
from sqlalchemy.dialects import registry  # noqa: E402

registry.register(
    "spark_connect_uc", "spark_connect_uc.sqlalchemy_dialect", "SparkConnectUCDialect"
)

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Behind the Caddy TLS gateway: trust X-Forwarded-* so Superset builds HTTPS URLs
# (incl. its Keycloak OAuth redirect_uri). Without this it emits an http:// callback
# that doesn't match the registered https://superset.localtest.me/* -> Keycloak
# returns "Invalid parameter: redirect_uri".
ENABLE_PROXY_FIX = True
PREFERRED_URL_SCHEME = "https"

# --- Metadata DB in Postgres ---
_user = os.environ.get("POSTGRES_USER", "platform")
_pw = os.environ.get("POSTGRES_PASSWORD", "platform")
_host = os.environ.get("POSTGRES_HOST", "postgres")
_db = os.environ.get("SUPERSET_DB", "superset")
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{_user}:{_pw}@{_host}:5432/{_db}"

SQLALCHEMY_TRACK_MODIFICATIONS = False
WTF_CSRF_ENABLED = True
SUPERSET_WEBSERVER_TIMEOUT = 120

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    # SIP-85: let Superset drive per-database OAuth2 and forward tokens.
    "DATABASE_OAUTH2": True,
}

# ============================================================
# Keycloak SSO (Flask-AppBuilder OAuth)
# ============================================================
from flask_appbuilder.security.manager import AUTH_OAUTH  # noqa: E402

_realm = os.environ.get("KEYCLOAK_REALM", "datalake")
_kc_browser = os.environ.get("KEYCLOAK_BROWSER_BASE", "https://keycloak.localtest.me")
_kc_internal = os.environ.get("KEYCLOAK_INTERNAL_BASE", "http://keycloak:8080")
_client_id = os.environ.get("SUPERSET_OIDC_CLIENT_ID", "superset")
_client_secret = os.environ.get("SUPERSET_OIDC_CLIENT_SECRET", "superset-secret")

_authorize_url = f"{_kc_browser}/realms/{_realm}/protocol/openid-connect/auth"
_token_url = f"{_kc_internal}/realms/{_realm}/protocol/openid-connect/token"
_jwks_url = f"{_kc_internal}/realms/{_realm}/protocol/openid-connect/certs"
_api_base = f"{_kc_internal}/realms/{_realm}/protocol/openid-connect/"

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Gamma"  # fallback for principals with no persona
AUTH_ROLES_SYNC_AT_LOGIN = True

# Map Keycloak persona (realm) roles -> Superset app roles. This gates only
# UI/feature access (SQL Lab, dataset/chart authoring); the actual DATA access
# is enforced by Unity Catalog per-user at query time via the Spark Connect
# connector. Both personas get SQL Lab + broad datasource access at the app
# layer precisely because UC -- not Superset -- is the single data authority.
AUTH_ROLES_MAPPING = {
    "analyst": ["Alpha", "sql_lab"],
    "data-engineer": ["Alpha", "sql_lab"],
}

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        # SIP-85: persist the login token (per-user identity for the Spark DB).
        "save_token": True,
        "remote_app": {
            "client_id": _client_id,
            "client_secret": _client_secret,
            "client_kwargs": {"scope": "openid email profile"},
            "api_base_url": _api_base,
            "access_token_url": _token_url,
            "authorize_url": _authorize_url,
            "jwks_uri": _jwks_url,
            "request_token_url": None,
        },
    }
]


class CustomSsoSecurityManager:  # pragma: no cover - referenced below
    pass


# Map the Keycloak userinfo to a Superset user (identity == email == UC principal).
from superset.security import SupersetSecurityManager  # noqa: E402


class KeycloakSecurityManager(SupersetSecurityManager):
    def oauth_user_info(self, provider, response=None):
        if provider != "keycloak":
            return {}
        # Read identity claims from the id_token we already received over the
        # token endpoint (trusted backchannel). This avoids a second userinfo
        # round-trip, which was returning 401/empty here because the split
        # browser/internal hosts prevented Authlib from attaching the token.
        # Keycloak includes preferred_username/email/name in the id_token for
        # the openid+email+profile scopes we request.
        data = {}
        id_token = (response or {}).get("id_token")
        if id_token:
            import base64
            import json as _json

            payload = id_token.split(".")[1]
            payload += "=" * (-len(payload) % 4)
            data = _json.loads(base64.urlsafe_b64decode(payload))
        if not (data.get("preferred_username") or data.get("email")):
            # Fallback: userinfo endpoint with the explicit token.
            me = self.appbuilder.sm.oauth_remotes[provider].get(
                "userinfo", token=response
            )
            data = me.json()
        # Keycloak puts realm roles in the ACCESS token (not the id_token), so
        # decode it for AUTH_ROLES_MAPPING (persona -> Superset role) sync.
        role_keys = []
        access_token = (response or {}).get("access_token")
        if access_token:
            try:
                import base64
                import json as _json

                ap = access_token.split(".")[1]
                ap += "=" * (-len(ap) % 4)
                access_claims = _json.loads(base64.urlsafe_b64decode(ap))
                role_keys = access_claims.get("realm_access", {}).get("roles", [])
            except Exception:  # noqa: BLE001 - roles are best-effort
                role_keys = []
        return {
            "username": data.get("preferred_username") or data.get("email"),
            "email": data.get("email"),
            "first_name": data.get("given_name", ""),
            "last_name": data.get("family_name", ""),
            "role_keys": role_keys,
        }


CUSTOM_SECURITY_MANAGER = KeycloakSecurityManager

# ============================================================
# Per-user OAuth2 for the Spark Connect database (SIP-85)
# The base engine-spec looks up DATABASE_OAUTH2_CLIENTS by ENGINE_NAME, so the
# key MUST equal SparkConnectUCEngineSpec.engine_name. Superset obtains each
# user's Keycloak token via this client (transparent SSO dance) and hands it to
# the engine-spec, which exchanges it for a UC token -> per-user RBAC.
# ============================================================
DATABASE_OAUTH2_CLIENTS = {
    "Spark Connect (Unity Catalog)": {
        "id": _client_id,
        "secret": _client_secret,
        "scope": "openid email profile",
        "authorization_request_uri": _authorize_url,
        "token_request_uri": _token_url,
    }
}
DATABASE_OAUTH2_REDIRECT_URI = (
    os.environ.get("SUPERSET_PUBLIC_BASE", "http://localhost:8088")
    + "/api/v1/database/oauth2/"
)
