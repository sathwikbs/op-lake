"""JupyterHub: per-user, SSO'd interactive dev on the human/BI plane.

Every user logs in through Keycloak (the same IdP as Superset and Unity Catalog).
The spawner injects the logged-in user's Keycloak tokens into their notebook, and
`uc_notebook.uc_session()` exchanges them for a Unity Catalog token so Spark
Connect runs as THAT user. There is no shared admin identity: UC enforces RBAC
per person, exactly like the BI plane.

Keycloak via the TLS gateway: the browser hits Keycloak at KEYCLOAK_BROWSER_BASE
(https://keycloak.localtest.me) for the interactive authorize step, while the hub
does the code->token and userinfo calls over the internal network (keycloak:8080).
With KC_HOSTNAME set, Keycloak stamps a SINGLE deterministic issuer
(https://keycloak.localtest.me), and UC fetches its JWKS from that one issuer over
TLS via the gateway (the keycloak.localtest.me docker alias). No split-horizon.

Local dev uses SimpleLocalProcessSpawner (one container, per-user processes);
the data-isolation guarantee comes from Unity Catalog, not the OS. In cloud this
swaps to KubeSpawner/DockerSpawner with the same auth_state hook.
"""
import os

from jupyterhub.spawner import SimpleLocalProcessSpawner
from oauthenticator.generic import GenericOAuthenticator

c = get_config()  # noqa: F821 (provided by jupyterhub at exec time)

realm = os.environ.get("KEYCLOAK_REALM", "datalake")
kc_browser = os.environ.get("KEYCLOAK_BROWSER_BASE", "https://keycloak.localtest.me")
kc_internal = os.environ.get("KEYCLOAK_INTERNAL_BASE", "http://keycloak:8080")
public_base = os.environ.get("JUPYTERHUB_PUBLIC_BASE", "http://localhost:8000")

# ---------------------------------------------------------------------------
# Hub / proxy
# ---------------------------------------------------------------------------
c.JupyterHub.bind_url = "http://0.0.0.0:8000"
c.JupyterHub.hub_ip = "0.0.0.0"
c.JupyterHub.cleanup_servers = True

# ---------------------------------------------------------------------------
# Authentication: Keycloak OIDC
# ---------------------------------------------------------------------------
c.JupyterHub.authenticator_class = GenericOAuthenticator
c.GenericOAuthenticator.client_id = os.environ["OIDC_CLIENT_ID"]
c.GenericOAuthenticator.client_secret = os.environ["OIDC_CLIENT_SECRET"]
c.GenericOAuthenticator.oauth_callback_url = f"{public_base}/hub/oauth_callback"
# Browser-facing authorize; backchannel token + userinfo (internal DNS).
c.GenericOAuthenticator.authorize_url = (
    f"{kc_browser}/realms/{realm}/protocol/openid-connect/auth"
)
c.GenericOAuthenticator.token_url = (
    f"{kc_internal}/realms/{realm}/protocol/openid-connect/token"
)
# The browser authorizes at the gateway issuer (https://keycloak.localtest.me)
# while the hub does the backchannel token call at keycloak:8080. To avoid a
# cross-host /userinfo call, we read identity claims straight from the id_token
# JWT (same approach Superset uses). auth_state still captures the access_token +
# refresh_token from the token response for the notebook.
c.GenericOAuthenticator.userdata_from_id_token = True
c.GenericOAuthenticator.scope = ["openid", "email", "profile"]
c.GenericOAuthenticator.username_claim = "preferred_username"
# Any authenticated realm user may log in; DATA access is enforced downstream by
# Unity Catalog per user (an analyst simply sees only what they're granted).
c.GenericOAuthenticator.allow_all = True
# Keep the user's tokens so we can hand them to the notebook (needs a crypt key).
c.GenericOAuthenticator.enable_auth_state = True

# ---------------------------------------------------------------------------
# Spawner: local per-user notebook servers
# ---------------------------------------------------------------------------
c.JupyterHub.spawner_class = SimpleLocalProcessSpawner
c.Spawner.default_url = "/lab"
# Per-user notebook root ({username} is templated by the spawner). This gives
# each user an ISOLATED, PERSISTED workspace even under SimpleLocalProcessSpawner
# (which otherwise runs everyone as one OS user in one dir). The parent is a host
# bind-mount, so per-user work survives logout/idle-cull and container restarts.
# (In cloud, KubeSpawner/DockerSpawner give the same via a per-user volume.)
NOTEBOOKS_ROOT = os.environ.get("NOTEBOOKS_ROOT", "/home/dev/notebooks")
c.Spawner.notebook_dir = NOTEBOOKS_ROOT + "/{username}"
# Hub runs as root in this image; allow the single-user server to as well.
c.Spawner.args = ["--allow-root"]
c.Spawner.start_timeout = 120
c.Spawner.http_timeout = 120

# Static environment every notebook needs to reach the compute edge + do the
# UC token exchange. Per-user Keycloak tokens are added in pre_spawn_hook.
c.Spawner.environment = {
    "SPARK_REMOTE": os.environ["SPARK_REMOTE"],
    "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH": os.environ.get(
        "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", "/certs/server.crt"
    ),
    "UC_CATALOG": os.environ.get("UC_CATALOG", "analytics"),
    "UC_EXCHANGE_URL": os.environ.get(
        "UC_EXCHANGE_URL",
        "http://unitycatalog:8080/api/1.0/unity-control/auth/tokens",
    ),
    "KC_INTERNAL_TOKEN_URL": (
        f"{kc_internal}/realms/{realm}/protocol/openid-connect/token"
    ),
    "OIDC_CLIENT_ID": os.environ["OIDC_CLIENT_ID"],
    "OIDC_CLIENT_SECRET": os.environ["OIDC_CLIENT_SECRET"],
    "PYTHONPATH": "/opt/nbtools",
    "PYTHONWARNINGS": "ignore",
}


def _ensure_user_workspace(username: str) -> None:
    """Create the user's isolated, persisted notebook dir and seed the starter
    notebooks into it on first login (copied from the shared bind-mount root)."""
    import glob
    import shutil

    user_dir = os.path.join(NOTEBOOKS_ROOT, username)
    os.makedirs(user_dir, exist_ok=True)
    # Seed starter notebooks only when the workspace is brand new, so we never
    # clobber a returning user's saved work.
    if not any(f.endswith(".ipynb") for f in os.listdir(user_dir)):
        for src in glob.glob(os.path.join(NOTEBOOKS_ROOT, "*.ipynb")):
            try:
                shutil.copy2(src, user_dir)
            except OSError:
                pass
    # Seed a per-user EDITABLE copy of the dbt project on first login so dev
    # happens in-place (each user edits + runs their own models). profiles.yml
    # ships inside it; `uc-dbt` supplies the per-user UC token at run time so
    # Unity Catalog enforces THAT user's grants.
    dbt_src = os.environ.get("DBT_PROJECT_TEMPLATE", "/opt/dbt-project")
    dbt_dst = os.path.join(user_dir, "dbt")
    if os.path.isdir(dbt_src) and not os.path.isdir(dbt_dst):
        try:
            shutil.copytree(
                dbt_src, dbt_dst,
                ignore=shutil.ignore_patterns("target", "dbt_packages", "logs", "*.duckdb"),
            )
        except OSError:
            pass


async def pre_spawn_hook(spawner):
    """Give the user an isolated workspace and inject their Keycloak tokens so
    uc_notebook.uc_session() can act as that user."""
    _ensure_user_workspace(spawner.user.name)
    # Point dbt at the user's own project copy (profiles.yml lives inside it).
    user_dbt = os.path.join(NOTEBOOKS_ROOT, spawner.user.name, "dbt")
    spawner.environment["DBT_PROJECT_DIR"] = user_dbt
    spawner.environment["DBT_PROFILES_DIR"] = user_dbt
    auth_state = await spawner.user.get_auth_state()
    if not auth_state:
        return
    spawner.environment["KC_ACCESS_TOKEN"] = auth_state.get("access_token", "")
    spawner.environment["KC_REFRESH_TOKEN"] = auth_state.get("refresh_token", "")
    spawner.environment["KC_USERNAME"] = spawner.user.name


c.Spawner.pre_spawn_hook = pre_spawn_hook
