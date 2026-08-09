"""Automation identity for the orchestration plane.

Dagster runs must NOT use Unity Catalog admin. Instead each run authenticates as
a *team service account* (passwordless, client-credentials) and lets Unity
Catalog enforce that SA's grants. This module mints the SA's UC token and is used
to bind it as the per-session catalog token for both PySpark assets and dbt.

Flow (same RFC 8693 exchange the human plane uses, but M2M):
    Keycloak client-credentials grant  ->  UC token exchange  ->  UC token
which is set as `spark.sql.catalog.<catalog>.token` on the Spark Connect session.

Secret handling goes through `SecretProvider`, a thin seam: a local env-backed
stub today, swappable to Vault / cloud secret manager / Workload Identity
Federation in production without touching call sites.
"""
from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request

# Internal (backchannel) endpoints -- reachable from the Dagster containers.
KC_TOKEN_URL = os.environ.get(
    "KC_TOKEN_URL",
    "http://keycloak:8080/realms/datalake/protocol/openid-connect/token",
)
UC_EXCHANGE_URL = os.environ.get(
    "UC_EXCHANGE_URL", "http://unitycatalog:8080/api/1.0/unity-control/auth/tokens"
)
CATALOG = os.environ.get("UC_CATALOG", "analytics")


class SecretProvider:
    """Resolve a service-account client secret by logical name.

    Local stub: reads `UC_SA_SECRET__<UPPER_NAME>` or falls back to a shared
    `UC_AUTOMATION_SA_SECRET`. In cloud, replace this class with a Vault / cloud
    secret-manager client (same `get_secret` contract) or drop it entirely in
    favour of Workload Identity Federation (no static secret at all).
    """

    def get_secret(self, sa_client_id: str) -> str:
        key = "UC_SA_SECRET__" + sa_client_id.replace("-", "_").upper()
        val = os.environ.get(key) or os.environ.get("UC_AUTOMATION_SA_SECRET")
        if not val:
            raise RuntimeError(
                f"No secret for service account '{sa_client_id}' "
                f"(looked up {key} / UC_AUTOMATION_SA_SECRET)"
            )
        return val


class VaultSecretProvider(SecretProvider):
    """Fetch SA client secrets from HashiCorp Vault (KV v2) at runtime.

    This is the production-grade provider (Phase 2 hardening): no static SA
    secret sits in the process environment; each secret is read on demand from
    Vault at `secret/data/<prefix>/<sa_client_id>` (field `value`) using a
    scoped, read-only token. In cloud, swap the token for AppRole / Kubernetes
    auth / Workload Identity Federation without changing call sites.
    """

    def __init__(self) -> None:
        self.addr = os.environ.get("VAULT_ADDR", "").rstrip("/")
        self.token = os.environ.get("VAULT_TOKEN", "")
        self.mount = os.environ.get("VAULT_KV_MOUNT", "secret")
        self.prefix = os.environ.get("VAULT_SA_PREFIX", "platform/sa")

    def _read(self, path: str) -> str:
        url = f"{self.addr}/v1/{self.mount}/data/{path}"
        req = urllib.request.Request(url, headers={"X-Vault-Token": self.token})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.load(r)
        return data["data"]["data"]["value"]

    def get_secret(self, sa_client_id: str) -> str:
        try:
            return self._read(f"{self.prefix}/{sa_client_id}")
        except Exception as e:  # noqa: BLE001 - fall back to env for local/dev
            fallback = SecretProvider()
            try:
                return fallback.get_secret(sa_client_id)
            except Exception:
                raise RuntimeError(
                    f"Vault read for service account '{sa_client_id}' failed "
                    f"({e}); no env fallback available."
                ) from e


def default_secret_provider() -> SecretProvider:
    """VaultSecretProvider when Vault is wired (VAULT_ADDR set), else the local
    env-backed stub. Lets the same code run locally and against Vault."""
    if os.environ.get("VAULT_ADDR") and os.environ.get("VAULT_TOKEN"):
        return VaultSecretProvider()
    return SecretProvider()


def _post_form(url: str, form: dict) -> dict:
    data = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


# Simple in-process token cache keyed by client_id: (token, expires_at).
_CACHE: dict[str, tuple[str, float]] = {}


def uc_token_for_sa(sa_client_id: str, secret_provider: SecretProvider | None = None) -> str:
    """Return a UC token for the given service account (cached until ~expiry)."""
    now = time.time()
    cached = _CACHE.get(sa_client_id)
    if cached and cached[1] - 30 > now:
        return cached[0]

    secret_provider = secret_provider or default_secret_provider()
    secret = secret_provider.get_secret(sa_client_id)

    kc = _post_form(KC_TOKEN_URL, {
        "grant_type": "client_credentials",
        "client_id": sa_client_id,
        "client_secret": secret,
        "scope": "openid",
    })
    kc_token = kc["access_token"]

    uc = _post_form(UC_EXCHANGE_URL, {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token": kc_token,
    })
    uc_token = uc["access_token"]
    # UC tokens are short-lived; cache conservatively off the KC expiry.
    ttl = float(kc.get("expires_in", 300))
    _CACHE[sa_client_id] = (uc_token, now + ttl)
    return uc_token


def automation_sa_client_id() -> str | None:
    """The default automation SA for the platform (Phase 1). Team-scoped SAs
    (Phase 6) override this per job via tags."""
    return os.environ.get("UC_AUTOMATION_SA_CLIENT_ID")


def team_sa_client_id(team: str, role: str | None = None) -> str:
    """Service-account client_id for a team, by CONVENTION:
      * single SA        -> `sa-team-<team>`
      * per-function SA  -> `sa-team-<team>-<role>`   (e.g. ...-ingest / ...-build)

    Matched exactly by the IAM reconciler, which auto-provisions each declared SA
    (Keycloak client + Vault secret + UC grants) from personas.yaml
    `teams.<team>.service_account(s)`. No env map, no per-team config."""
    return f"sa-team-{team}-{role}" if role else f"sa-team-{team}"


def _sa_secret_exists(client_id: str, secret_provider: SecretProvider) -> bool:
    """True if a secret for `client_id` is resolvable (i.e. the SA is
    provisioned). Used to decide whether a per-function role SA exists before
    falling back to the team's single SA."""
    try:
        secret_provider.get_secret(client_id)
        return True
    except Exception:  # noqa: BLE001
        return False


def sa_for_team(team: str | None, role: str | None = None,
                secret_provider: SecretProvider | None = None) -> str | None:
    """Resolve a team (+ optional function role) to its SA client_id.

    A per-function role SA (`sa-team-<team>-<role>`) is used when it is
    provisioned (its secret exists in the store); otherwise this gracefully falls
    back to the team's single SA (`sa-team-<team>`), so both the multi-SA and the
    single-SA team layouts work with the same generic assets. Untagged run ->
    the least-privilege default automation SA."""
    if not team:
        return automation_sa_client_id()
    base = team_sa_client_id(team)
    if not role:
        return base
    candidate = team_sa_client_id(team, role)
    sp = secret_provider or default_secret_provider()
    return candidate if _sa_secret_exists(candidate, sp) else base


# ---------------------------------------------------------------------------
# Team-launch authorization (fail-closed).
#
# Dagster OSS has no per-job UI authorization, so we enforce team scoping at the
# point where a run acquires its identity. Rules:
#   * The `team` run/op tag names the team whose SA (and data) the run will use.
#   * A HUMAN launch via the UI is stamped SERVER-SIDE by the identity-injector
#     middleware (infra/dagster/serve.py) with `launched_by` + `launched_by_groups`
#     read from the oauth2-proxy `X-Auth-Request-*` headers -- the user cannot set
#     or forge these.
#   * To launch a team's pipeline the launcher must hold that team's role
#     (`team-<team>`); `platform-admin` may launch any team.
#   * A launch WITHOUT `launched_by` is a trusted backend launch (schedule,
#     daemon, or CLI, which never traverse the authenticated UI edge) and is
#     allowed -- automation is the platform's own identity.
# ---------------------------------------------------------------------------

PLATFORM_ADMIN_ROLE = "platform-admin"


def team_required_role(team: str) -> str:
    """The realm role a human must hold to launch `team`'s pipelines."""
    return f"team-{team}"


class TeamAuthorizationError(RuntimeError):
    """Raised (fail-closed) when a human launches a team job they may not run."""


def _run_tag(context, key: str):
    try:
        val = (context.run.tags or {}).get(key)
        if val:
            return val
    except Exception:  # noqa: BLE001 - not all contexts expose run tags
        pass
    try:
        return (context.op_def.tags or {}).get(key)
    except Exception:  # noqa: BLE001
        return None


def authorize_team_run(context) -> None:
    """Fail closed if the human launcher isn't authorized for the job's team."""
    team = _run_tag(context, "team")

    launched_by = _run_tag(context, "launched_by")
    if not launched_by:
        # No authenticated launcher on the run -> trusted backend (schedule /
        # daemon / CLI). Automation identity is governed by the SA, not this gate.
        return

    raw_groups = _run_tag(context, "launched_by_groups") or ""
    groups = {g.strip() for g in raw_groups.split(",") if g.strip()}

    if PLATFORM_ADMIN_ROLE in groups:
        return  # platform admins may launch any team's pipelines
    if not team:
        # Untagged job: launcher already passed the persona gate (data-engineer /
        # platform-admin) at the UI edge; no team scope to enforce.
        return

    required = team_required_role(team)
    if required not in groups:
        raise TeamAuthorizationError(
            f"'{launched_by}' is not authorized to launch team '{team}' pipelines "
            f"(missing role '{required}'; has {sorted(groups) or 'no team roles'})."
        )


def team_from_context(context) -> str | None:
    """The `team` run/op tag for this run (None for the untagged platform job)."""
    return _run_tag(context, "team")


def sa_from_context(context, role: str | None = None) -> str | None:
    """Pick the service account a Dagster run should use: the `team` run-tag maps
    to that team's SA (team-scoped, least privilege); otherwise the platform SA.
    `role` selects a per-function team SA (e.g. 'ingest' vs 'build') when one is
    provisioned, else falls back to the team's single SA.

    Enforces team-launch authorization first (fail-closed) so a run can never
    acquire a team SA it isn't entitled to."""
    authorize_team_run(context)

    team = _run_tag(context, "team")
    return sa_for_team(team, role)


def apply_uc_token(spark, sa_client_id: str | None = None) -> str | None:
    """Bind the SA's UC token as the per-session catalog token. Returns the SA
    client_id used, or None if no automation identity is configured (in which
    case the session falls back to the Spark server default)."""
    sa_client_id = sa_client_id or automation_sa_client_id()
    if not sa_client_id:
        return None
    token = uc_token_for_sa(sa_client_id)
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.token", token)
    return sa_client_id
