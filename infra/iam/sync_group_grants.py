#!/usr/bin/env python3
"""Group emulation for Unity Catalog OSS via a persona reconciler.

WHY: Unity Catalog OSS (v0.5.x) has no native group principal -- every grant
targets one principal (a user/SA email), and query-time authorization resolves
a token to a single principal and checks THAT principal's grants. It never reads
group claims. So "groups" are emulated at the management layer:

    Keycloak group  --(membership)-->  members
         |                                 |
         | maps to a UC persona            | resolved to UC principals (emails)
         v                                 v
    persona grant template  --RECONCILE-->  explicit per-principal UC grants

You manage PEOPLE in a Keycloak group; this reconciler keeps Unity Catalog in
sync -- materializing a member's persona grants when they join and REVOKING them
when they leave. UC stays the single data authority; the group is just the
membership source of truth (and, in cloud/managed UC, these same groups map onto
native UC groups and this reconciler retires).

Two things a group confers on a member, both keyed off the SAME membership:
  (a) Keycloak realm roles  -> app-layer access (Superset feature roles) and
      entitlements (sa-creator), carried in the token.
  (b) UC data grants        -> materialized here as per-principal grants.

State: because UC 0.5.0's `permission get` is buggy ("No authorization
expression found", fixed in 0.5.1), we do NOT read UC to compute drift. Instead
this is a DECLARATIVE reconciler with a state file (state/group_grants_state.json):
desired = union of persona templates for a member's current groups; we apply
(desired - state) and revoke (state - desired), then persist desired. `permission
create`/`delete` are idempotent, so re-runs are safe.

Commands:
  sync                      ensure groups exist, then reconcile grants (default)
  ensure-groups             create Keycloak groups + attach realm roles (idempotent)
  reconcile                 reconcile UC grants from current membership
  add-member    --user U --group G     add a Keycloak user to a group
  remove-member --user U --group G     remove a Keycloak user from a group
  show                      print groups, members, and desired grants

Run on the host (talks to Keycloak via the gateway https://keycloak.localtest.me
and the unitycatalog container), same as provision_service_account.py.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import urllib.error
import urllib.request
from pathlib import Path

# Reuse the identity/UC plumbing from the SA provisioner (its main() is guarded).
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import provision_service_account as sa  # noqa: E402
import iam_namespace  # noqa: E402

PERSONAS = sa.PERSONAS
NS = iam_namespace.Namespace(PERSONAS)   # generic catalog/schema topology engine
CATALOG = NS.primary_catalog             # default catalog for tooling/back-compat
GROUPS = PERSONAS["groups"]
TEAMS = PERSONAS.get("teams", {})
UMBRELLA_ADMIN_ROLE = PERSONAS.get("umbrella_admin_role")  # composite over all team roles
AUTOMATION = PERSONAS.get("automation", {})
DEFAULT_SA = AUTOMATION.get("default_service_account")     # {name, persona} | None

# Vault (KV v2) access for auto-provisioning SA secrets. Rendered into the
# reconciler's env by vault-init (reconciler.env) and sourced by inject.sh.
VAULT_ADDR = os.environ.get("VAULT_ADDR", "").rstrip("/")
VAULT_TOKEN = os.environ.get("VAULT_TOKEN", "")
VAULT_MOUNT = os.environ.get("VAULT_KV_MOUNT", "secret")
VAULT_SA_PREFIX = os.environ.get("VAULT_SA_PREFIX", "platform/sa")
STATE_DIR = HERE / "state"
STATE_FILE = STATE_DIR / "group_grants_state.json"


def team_role_name(team: str) -> str:
    """Realm role that gates launching `team`'s Dagster pipelines."""
    return f"team-{team}"


def team_group_name(team: str) -> str:
    """Keycloak group whose members inherit the team launch role."""
    return TEAMS.get(team, {}).get("group", team_role_name(team))


def _valid_group_names() -> set:
    """Persona groups + team groups accepted by add-member/remove-member."""
    return set(GROUPS) | {team_group_name(t) for t in TEAMS}


# --------------------------------------------------------------------------- #
# state helpers
# --------------------------------------------------------------------------- #
def _load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"version": 1, "managed": {}}


def _save_state(state: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def _grant_key(g: dict) -> tuple:
    return (g["securable"], g["name"], g["privilege"])


def _persona_grants(persona_name: str) -> list[dict]:
    """Expand a persona's grant template over the namespace (dimensions +
    templates), honouring an optional persona-level `scope:` filter."""
    p = PERSONAS["personas"][persona_name]
    return NS.expand(p["grants"], scope=p.get("scope"))


def _team_grants(team: str) -> list[dict]:
    """The default DATA scope a team confers on its members (may be empty).

    If a `team` dimension exists (per-team topology), each team is auto-scoped to
    its own value so `team: analytics` lands in catalog `analytics`."""
    cfg = TEAMS.get(team, {})
    scope = dict(cfg.get("scope") or {})
    if "team" in NS.dimensions:
        scope.setdefault("team", [team])
    return NS.expand(cfg.get("grants", []), scope=scope, extra={"team": team})


def _all_team_grants() -> list[dict]:
    """Union of every team's data grants -- what the umbrella admin inherits."""
    out: dict[tuple, dict] = {}
    for t in TEAMS:
        for g in _team_grants(t):
            out[_grant_key(g)] = g
    return list(out.values())


# --------------------------------------------------------------------------- #
# Keycloak admin helpers (built on provision_service_account._kc_admin_*)
# --------------------------------------------------------------------------- #
def _kc_get(token: str, path: str):
    status, body = sa._kc_admin_call("GET", path, token)
    if status != 200:
        raise RuntimeError(f"Keycloak GET {path} -> {status}: {body[:200]}")
    return json.loads(body) if body else None


def _group_id(token: str, name: str) -> str | None:
    for g in _kc_get(token, "/groups?max=200") or []:
        if g.get("name") == name:
            return g["id"]
    return None


def _role_rep(token: str, role_name: str) -> dict:
    return _kc_get(token, f"/roles/{role_name}")


def _ensure_realm_role(token: str, name: str, description: str = "") -> None:
    """Create a realm role if absent (idempotent)."""
    status, _ = sa._kc_admin_call("GET", f"/roles/{name}", token)
    if status == 200:
        return
    body = {"name": name, "description": description}
    status, resp = sa._kc_admin_call("POST", "/roles", token, body)
    if status not in (201, 409):
        raise RuntimeError(f"create realm role {name} -> {status}: {resp[:200]}")
    print(f"  [keycloak] created realm role '{name}'")


def _ensure_composite(token: str, parent_role: str, child_role: str) -> None:
    """Make `parent_role` a composite that INCLUDES `child_role` (idempotent).

    This is how inheritance works: a holder of `parent_role` automatically gains
    every `child_role` folded into it -- e.g. platform-admin over all team roles.
    """
    child = _role_rep(token, child_role)
    status, body = sa._kc_admin_call(
        "POST", f"/roles/{parent_role}/composites", token,
        [{"id": child["id"], "name": child["name"]}])
    if status not in (204, 201, 200):
        print(f"    ! {parent_role} inherit {child_role} -> {status}: {body[:160]}")
    else:
        print(f"    -> {parent_role} inherits {child_role}")


def _ensure_group_with_roles(token: str, gname: str, want_roles: list) -> str:
    """Create a Keycloak group if absent and attach the given realm roles."""
    gid = _group_id(token, gname)
    if gid is None:
        status, body = sa._kc_admin_call("POST", "/groups", token, {"name": gname})
        if status not in (201, 409):
            raise RuntimeError(f"create group {gname} -> {status}: {body[:200]}")
        gid = _group_id(token, gname)
        print(f"  [keycloak] created group '{gname}'")
    else:
        print(f"  [keycloak] group '{gname}' exists")
    if want_roles:
        reps = [{"id": r["id"], "name": r["name"]} for r in (_role_rep(token, r) for r in want_roles)]
        status, body = sa._kc_admin_call(
            "POST", f"/groups/{gid}/role-mappings/realm", token, reps)
        if status not in (204, 201, 200):
            print(f"    ! attach roles {want_roles} -> {status}: {body[:160]}")
        else:
            print(f"    -> realm roles {want_roles}")
    return gid


def _user_rep(token: str, username: str) -> dict:
    users = _kc_get(token, f"/users?username={username}&exact=true") or []
    if not users:
        raise RuntimeError(f"Keycloak user '{username}' not found")
    return users[0]


def _group_members(token: str, group_id: str) -> list[dict]:
    return _kc_get(token, f"/groups/{group_id}/members?max=500") or []


# --------------------------------------------------------------------------- #
# ensure-groups: create KC groups + attach realm roles (idempotent)
# --------------------------------------------------------------------------- #
def ensure_groups() -> None:
    token = sa._kc_admin_token()
    for gname, cfg in GROUPS.items():
        gid = _group_id(token, gname)
        if gid is None:
            status, body = sa._kc_admin_call("POST", "/groups", token, {"name": gname})
            if status not in (201, 409):
                raise RuntimeError(f"create group {gname} -> {status}: {body[:200]}")
            gid = _group_id(token, gname)
            print(f"  [keycloak] created group '{gname}'")
        else:
            print(f"  [keycloak] group '{gname}' exists")
        # Attach realm roles so members inherit app-layer access + entitlements.
        want_roles = cfg.get("realm_roles", [])
        if want_roles:
            reps = [_role_rep(token, r) for r in want_roles]
            reps = [{"id": r["id"], "name": r["name"]} for r in reps]
            status, body = sa._kc_admin_call(
                "POST", f"/groups/{gid}/role-mappings/realm", token, reps)
            if status not in (204, 201, 200):
                print(f"    ! attach roles {want_roles} -> {status}: {body[:160]}")
            else:
                print(f"    -> realm roles {want_roles}")


# --------------------------------------------------------------------------- #
# ensure-teams: create the team launch role + team group (idempotent)
# --------------------------------------------------------------------------- #
def ensure_teams() -> None:
    """Provision each declared team's launch scope from personas.yaml:
    a `team-<name>` realm role and a Keycloak group `team-<name>` carrying it.

    This makes team-launch authorization (dagster/serve.py +
    uc_identity.authorize_team_run) declarative: add a team here and its role +
    group appear with no manual Keycloak clicks; people join by group membership.
    """
    if not TEAMS:
        print("  [teams] none declared in personas.yaml")
        return
    token = sa._kc_admin_token()
    for team, cfg in TEAMS.items():
        role = team_role_name(team)
        gname = team_group_name(team)
        _ensure_realm_role(token, role, cfg.get("description", f"Team launch scope: {team}"))
        _ensure_group_with_roles(token, gname, [role])
        # Inheritance: fold every team role into the umbrella admin role so
        # platform-admin sits above all teams (and new teams roll up automatically).
        if UMBRELLA_ADMIN_ROLE:
            _ensure_composite(token, UMBRELLA_ADMIN_ROLE, role)
        print(f"  [teams] '{team}' -> role '{role}', group '{gname}'")


# --------------------------------------------------------------------------- #
# membership management
# --------------------------------------------------------------------------- #
def add_member(username: str, group: str) -> None:
    valid = _valid_group_names()
    if group not in valid:
        raise SystemExit(f"unknown group '{group}'. Known: {sorted(valid)}")
    token = sa._kc_admin_token()
    uid = _user_rep(token, username)["id"]
    gid = _group_id(token, group)
    if gid is None:
        raise SystemExit(f"group '{group}' does not exist; run ensure-groups first")
    status, body = sa._kc_admin_call("PUT", f"/users/{uid}/groups/{gid}", token)
    if status not in (204, 201):
        raise SystemExit(f"add-member -> {status}: {body[:200]}")
    print(f"  [keycloak] added '{username}' to group '{group}'")
    sa.audit(action="group_add_member", user=username, group=group)


def remove_member(username: str, group: str) -> None:
    valid = _valid_group_names()
    if group not in valid:
        raise SystemExit(f"unknown group '{group}'. Known: {sorted(valid)}")
    token = sa._kc_admin_token()
    uid = _user_rep(token, username)["id"]
    gid = _group_id(token, group)
    status, body = sa._kc_admin_call("DELETE", f"/users/{uid}/groups/{gid}", token)
    if status not in (204, 200):
        raise SystemExit(f"remove-member -> {status}: {body[:200]}")
    print(f"  [keycloak] removed '{username}' from group '{group}'")
    sa.audit(action="group_remove_member", user=username, group=group)


# --------------------------------------------------------------------------- #
# reconcile: membership -> desired per-principal grants -> apply/revoke
# --------------------------------------------------------------------------- #
def _desired_from_membership(token: str):
    """Return {principal_email: {grant_key: grant_dict}} and a display map
    {principal_email: {"username":.., "groups":[..]}} from current membership.

    Flattens the whole hierarchy into explicit per-principal grants (UC OSS has
    no groups/roles/inheritance), combining three sources:
      1. persona groups  -> the member's persona grant template;
      2. umbrella admin  -> members of a group conferring `umbrella_admin_role`
         inherit the UNION of every team's data grants (admin above all teams);
      3. team groups     -> members inherit that team's default data scope.
    """
    desired: dict[str, dict[tuple, dict]] = {}
    who: dict[str, dict] = {}

    def _add(email: str, username: str, gname: str, grants: list[dict]):
        if not email:
            print(f"  ! skip '{username}' in {gname}: no email (UC maps by email)")
            return
        info = who.setdefault(email, {"username": username, "groups": []})
        if gname not in info["groups"]:
            info["groups"].append(gname)
        bucket = desired.setdefault(email, {})
        for g in grants:
            bucket[_grant_key(g)] = g

    # (1) + (2) persona groups, with umbrella-admin data inheritance.
    for gname, cfg in GROUPS.items():
        gid = _group_id(token, gname)
        if gid is None:
            continue
        grants = _persona_grants(cfg["persona"])
        if UMBRELLA_ADMIN_ROLE and UMBRELLA_ADMIN_ROLE in cfg.get("realm_roles", []):
            grants = grants + _all_team_grants()  # inherits every team's data scope
        for m in _group_members(token, gid):
            if m.get("enabled", True) is False:
                continue  # DISABLED = deprovisioned: excluded so UC grants get revoked
            _add(m.get("email"), m.get("username"), gname, grants)

    # (3) team groups -> the team's default data scope.
    for team in TEAMS:
        gname = team_group_name(team)
        gid = _group_id(token, gname)
        if gid is None:
            continue
        grants = _team_grants(team)
        if not grants:
            continue
        for m in _group_members(token, gid):
            if m.get("enabled", True) is False:
                continue  # DISABLED = deprovisioned: excluded so UC grants get revoked
            _add(m.get("email"), m.get("username"), gname, grants)

    return desired, who


def _ensure_uc_user(name: str, email: str) -> None:
    """Idempotently register the UC principal (matched by email at query time)."""
    r = sa._uc("user", "list", "--output", "json")
    existing = []
    if r.returncode == 0:
        try:
            existing = [u.get("email") for u in json.loads(r.stdout or "[]")]
        except json.JSONDecodeError:
            existing = []
    if email not in existing:
        sa._uc("user", "create", "--name", name or email.split("@")[0], "--email", email)
        print(f"  [uc] registered principal {email}")


def _apply_grant(email: str, g: dict) -> bool:
    r = sa._uc("permission", "create", "--securable_type", g["securable"],
               "--name", g["name"], "--privilege", g["privilege"], "--principal", email)
    return r.returncode == 0 and "Exception" not in (r.stdout + r.stderr)


def _revoke_grant(email: str, g: dict) -> bool:
    r = sa._uc("permission", "delete", "--securable_type", g["securable"],
               "--name", g["name"], "--privilege", g["privilege"], "--principal", email)
    return r.returncode == 0 and "Exception" not in (r.stdout + r.stderr)


# --------------------------------------------------------------------------- #
# bootstrap: ensure the declared namespace (catalogs + schemas) exists in UC so
# USE-CATALOG / USE-SCHEMA grants always have a target. Idempotent
# create-and-tolerate (same pattern as unitycatalog/bootstrap/bootstrap.sh).
# --------------------------------------------------------------------------- #
def _already_exists(blob: str) -> bool:
    b = blob.lower()
    return "already exists" in b or "already_exists" in b


def _ensure_catalog(cat: str) -> bool:
    r = sa._uc("catalog", "create", "--name", cat,
               "--comment", "auto-provisioned by IAM reconciler")
    blob = r.stdout + r.stderr
    if r.returncode == 0 and "Exception" not in blob:
        print(f"  [uc] created catalog {cat}")
        return True
    if not _already_exists(blob):
        print(f"  [uc] ! ensure catalog {cat}: {blob.strip()[:160]}")
    return False


def _ensure_schema(cat: str, sch: str) -> bool:
    r = sa._uc("schema", "create", "--catalog", cat, "--name", sch)
    blob = r.stdout + r.stderr
    if r.returncode == 0 and "Exception" not in blob:
        print(f"  [uc] created schema {cat}.{sch}")
        return True
    if not _already_exists(blob):
        print(f"  [uc] ! ensure schema {cat}.{sch}: {blob.strip()[:160]}")
    return False


def bootstrap_namespace() -> None:
    """Ensure every catalog + schema implied by the namespace model exists."""
    catalogs, schemas = NS.declared_securables()
    nc = sum(_ensure_catalog(c) for c in catalogs)
    nsch = sum(_ensure_schema(c, s) for (c, s) in schemas)
    print(f"  [bootstrap] namespace ensured: {len(catalogs)} catalog(s) (+{nc} new), "
          f"{len(schemas)} schema(s) (+{nsch} new) "
          f"[catalog_template='{NS.catalog_template}', schema_template='{NS.schema_template}']")


# --------------------------------------------------------------------------- #
# service accounts: auto-provision each declared SA (default automation SA +
# per-team SA) -- Keycloak client + Vault secret + UC principal + persona grants.
# So "declare a team.service_account" is enough; no manual client/secret/env.
# --------------------------------------------------------------------------- #
def _vault_available() -> bool:
    return bool(VAULT_ADDR and VAULT_TOKEN)


def _vault_sa_url(client_id: str) -> str:
    return f"{VAULT_ADDR}/v1/{VAULT_MOUNT}/data/{VAULT_SA_PREFIX}/{client_id}"


def _vault_read_sa(client_id: str) -> str | None:
    req = urllib.request.Request(_vault_sa_url(client_id),
                                 headers={"X-Vault-Token": VAULT_TOKEN})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)["data"]["data"]["value"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def _vault_write_sa(client_id: str, secret: str) -> bool:
    body = json.dumps({"data": {"value": secret}}).encode()
    req = urllib.request.Request(
        _vault_sa_url(client_id), data=body, method="POST",
        headers={"X-Vault-Token": VAULT_TOKEN, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status in (200, 204)


def _sa_specs() -> list[tuple[str, str, str | None]]:
    """(client_id, persona_name, team|None) for every declared service account:
    the default automation SA + each team's `service_account`."""
    specs: list[tuple[str, str, str | None]] = []
    if DEFAULT_SA:
        specs.append((f"sa-{DEFAULT_SA['name']}", DEFAULT_SA["persona"], None))
    for team, cfg in TEAMS.items():
        # Client ids follow the convention matched by Dagster's uc_identity:
        #   singular service_account         -> sa-team-<team>
        #   plural  service_accounts.<role>  -> sa-team-<team>-<role>
        single = cfg.get("service_account")
        if single:
            specs.append((f"sa-team-{team}", single["persona"], team))
        for role, rcfg in (cfg.get("service_accounts") or {}).items():
            specs.append((f"sa-team-{team}-{role}", rcfg["persona"], team))
    return specs


def _find_client(token: str, client_id: str) -> str | None:
    clients = _kc_get(token, f"/clients?clientId={client_id}") or []
    return clients[0]["id"] if clients else None


def _sa_client_rep(client_id: str, sa_email: str, secret: str) -> dict:
    """Keycloak client rep for a passwordless (client-credentials) SA, carrying
    the aud=unitycatalog + hardcoded email claim UC needs for authorization."""
    return {
        "clientId": client_id, "enabled": True, "protocol": "openid-connect",
        "publicClient": False, "serviceAccountsEnabled": True,
        "standardFlowEnabled": False, "directAccessGrantsEnabled": False,
        "secret": secret,
        "attributes": {"created_by": "iam-reconciler"},
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


def ensure_service_accounts() -> None:
    """Ensure each declared SA has a Keycloak client, a Vault-stored secret, and
    a registered UC principal. Idempotent; NEVER rotates an existing client's
    secret (only backfills Vault if missing)."""
    specs = _sa_specs()
    if not specs:
        print("  [sa] none declared in personas.yaml")
        return
    if not _vault_available():
        print("  [sa] ! VAULT_ADDR/VAULT_TOKEN unset; cannot manage SA secrets -- "
              "skipping SA client provisioning (grants still reconcile).")
        return
    token = sa._kc_admin_token()
    for client_id, persona_name, _team in specs:
        sa_email = f"{client_id}@platform.local"
        cid = _find_client(token, client_id)
        if cid is None:
            secret = secrets.token_urlsafe(24)
            status, body = sa._kc_admin_call(
                "POST", "/clients", token, _sa_client_rep(client_id, sa_email, secret))
            if status not in (201, 409):
                print(f"  [sa] ! create client {client_id} -> {status}: {body[:160]}")
                continue
            _vault_write_sa(client_id, secret)
            print(f"  [sa] provisioned client {client_id} (+ secret in Vault)")
        elif _vault_read_sa(client_id) is None:
            # Existing client (e.g. seeded) but no Vault secret yet: backfill from KC.
            st, b = sa._kc_admin_call("GET", f"/clients/{cid}/client-secret", token)
            kcsec = None
            try:
                kcsec = json.loads(b).get("value") if b else None
            except json.JSONDecodeError:
                pass
            if kcsec:
                _vault_write_sa(client_id, kcsec)
                print(f"  [sa] backfilled Vault secret for existing client {client_id}")
        # Register the UC principal (matched by email at query time).
        _ensure_uc_user(client_id, sa_email)


def _all_persona_grants() -> dict:
    """Union of every persona's expanded grants -- the bounded UNIVERSE an SA
    could ever hold. Used as the revoke set so SA grants are reconciled without
    reading UC (revoking an absent grant is a harmless no-op)."""
    out: dict[tuple, dict] = {}
    for name, p in PERSONAS["personas"].items():
        for g in NS.expand(p["grants"], scope=p.get("scope")):
            out[_grant_key(g)] = g
    return out


def reconcile_service_accounts(dry_run: bool = False) -> tuple[int, int]:
    """Apply each SA's persona grants and REVOKE the complement (universe -
    desired). Stateless + idempotent: this both grants a new team SA its builder
    scope AND downgrades an over-privileged SA (e.g. the default automation SA to
    catalog-only). Run at startup / on config change, not in the hot loop."""
    specs = _sa_specs()
    if not specs:
        return 0, 0
    universe = _all_persona_grants()
    n_apply = n_revoke = 0
    for client_id, persona_name, team in specs:
        sa_email = f"{client_id}@platform.local"
        p = PERSONAS["personas"].get(persona_name)
        if not p:
            print(f"  [sa] ! unknown persona '{persona_name}' for {client_id}")
            continue
        scope = dict(p.get("scope") or {})
        if team and "team" in NS.dimensions:
            scope.setdefault("team", [team])
        desired = {_grant_key(g): g for g in NS.expand(p["grants"], scope=scope)}
        for k, g in desired.items():
            if dry_run:
                print(f"  [DRY][sa] + {sa_email}: {g['privilege']} on {g['name']}")
                n_apply += 1
                continue
            if _apply_grant(sa_email, g):
                n_apply += 1
        for k, g in universe.items():
            if k in desired:
                continue
            if dry_run:
                n_revoke += 1
                continue
            if _revoke_grant(sa_email, g):
                n_revoke += 1
        print(f"  [sa] {client_id} <- persona '{persona_name}': "
              f"{len(desired)} grant(s){' [dry-run]' if dry_run else ''}")
    print(f"  [sa] reconcile summary: +{n_apply} applied, -{n_revoke} revoked "
          f"across {len(specs)} service account(s).")
    return n_apply, n_revoke


def ensure_sas(dry_run: bool = False) -> None:
    """Provision SA clients/secrets/principals, then reconcile their grants."""
    ensure_service_accounts()
    reconcile_service_accounts(dry_run=dry_run)


def _disabled_members(token: str) -> dict:
    """{user_id: username} for members of any managed group who are DISABLED in
    Keycloak (`enabled=false`). A disabled account is the deprovision signal:
    set by an admin, or pushed/pulled from the org directory (SCIM / LDAP sync)
    when someone leaves the org."""
    disabled: dict[str, str] = {}
    gnames = list(GROUPS.keys()) + [team_group_name(t) for t in TEAMS]
    for gname in gnames:
        gid = _group_id(token, gname)
        if gid is None:
            continue
        members = _group_members(token, gid)
        for m in members:
            if m.get("enabled", True) is False:
                disabled[m["id"]] = m.get("username", m["id"])
    return disabled


def _deprovision_disabled(token: str, dry_run: bool = False) -> int:
    """Force-logout disabled users so a deactivated account cannot keep minting
    access tokens via an existing session or offline/refresh token (Keycloak's
    refresh flow does NOT re-check the upstream IdP). This closes the leaver gap:
    login is already blocked at the org IdP; this kills live sessions too, and
    the normal reconcile revokes their UC grants (they're excluded from desired
    membership above). `POST /users/{id}/logout` revokes sessions + offline tokens."""
    disabled = _disabled_members(token)
    n = 0
    for uid, uname in disabled.items():
        if dry_run:
            print(f"  [DRY] force-logout disabled user '{uname}'")
            n += 1
            continue
        status, body = sa._kc_admin_call("POST", f"/users/{uid}/logout", token)
        ok = status in (204, 200)
        print(f"  [keycloak] force-logout disabled user '{uname}' "
              f"{'ok' if ok else f'-> {status}: {body[:120]}'}")
        sa.audit(action="deprovision_logout", user=uname, ok=ok)
        n += ok
    return n


def reconcile(dry_run: bool = False) -> None:
    token = sa._kc_admin_token()
    # Deprovision leavers FIRST: force-logout any disabled users (kills live
    # sessions + offline tokens); their UC grants are then revoked below because
    # disabled members are excluded from desired membership.
    n_logout = _deprovision_disabled(token, dry_run)
    desired, who = _desired_from_membership(token)
    state = _load_state()
    managed: dict[str, list] = state.get("managed", {})

    # Union of every principal we might touch: current members + previously managed.
    principals = set(desired) | set(managed)
    new_managed: dict[str, list] = {}
    n_apply = n_revoke = 0

    for email in sorted(principals):
        want = desired.get(email, {})                       # {key: grant}
        prev = {(_grant_key(g)): g for g in managed.get(email, [])}
        to_apply = [g for k, g in want.items() if k not in prev]
        to_revoke = [g for k, g in prev.items() if k not in want]

        if want:
            _ensure_uc_user(who.get(email, {}).get("username", ""), email)

        for g in to_apply:
            label = f"{g['privilege']} on {g['name']}"
            if dry_run:
                print(f"  [DRY] + {email}: {label}")
                n_apply += 1
                continue
            ok = _apply_grant(email, g)
            print(f"  [uc] + {email}: {label} {'ok' if ok else 'FAILED'}")
            sa.audit(action="group_grant_apply", principal=email, grant=g, ok=ok,
                     groups=who.get(email, {}).get("groups", []))
            n_apply += ok

        for g in to_revoke:
            label = f"{g['privilege']} on {g['name']}"
            if dry_run:
                print(f"  [DRY] - {email}: {label}")
                n_revoke += 1
                continue
            ok = _revoke_grant(email, g)
            print(f"  [uc] - {email}: {label} {'ok' if ok else 'FAILED'}")
            sa.audit(action="group_grant_revoke", principal=email, grant=g, ok=ok)
            n_revoke += ok

        # New managed state = what we intend to hold (desired). Drop principals
        # with no desired grants (fully removed from all groups).
        if want:
            new_managed[email] = list(want.values())

    if not dry_run:
        state["managed"] = new_managed
        _save_state(state)

    print(f"\nReconcile summary: +{n_apply} grants, -{n_revoke} grants, "
          f"{n_logout} disabled-user logout(s) across {len(principals)} principal(s). "
          f"state={STATE_FILE if not dry_run else '(dry-run, not written)'}")


# --------------------------------------------------------------------------- #
# show
# --------------------------------------------------------------------------- #
def show() -> None:
    token = sa._kc_admin_token()
    desired, who = _desired_from_membership(token)
    print("Group -> persona map:")
    for g, cfg in GROUPS.items():
        print(f"  {g:16s} -> persona '{cfg['persona']}', realm_roles {cfg.get('realm_roles', [])}")
    if TEAMS:
        print("\nTeam -> launch role / group / default data scope:")
        for t, cfg in TEAMS.items():
            print(f"  {t:16s} -> role '{team_role_name(t)}', group '{team_group_name(t)}'")
            for g in _team_grants(t):
                print(f"      data: {g['privilege']} on {g['name']}")
        if UMBRELLA_ADMIN_ROLE:
            print(f"  inheritance: '{UMBRELLA_ADMIN_ROLE}' inherits every team role "
                  f"({', '.join(team_role_name(t) for t in TEAMS)}) "
                  f"AND the union of all team data grants")
    print("\nMembership -> desired UC grants:")
    if not who:
        print("  (no members in any managed group)")
    for email, info in sorted(who.items()):
        print(f"  {email}  (user={info['username']}, groups={info['groups']})")
        for g in desired[email].values():
            print(f"      {g['privilege']} on {g['name']}")
    st = _load_state().get("managed", {})
    print(f"\nState file currently manages {len(st)} principal(s): {sorted(st)}")


# --------------------------------------------------------------------------- #
def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd")
    sub.add_parser("sync", help="ensure-groups + ensure-teams + bootstrap + reconcile (default)")
    sub.add_parser("ensure-groups", help="create KC groups + attach realm roles")
    sub.add_parser("ensure-teams", help="create team launch roles + team groups")
    sub.add_parser("bootstrap", help="ensure the declared catalogs + schemas exist in UC")
    es = sub.add_parser("ensure-sas", help="provision declared service accounts + reconcile their grants")
    es.add_argument("--dry-run", action="store_true")
    rp = sub.add_parser("reconcile", help="reconcile UC grants from membership")
    rp.add_argument("--dry-run", action="store_true")
    am = sub.add_parser("add-member")
    am.add_argument("--user", required=True)
    am.add_argument("--group", required=True)
    rm = sub.add_parser("remove-member")
    rm.add_argument("--user", required=True)
    rm.add_argument("--group", required=True)
    sub.add_parser("show")
    a = p.parse_args()

    cmd = a.cmd or "sync"
    if cmd == "sync":
        ensure_groups()
        ensure_teams()
        bootstrap_namespace()
        ensure_sas()
        reconcile()
    elif cmd == "ensure-groups":
        ensure_groups()
    elif cmd == "ensure-teams":
        ensure_teams()
    elif cmd == "bootstrap":
        bootstrap_namespace()
    elif cmd == "ensure-sas":
        ensure_sas(dry_run=a.dry_run)
    elif cmd == "reconcile":
        reconcile(dry_run=a.dry_run)
    elif cmd == "add-member":
        add_member(a.user, a.group)
    elif cmd == "remove-member":
        remove_member(a.user, a.group)
    elif cmd == "show":
        show()
    return 0


if __name__ == "__main__":
    sys.exit(main())
