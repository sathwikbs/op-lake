# Security Model & Pentest-Readiness

This document is the threat model and pentest-readiness checklist for the
platform. It maps every hardening decision to the phase that delivered it and to
the automated check that proves it still holds. The regression gate for
**all** of it is one script:

```bash
./infra/test/smoke.sh      # 9 in-container + reachability + no-static-key checks
```

The harness is re-run after every change; a green harness is the contract that
no capability or control has regressed.

---

## 1. Trust model (who is trusted with what)

- **Unity Catalog is the single data-authority.** Every read/write of a table is
  authorized by UC against the *calling principal's* grants. There is no ambient
  "admin" identity on the compute edge (Spark Connect holds **zero** default UC
  token; a session with no token is nobody and is denied).
- **Two planes, one engine, one authority:**
  - *Automation plane* — Dagster runs authenticate as a **team service account**
    (Keycloak client-credentials → UC token exchange). UC enforces that SA's
    grants, never admin.
  - *Human/BI plane* — Superset & JupyterHub forward the **logged-in user's**
    Keycloak token, exchanged for a UC token per session, so UC enforces RBAC as
    that person.
- **Keycloak is the single identity provider** with **one deterministic issuer**
  (`https://keycloak.localtest.me`). **Vault is the single secret store.**
- **No static object-store key exists anywhere in the compute plane.** Storage
  access is always UC-vended, per-session, path-scoped credentials.

---

## 2. Architecture & network tiers (Phase 6)

```
Browser ──HTTPS(local CA)──▶ Caddy gateway (edge, only 80/443 published)
                                │
        ┌───────────────────────┴──────────── app tier (default net) ───────────────┐
        │  superset  jupyterhub  dagster-auth  dagster(web/daemon)  spark-connect    │
        │  keycloak  unitycatalog  postgres(metadata)  iam-reconciler  landing       │
        └───────┬───────────────────────────────────┬───────────────────────────────┘
                │ (per-user / SA UC token)           │ (Dagster only)
        ┌───────▼──────── data tier ────────┐  ┌─────▼──── admin tier ───────┐
        │  spark   minio   unitycatalog      │  │ vault  vault-init           │
        │  keycloak  spark-connect(bridge)   │  │ docker-socket-proxy         │
        └────────────────────────────────────┘  │ uc-cred-refresher           │
                                                 └─────────────────────────────┘
```

- App-tier UIs **cannot** reach `minio` (lakehouse), raw `spark`, `vault`, or the
  `docker-socket-proxy` — they are on separate Docker networks. The only path to
  storage is `spark-connect → spark → UC-vended creds`.
- The **only** host-published ports are the Caddy gateway (`80/443`) and the
  TLS+token Spark Connect proxy (`15003`). Everything else is in-cluster only.

---

## 3. Threat model → mitigation → phase → proof

| # | Threat / attacker goal | Mitigation | Phase | Proof |
|---|---|---|---|---|
| T1 | **Read another team's / bronze data** via a dashboard or notebook | Per-user UC token exchange; UC RBAC per principal; analyst has gold-only grant | Foundational | harness **G1** (analyst denied bronze), **G2** (analyst allowed gold) |
| T2 | **Bypass UC by reading `s3a://` directly** from a shared Spark session using a server key | No static S3 key in the compute plane; raw `s3a` read has no creds → fail-closed | Foundational | harness **G4** (raw s3a denied), no-static-key env scan |
| T3 | **Hijack managed data** by registering an external table over another table's storage path | No principal holds an external-location / storage-credential / `CREATE EXTERNAL TABLE` grant; UC denies `generateTemporaryPathCredentials` | Phase 5 | harness **G5** (managed un-hijackable) + `personas.json` `_managed_only_comment` |
| T4 | **Steal secrets from committed files / images** | All secrets generated & stored in **Vault** (KV v2); rendered to runtime-only volume; committed files carry sentinels only; `.env` git-ignored | Phase 2 | `grep` shows `set-by-vault`/`__RENDER_*__` sentinels; SA auth works only via Vault (harness **A1/A3**) |
| T5 | **Sniff / MITM browser or token traffic** | Single TLS gateway (Caddy) with a local CA; deterministic issuer; internal ports unpublished | Phase 1 + 3 | gateway routes 200/302 over HTTPS; host cannot reach 5432/9000/8080/15002 |
| T6 | **Escalate from a sidecar to host root via the Docker socket** | Raw `docker.sock` confined to an nginx proxy that allows only `GET /_ping,/version,/containers/json` + `POST /containers/{id}/restart` | Phase 4 | capability probe: restart allowed; stop/rm/run/exec/images denied |
| T7 | **Pivot from a user-facing UI to the data/secret backend** | Docker network segmentation (app/data/admin); UIs can't resolve `minio`/`vault`/`spark`/socket-proxy | Phase 6 | cross-tier probe from Superset: all backend hosts `gaierror` (blocked) |
| T8 | **Forge a team identity in Dagster** to run another team's pipeline | Server-side identity injection (oauth2-proxy `X-Auth-Request-*`); fail-closed `authorize_team_run`; SA secret fetched from Vault at runtime | Phase 2 + foundational | `uc_identity.authorize_team_run`; SA scoping (harness **G3**) |
| T9 | **Expired storage credential breaks running jobs** | UC mint-on-start + admin-plane `uc-cred-refresher` restarts UC inside the STS TTL | Foundational | refresher restarts UC via the socket proxy (Phase 4 probe) |
| T10 | **Reach Vault or the secret material** from an app/user container | Vault is admin-tier only; only Dagster (admin+app) reads it with a scoped read-only token | Phase 2 + 6 | cross-tier probe: Superset → vault:8200 blocked |

---

## 4. Pentest-readiness checklist

- [x] **No secrets in the repo** — Vault is source of truth; committed files hold
  only sentinels; `.env` is git-ignored (Phase 2).
- [x] **Strong, unique secrets** — every secret is randomly generated by
  `vault-init` on first boot, never the old defaults (Phase 2).
- [x] **TLS everywhere at the edge** — single Caddy gateway, local CA, HTTP→
  HTTPS, one deterministic OIDC issuer (Phase 3).
- [x] **Minimal attack surface** — only `443`/`80` and Spark Connect `15003`
  published; all datastores/engines internal-only (Phase 1).
- [x] **Least-privilege Docker socket** — restart-only proxy, admin-tier (Phase 4).
- [x] **Managed-only storage** — no external-table/path-credential grants; the
  managed root cannot be hijacked (Phase 5).
- [x] **Network segmentation** — app UIs cannot reach storage/secret/admin tiers
  (Phase 6).
- [x] **Per-user & per-SA RBAC** enforced by UC on the one shared engine
  (foundational; harness G1–G3).
- [x] **No static object-store key** in the compute plane (foundational; G4).
- [x] **Admin console gated** — `admin.localtest.me` behind oauth2-proxy,
  `platform-admin`-only, Secure cookies, dedicated content server (see §8).
- [x] **Continuous regression gate** — `infra/test/smoke.sh` (Phase 0).
- [ ] **Image CVE triage** — run `./infra/test/trivy-scan.sh` before release
  (Phase 7, opt-in; not part of the smoke gate).

---

## 5. Residual hardening (Phase 7)

### Data-at-rest encryption (MinIO)
Local dev stores objects unencrypted for simplicity. Two supported paths:

- **Local, self-contained:** enable MinIO's built-in KMS by setting
  `MINIO_KMS_SECRET_KEY="minio-local-key:<base64-32-bytes>"` (generate the key
  in Vault, render it into `minio.env`) and turn on auto-encryption for the
  bucket: `mc encrypt set sse-kms minio-local-key local/lakehouse`. Keep the KMS
  key **stable** (Vault-persisted) or previously-encrypted objects become
  unreadable. Left **off by default** so a mis-set key can't brick the lakehouse.
- **Cloud (recommended):** use the object store's native SSE — S3 SSE-KMS with a
  CMK, ADLS/GCS CMEK. UC vends downscoped creds; encryption is transparent.

### Non-root containers
Already non-root: Spark (uid 185), Unity Catalog (drops to `unitycatalog`).
Running as root today (document / migrate in cloud): `minio`, `keycloak`,
`vault`, `caddy`, the nginx proxies, `postgres` (official image drops to
`postgres` internally). The `uc-cred-refresher`'s root-equivalence is already
neutralized by the restart-only socket proxy (Phase 4).

### Image CVE scanning
`./infra/test/trivy-scan.sh` scans every compose image for HIGH/CRITICAL CVEs via
the `aquasec/trivy` container (no host install). Run before releases/pentests.

### Audit logging
- **Unity Catalog** logs authorization decisions to stdout (captured by Docker).
- **oauth2-proxy** logs every auth/authz event (who reached the Dagster UI).
- **iam-reconciler** appends grant reconciliation to `infra/iam/state/audit.log`.
- **Caddy** logs every gateway request.
Ship these to a central sink (Loki/ELK/CloudWatch) in production; locally they
are available via `docker logs` / the mounted `audit.log`.

---

## 6. Known local-dev caveats (tighten in cloud)

- Vault unseal key + root token are file-persisted (`vault_keys` volume) for
  unattended local restarts. **Cloud:** KMS auto-unseal + AppRole/Workload
  Identity, no persisted root token.
- Caddy uses a **local** CA (browser shows "untrusted" until you trust it).
  **Cloud:** real ACME/managed certs.
- Postgres is reachable by Superset/Dagster (their own metadata DB) — this is a
  legitimate app-tier dependency, not the lakehouse. The lakehouse (`minio`) is
  fully isolated from the app tier.
- Rendered secret env files live on a shared runtime volume readable within each
  consuming container. **Cloud:** per-service Vault Agent templates / mounted
  tmpfs with per-service tokens.

---

## 7. Verifying the whole model

```bash
# 1. Everything works + all governance invariants hold:
./infra/test/smoke.sh

# 2. Socket proxy is restart-only:
docker exec dataplatform-uc-cred-refresher-1 sh -c \
  'CID=$(docker ps -q --filter label=com.docker.compose.service=unitycatalog); \
   docker restart $CID && ! docker stop $CID && ! docker run --rm alpine true'

# 3. App tier cannot reach storage/secret/admin tiers:
docker exec dataplatform-superset-1 python -c \
  "import socket
for h,p in [('minio',9000),('vault',8200),('spark',15002)]:
    s=socket.socket(); s.settimeout(3)
    try: s.connect((h,p)); print(h,'REACHABLE (bad)')
    except Exception: print(h,'blocked (good)')"

# 4. CVE triage (opt-in):
./infra/test/trivy-scan.sh
```

---

## 8. Admin console gating (implemented)

The privileged/break-glass launchpad `admin.localtest.me` is fronted by a
dedicated oauth2-proxy (`admin-auth`) restricted to the **`platform-admin`**
group. It's tighter than the Dagster gate: **Secure** cookies (`--cookie-secure`
+ `--reverse-proxy` behind the Caddy TLS gateway), a short **1h** session, no
token pass-through, and a **dedicated `admin-landing`** nginx so the public
`landing` never serves the admin page (defense in depth). The Dagster gate was
made consistent (Secure cookies too). MinIO was removed from the public home
page and now appears only on the gated admin console.

Boundary note: `admin-auth` currently **reuses the `dagster` OIDC client** (with
an added, exact `admin.localtest.me/oauth2/callback` redirect URI). This does not
weaken the boundary — authorization is enforced per-proxy (`platform-admin`
only), cookies are host-scoped (no cross-subdomain replay), and redirect URIs are
exact controlled HTTPS URLs.

## 9. Deferred (next version / Kubernetes migration)

- **Dedicated `admin-portal` OIDC client** for the admin console instead of
  reusing the `dagster` client — gives independent secret rotation and cleaner
  audit separation. (Not a security fix; a hygiene refinement. To do: add the
  client to `realm-export.json` + Vault-rendered secret, point `admin-auth` at
  it, drop the extra redirect URI from the `dagster` client.)
- **Scalable UC metadata backend (MySQL candidate).** A throwaway test confirmed
  MySQL accepts the two statements Postgres rejects (`BINARY(16)`,
  `DELETE … LIMIT`), so MySQL is the leading candidate to move UC off H2 for
  HA/scale. To do: add the MySQL JDBC driver to UC's classpath, run UC-on-MySQL
  end-to-end (create/read managed tables), migrate metadata, and gate on the
  smoke harness. Blocked-by-choice today: we stay on the known-good
  `UC v0.5.0 + H2 + Spark-managed writes` island.
- **Generic-namespace consumer plumbing.** The IAM layer is now topology-generic:
  `personas.yaml` has a `namespace:` block (named dimensions + `catalog_template`/
  `schema_template`) and `iam_namespace.py` expands grants over the cartesian
  product, so switching to catalog-per-env / env×domain / per-team / per-tenant is
  a config-only change and the reconciler bootstraps the catalogs/schemas. What is
  NOT yet threaded: the **data producers** still target the single catalog name
  `analytics` — dbt (profile/`--vars`), the dlt destination, and Spark defaults.
  To do (next version): parameterize dbt/dlt/Spark off `namespace.primary_catalog`
  (or a per-run env/team var) so data actually lands where the grants point, then
  extend the smoke harness with a non-default topology (e.g. `dev`/`prod` catalogs)
  to prove end-to-end. Until then a multi-catalog topology governs correctly but
  the pipelines write only to `analytics`.
- **Break-glass admin (opt-out of data inheritance).** Today `platform-admin` is
  the `umbrella_admin_role` in `personas.yaml`, so it is a Keycloak composite over
  every `team-<name>` role and the reconciler flattens the **union of all teams'
  data grants** onto it — i.e. an admin can `SELECT` any team's tables (e.g.
  `analytics.gold.customer_order_summary`) through real UC grants. This is
  working-as-designed hierarchy. If a deployment prefers admins to **not**
  auto-inherit team data (break-glass / least-privilege), set
  `umbrella_admin_role: null` and grant admins data explicitly (or via an
  auditable, time-boxed elevation). Trade-off: new teams no longer roll up to the
  admin automatically, and admins can't launch arbitrary teams' pipelines. Keep
  inheritance ON for small teams; consider OFF for regulated / multi-tenant orgs.
- **Reconciler drift detection (UC read-back).** `sync_group_grants.py` is a
  DECLARATIVE reconciler: it computes `desired` grants from persona/team templates
  and diffs against its OWN state file (`infra/iam/state/group_grants_state.json`)
  — it does **not** read current grants back from UC. This is a deliberate
  workaround for a **UC 0.5.0 bug**: the `permission get` API returns
  `"No authorization expression found"` (fixed upstream in **UC 0.5.1**). Consequence:
  the reconciler cannot detect **out-of-band** changes (a grant added/removed
  directly in UC, or a lost/corrupt state file) — it trusts its state file as
  truth. `permission create`/`delete` are idempotent so re-runs are safe, and the
  state file is persisted, but this is not true drift reconciliation. Long-term
  fix: when UC moves to **0.5.1+** (bundle it with the MySQL-backend upgrade
  above), switch the reconciler to read live UC grants as the `state` source so it
  self-heals out-of-band drift; then the local state file becomes a cache, not the
  source of truth. Until then, treat UC grants as reconciler-owned (don't edit
  them by hand) and back up the state file.
- **Realm drift flush.** Earlier realm edits (removed SA→MinIO mappers; MinIO
  admin-only) are committed to `realm-export.json` but only apply on a fresh
  Keycloak seed; the running instance still emits the now-unusable SA `minio_policy`
  claim (harmless — the `team-analytics` MinIO policy is deleted and MinIO is
  network-isolated). A clean re-provision flushes it.
- **README refresh.** The README's access table still lists pre-gateway
  `localhost:*` ports/creds. Update it to the `*.localtest.me` gateway URLs, the
  demo logins, the local-CA-trust step, and the first-run admin tasks.
- **Seeded demo accounts.** `platformadmin`, `engineer`, `engineer2` are now
  forced to reset their (repo-seeded, single-use) passwords on first login;
  `analyst` is intentionally left usable for the smoke harness and must be
  disabled/deleted in real deployments. Longer term, generate the bootstrap admin
  password in Vault instead of committing a seed value.
- **Leaver / deprovisioning — enforcement IMPLEMENTED; trigger deferred.** The
  IAM reconciler now treats a **disabled Keycloak user (`enabled=false`) as fully
  deprovisioned**: on each cron pass it (1) **force-logs-out** the user
  (`POST /users/{id}/logout`, killing live sessions + offline/refresh tokens so a
  deactivated account can't keep minting access tokens) and (2) **revokes all
  their UC grants** (disabled users are excluded from desired membership).
  Verified: disabling `engineer` → 1 logout + 13 grants revoked; re-enabling
  restores them. What remains for real org SSO is only the **trigger** that flips
  `enabled=false` when someone leaves — a directory sync (**SCIM push** or
  **LDAP/AD federation**, or a scheduled diff against the org directory), since
  pure OIDC brokering can't pull deletions. Once that sets `enabled=false`, the
  reconciler does the rest automatically.
