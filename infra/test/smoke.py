#!/usr/bin/env python3
"""Phase-gate smoke harness (IN-CONTAINER half).

Runs INSIDE the Dagster container (has pyspark[connect], dlt, dbt, the
`data_platform` package, SPARK_REMOTE + the SA secrets). It exercises the whole
platform end-to-end and asserts the security invariants that MUST hold after
every hardening phase. The host half (`smoke.sh`) drives this and adds
host-side reachability + "no static key in the compute plane" checks.

Checks
  AUTOMATION
    A1  dlt -> UC-managed bronze  (as the team INGEST SA, bronze-only)
    A2  read bronze counts as the team BUILD SA  (> 0 rows)
    A3  dbt build silver + gold  (as the team BUILD SA, data-engineer)
    A4  read gold count as the team BUILD SA  (> 0 rows)
  GOVERNANCE (must stay true every phase)
    G1  analyst is DENIED bronze          (per-user RBAC, fail-closed)
    G2  analyst is ALLOWED gold           (SSO + grant path works)
    G3  build SA is ALLOWED bronze        (SA identity is scoped, not admin)
    G4  raw s3a read of a managed path is DENIED (no static key bypass)
    G5  team SA cannot hijack a managed path via an external LOCATION (denied)
    G6  default automation SA is least-privilege -> DENIED data (catalog-only,
        so untagged runs can't touch data -> real incentive to team-tag)
    G7  ingest SA is bronze-only -> DENIED gold (intra-team least privilege:
        a compromised ingestion identity cannot reach curated data)

Exit code is non-zero if any check fails.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, "/opt/dagster/app")


def _load_rendered_env(path: str = "/run/secrets-rendered/dagster.env") -> None:
    """`docker exec` processes don't inherit the entrypoint-sourced (inject.sh)
    env, so the harness loads Dagster's rendered secrets itself: VAULT_ADDR /
    VAULT_TOKEN (for VaultSecretProvider) and SUPERSET_OIDC_CLIENT_SECRET (for
    the analyst password-grant probe). No-op pre-Phase-2 (file absent)."""
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k, v)
    except FileNotFoundError:
        pass
    # inject.sh builds SPARK_REMOTE from the token at PID1 start; docker-exec
    # processes (this harness) don't inherit it, so rebuild it here.
    if "SPARK_REMOTE" not in os.environ and os.environ.get("SPARK_CONNECT_TOKEN"):
        tok = os.environ["SPARK_CONNECT_TOKEN"]
        os.environ["SPARK_REMOTE"] = (
            f"sc://spark-connect:15003/;use_ssl=true;token={tok}"
        )


_load_rendered_env()

from data_platform import dlt_ingest  # noqa: E402
from data_platform.uc_identity import CATALOG, uc_token_for_sa  # noqa: E402

# Per-function team SAs (intra-team least privilege): the ingest SA can write
# ONLY bronze (ingestion-bot persona); the build SA owns silver/gold (data-engineer).
INGEST_SA = os.environ.get("SMOKE_INGEST_SA", "sa-team-analytics-ingest")
BUILD_SA = os.environ.get("SMOKE_BUILD_SA", "sa-team-analytics-build")
AUTOMATION_SA = os.environ.get("UC_AUTOMATION_SA_CLIENT_ID", "sa-platform-automation")
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/dagster/app/dbt")

# Human password-grant (the analyst governance probe). A confidential client that
# carries the audience/email mappers UC needs; the seeded 'superset' client works.
KC_TOKEN_URL = os.environ.get(
    "KC_TOKEN_URL",
    "http://keycloak:8080/realms/datalake/protocol/openid-connect/token",
)
UC_EXCHANGE_URL = os.environ.get(
    "UC_EXCHANGE_URL", "http://unitycatalog:8080/api/1.0/unity-control/auth/tokens"
)
PW_CLIENT_ID = os.environ.get("SMOKE_PW_CLIENT_ID", "superset")
PW_CLIENT_SECRET = (
    os.environ.get("SMOKE_PW_CLIENT_SECRET")
    or os.environ.get("SUPERSET_OIDC_CLIENT_SECRET")
    or "superset-secret"
)
ANALYST_USER = os.environ.get("SMOKE_ANALYST_USER", "analyst")
ANALYST_PASS = os.environ.get("SMOKE_ANALYST_PASS", "analyst")

_RESULTS: list[tuple[str, bool, str]] = []


def _record(name: str, ok: bool, detail: str = "") -> bool:
    _RESULTS.append((name, ok, detail))
    mark = "PASS" if ok else "FAIL"
    line = f"  [{mark}] {name}"
    if detail:
        line += f"  -- {detail}"
    print(line, flush=True)
    return ok


def _post_form(url: str, form: dict) -> dict:
    data = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _analyst_uc_token() -> str:
    kc = _post_form(KC_TOKEN_URL, {
        "grant_type": "password", "client_id": PW_CLIENT_ID,
        "client_secret": PW_CLIENT_SECRET, "username": ANALYST_USER,
        "password": ANALYST_PASS, "scope": "openid",
    })["access_token"]
    return _post_form(UC_EXCHANGE_URL, {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "subject_token": kc,
    })["access_token"]


def _session(uc_token: str):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).create()
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.token", uc_token)
    try:
        spark.catalog.setCurrentCatalog(CATALOG)
    except Exception:  # noqa: BLE001
        pass
    return spark


def _is_denied(err: Exception) -> bool:
    m = str(err).lower()
    return any(s in m for s in ("403", "permission_denied", "denied", "not authorized",
                                "forbidden", "access denied", "no credentials"))


def _count(spark, fqtn: str) -> int:
    return spark.sql(f"SELECT count(*) FROM {fqtn}").collect()[0][0]


def main() -> int:
    print("== AUTOMATION ==", flush=True)

    # A1: dlt -> UC-managed bronze as the INGEST SA (bronze-only ingestion-bot).
    try:
        info = dlt_ingest.run_ingestion(sa_client_id=INGEST_SA, write_disposition="replace")
        _record("A1 dlt->bronze (ingest SA)", True, f"load package {info.loads_ids[0]}")
    except Exception as e:  # noqa: BLE001
        _record("A1 dlt->bronze (ingest SA)", False, str(e).splitlines()[0][:160])

    # The BUILD SA (data-engineer) reads across layers for the automation checks.
    sa_spark = _session(uc_token_for_sa(BUILD_SA))

    # A2: read bronze as the build SA.
    try:
        o = _count(sa_spark, f"{CATALOG}.bronze.orders")
        c = _count(sa_spark, f"{CATALOG}.bronze.customers")
        _record("A2 read bronze (build SA)", o > 0 and c > 0, f"orders={o} customers={c}")
    except Exception as e:  # noqa: BLE001
        _record("A2 read bronze (build SA)", False, str(e).splitlines()[0][:160])

    # A3: dbt build silver + gold as the BUILD SA (data-engineer). The default
    # automation SA is least-privilege and the INGEST SA is bronze-only; neither
    # can build curated layers.
    try:
        env = dict(os.environ)
        env["DBT_UC_TOKEN"] = uc_token_for_sa(BUILD_SA)
        env.pop("DBT_PROFILE", None)
        env.pop("DBT_TARGET", None)
        proc = subprocess.run(
            ["dbt", "build", "--select", "stg_orders", "stg_customers",
             "customer_order_summary", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR],
            cwd=DBT_DIR, env=env, capture_output=True, text=True, timeout=900,
        )
        tail = (proc.stdout or proc.stderr).strip().splitlines()[-1:] or [""]
        _record("A3 dbt build silver+gold", proc.returncode == 0, tail[0][:160])
    except Exception as e:  # noqa: BLE001
        _record("A3 dbt build silver+gold", False, str(e).splitlines()[0][:160])

    # A4: read gold as the team SA.
    try:
        g = _count(sa_spark, f"{CATALOG}.gold.customer_order_summary")
        _record("A4 read gold (build SA)", g > 0, f"rows={g}")
    except Exception as e:  # noqa: BLE001
        _record("A4 read gold (team SA)", False, str(e).splitlines()[0][:160])

    print("== GOVERNANCE ==", flush=True)

    # G1/G2: analyst denied bronze, allowed gold.
    try:
        an_spark = _session(_analyst_uc_token())
        try:
            _count(an_spark, f"{CATALOG}.bronze.orders")
            _record("G1 analyst DENIED bronze", False, "analyst READ bronze (should be denied)")
        except Exception as e:  # noqa: BLE001
            _record("G1 analyst DENIED bronze", _is_denied(e), str(e).splitlines()[0][:120])
        try:
            g = _count(an_spark, f"{CATALOG}.gold.customer_order_summary")
            _record("G2 analyst ALLOWED gold", g >= 0, f"rows={g}")
        except Exception as e:  # noqa: BLE001
            _record("G2 analyst ALLOWED gold", False, str(e).splitlines()[0][:120])
        an_spark.stop()
    except Exception as e:  # noqa: BLE001
        _record("G1 analyst DENIED bronze", False, f"analyst auth failed: {str(e)[:120]}")

    # G3: build SA allowed bronze (already proven in A2, restated as a governance fact).
    try:
        o = _count(sa_spark, f"{CATALOG}.bronze.orders")
        _record("G3 build SA ALLOWED bronze", o > 0, f"orders={o}")
    except Exception as e:  # noqa: BLE001
        _record("G3 build SA ALLOWED bronze", False, str(e).splitlines()[0][:120])

    # G4: raw s3a read of a managed path is DENIED (no static key in compute plane).
    managed_loc = None
    try:
        managed_loc = sa_spark.sql(
            f"DESCRIBE DETAIL {CATALOG}.bronze.orders"
        ).collect()[0]["location"]
    except Exception:  # noqa: BLE001
        pass
    if managed_loc:
        raw = managed_loc.replace("s3://", "s3a://")
        try:
            sa_spark.read.format("delta").load(raw).limit(1).collect()
            _record("G4 raw s3a DENIED", False, f"raw read of {raw} SUCCEEDED (static key!)")
        except Exception as e:  # noqa: BLE001
            _record("G4 raw s3a DENIED", True, str(e).splitlines()[0][:120])
    else:
        _record("G4 raw s3a DENIED", False, "could not resolve managed location")

    # G5: team SA cannot hijack a managed path via an external LOCATION.
    if managed_loc:
        raw = managed_loc.replace("s3://", "s3a://")
        try:
            sa_spark.sql(
                f"CREATE TABLE {CATALOG}.bronze.smoke_hijack (id INT) "
                f"USING delta LOCATION '{raw}'"
            )
            _record("G5 managed un-hijackable", False, "external table over managed path CREATED")
            try:
                sa_spark.sql(f"DROP TABLE {CATALOG}.bronze.smoke_hijack")
            except Exception:  # noqa: BLE001
                pass
        except Exception as e:  # noqa: BLE001
            _record("G5 managed un-hijackable", _is_denied(e) or "location" in str(e).lower(),
                    str(e).splitlines()[0][:120])
    else:
        _record("G5 managed un-hijackable", False, "could not resolve managed location")

    # G6: the DEFAULT automation SA is least-privilege (USE CATALOG only). An
    # untagged run authenticates as this SA, so it must be DENIED reading data --
    # this is what makes team-tagging necessary for any real work.
    try:
        def_spark = _session(uc_token_for_sa(AUTOMATION_SA))
        try:
            _count(def_spark, f"{CATALOG}.gold.customer_order_summary")
            _record("G6 default SA DENIED data", False,
                    "default automation SA READ gold (should be catalog-only)")
        except Exception as e:  # noqa: BLE001
            _record("G6 default SA DENIED data", _is_denied(e), str(e).splitlines()[0][:120])
        def_spark.stop()
    except Exception as e:  # noqa: BLE001
        _record("G6 default SA DENIED data", False, f"default SA session failed: {str(e)[:120]}")

    # G7: the INGEST SA is bronze-only. It must be able to read bronze (its own
    # layer) but DENIED gold -- proving intra-team least privilege (a compromised
    # ingestion identity cannot reach curated data).
    try:
        ing_spark = _session(uc_token_for_sa(INGEST_SA))
        try:
            b = _count(ing_spark, f"{CATALOG}.bronze.orders")
            bronze_ok = b >= 0
        except Exception:  # noqa: BLE001
            bronze_ok = False
        try:
            _count(ing_spark, f"{CATALOG}.gold.customer_order_summary")
            _record("G7 ingest SA DENIED gold", False,
                    "ingest SA READ gold (should be bronze-only)")
        except Exception as e:  # noqa: BLE001
            _record("G7 ingest SA DENIED gold", _is_denied(e) and bronze_ok,
                    f"bronze_ok={bronze_ok}; {str(e).splitlines()[0][:100]}")
        ing_spark.stop()
    except Exception as e:  # noqa: BLE001
        _record("G7 ingest SA DENIED gold", False, f"ingest SA session failed: {str(e)[:120]}")

    try:
        sa_spark.stop()
    except Exception:  # noqa: BLE001
        pass

    failed = [n for n, ok, _ in _RESULTS if not ok]
    print("\n== SUMMARY ==", flush=True)
    print(f"  {len(_RESULTS) - len(failed)}/{len(_RESULTS)} passed", flush=True)
    if failed:
        print(f"  FAILED: {', '.join(failed)}", flush=True)
        return 1
    print("  ALL IN-CONTAINER CHECKS GREEN", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
