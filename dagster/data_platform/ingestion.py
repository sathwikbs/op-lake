"""Bronze ingestion assets.

dlt reads the raw sources and writes them **directly** as UC-MANAGED Delta tables
(`analytics.bronze.orders`, `analytics.bronze.customers`) via the `uc_managed`
destination -- delta-rs + UC credential vending, no Spark and no static/root key.
Managed tables are the multi-tenant-safe primitive (un-hijackable), so bronze
lands governed with nothing exposed on object storage.

The asset keys (["bronze","orders"], ["bronze","customers"]) match the dbt
`bronze` source, so Dagster links them to the downstream silver/gold models.
"""
# No `from __future__ import annotations`: Dagster 1.11 validates the real
# `AssetExecutionContext` class, which PEP 563 would stringify.
import os

from dagster import AssetExecutionContext, AssetSpec, MaterializeResult, multi_asset

from .dlt_ingest import run_ingestion
from .uc_identity import sa_from_context

CATALOG = os.environ.get("UC_CATALOG", "analytics")
BRONZE_SCHEMA = "bronze"


@multi_asset(
    specs=[
        AssetSpec(key=["bronze", "orders"], group_name="bronze",
                  description="analytics.bronze.orders (UC-managed Delta, dlt via delta-rs)"),
        AssetSpec(key=["bronze", "customers"], group_name="bronze",
                  description="analytics.bronze.customers (UC-managed Delta, dlt via delta-rs)"),
    ],
    compute_kind="dlt",
)
def bronze_ingest(context: AssetExecutionContext):
    """Run dlt -> UC-managed bronze tables as the run's team service account.

    UC enforces that SA's grants; the write uses UC-vended, per-table creds
    (no MinIO root/static key). One run produces both bronze tables.
    """
    # role='ingest' -> the team's bronze-only ingestion SA (sa-team-<team>-ingest),
    # falling back to the team's single SA if no ingest role SA is provisioned.
    sa = sa_from_context(context, role="ingest")  # fail-closed team-launch authorization
    context.log.info(f"dlt bronze ingest running as {sa!r} -> {CATALOG}.{BRONZE_SCHEMA}.* (UC-managed, no static key)")
    info = run_ingestion(sa_client_id=sa, catalog=CATALOG, schema=BRONZE_SCHEMA)
    context.log.info(str(info))
    yield MaterializeResult(asset_key=["bronze", "orders"], metadata={"engine": "dlt+delta-rs", "table_type": "MANAGED"})
    yield MaterializeResult(asset_key=["bronze", "customers"], metadata={"engine": "dlt+delta-rs", "table_type": "MANAGED"})


bronze_assets = [bronze_ingest]
