"""dlt-based ingestion into UC-MANAGED Delta tables (no Spark, no static key).

dlt extracts + normalises the source (here sample CSVs; any dlt source works),
and the custom `uc_managed` destination writes governed, UC-managed Delta tables
via delta-rs + UC credential vending -- as the run's team service account (or a
developer's identity locally). See data_platform.uc_managed_destination.

Resources are named after their target tables: `orders` -> analytics.bronze.orders,
`customers` -> analytics.bronze.customers (the dbt `bronze` source).
"""
from __future__ import annotations

import csv
import os

import dlt

DATA_DIR = os.environ.get("RAW_DATA_DIR", "/opt/dagster/app/data")


def _read_csv(filename: str):
    with open(os.path.join(DATA_DIR, filename), newline="") as fh:
        yield from csv.DictReader(fh)


# Explicit column types (dlt COERCES the raw CSV strings to these during
# normalize; the Arrow batch then carries real types, which the Spark adapter
# preserves into the UC-managed table). Alternative: pass a Pydantic model as
# `columns` for a validated schema contract.
@dlt.resource(name="orders", write_disposition="replace", columns={
    "order_id": {"data_type": "bigint"},
    "customer_id": {"data_type": "bigint"},
    "order_ts": {"data_type": "timestamp"},
    "status": {"data_type": "text"},
    "amount": {"data_type": "decimal", "precision": 18, "scale": 2},
    "currency": {"data_type": "text"},
})
def orders():
    yield from _read_csv("orders.csv")


@dlt.resource(name="customers", write_disposition="replace", columns={
    "customer_id": {"data_type": "bigint"},
    "name": {"data_type": "text"},
    "email": {"data_type": "text"},
    "country": {"data_type": "text"},
    "signup_date": {"data_type": "date"},
})
def customers():
    yield from _read_csv("customers.csv")


@dlt.source(name="bronze_raw")
def raw_source():
    return [orders(), customers()]


def run_ingestion(sa_client_id: str | None = None, catalog: str | None = None,
                  schema: str = "bronze", write_disposition: str | None = None):
    """Run the dlt pipeline as `sa_client_id`, writing UC-managed Delta tables.

    The `uc_managed` destination writes governed managed tables via Spark Connect
    (bound to the SA's UC token) + UC credential vending -- no static/root key.

    `write_disposition` controls how each run lands data (overrides the resource
    default), from the arg or UC_INGEST_WRITE_DISPOSITION env:
      * "replace" (default): each run fully refreshes the table (idempotent).
      * "append": first run creates the table, every subsequent run APPENDS
        (accumulating history) -- append-only ingestion.
    Returns the dlt LoadInfo.
    """
    from .uc_managed_destination import make_uc_managed_destination

    disposition = write_disposition or os.environ.get("UC_INGEST_WRITE_DISPOSITION", "replace")
    if disposition not in ("replace", "append"):
        raise ValueError(f"write_disposition must be replace|append, got {disposition!r}")

    # Single-writer load: one writer per managed Delta table avoids commit
    # conflicts; dlt extract/normalize parallelism is unaffected.
    os.environ.setdefault("LOAD__WORKERS", "1")

    destination, close = make_uc_managed_destination(
        sa_client_id=sa_client_id, catalog=catalog, schema=schema
    )
    try:
        pipeline = dlt.pipeline(
            pipeline_name="bronze_ingestion",
            destination=destination,
            dataset_name=schema,
            pipelines_dir="/tmp/dlt",
        )
        # `write_disposition` here overrides each resource's default, so the same
        # pipeline can run replace (full refresh) or append (accumulate) per env.
        return pipeline.run(raw_source(), write_disposition=disposition)
    finally:
        close()  # stop the Spark Connect session opened by the destination
