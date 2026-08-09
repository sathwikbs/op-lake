"""dlt custom destination: write UC-MANAGED Delta tables via Spark Connect.

Managed tables are the multi-tenant-safe primitive (un-hijackable): UC owns the
storage path and no other principal can register over it. On released UC, only
Spark (the UC connector's coordinated-commit path) can write catalog-managed
tables -- delta-rs/DuckDB/Flink catalog-commit writers exist but are Delta-4.2-era
bleeding-edge, not in released pip packages (see the spike notes). So this
destination pushes each dlt-normalized batch through a **Spark Connect** session
bound to the run's team-SA UC token, and writes a managed Delta table via UC
credential vending -- no MinIO root/static key; UC enforces the SA's grants.

Performance: dlt hands Arrow (loader_file_format="parquet"); we build the Spark
DataFrame **Arrow-natively** (no pandas round-trip when the connect client
supports it) and can repartition so the write itself is distributed/MPP. The
client->Connect handoff is the one serial hop -- inherent to dlt's single-node
extract; everything downstream (dbt silver/gold) is full MPP.

Only `replace`/`append` dispositions. Identity: the team SA in Dagster
(uc_token_for_sa) or a developer's token locally.
"""
from __future__ import annotations

import os
import threading


def _to_spark_df(spark, arrow_table):
    """Arrow-native -> Spark DataFrame, avoiding the pandas round-trip when the
    Spark Connect client supports building a DataFrame directly from Arrow."""
    try:
        return spark.createDataFrame(arrow_table)  # Spark 4.x Connect: Arrow Table
    except (TypeError, ValueError, NotImplementedError):
        # Fallback: Arrow-backed pandas (still uses Arrow for the gRPC transfer
        # when spark.sql.execution.arrow.pyspark.enabled=true).
        return spark.createDataFrame(arrow_table.to_pandas())


def make_uc_managed_destination(
    sa_client_id: str | None = None,
    catalog: str | None = None,
    schema: str = "bronze",
    batch_size: int | None = None,
    repartition: int | None = None,
):
    """Return (destination, close): a dlt custom destination writing UC-managed
    Delta tables via Spark Connect, plus a `close()` to stop the session.

    Tables land as `<catalog>.<schema>.<resource_name>`. `repartition` (optional)
    fans the write out across N Spark partitions for large batches.
    """
    import dlt

    catalog = catalog or os.environ.get("UC_CATALOG", "analytics")
    batch_size = int(batch_size or os.environ.get("UC_INGEST_BATCH_SIZE", "100000"))

    state: dict = {"spark": None}
    lock = threading.Lock()
    handled: set[str] = set()  # tables create/reset-handled this load

    def _spark():
        if state["spark"] is None:
            from pyspark.sql import SparkSession
            from .spark import spark_remote_url
            from .uc_identity import apply_uc_token

            s = SparkSession.builder.remote(spark_remote_url()).create()
            s.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            apply_uc_token(s, sa_client_id)  # bind the SA's UC token (per-session)
            state["spark"] = s
        return state["spark"]

    @dlt.destination(loader_file_format="parquet", batch_size=batch_size, name="uc_managed")
    def uc_managed(items, table) -> None:
        import pyarrow as pa

        name = table["name"]
        if name.startswith("_dlt"):  # skip dlt bookkeeping tables
            return
        disp = table.get("write_disposition", "append")
        if disp not in ("replace", "append"):
            raise ValueError(f"uc_managed supports only replace/append, got {disp!r} for {name}")

        if isinstance(items, pa.Table):
            arrow = items
        elif isinstance(items, pa.RecordBatch):
            arrow = pa.Table.from_batches([items])
        elif isinstance(items, list) and items and isinstance(items[0], pa.RecordBatch):
            arrow = pa.Table.from_batches(items)
        else:
            arrow = pa.Table.from_pylist(list(items))

        full = f"{catalog}.{schema}.{name}"
        # Serialise per Delta table: single-writer avoids commit conflicts, and
        # makes "first batch of this load" deterministic across load workers.
        with lock:
            spark = _spark()
            sdf = _to_spark_df(spark, arrow)
            if repartition:
                sdf = sdf.repartition(repartition)
            first = full not in handled
            writer = sdf.writeTo(full).using("delta")
            if disp == "replace":
                if first:
                    writer.createOrReplace()          # fresh table = this load only
                else:
                    writer.append()                   # subsequent batches accumulate
            else:  # append
                if first and not spark.catalog.tableExists(full):
                    writer.create()
                else:
                    writer.append()
            handled.add(full)

    def close():
        if state["spark"] is not None:
            try:
                state["spark"].stop()
            except Exception:  # noqa: BLE001
                pass
            state["spark"] = None

    return uc_managed, close
