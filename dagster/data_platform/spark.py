"""Helpers for talking to the Spark Connect server.

All heavy lifting (Delta writes, Unity Catalog DDL, S3A access) happens on the
Spark server. The client here only submits SQL strings, so this module has no
Delta/Hadoop dependencies of its own.
"""
from __future__ import annotations

import os
from contextlib import contextmanager

from pyspark.sql import SparkSession

from .uc_identity import apply_uc_token


def spark_remote_url() -> str:
    # Prefer an explicit sc:// URL; otherwise build one from host/port.
    if os.environ.get("SPARK_REMOTE"):
        return os.environ["SPARK_REMOTE"]
    host = os.environ.get("SPARK_CONNECT_HOST", "spark")
    port = os.environ.get("SPARK_CONNECT_PORT", "15002")
    return f"sc://{host}:{port}"


@contextmanager
def spark_session(sa_client_id: str | None = None):
    """Yield a Spark Connect session that runs as a team SERVICE ACCOUNT.

    We bind the SA's Unity Catalog token as the per-session catalog token so UC
    enforces the SA's grants (not admin). `sa_client_id` lets a caller pin a
    specific team SA (Phase 6); by default the platform automation SA
    (UC_AUTOMATION_SA_CLIENT_ID) is used. If no automation identity is
    configured, the session falls back to the Spark server default.
    """
    spark = SparkSession.builder.remote(spark_remote_url()).getOrCreate()
    used = apply_uc_token(spark, sa_client_id)
    if used:
        try:
            spark.catalog.setCurrentCatalog(os.environ.get("UC_CATALOG", "analytics"))
        except Exception:  # noqa: BLE001 - default catalog server-side is fine too
            pass
    try:
        yield spark
    finally:
        spark.stop()
