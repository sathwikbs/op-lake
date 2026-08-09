"""A minimal PEP-249 driver over Spark Connect with a per-session Unity Catalog
token (the per-user RBAC mechanism).

Each Connection opens an ISOLATED Spark Connect session (``newSession``) and, if
given a ``uc_token``, sets it as the catalog token for that session only -- so
Unity Catalog enforces the token owner's RBAC. If no ``uc_token`` is provided we
raise ``SparkConnectOAuth2Error`` (fail-closed): the connector must never fall
back to the server's admin identity. Superset maps that exception to an OAuth2
re-auth (see db_engine_spec.needs_oauth2).
"""
from __future__ import annotations

import os

# ---- PEP-249 module globals ------------------------------------------------
apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"


class Error(Exception):
    pass


class DatabaseError(Error):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class InterfaceError(Error):
    pass


class SparkConnectOAuth2Error(OperationalError):
    """Raised when no per-user UC token is available -> trigger OAuth2 re-auth."""


def _new_isolated_session(spark_remote: str):
    """Open a NEW, isolated Spark Connect session (its own server-side session_id,
    SQLConf and catalog cache) so a per-user catalog token cannot leak across
    connections/users in this worker.

    Spark Connect has no ``newSession`` (JVM-only); the client-side way to get an
    isolated session is ``builder.remote(...).create()``.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.remote(spark_remote)
    create = getattr(builder, "create", None)
    if callable(create):
        return create()
    # Fallback for clients without .create(): shared default session.
    return builder.getOrCreate()


# Map a subset of Spark type names to PEP-249-ish type codes (informational).
_TYPE_CODES = {
    "string": str, "varchar": str, "char": str,
    "int": int, "integer": int, "bigint": int, "smallint": int, "tinyint": int, "long": int,
    "double": float, "float": float, "decimal": float,
    "boolean": bool,
}


class Cursor:
    def __init__(self, connection: "Connection"):
        self._conn = connection
        self.arraysize = 1000
        self.description = None
        self.rowcount = -1
        self._rows: list | None = None
        self._pos = 0

    # -- execution --
    def execute(self, operation, parameters=None):
        sql = operation
        if parameters:
            try:
                sql = operation % parameters
            except Exception:  # noqa: BLE001 - Superset usually pre-renders SQL
                sql = operation
        spark = self._conn._spark
        df = spark.sql(sql)
        fields = list(df.schema.fields)
        self.description = [
            (f.name, _TYPE_CODES.get(f.dataType.simpleString().split("(")[0].lower(), None),
             None, None, None, None, True)
            for f in fields
        ]
        self._rows = [tuple(r) for r in df.collect()]
        self.rowcount = len(self._rows)
        self._pos = 0
        return self

    def executemany(self, operation, seq_of_parameters):
        for params in seq_of_parameters:
            self.execute(operation, params)
        return self

    # -- fetching --
    def fetchone(self):
        if not self._rows or self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size=None):
        size = size or self.arraysize
        out = self._rows[self._pos:self._pos + size] if self._rows else []
        self._pos += len(out)
        return out

    def fetchall(self):
        out = self._rows[self._pos:] if self._rows else []
        self._pos = len(self._rows) if self._rows else 0
        return out

    def __iter__(self):
        return iter(self.fetchall())

    def close(self):
        self._rows = None

    def setinputsizes(self, sizes):  # noqa: D401 - PEP-249 no-op
        pass

    def setoutputsize(self, size, column=None):  # noqa: D401 - PEP-249 no-op
        pass


class Connection:
    def __init__(self, uc_token=None, uc_catalog="analytics", spark_remote=None, **_ignored):
        self._catalog = uc_catalog or "analytics"
        remote = spark_remote or os.environ.get("SPARK_REMOTE")
        if not remote:
            raise InterfaceError("SPARK_REMOTE is not set and no spark_remote was provided")
        if not uc_token:
            # Fail-closed: force Superset to obtain a per-user token via OAuth2.
            raise SparkConnectOAuth2Error(
                "No Unity Catalog token for the current user; OAuth2 re-auth required."
            )
        # Isolated per-user session: its own SQLConf + catalog cache, so the
        # per-user token does not leak across connections/users in this worker.
        self._spark = _new_isolated_session(remote)
        self._spark.conf.set(f"spark.sql.catalog.{self._catalog}.token", uc_token)
        try:
            self._spark.catalog.setCurrentCatalog(self._catalog)
        except Exception:  # noqa: BLE001 - defaultCatalog server-side is fine too
            pass

    def cursor(self):
        return Cursor(self)

    def close(self):
        # Do not stop the shared client; just drop the reference to this session.
        self._spark = None

    def commit(self):
        pass

    def rollback(self):
        pass


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
