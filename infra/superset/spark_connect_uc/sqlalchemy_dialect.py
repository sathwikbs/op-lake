"""SQLAlchemy dialect for Spark Connect + Unity Catalog (per-user).

URL form: ``spark_connect_uc://<ignored-host>/<catalog>`` (default catalog
``analytics``). The Spark Connect endpoint and the per-user UC token are NOT in
the URL: the endpoint comes from ``SPARK_REMOTE`` (env), and the UC token is
injected per connection via ``connect_args['uc_token']`` by the Superset
engine-spec (update_impersonation_config). Introspection uses SHOW/DESCRIBE so it
is automatically RBAC-scoped to the connected user.
"""
from __future__ import annotations

from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

from . import dbapi

# Spark -> SQLAlchemy type mapping (best-effort; unknown -> String).
_TYPE_MAP = {
    "string": sqltypes.String, "varchar": sqltypes.String, "char": sqltypes.String,
    "binary": sqltypes.LargeBinary,
    "boolean": sqltypes.Boolean,
    "tinyint": sqltypes.SmallInteger, "smallint": sqltypes.SmallInteger,
    "int": sqltypes.Integer, "integer": sqltypes.Integer,
    "bigint": sqltypes.BigInteger, "long": sqltypes.BigInteger,
    "float": sqltypes.Float, "double": sqltypes.Float, "real": sqltypes.Float,
    "decimal": sqltypes.Numeric, "numeric": sqltypes.Numeric,
    "date": sqltypes.Date,
    "timestamp": sqltypes.DateTime, "timestamp_ntz": sqltypes.DateTime,
}


def _sqla_type(spark_type: str):
    base = spark_type.split("(")[0].strip().lower()
    return _TYPE_MAP.get(base, sqltypes.String)()


class SparkConnectUCIdentifierPreparer(compiler.IdentifierPreparer):
    # Spark SQL quotes identifiers with backticks.
    initial_quote = "`"
    final_quote = "`"


class SparkConnectUCCompiler(compiler.SQLCompiler):
    pass


class SparkConnectUCTypeCompiler(compiler.GenericTypeCompiler):
    pass


class SparkConnectUCDialect(default.DefaultDialect):
    name = "spark_connect_uc"
    driver = "connect"
    preparer = SparkConnectUCIdentifierPreparer
    statement_compiler = SparkConnectUCCompiler
    type_compiler = SparkConnectUCTypeCompiler

    supports_statement_cache = False
    supports_native_boolean = True
    supports_sane_rowcount = False
    supports_multivalues_insert = False
    supports_views = True
    default_paramstyle = "pyformat"
    postfetch_lastrowid = False

    @classmethod
    def import_dbapi(cls):  # SQLAlchemy 2.0
        return dbapi

    @classmethod
    def dbapi(cls):  # SQLAlchemy 1.4
        return dbapi

    def create_connect_args(self, url):
        catalog = (url.database or "analytics").strip("/")
        return [], {"uc_catalog": catalog}

    # -- lightweight helpers ------------------------------------------------
    def _default_catalog(self, connection) -> str:
        raw = connection.engine.url.database or "analytics"
        return raw.strip("/")

    def _rows(self, connection, sql):
        return list(connection.exec_driver_sql(sql))

    # -- introspection (RBAC-scoped via SHOW/DESCRIBE) ----------------------
    def get_schema_names(self, connection, **kw):
        cat = self._default_catalog(connection)
        return [r[0] for r in self._rows(connection, f"SHOW SCHEMAS IN `{cat}`")]

    def get_table_names(self, connection, schema=None, **kw):
        cat = self._default_catalog(connection)
        if not schema:
            return []
        rows = self._rows(connection, f"SHOW TABLES IN `{cat}`.`{schema}`")
        # SHOW TABLES columns: namespace, tableName, isTemporary
        out = []
        for r in rows:
            d = r._mapping if hasattr(r, "_mapping") else None
            if d and "tableName" in d:
                out.append(d["tableName"])
            else:
                out.append(r[1] if len(r) > 1 else r[0])
        return out

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_columns(self, connection, table_name, schema=None, **kw):
        cat = self._default_catalog(connection)
        fq = f"`{cat}`.`{schema}`.`{table_name}`" if schema else f"`{cat}`.`{table_name}`"
        cols = []
        for r in self._rows(connection, f"DESCRIBE TABLE {fq}"):
            name = r[0]
            dtype = r[1] if len(r) > 1 else "string"
            # DESCRIBE emits a blank line then partition/detail sections -- stop.
            if not name or name.startswith("#") or name.strip() == "":
                break
            cols.append({
                "name": name,
                "type": _sqla_type(dtype or "string"),
                "nullable": True,
                "default": None,
                "comment": (r[2] if len(r) > 2 else None) or None,
            })
        return cols

    def has_table(self, connection, table_name, schema=None, **kw):
        try:
            self.get_columns(connection, table_name, schema=schema)
            return True
        except Exception:  # noqa: BLE001
            return False

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        return {"text": None}

    def do_rollback(self, dbapi_connection):
        pass

    def do_ping(self, dbapi_connection):
        # A trivial query that never touches the catalog -> no token needed.
        cur = dbapi_connection.cursor()
        try:
            cur.execute("SELECT 1")
            cur.fetchall()
            return True
        finally:
            cur.close()

    def _get_server_version_info(self, connection):
        return (4, 1, 0)

    def _get_default_schema_name(self, connection):
        return None


dialect = SparkConnectUCDialect
