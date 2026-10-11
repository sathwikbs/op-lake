"""A minimal PEP-249 driver over Spark Connect with a per-session Unity Catalog
token (the per-user RBAC mechanism).

Each Connection works on an ISOLATED Spark Connect session whose catalog token is
the caller's UC token, so Unity Catalog enforces that token owner's RBAC. If no
``uc_token`` is provided we raise ``SparkConnectOAuth2Error`` (fail-closed): the
connector must never fall back to the server's admin identity. Superset maps that
exception to an OAuth2 re-auth (see db_engine_spec.needs_oauth2).

Session lifecycle
-----------------
Opening a Spark Connect session costs a TLS handshake, a session on the server and
a catalog round-trip, and sessions that were dropped without ``stop()`` were closed
later by garbage collection on arbitrary threads (which deadlocked PySpark 4.1, see
``_pyspark_backport``). Sessions therefore live in a small per-process pool:

* keyed by the UC token (+ catalog + remote): a session is only ever reused for the
  same token, i.e. the same user, so no session is shared across identities;
* exclusive checkout: one Connection uses a session at a time;
* a session whose query changed session state (USE, SET, temporary views, ...) is
  stopped instead of returned, so every query still starts from a clean session;
* idle sessions are stopped after ``SPARK_CONNECT_UC_POOL_IDLE_SECONDS``, and the pool
  keeps at most ``..._POOL_MAX_IDLE_PER_USER`` per token and ``..._POOL_MAX_IDLE`` total.

Every statement runs under a Spark job tag and is interrupted (that tag only) after
``SPARK_CONNECT_UC_QUERY_TIMEOUT_SECONDS``.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
import time
import uuid

from . import _pyspark_backport

logger = logging.getLogger(__name__)
_pyspark_backport.apply()

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


def _env_number(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        logger.warning("spark_connect_uc: ignoring non-numeric %s", name)
        return default


QUERY_TIMEOUT_SECONDS = _env_number("SPARK_CONNECT_UC_QUERY_TIMEOUT_SECONDS", 300)
POOL_IDLE_SECONDS = _env_number("SPARK_CONNECT_UC_POOL_IDLE_SECONDS", 300)
POOL_MAX_IDLE_PER_USER = int(_env_number("SPARK_CONNECT_UC_POOL_MAX_IDLE_PER_USER", 4))
POOL_MAX_IDLE = int(_env_number("SPARK_CONNECT_UC_POOL_MAX_IDLE", 32))

# Statements that leave state behind in the Spark session.
_LEADING_COMMENTS = re.compile(r"^\s*(?:--[^\n]*\n\s*|/\*.*?\*/\s*)*", re.S)
_STATEFUL_SQL = re.compile(
    r"^(?:use|set|reset|cache|uncache|clear\s+cache|add|declare)\b"
    r"|\b(?:create|drop|alter)\s+(?:or\s+replace\s+)?(?:global\s+)?temp(?:orary)?\b",
    re.I | re.S,
)
# The server no longer knows the session (idle timeout, server restart).
_STALE_SESSION = re.compile(r"INVALID_HANDLE\.SESSION_(?:NOT_FOUND|CLOSED|CHANGED)", re.I)


def _is_stateful(sql: str) -> bool:
    return bool(_STATEFUL_SQL.search(_LEADING_COMMENTS.sub("", sql, count=1)))


def _new_isolated_session(spark_remote: str):
    """Open a NEW, isolated Spark Connect session (its own server-side session_id,
    SQLConf and catalog cache) so a per-user catalog token cannot leak across
    connections/users in this worker. Returns (session, isolated).

    Spark Connect has no ``newSession`` (JVM-only); the client-side way to get an
    isolated session is ``builder.remote(...).create()``.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.remote(spark_remote)
    create = getattr(builder, "create", None)
    if callable(create):
        return create(), True
    # Fallback for clients without .create(): shared default session. It is never
    # pooled or stopped, because other connections in this process use it too.
    logger.warning("spark_connect_uc: Spark Connect client has no create(); using the "
                   "shared default session")
    return builder.getOrCreate(), False


class _PooledSession:
    __slots__ = ("spark", "key", "isolated", "dirty", "last_used")

    def __init__(self, spark, key: str, isolated: bool):
        self.spark = spark
        self.key = key
        self.isolated = isolated
        self.dirty = False
        self.last_used = time.monotonic()


def _stop_sessions(entries) -> None:
    for entry in entries:
        if not entry.isolated:
            continue
        try:
            entry.spark.stop()   # releases the server-side session, closes the channel
        except Exception as ex:  # noqa: BLE001 - best effort; the server times it out anyway
            logger.info("spark_connect_uc: stopping a Spark Connect session failed: %s", ex)


class _SessionPool:
    """Per-process pool of idle Spark Connect sessions, keyed by UC token."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._idle: dict[str, list[_PooledSession]] = {}
        self._pid = os.getpid()

    def _reset_after_fork_locked(self) -> None:
        # gRPC channels do not survive fork(); never reuse a parent's sessions.
        if os.getpid() != self._pid:
            self._idle = {}
            self._pid = os.getpid()

    def _evict_idle_locked(self, now: float) -> list[_PooledSession]:
        evicted: list[_PooledSession] = []
        for key in list(self._idle):
            keep = []
            for entry in self._idle[key]:
                (evicted if now - entry.last_used >= POOL_IDLE_SECONDS else keep).append(entry)
            if keep:
                self._idle[key] = keep
            else:
                del self._idle[key]
        return evicted

    def acquire(self, key: str, open_session) -> tuple[_PooledSession, bool]:
        """Return (session, reused). Opening and stopping happen outside the lock."""
        with self._lock:
            self._reset_after_fork_locked()
            evicted = self._evict_idle_locked(time.monotonic())
            bucket = self._idle.get(key)
            entry = bucket.pop() if bucket else None
            if bucket is not None and not bucket:
                del self._idle[key]
        _stop_sessions(evicted)
        if entry is not None:
            return entry, True
        spark, isolated = open_session()
        return _PooledSession(spark, key, isolated), False

    def release(self, entry: _PooledSession, reusable: bool) -> None:
        to_stop: list[_PooledSession] = []
        pooled = False
        if reusable and entry.isolated and not entry.dirty:
            entry.last_used = time.monotonic()
            with self._lock:
                self._reset_after_fork_locked()
                to_stop = self._evict_idle_locked(entry.last_used)
                bucket = self._idle.setdefault(entry.key, [])
                total = sum(len(b) for b in self._idle.values())
                if len(bucket) < POOL_MAX_IDLE_PER_USER and total < POOL_MAX_IDLE:
                    bucket.append(entry)
                    pooled = True
                elif not bucket:
                    del self._idle[entry.key]
        if not pooled:
            to_stop.append(entry)
        _stop_sessions(to_stop)


_POOL = _SessionPool()


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
        stateful = _is_stateful(sql)
        try:
            fields, rows = self._run(sql, stateful)
        except Exception as ex:  # noqa: BLE001
            if not (self._conn._reused and _STALE_SESSION.search(str(ex))):
                raise
            logger.info("spark_connect_uc: pooled session expired on the server; "
                        "retrying once on a new session")
            self._conn._replace_session()
            fields, rows = self._run(sql, stateful)
        self.description = [
            (f.name, _TYPE_CODES.get(f.dataType.simpleString().split("(")[0].lower(), None),
             None, None, None, None, True)
            for f in fields
        ]
        self._rows = rows
        self.rowcount = len(self._rows)
        self._pos = 0
        return self

    def _run(self, sql: str, stateful: bool):
        entry = self._conn._entry
        if stateful:
            entry.dirty = True   # never hand this session to another query
        spark = entry.spark
        tag = f"spark_connect_uc-{uuid.uuid4().hex}"
        timed_out = threading.Event()

        def cancel() -> None:
            timed_out.set()
            try:
                spark.interruptTag(tag)
            except Exception as ex:  # noqa: BLE001
                logger.warning("spark_connect_uc: interrupting a timed-out query failed: %s", ex)

        timer = threading.Timer(QUERY_TIMEOUT_SECONDS, cancel)
        timer.daemon = True
        spark.addTag(tag)   # thread-local: only operations started by this thread
        timer.start()
        try:
            df = spark.sql(sql)
            fields = list(df.schema.fields)
            rows = [tuple(r) for r in df.collect()]
            return fields, rows
        except Exception as ex:  # noqa: BLE001
            if timed_out.is_set():
                entry.dirty = True
                raise OperationalError(
                    f"Query cancelled after {QUERY_TIMEOUT_SECONDS:g}s "
                    "(SPARK_CONNECT_UC_QUERY_TIMEOUT_SECONDS)") from ex
            raise
        finally:
            timer.cancel()
            try:
                spark.removeTag(tag)
            except Exception:  # noqa: BLE001
                pass

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
        self._remote = spark_remote or os.environ.get("SPARK_REMOTE")
        if not self._remote:
            raise InterfaceError("SPARK_REMOTE is not set and no spark_remote was provided")
        if not uc_token:
            # Fail-closed: force Superset to obtain a per-user token via OAuth2.
            raise SparkConnectOAuth2Error(
                "No Unity Catalog token for the current user; OAuth2 re-auth required."
            )
        self._uc_token = uc_token
        self._key = hashlib.sha256(
            "\0".join((uc_token, self._catalog, self._remote)).encode()).hexdigest()
        self._entry, self._reused = _POOL.acquire(self._key, self._open_session)
        self._closed = False

    def _open_session(self):
        # Isolated per-user session: its own SQLConf + catalog cache, so the
        # per-user token does not leak across connections/users in this worker.
        spark, isolated = _new_isolated_session(self._remote)
        spark.conf.set(f"spark.sql.catalog.{self._catalog}.token", self._uc_token)
        try:
            spark.catalog.setCurrentCatalog(self._catalog)
        except Exception:  # noqa: BLE001 - defaultCatalog server-side is fine too
            pass
        return spark, isolated

    def _replace_session(self) -> None:
        """Drop a pooled session the server no longer knows; continue on a new one."""
        _POOL.release(self._entry, reusable=False)
        spark, isolated = self._open_session()
        self._entry = _PooledSession(spark, self._key, isolated)
        self._reused = False

    @property
    def _spark(self):
        return self._entry.spark

    def cursor(self):
        return Cursor(self)

    def close(self):
        if self._closed:
            return
        self._closed = True
        _POOL.release(self._entry, reusable=True)

    def commit(self):
        pass

    def rollback(self):
        pass


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
