"""Backport of upstream PySpark Connect fixes needed by a multi-session server process.

Why
---
PySpark 4.1.x keeps ONE release thread pool per process on
``ExecutePlanResponseReattachableIterator`` and every ``SparkConnectClient.close()``
calls its class-level ``shutdown()``. That method holds a process-wide lock while
waiting (``wait=True``) for every pending ReleaseExecute RPC of every session.

Superset opens many Spark Connect sessions in one worker process. When one of them
is closed from the wrong thread (observed: ``SparkSession.__del__`` run by garbage
collection inside a gRPC metadata-plugin thread), the wait can never finish, the
lock is never released, and every later query in that worker blocks in
``_get_or_create_release_thread_pool``. Load test before this patch: 54 of 120
SQL Lab requests timed out and the Superset health check stopped answering.

Upstream fixed this on Spark master, not in any 4.1.x release:
  * SPARK-55362  Don't wait for threadpool shutdown
  * SPARK-55406  Reimplement the thread pool for ExecutePlanResponseReattachableIterator
  * SPARK-55020  Disable gc when executing gRPC command (deadlock follow-ups)

What
----
The equivalent of upstream's ``shutdown_threadpool_if_idle``: track live iterators,
and make ``shutdown()`` shut the shared pool down only when no iterator is alive,
without waiting. Applied only to PySpark 4.1.x with the exact 4.1 shape; any other
version is left untouched and logged. Remove this module once the platform runs a
PySpark release that contains the fixes above.
"""
from __future__ import annotations

import functools
import logging
import weakref

logger = logging.getLogger(__name__)

_applied = False


def apply() -> bool:
    """Patch PySpark in place (idempotent). Returns True if the patch is active."""
    global _applied
    if _applied:
        return True

    import pyspark
    from pyspark.sql.connect.client import reattach

    cls = reattach.ExecutePlanResponseReattachableIterator
    if hasattr(cls, "shutdown_threadpool_if_idle"):
        logger.info("spark_connect_uc: PySpark %s already has the upstream thread-pool "
                    "fix; backport not needed", pyspark.__version__)
        return False
    expected = ("_lock", "_release_thread_pool_instance", "shutdown", "__init__")
    if not pyspark.__version__.startswith("4.1.") or not all(hasattr(cls, a) for a in expected):
        logger.warning("spark_connect_uc: PySpark %s not recognised; thread-pool backport "
                       "NOT applied (re-check the upstream fix status)", pyspark.__version__)
        return False

    live: "weakref.WeakSet" = weakref.WeakSet()
    original_init = cls.__init__

    @functools.wraps(original_init)
    def tracked_init(self, *args, **kwargs):
        # Register under the class lock before the original __init__ touches the
        # pool, so shutdown() can never retire a pool an iterator is about to use.
        with cls._lock:
            live.add(self)
        original_init(self, *args, **kwargs)

    def shutdown_if_idle(klass) -> None:
        """Retire the shared pool only when no iterator is alive; never wait."""
        with klass._lock:
            pool = klass._release_thread_pool_instance
            if pool is not None and not live:
                klass._release_thread_pool_instance = None
                pool.shutdown(wait=False)

    cls.__init__ = tracked_init
    cls.shutdown = classmethod(shutdown_if_idle)
    _applied = True
    logger.info("spark_connect_uc: applied PySpark %s thread-pool backport "
                "(SPARK-55362/SPARK-55406)", pyspark.__version__)
    return True
