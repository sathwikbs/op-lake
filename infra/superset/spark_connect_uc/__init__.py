"""spark_connect_uc: a Superset connector for Spark Connect with per-user Unity
Catalog RBAC.

The human/BI plane runs on the SAME Spark engine as the automation plane. What
makes it per-user is a small trick proven against this stack: a Spark Connect
*client session* can override ``spark.sql.catalog.<cat>.token`` with a UC token
minted for the logged-in user, and Unity Catalog then enforces that user's RBAC
(they see only what they were granted).

This package wires that into Superset:
  * ``db_engine_spec`` -- declares OAuth2 support; on each per-user connection it
    exchanges the viewer's Keycloak token for a UC token (RFC 8693) and injects
    it into ``connect_args`` (fail-closed: no token -> trigger the OAuth2 dance,
    never fall back to the admin identity).
  * ``sqlalchemy_dialect`` + ``dbapi`` -- a thin SQLAlchemy/PEP-249 driver that
    opens an isolated Spark Connect session, sets the per-user catalog token, and
    runs SQL / introspection (SHOW / DESCRIBE).
"""

__all__ = ["dbapi", "sqlalchemy_dialect", "db_engine_spec", "token_exchange"]
